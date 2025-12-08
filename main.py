#!/usr/bin/env python3
"""
将 OpenRCA metric、trace 和 log 数据导入到 SigNoz
使用 CSV 中的时间戳作为上报数据的时间
顺序：先导入 log，再导入 metric，最后导入 trace
"""

import csv
import glob
from pathlib import Path
from typing import List, Dict, Optional
from collections import defaultdict
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# Log 相关的导入（参考 test.py）
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry._logs._internal import LogRecord
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import SpanContext, TraceFlags, Status, StatusCode
from opentelemetry.util.types import AttributeValue
from opentelemetry.sdk.trace import InstrumentationScope
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, MetricExportResult, MetricsData
from opentelemetry.sdk.metrics.view import View, ExplicitBucketHistogramAggregation
from opentelemetry.sdk.metrics._internal.point import (
    ResourceMetrics, ScopeMetrics, Metric, NumberDataPoint, HistogramDataPoint,
    Sum, Gauge, Histogram as HistogramData
)
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
import argparse
from datetime import datetime, timezone, timedelta
import time
from tqdm import tqdm
import logging
import traceback

# 配置日志
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ============================================================================
# 配置管理类
# ============================================================================

class Config:
    """统一管理配置参数"""
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_EXPORTER_WAIT_TIME = 2  # 秒
    DEFAULT_SERVICE_NAME = "openrca-bank"
    DEFAULT_SERVICE_VERSION = "1.0.0"
    
    def __init__(
        self,
        signoz_endpoint: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        service_name: str = DEFAULT_SERVICE_NAME,
        time_offset_ms: int = 0,
        exporter_wait_time: int = DEFAULT_EXPORTER_WAIT_TIME
    ):
        self.signoz_endpoint = signoz_endpoint
        self.batch_size = batch_size
        self.service_name = service_name
        self.time_offset_ms = time_offset_ms
        self.exporter_wait_time = exporter_wait_time


# ============================================================================
# Resource 管理器
# ============================================================================

class ResourceManager:
    """管理 OpenTelemetry Resource 的创建和缓存"""
    
    def __init__(self, service_version: str = "1.0.0"):
        self._cache: Dict[str, Resource] = {}
        self.service_version = service_version
    
    def get_resource(self, service_name: str) -> Resource:
        """获取或创建 Resource，使用缓存避免重复创建"""
        if service_name not in self._cache:
            self._cache[service_name] = Resource.create({
                "service.name": service_name,
                "service.version": self.service_version,
            })
        return self._cache[service_name]
    
    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()


# ============================================================================
# Exporter 管理器
# ============================================================================

class ExporterManager:
    """统一管理 OpenTelemetry Exporters 的创建和关闭"""
    
    def __init__(self, config: Config):
        self.config = config
        self._trace_exporter: Optional[OTLPSpanExporter] = None
        self._metric_exporter: Optional[OTLPMetricExporter] = None
        self._log_exporter: Optional[OTLPLogExporter] = None
        self._logger_provider: Optional[LoggerProvider] = None
        self._logger_providers: Dict[str, LoggerProvider] = {}  # 存储不同 resource 的 LoggerProvider
        self._loggers: Dict[str, any] = {}  # 存储不同 service 的 logger
    
    def _normalize_endpoint(self, endpoint: str) -> str:
        """规范化 endpoint URL"""
        if not endpoint.startswith('http'):
            return f"http://{endpoint}"
        return endpoint
    
    def _create_exporter(self, exporter_class, endpoint: str, **kwargs):
        """通用的 exporter 创建方法，带错误处理"""
        endpoint_url = self._normalize_endpoint(endpoint)
        try:
            return exporter_class(endpoint=endpoint_url, insecure=True, **kwargs)
        except Exception:
            # 如果失败，尝试不使用 http:// 前缀
            return exporter_class(endpoint=endpoint, insecure=True, **kwargs)
    
    def get_trace_exporter(self) -> OTLPSpanExporter:
        """获取或创建 Trace Exporter"""
        if self._trace_exporter is None:
            self._trace_exporter = self._create_exporter(
                OTLPSpanExporter,
                self.config.signoz_endpoint
            )
        return self._trace_exporter
    
    def get_metric_exporter(self) -> OTLPMetricExporter:
        """获取或创建 Metric Exporter"""
        if self._metric_exporter is None:
            self._metric_exporter = self._create_exporter(
                OTLPMetricExporter,
                self.config.signoz_endpoint
            )
        return self._metric_exporter
    
    def get_log_exporter(self) -> OTLPLogExporter:
        """获取或创建 Log Exporter"""
        if self._log_exporter is None:
            self._log_exporter = self._create_exporter(
                OTLPLogExporter,
                self.config.signoz_endpoint
            )
        return self._log_exporter
    
    def get_logger_provider(self, resource: Optional[Resource] = None) -> LoggerProvider:
        """获取或创建 LoggerProvider（支持自定义 resource）"""
        # 如果没有指定 resource，使用默认的
        if resource is None:
            if self._logger_provider is None:
                # 创建默认 resource
                default_resource = Resource.create({
                    "service.name": self.config.service_name,
                    "service.version": Config.DEFAULT_SERVICE_VERSION,
                })
                self._logger_provider = LoggerProvider(resource=default_resource)
                
                # 创建 exporter 和 processor
                exporter = self.get_log_exporter()
                processor = BatchLogRecordProcessor(exporter)
                self._logger_provider.add_log_record_processor(processor)
            
            return self._logger_provider
        else:
            # 为不同的 resource 创建不同的 LoggerProvider
            # 使用 resource 的 attributes 作为 key
            resource_key = str(sorted(resource.attributes.items()))
            if resource_key not in self._logger_providers:
                logger_provider = LoggerProvider(resource=resource)
                exporter = self.get_log_exporter()
                processor = BatchLogRecordProcessor(exporter)
                logger_provider.add_log_record_processor(processor)
                self._logger_providers[resource_key] = logger_provider
            return self._logger_providers[resource_key]
    
    def get_logger(self, service_name: str, resource: Optional[Resource] = None) -> any:
        """获取指定 service 的 logger（支持自定义 resource）"""
        # 使用 (service_name, resource_key) 作为组合 key
        if resource is None:
            resource_key = "default"
        else:
            resource_key = str(sorted(resource.attributes.items()))
        
        cache_key = f"{service_name}:{resource_key}"
        if cache_key not in self._loggers:
            logger_provider = self.get_logger_provider(resource)
            # 使用 service_name 作为 logger name
            self._loggers[cache_key] = logger_provider.get_logger(service_name)
        return self._loggers[cache_key]
    
    def shutdown_all(self):
        """关闭所有 exporters"""
        if self._trace_exporter:
            try:
                time.sleep(self.config.exporter_wait_time)
                self._trace_exporter.shutdown()
            except Exception as e:
                logger.warning(f"关闭 Trace Exporter 时出错: {e}")
        
        if self._metric_exporter:
            try:
                time.sleep(self.config.exporter_wait_time)
                self._metric_exporter.shutdown()
            except Exception as e:
                logger.warning(f"关闭 Metric Exporter 时出错: {e}")
        
        # 关闭所有 LoggerProvider
        if self._logger_provider:
            try:
                time.sleep(self.config.exporter_wait_time)
                self._logger_provider.shutdown()
            except Exception as e:
                logger.warning(f"关闭 Log LoggerProvider 时出错: {e}")
        
        for resource_key, logger_provider in self._logger_providers.items():
            try:
                time.sleep(self.config.exporter_wait_time)
                logger_provider.shutdown()
            except Exception as e:
                logger.warning(f"关闭 Log LoggerProvider ({resource_key}) 时出错: {e}")
        
        if self._log_exporter and not self._logger_provider and not self._logger_providers:
            try:
                time.sleep(self.config.exporter_wait_time)
                self._log_exporter.shutdown()
            except Exception as e:
                logger.warning(f"关闭 Log Exporter 时出错: {e}")


# ============================================================================
# 工具函数
# ============================================================================

def convert_to_trace_id(trace_id_str: str) -> int:
    """将 trace_id 字符串转换为 OpenTelemetry 格式的 trace_id (128位整数)"""
    # 使用 hash() 方法（与之前能正常工作的版本一致）
    # 注意：虽然 hash() 可能因 hash randomization 在不同运行间产生不同值，
    # 但在同一进程内是稳定的，这对于导入历史数据是可以接受的
    hash_val = abs(hash(trace_id_str))
    # trace_id 是 128 位，所以取模 2^128
    trace_id = hash_val % (2**128)
    # OpenTelemetry 要求 trace_id 不能为全零
    if trace_id == 0:
        trace_id = 1
    return trace_id


def convert_to_span_id(span_id_str: str) -> int:
    """将 span_id 字符串转换为 OpenTelemetry 格式的 span_id (64位整数)"""
    # 使用 hash() 方法（与之前能正常工作的版本一致）
    hash_val = abs(hash(span_id_str))
    # span_id 是 64 位，所以取模 2^64
    span_id = hash_val % (2**64)
    # OpenTelemetry 要求 span_id 不能为全零
    if span_id == 0:
        span_id = 1
    return span_id


def parse_timestamp(timestamp_str: str, time_offset_ms: int = 0, is_seconds: bool = False) -> int:
    """将时间戳字符串转换为纳秒（OpenTelemetry 使用纳秒）
    
    Args:
        timestamp_str: CSV 中的时间戳
        time_offset_ms: 时间偏移量（毫秒），用于将历史时间映射到目标时间
        is_seconds: 如果为True，时间戳是秒；如果为False，时间戳是毫秒
    """
    try:
        if is_seconds:
            # CSV 中的时间戳是秒（metric文件）
            timestamp_s = int(float(timestamp_str))
            # 转换为毫秒
            timestamp_ms = timestamp_s * 1000
        else:
            # CSV 中的时间戳是毫秒（trace文件）
            timestamp_ms = int(float(timestamp_str))
        # 应用时间偏移
        timestamp_ms = timestamp_ms + time_offset_ms
        # 转换为纳秒
        return timestamp_ms * 1_000_000
    except (ValueError, TypeError):
        raise ValueError(f"无法解析时间戳: {timestamp_str}")


class SimpleSpan(ReadableSpan):
    """简单的 Span 实现，用于从 CSV 数据创建"""
    
    def __init__(
        self,
        name: str,
        context: SpanContext,
        parent: Optional[SpanContext],
        resource: Resource,
        attributes: Dict[str, AttributeValue],
        start_time_ns: int,
        end_time_ns: int,
    ):
        self._name = name
        self._context = context
        self._parent = parent
        self._resource = resource
        self._attributes = attributes
        self._start_time = start_time_ns
        self._end_time = end_time_ns
        self._events = []
        self._links = []
        self._kind = trace.SpanKind.INTERNAL
        self._status = Status(StatusCode.OK)
        # 创建默认的 InstrumentationScope
        self._instrumentation_scope = InstrumentationScope(
            name=__name__,
            version=None
        )
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def context(self) -> SpanContext:
        return self._context
    
    @property
    def parent(self) -> Optional[SpanContext]:
        return self._parent
    
    @property
    def resource(self) -> Resource:
        return self._resource
    
    @property
    def attributes(self) -> Dict[str, AttributeValue]:
        return self._attributes
    
    @property
    def events(self) -> List:
        return self._events
    
    @property
    def links(self) -> List:
        return self._links
    
    @property
    def kind(self) -> trace.SpanKind:
        return self._kind
    
    @property
    def status(self) -> Status:
        return self._status
    
    @property
    def start_time(self) -> int:
        return self._start_time
    
    @property
    def end_time(self) -> int:
        return self._end_time
    
    @property
    def instrumentation_scope(self) -> InstrumentationScope:
        return self._instrumentation_scope


def create_span_data_from_row(
    row: Dict[str, str], 
    time_offset_ms: int = 0,
    resource_manager: Optional[ResourceManager] = None
) -> Optional[ReadableSpan]:
    """从 CSV 行创建 OpenTelemetry SpanData
    
    Args:
        row: CSV 行数据
        time_offset_ms: 时间偏移量（毫秒），用于将历史时间映射到目标时间
        resource_manager: Resource 管理器，如果为 None 则使用全局实例
    """
    if resource_manager is None:
        resource_manager = _resource_manager
    
    try:
        # 解析时间戳（应用时间偏移）
        start_time_ns = parse_timestamp(row['timestamp'], time_offset_ms)
        
        # 解析持续时间（毫秒转纳秒）
        duration_ms = int(float(row.get('duration', 0)))
        duration_ns = duration_ms * 1_000_000
        end_time_ns = start_time_ns + duration_ns if duration_ns > 0 else start_time_ns + 1
        
        # 获取 span 信息
        span_id_str = row.get('span_id', '')
        trace_id_str = row.get('trace_id', '')
        parent_id_str = row.get('parent_id', '')
        cmdb_id = row.get('cmdb_id', 'unknown')
        
        # 根据 cmdb_id 创建对应的 resource
        resource = resource_manager.get_resource(cmdb_id)
        
        # 转换 trace_id 和 span_id
        trace_id = convert_to_trace_id(trace_id_str)
        span_id = convert_to_span_id(span_id_str)
        
        # 创建 SpanContext
        span_context = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=False,
            trace_flags=TraceFlags(0x01)  # SAMPLED
        )
        
        # 创建父 SpanContext（如果有）
        parent_context = None
        if parent_id_str and parent_id_str.strip():
            parent_span_id = convert_to_span_id(parent_id_str)
            parent_context = SpanContext(
                trace_id=trace_id,  # 同一 trace 使用相同的 trace_id
                span_id=parent_span_id,
                is_remote=False,
                trace_flags=TraceFlags(0x01)
            )
        
        # 创建属性
        attributes: Dict[str, AttributeValue] = {
            "cmdb_id": cmdb_id,
            "span_id": span_id_str,
            "trace_id": trace_id_str,
            "duration_ms": duration_ms,
        }
        if parent_id_str:
            attributes["parent_id"] = parent_id_str
        
        # 创建 SimpleSpan
        span = SimpleSpan(
            name=cmdb_id,
            context=span_context,
            parent=parent_context,
            resource=resource,
            attributes=attributes,
            start_time_ns=start_time_ns,
            end_time_ns=end_time_ns,
        )
        
        return span
            
    except Exception as e:
        logger.warning(f"创建 span 时出错: {e}")
        return None




# 全局 Resource 管理器实例（向后兼容）
_resource_manager = ResourceManager()

def create_resource_for_service(service_name: str) -> Resource:
    """根据服务名称创建 Resource（向后兼容函数）
    使用 ResourceManager 进行缓存管理
    """
    return _resource_manager.get_resource(service_name)


def process_metric_app_file(
    file_path: str,
    exporter: OTLPMetricExporter,
    batch_size: int,
    time_range_info: Optional[Dict] = None,
    time_offset_ms: int = 0,
    show_progress: bool = True,
    resource_manager: Optional[ResourceManager] = None
) -> int:
    """处理 metric_app.csv 文件，使用自定义时间戳
    格式: timestamp,rr,sr,cnt,mrt,tc
    - timestamp: 时间戳（秒）
    - rr: 请求率 (Request Rate)
    - sr: 成功率 (Success Rate)
    - cnt: 计数 (Count)
    - mrt: 平均响应时间 (Mean Response Time)
    - tc: 服务名称 (Service Name)
    
    按 (timestamp, tc) 分组，每个分组创建一个 ResourceMetrics
    """
    if resource_manager is None:
        resource_manager = _resource_manager
    
    count = 0
    batch_rows = []  # 存储待处理的原始行数据
    
    # 创建InstrumentationScope
    instrumentation_scope = InstrumentationScope(
        name=__name__,
        version=None
    )
    
    def create_resource_metrics_from_group(group_key, group_rows):
        """从分组数据创建 ResourceMetrics"""
        service_name = group_key
                        
        # 根据 service_name 创建对应的 resource
        resource = resource_manager.get_resource(service_name)
        
        # 创建属性
        attributes = {"service.name": service_name}
        
        # 收集该分组的所有数据点
        rr_data_points = []
        sr_data_points = []
        cnt_data_points = []
        mrt_data_points = []
        
        for row in group_rows:
            try:
                rr = float(row.get('rr', 0))
                sr = float(row.get('sr', 0))
                cnt = float(row.get('cnt', 0))
                mrt = float(row.get('mrt', 0))
                
                timestamp_s = int(float(row.get('timestamp', 0)))
                if timestamp_s <= 0:
                    continue
                
                timestamp_ms = timestamp_s * 1000 + time_offset_ms
                timestamp_ns = timestamp_ms * 1_000_000
                
                # 跟踪时间范围
                if time_range_info is not None:
                    if time_range_info.get('min') is None or timestamp_ms < time_range_info['min']:
                        time_range_info['min'] = timestamp_ms
                    if time_range_info.get('max') is None or timestamp_ms > time_range_info['max']:
                        time_range_info['max'] = timestamp_ms
                        
                # app_request_rate (rr) - Counter
                if rr > 0:
                    rr_data_points.append(NumberDataPoint(
                        attributes=attributes,
                        start_time_unix_nano=timestamp_ns,
                        time_unix_nano=timestamp_ns,
                        value=rr
                    ))
                
                # app_success_rate (sr) - Gauge
                if sr >= 0:
                    sr_data_points.append(NumberDataPoint(
                        attributes=attributes,
                        start_time_unix_nano=timestamp_ns,
                        time_unix_nano=timestamp_ns,
                        value=sr
                    ))
                
                # app_request_count (cnt) - Counter
                if cnt > 0:
                    cnt_data_points.append(NumberDataPoint(
                        attributes=attributes,
                        start_time_unix_nano=timestamp_ns,
                        time_unix_nano=timestamp_ns,
                        value=cnt
                    ))
                
                # app_response_time (mrt) - Histogram
                if mrt > 0:
                    explicit_bounds = [0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0]
                    bucket_counts = [0] * (len(explicit_bounds) + 1)
                    bucket_idx = len(explicit_bounds)
                    for i, bound in enumerate(explicit_bounds):
                        if mrt <= bound:
                            bucket_idx = i
                            break
                    bucket_counts[bucket_idx] = 1
                    
                    mrt_data_points.append(HistogramDataPoint(
                        attributes=attributes,
                        start_time_unix_nano=timestamp_ns,
                        time_unix_nano=timestamp_ns,
                        count=1,
                        sum=mrt,
                        bucket_counts=bucket_counts,
                        explicit_bounds=explicit_bounds,
                        min=mrt,
                        max=mrt
                    ))
            except (ValueError, TypeError):
                continue
        
        # 创建 metrics
        metrics_list = []
        
        if rr_data_points:
            metrics_list.append(Metric(
                name="app_request_rate",
                description="Application request rate",
                unit="1",
                data=Sum(
                    data_points=rr_data_points,
                    aggregation_temporality=1,
                    is_monotonic=True
                )
            ))
        
        if sr_data_points:
            metrics_list.append(Metric(
                name="app_success_rate",
                description="Application success rate",
                unit="1",
                data=Gauge(data_points=sr_data_points)
            ))
        
        if cnt_data_points:
            metrics_list.append(Metric(
                name="app_request_count",
                description="Application request count",
                unit="1",
                data=Sum(
                    data_points=cnt_data_points,
                    aggregation_temporality=1,
                    is_monotonic=True
                )
            ))
        
        if mrt_data_points:
            metrics_list.append(Metric(
                name="app_response_time",
                description="Application mean response time",
                unit="ms",
                data=HistogramData(
                    data_points=mrt_data_points,
                    aggregation_temporality=1
                )
            ))
        
        if not metrics_list:
            return None
        
        # 创建 ResourceMetrics
        scope_metrics = ScopeMetrics(
            scope=instrumentation_scope,
            metrics=metrics_list,
            schema_url=""
        )
        return ResourceMetrics(
            resource=resource,
            scope_metrics=[scope_metrics],
            schema_url=""
        )
    
    def process_batch():
        """处理当前批次的数据：分组并创建 ResourceMetrics"""
        if not batch_rows:
            return []
        
        # 按 (timestamp, tc) 分组
        groups = defaultdict(list)
        for row in batch_rows:
            service_name = row.get('tc', 'unknown')
            try:                
                group_key = service_name
                groups[group_key].append(row)
            except (ValueError, TypeError):
                continue
        
        # 为每个分组创建 ResourceMetrics
        resource_metrics_list = []
        for group_key, group_rows in groups.items():
            resource_metrics = create_resource_metrics_from_group(group_key, group_rows)
            if resource_metrics:
                resource_metrics_list.append(resource_metrics)
        
        return resource_metrics_list
    
    try:
        total_lines = count_file_lines(file_path) if show_progress else 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            pbar = None
            if show_progress and total_lines > 0:
                pbar = tqdm(
                    total=total_lines,
                    desc="    处理中",
                    unit="行",
                    unit_scale=True,
                    leave=False,
                    ncols=100,
                    mininterval=0.5,
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
                )
            
            try:
                for row in reader:
                    try:
                        batch_rows.append(row)
                        count += 1
                        
                        if pbar:
                            pbar.update(1)
                        
                        # 当批次达到指定大小时，处理并发送
                        if len(batch_rows) >= batch_size:
                            resource_metrics_list = process_batch()
                            
                            if resource_metrics_list:
                                try:
                                    metrics_data = MetricsData(resource_metrics=resource_metrics_list)
                                    result = exporter.export(metrics_data)
                                    if result != MetricExportResult.SUCCESS:
                                        logger.warning(f"批次导出返回: {result}")
                                except Exception as e:
                                    logger.exception("导出批次时出错")
                            
                            batch_rows = []
                            
                    except Exception as e:
                        logger.warning(f"处理行时出错: {e}")
                        continue
                        
            finally:
                if pbar:
                    pbar.close()
        
        # 处理并发送剩余的批次
        if batch_rows:
            resource_metrics_list = process_batch()
            if resource_metrics_list:
                try:
                    metrics_data = MetricsData(resource_metrics=resource_metrics_list)
                    result = exporter.export(metrics_data)
                    if result != MetricExportResult.SUCCESS:
                        logger.warning(f"最后批次导出返回: {result}")
                except Exception as e:
                    logger.exception("导出最后批次时出错")
        
        return count
        
    except Exception as e:
        logger.exception("处理文件时出错")
        return 0


def extract_kpi_category(kpi_name: str) -> str:
    """从kpi_name中提取大类
    
    支持的分类：
    - Mysql: MySQL数据库相关指标
    - OSLinux: 操作系统Linux相关指标
    - redis: Redis相关指标
    - Container: Docker容器相关指标
    - Tomcat: Tomcat应用服务器相关指标
    - JVM: Java虚拟机相关指标
    
    Returns:
        大类名称（小写），例如：mysql, oslinux, redis, container, tomcat, jvm
    """
    if not kpi_name:
        return 'other'
    
    # 移除引号
    kpi_name = kpi_name.strip('"')
    
    # 提取第一个'-'之前的部分作为大类
    if '-' in kpi_name:
        category = kpi_name.split('-')[0].strip()
    else:
        return 'other'
    
    # 标准化类别名称
    category_lower = category.lower()
    
    # 映射到标准类别名称
    category_mapping = {
        'mysql': 'mysql',
        'oslinux': 'oslinux',
        'redis': 'redis',
        'container': 'container',
        'tomcat': 'tomcat',
        'jvm': 'jvm',
    }
    
    return category_mapping.get(category_lower, 'other')


def process_metric_container_file(
    file_path: str,
    exporter: OTLPMetricExporter,
    batch_size: int,
    time_range_info: Optional[Dict] = None,
    time_offset_ms: int = 0,
    show_progress: bool = True,
    resource_manager: Optional[ResourceManager] = None
) -> int:
    """处理 metric_container.csv 文件，使用自定义时间戳
    格式: timestamp,cmdb_id,kpi_name,value
    - timestamp: 时间戳（秒）
    - cmdb_id: 组件ID
    - kpi_name: KPI指标名称
    - value: 指标值
    
    按 (timestamp, cmdb_id) 分组，每个分组创建一个 ResourceMetrics
    默认按大类分组创建metric（例如：container_mysql_metric, container_oslinux_metric等）。
    将349个metric减少到6个（每个大类一个）。
    """
    if resource_manager is None:
        resource_manager = _resource_manager
    
    count = 0  # 读取的CSV行数
    total_data_points = 0  # 实际创建的metric数据点总数
    total_batches_exported = 0  # 导出的批次总数
    total_resource_metrics = 0  # 创建的ResourceMetrics总数
    batch_rows = []  # 存储待处理的原始行数据
    
    # 创建InstrumentationScope
    instrumentation_scope = InstrumentationScope(
        name=__name__,
        version=None
    )
    
    def create_resource_metrics_from_group(group_key, group_rows):
        """从分组数据创建 ResourceMetrics"""
        cmdb_id = group_key
        
        # 根据 cmdb_id 创建对应的 resource
        resource = resource_manager.get_resource(cmdb_id)
        
        # 按大类分组收集数据点
        # category -> metric_name -> data_points
        category_metrics = defaultdict(lambda: defaultdict(list))
        
        for row in group_rows:
            try:
                timestamp_s = int(float(row.get('timestamp', 0)))
                if timestamp_s <= 0:
                    continue
                timestamp_ms = timestamp_s * 1000 + time_offset_ms
                timestamp_ns = timestamp_ms * 1_000_000
                
                # 跟踪时间范围
                if time_range_info is not None:
                    if time_range_info.get('min') is None or timestamp_ms < time_range_info['min']:
                        time_range_info['min'] = timestamp_ms
                    if time_range_info.get('max') is None or timestamp_ms > time_range_info['max']:
                        time_range_info['max'] = timestamp_ms
                        
                kpi_name = row.get('kpi_name', '')
                if not kpi_name:
                    continue
                
                value = float(row.get('value', 0))
                
                # 按大类确定metric名称
                category = extract_kpi_category(kpi_name)
                metric_name = f"container_{category}_metric"
                
                # 创建属性
                attributes = {
                    "cmdb_id": cmdb_id,
                    "kpi_name": kpi_name,
                }
                
                # 创建数据点
                data_point = NumberDataPoint(
                    attributes=attributes,
                    start_time_unix_nano=timestamp_ns,
                    time_unix_nano=timestamp_ns,
                    value=value
                )
                
                category_metrics[category][metric_name].append(data_point)
            except (ValueError, TypeError):
                continue
        
        if not category_metrics:
            return None
        
        # 为每个大类创建 Metric
        metrics_list = []
        for category, metric_dict in category_metrics.items():
            for metric_name, data_points in metric_dict.items():
                gauge_data = Gauge(data_points=data_points)
                metric = Metric(
                    name=metric_name,
                    description=f"Container metrics for {category} category. Use kpi_name label to filter specific metrics.",
                    unit="1",
                    data=gauge_data
                )
                metrics_list.append(metric)
        
        if not metrics_list:
            return None
        
        # 创建 ResourceMetrics
        scope_metrics = ScopeMetrics(
            scope=instrumentation_scope,
            metrics=metrics_list,
            schema_url=""
        )
        return ResourceMetrics(
            resource=resource,
            scope_metrics=[scope_metrics],
            schema_url=""
        )
    
    def process_batch():
        """处理当前批次的数据：分组并创建 ResourceMetrics"""
        if not batch_rows:
            return []
        
        # 按 (timestamp, cmdb_id) 分组
        groups = defaultdict(list)
        for row in batch_rows:
            try:
                cmdb_id = row.get('cmdb_id', 'unknown')
                
                group_key = cmdb_id
                groups[group_key].append(row)
            except (ValueError, TypeError):
                continue
        
        # 为每个分组创建 ResourceMetrics
        resource_metrics_list = []
        for group_key, group_rows in groups.items():
            resource_metrics = create_resource_metrics_from_group(group_key, group_rows)
            if resource_metrics:
                resource_metrics_list.append(resource_metrics)
        
        return resource_metrics_list
    
    try:
        total_lines = count_file_lines(file_path) if show_progress else 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            pbar = None
            if show_progress and total_lines > 0:
                pbar = tqdm(
                    total=total_lines,
                    desc="    处理中",
                    unit="行",
                    unit_scale=True,
                    leave=False,
                    ncols=100,
                    mininterval=0.5,
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
                )
            
            try:
                for row in reader:
                    try:
                        batch_rows.append(row)
                        count += 1
                        
                        if pbar:
                            pbar.update(1)
                        
                        # 当批次达到指定大小时，处理并发送
                        if len(batch_rows) >= batch_size:
                            resource_metrics_list = process_batch()
                            
                            if resource_metrics_list:
                                try:
                                    metrics_data = MetricsData(resource_metrics=resource_metrics_list)
                                    result = exporter.export(metrics_data)
                                    total_batches_exported += 1
                                    total_resource_metrics += len(resource_metrics_list)
                                    # 统计数据点
                                    for rm in resource_metrics_list:
                                        for sm in rm.scope_metrics:
                                            for metric in sm.metrics:
                                                if hasattr(metric.data, 'data_points'):
                                                    total_data_points += len(metric.data.data_points)
                                    
                                    if result != MetricExportResult.SUCCESS:
                                        logger.warning(f"批次 #{total_batches_exported} 导出返回: {result}")
                                except Exception as e:
                                    logger.exception("导出批次时出错")
                            
                            batch_rows = []
                            
                    except Exception as e:
                        if pbar:
                            pbar.write(f"    ⚠️  处理行时出错: {e}")
                        continue
                
            finally:
                if pbar:
                    pbar.close()
        
        # 处理并发送剩余的批次
        if batch_rows:
            resource_metrics_list = process_batch()
            if resource_metrics_list:
                try:
                    metrics_data = MetricsData(resource_metrics=resource_metrics_list)
                    result = exporter.export(metrics_data)
                    total_batches_exported += 1
                    total_resource_metrics += len(resource_metrics_list)
                    # 统计数据点
                    for rm in resource_metrics_list:
                        for sm in rm.scope_metrics:
                            for metric in sm.metrics:
                                if hasattr(metric.data, 'data_points'):
                                    total_data_points += len(metric.data.data_points)
                    
                    if result != MetricExportResult.SUCCESS:
                        logger.warning(f"最后批次导出返回: {result}")
                except Exception as e:
                    logger.exception("导出最后批次时出错")
        
        return count
        
    except Exception as e:
        logger.exception("处理文件时出错")
        return 0


# ============================================================================
# 批处理工具类
# ============================================================================

class BatchProcessor:
    """通用的批处理工具类，用于处理 CSV 数据并批量导出"""
    
    def __init__(self, batch_size: int, exporter, show_progress: bool = True):
        self.batch_size = batch_size
        self.exporter = exporter
        self.show_progress = show_progress
    
    def export_batch(self, batch_data, batch_type: str = "data"):
        """导出批次数据，统一的错误处理"""
        if not batch_data:
            return True
        
        try:
            if batch_type == "metrics":
                result = self.exporter.export(batch_data)
                return result == MetricExportResult.SUCCESS
            elif batch_type == "spans":
                result = self.exporter.export(batch_data)
                return result == SpanExportResult.SUCCESS
            else:
                logger.warning(f"未知的批次类型: {batch_type}")
                return False
        except Exception as e:
            logger.error(f"导出批次时出错: {e}", exc_info=True)
            return False


def count_file_lines(file_path: str) -> int:
    """快速统计文件行数（不包括表头）"""
    try:
        # 使用二进制模式读取，速度更快
        with open(file_path, 'rb') as f:
            # 跳过表头行
            next(f)
            # 统计剩余行数（使用缓冲区读取）
            count = 0
            buf_size = 1024 * 1024  # 1MB 缓冲区
            while True:
                buf = f.read(buf_size)
                if not buf:
                    break
                count += buf.count(b'\n')
            return count
    except Exception:
        # 如果二进制模式失败，回退到文本模式
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                next(f)  # 跳过表头
                return sum(1 for _ in f)
        except Exception:
            return 0


def process_file_streaming(
    file_path: str,
    exporter: OTLPSpanExporter,
    batch_size: int,
    time_range_info: Optional[Dict] = None,
    time_offset_ms: int = 0,
    show_progress: bool = True,
    resource_manager: Optional[ResourceManager] = None
) -> int:
    """流式处理单个文件，边读边发送"""
    count = 0
    batch = []
    file_count = 0
    min_timestamp = None
    max_timestamp = None
    
    try:
        # 统计文件总行数用于进度条
        total_lines = count_file_lines(file_path) if show_progress else 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # 创建进度条
            pbar = None
            if show_progress and total_lines > 0:
                pbar = tqdm(
                    total=total_lines,
                    desc="    处理中",
                    unit="行",
                    unit_scale=True,
                    leave=False,
                    ncols=100,
                    mininterval=0.5,  # 最小更新间隔0.5秒
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
                )
            
            try:
                for row in reader:
                    # 跟踪时间范围
                    try:
                        timestamp_ms = int(float(row.get('timestamp', 0)))
                        if timestamp_ms > 0:
                            if min_timestamp is None or timestamp_ms < min_timestamp:
                                min_timestamp = timestamp_ms
                            if max_timestamp is None or timestamp_ms > max_timestamp:
                                max_timestamp = timestamp_ms
                    except (ValueError, TypeError):
                        pass
                    
                    span = create_span_data_from_row(row, time_offset_ms, resource_manager)
                    if span:
                        batch.append(span)
                        file_count += 1
                        count += 1
                        
                        # 更新进度条
                        if pbar:
                            pbar.update(1)
                        
                        # 当批次达到指定大小时，立即发送
                        if len(batch) >= batch_size:
                            try:
                                result = exporter.export(batch)
                                if result != SpanExportResult.SUCCESS:
                                    logger.warning(f"批次导出返回: {result}")
                            except Exception as e:
                                logger.exception("导出批次时出错")
                            finally:
                                batch = []
                
                # 发送剩余的 spans
                if batch:
                    try:
                        result = exporter.export(batch)
                        if result != SpanExportResult.SUCCESS:
                            logger.warning(f"最后批次导出返回: {result}")
                    except Exception as e:
                        logger.exception("导出最后批次时出错")
            finally:
                if pbar:
                    pbar.close()
        
        # 更新全局时间范围信息
        if time_range_info is not None:
            if min_timestamp is not None:
                if time_range_info.get('min') is None or min_timestamp < time_range_info['min']:
                    time_range_info['min'] = min_timestamp
            if max_timestamp is not None:
                if time_range_info.get('max') is None or max_timestamp > time_range_info['max']:
                    time_range_info['max'] = max_timestamp
        
        return file_count
        
    except Exception as e:
        logger.exception("处理文件时出错")
        return 0


def process_log_file_streaming(
    file_path: str,
    exporter_manager: ExporterManager,
    batch_size: int,
    time_range_info: Optional[Dict] = None,
    time_offset_ms: int = 0,
    show_progress: bool = True,
    resource_manager: Optional[ResourceManager] = None
) -> int:
    """流式处理 log 文件，边读边发送（使用 LoggerProvider 方式）
    
    Args:
        file_path: log_service.csv 文件路径
        exporter_manager: ExporterManager 实例
        batch_size: 批处理大小（用于进度显示，实际由 BatchLogRecordProcessor 控制）
        time_range_info: 时间范围信息字典（可选）
        time_offset_ms: 时间偏移量（毫秒）
        show_progress: 是否显示进度条
        resource_manager: Resource 管理器
    """
    if resource_manager is None:
        resource_manager = _resource_manager
    
    count = 0
    file_count = 0
    min_timestamp = None
    max_timestamp = None
    
    try:
        # 统计文件总行数用于进度条
        total_lines = count_file_lines(file_path) if show_progress else 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # 创建进度条
            pbar = None
            if show_progress and total_lines > 0:
                pbar = tqdm(
                    total=total_lines,
                    desc="    处理中",
                    unit="行",
                    unit_scale=True,
                    leave=False,
                    ncols=100,
                    mininterval=0.5,
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
                )
            
            try:
                for row in reader:
                    try:
                        # 解析时间戳（应用时间偏移）
                        timestamp_s = int(float(row.get('timestamp', 0)))
                        if timestamp_s <= 0:
                            if pbar:
                                pbar.update(1)
                            continue
                        
                        timestamp_ms = timestamp_s * 1000
                        timestamp_ns = (timestamp_ms + time_offset_ms) * 1_000_000
                        
                        # 跟踪时间范围
                        if time_range_info is not None:
                            adjusted_timestamp_ms = timestamp_ms + time_offset_ms
                            if min_timestamp is None or adjusted_timestamp_ms < min_timestamp:
                                min_timestamp = adjusted_timestamp_ms
                            if max_timestamp is None or adjusted_timestamp_ms > max_timestamp:
                                max_timestamp = adjusted_timestamp_ms
                        
                        # 获取 log 信息
                        cmdb_id = row.get('cmdb_id', 'unknown')
                        log_name = row.get('log_name', '')
                        log_value = row.get('value', '')
                        log_id = row.get('log_id', '')
                        
                        # 获取对应 service 的 resource
                        service_resource = resource_manager.get_resource(cmdb_id)
                        
                        # 获取对应 service 的 logger（使用对应的 resource）
                        log_logger = exporter_manager.get_logger(cmdb_id, service_resource)
                        
                        # 创建 LogRecord（参考 test.py，不包含 resource 参数）
                        log_record = LogRecord(
                            timestamp=timestamp_ns,
                            observed_timestamp=time.time_ns(),
                            trace_id=0,
                            span_id=0,
                            trace_flags=None,
                            severity_text="INFO",
                            severity_number=SeverityNumber.INFO,
                            body=log_value,
                            attributes={
                                "cmdb_id": cmdb_id,
                                "log_name": log_name,
                                "log_id": log_id,
                            }
                        )
                        
                        # 发送日志
                        log_logger.emit(log_record)
                        file_count += 1
                        count += 1
                        
                        # 更新进度条
                        if pbar:
                            pbar.update(1)
                        
                    except Exception as e:
                        logger.warning(f"处理 log 行时出错: {e}")
                        if pbar:
                            pbar.update(1)
                        continue
                        
            finally:
                if pbar:
                    pbar.close()
        
        # 更新全局时间范围信息
        if time_range_info is not None:
            if min_timestamp is not None:
                if time_range_info.get('min') is None or min_timestamp < time_range_info['min']:
                    time_range_info['min'] = min_timestamp
            if max_timestamp is not None:
                if time_range_info.get('max') is None or max_timestamp > time_range_info['max']:
                    time_range_info['max'] = max_timestamp
        
        return file_count
        
    except Exception as e:
        logger.exception("处理文件时出错")
        return 0


def main():
    parser = argparse.ArgumentParser(description='将 OpenRCA metric 和 trace 数据导入到 SigNoz')
    parser.add_argument(
        '--signoz-endpoint',
        type=str,
        default='localhost:4317',
        help='SigNoz OTLP gRPC 端点 (默认: localhost:4317)'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default=None,
        help='数据目录路径 (可选，如果不提供则根据 --source-date 自动构建，例如: datasets/OpenRCA/Bank/telemetry/2021_03_04)'
    )
    parser.add_argument(
        '--service-name',
        type=str,
        default=Config.DEFAULT_SERVICE_NAME,
        help=f'服务名称 (默认: {Config.DEFAULT_SERVICE_NAME})'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=Config.DEFAULT_BATCH_SIZE,
        help=f'批处理大小 (默认: {Config.DEFAULT_BATCH_SIZE})'
    )
    parser.add_argument(
        '--source-date',
        type=str,
        required=True,
        metavar='YYYY-MM-DD',
        help='源日期：datasets 中的历史日期，格式 YYYY-MM-DD (例如: 2021-03-04)'
    )
    parser.add_argument(
        '--target-date',
        type=str,
        required=True,
        metavar='YYYY-MM-DD',
        help='目标日期：映射到的日期（必须在当前时间15天内），格式 YYYY-MM-DD (例如: 2024-12-19)'
    )
    
    args = parser.parse_args()
    
    # 创建配置对象
    config = Config(
        signoz_endpoint=args.signoz_endpoint,
        batch_size=args.batch_size,
        service_name=args.service_name
    )
    
    # 创建资源管理器和 exporter 管理器
    resource_manager = ResourceManager(service_version=Config.DEFAULT_SERVICE_VERSION)
    exporter_manager = ExporterManager(config)
    
    print("=" * 60)
    print("🚀 OpenRCA Metric, Trace & Log 数据导入工具")
    print("=" * 60)
    print(f"  📍 SigNoz 端点: {args.signoz_endpoint}")
    print(f"  📦 批处理大小: {args.batch_size}")
    print("=" * 60)
    print()
    
    # 确定数据目录路径
    if args.data_dir is None:
        # 如果没有指定 data-dir，根据 source-date 自动构建路径
        # 将 YYYY-MM-DD 格式转换为 YYYY_MM_DD 格式（文件夹命名格式）
        date_folder = args.source_date.replace('-', '_')
        data_dir = f'datasets/OpenRCA/Bank/telemetry/{date_folder}'
    else:
        # 使用指定的 data-dir
        data_dir = args.data_dir
    
    # 计算时间偏移量
    try:
        # 解析源日期和目标日期
        source_date = datetime.strptime(args.source_date, '%Y-%m-%d')
        target_date = datetime.strptime(args.target_date, '%Y-%m-%d')
        
        # 验证目标日期必须在当前时间15天内
        now = datetime.now(timezone.utc)
        target_datetime = target_date.replace(tzinfo=timezone.utc)
        
        if target_datetime > now:
            print(f"❌ 错误: 目标日期 {args.target_date} 不能晚于当前时间")
            return
        
        days_diff = (now - target_datetime).days
        if days_diff > 15:
            print(f"❌ 错误: 目标日期 {args.target_date} 必须在当前时间15天内")
            print(f"   当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print(f"   目标日期距离当前时间: {days_diff} 天")
            return
        
        # 计算偏移量（毫秒）
        # 将源日期的 0 点映射到目标日期的 0 点
        source_timestamp_ms = int(source_date.replace(tzinfo=timezone.utc).timestamp() * 1000)
        target_timestamp_ms = int(target_date.replace(tzinfo=timezone.utc).timestamp() * 1000)
        time_offset_ms = target_timestamp_ms - source_timestamp_ms
        config.time_offset_ms = time_offset_ms
        
    except ValueError as e:
        print(f"❌ 日期格式错误: {e}")
        print("   日期格式应为: YYYY-MM-DD (例如: 2021-03-04)")
        return
    
    # 检查数据目录
    data_dir_path = Path(data_dir)
    
    if not data_dir_path.exists():
        print(f"❌ 错误: 数据目录不存在: {data_dir}")
        print(f"   请检查 --source-date 参数是否正确（例如: 2021-03-04 对应 2021_03_04 文件夹）")
        return
    
    if not data_dir_path.is_dir():
        print(f"❌ 错误: 指定的路径不是目录: {data_dir}")
        return
    
    start_time = time.time()
    time_range_info = {'min': None, 'max': None}  # 跟踪时间范围
    
    # ========== 第一步：导入 Log 数据 ==========
    print("=" * 60)
    print("📝 第一步：导入 Log 数据")
    print("=" * 60)
    print()
    
    # 设置 Log LoggerProvider
    try:
        logger_provider = exporter_manager.get_logger_provider()
    except Exception as e:
        print(f"❌ Log LoggerProvider 初始化失败: {e}")
        logger.exception("Log LoggerProvider 初始化失败")
        return
    
    # 查找 log 文件
    log_files = sorted(glob.glob(str(data_dir_path / "**/log_service.csv"), recursive=True))
    
    total_logs = 0
    
    if not log_files:
        print(f"⚠️  未找到 log_service.csv 文件")
    else:
        print(f"📋 找到 {len(log_files)} 个 log 文件\n")
        
        # 流式处理每个文件
        with tqdm(total=len(log_files), desc="📝 处理 log 文件", unit="文件", ncols=100) as file_pbar:
            for file_idx, log_file in enumerate(log_files, 1):
                file_name = Path(log_file).name
                file_pbar.set_description(f"📄 [{file_idx}/{len(log_files)}] {file_name}")
                
                file_count = process_log_file_streaming(
                    log_file,
                    exporter_manager,
                    config.batch_size,
                    time_range_info=time_range_info,
                    time_offset_ms=config.time_offset_ms,
                    show_progress=True,
                    resource_manager=resource_manager
                )
                
                total_logs += file_count
                file_pbar.update(1)
                file_pbar.set_postfix({"logs": f"{file_count:,}", "总计": f"{total_logs:,}"})
        
        print()  # 空行分隔
    
    # ========== 第二步：导入 Metric 数据 ==========
    print("=" * 60)
    print("📊 第二步：导入 Metric 数据")
    print("=" * 60)
    print()
    
    # 设置 Metric Exporter
    try:
        metric_exporter = exporter_manager.get_metric_exporter()
    except Exception as e:
        print(f"❌ Metric Exporter 初始化失败: {e}")
        logger.exception("Metric Exporter 初始化失败")
        return
    
    # 查找 metric 文件
    metric_app_files = sorted(glob.glob(str(data_dir_path / "**/metric_app.csv"), recursive=True))
    metric_container_files = sorted(glob.glob(str(data_dir_path / "**/metric_container.csv"), recursive=True))
    
    total_metrics = 0
    
    # 处理 metric_app.csv 文件
    if metric_app_files:
        print(f"📋 找到 {len(metric_app_files)} 个 metric_app.csv 文件\n")
        with tqdm(total=len(metric_app_files), desc="📊 处理 metric_app 文件", unit="文件", ncols=100) as file_pbar:
            for file_idx, metric_file in enumerate(metric_app_files, 1):
                file_name = Path(metric_file).name
                file_pbar.set_description(f"📄 [{file_idx}/{len(metric_app_files)}] {file_name}")
                
                file_count = process_metric_app_file(
                    metric_file,
                    metric_exporter,
                    config.batch_size,
                    time_range_info=time_range_info,
                    time_offset_ms=config.time_offset_ms,
                    show_progress=True,
                    resource_manager=resource_manager
                )
                
                total_metrics += file_count
                file_pbar.update(1)
                file_pbar.set_postfix({"metrics": f"{file_count:,}", "总计": f"{total_metrics:,}"})
        print()
    else:
        print("⚠️  未找到 metric_app.csv 文件\n")
    
    # 处理 metric_container.csv 文件
    if metric_container_files:
        print(f"📋 找到 {len(metric_container_files)} 个 metric_container.csv 文件\n")
        with tqdm(total=len(metric_container_files), desc="📊 处理 metric_container 文件", unit="文件", ncols=100) as file_pbar:
            for file_idx, metric_file in enumerate(metric_container_files, 1):
                file_name = Path(metric_file).name
                file_pbar.set_description(f"📄 [{file_idx}/{len(metric_container_files)}] {file_name}")
                
                file_count = process_metric_container_file(
                    metric_file,
                    metric_exporter,
                    config.batch_size,
                    time_range_info=time_range_info,
                    time_offset_ms=config.time_offset_ms,
                    show_progress=True,
                    resource_manager=resource_manager
                )
                
                total_metrics += file_count
                file_pbar.update(1)
                file_pbar.set_postfix({"metrics": f"{file_count:,}", "总计": f"{total_metrics:,}"})
        print()
    else:
        print("⚠️  未找到 metric_container.csv 文件\n")
    
    # 等待 metric 数据发送完成（将在最后统一关闭所有 exporters）
    
    # ========== 第三步：导入 Trace 数据 ==========
    print("=" * 60)
    print("🔍 第三步：导入 Trace 数据")
    print("=" * 60)
    print()
    
    # 设置 Trace Exporter
    try:
        trace_exporter = exporter_manager.get_trace_exporter()
    except Exception as e:
        print(f"❌ Trace Exporter 初始化失败: {e}")
        logger.exception("Trace Exporter 初始化失败")
        return
    
    # 查找 trace 文件
    trace_files = sorted(glob.glob(str(data_dir_path / "**/trace_span.csv"), recursive=True))
    
    if not trace_files:
        print(f"⚠️  未找到 trace_span.csv 文件")
    else:
        print(f"📋 找到 {len(trace_files)} 个 trace 文件\n")
        
        # 流式处理每个文件
        total_spans = 0
        
        # 使用 tqdm 显示整体进度
        with tqdm(total=len(trace_files), desc="🔍 处理 trace 文件", unit="文件", ncols=100) as file_pbar:
            for file_idx, trace_file in enumerate(trace_files, 1):
                file_name = Path(trace_file).name
                file_pbar.set_description(f"📄 [{file_idx}/{len(trace_files)}] {file_name}")
                
                file_count = process_file_streaming(
                    trace_file,
                    trace_exporter,
                    config.batch_size,
                    time_range_info=time_range_info,
                    time_offset_ms=config.time_offset_ms,
                    show_progress=True,
                    resource_manager=resource_manager
                )
                
                total_spans += file_count
                file_pbar.update(1)
                file_pbar.set_postfix({"spans": f"{file_count:,}", "总计": f"{total_spans:,}"})
        
        print()  # 空行分隔
        
        # 确保所有 trace 数据都发送完成（将在最后统一关闭所有 exporters）
    
    # 统一关闭所有 exporters
    exporter_manager.shutdown_all()
    
    elapsed_time = time.time() - start_time
    print()
    print("=" * 60)
    print("✅ 导入完成!")
    if total_metrics > 0:
        print(f"  📊 Metric 记录数: {total_metrics:,}")
    if trace_files:
        print(f"  🔍 Trace spans 数: {total_spans:,}")
    if log_files:
        print(f"  📝 Log 记录数: {total_logs:,}")
    print(f"  ⏱️  总耗时: {elapsed_time:.2f} 秒")
    if total_metrics > 0:
        print(f"  📈 Metric 平均速度: {total_metrics / elapsed_time:.0f} metrics/秒")
    if trace_files and total_spans > 0:
        print(f"  📈 Trace 平均速度: {total_spans / elapsed_time:.0f} spans/秒")
    if log_files and total_logs > 0:
        print(f"  📈 Log 平均速度: {total_logs / elapsed_time:.0f} logs/秒")
    
    # 清理资源
    resource_manager.clear_cache()
    
    # 显示时间范围信息
    if time_range_info['min'] is not None and time_range_info['max'] is not None:
        # 使用 UTC 时间显示（OpenTelemetry 和 SigNoz 使用 UTC）
        min_dt_utc = datetime.utcfromtimestamp(time_range_info['min'] / 1000)
        max_dt_utc = datetime.utcfromtimestamp(time_range_info['max'] / 1000)
        min_dt_local = datetime.fromtimestamp(time_range_info['min'] / 1000)
        max_dt_local = datetime.fromtimestamp(time_range_info['max'] / 1000)
        
        print()
        print("📅 数据时间范围:")
        print(f"  ⏰ 最早时间 (UTC): {min_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"  ⏰ 最晚时间 (UTC): {max_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    print("=" * 60)


if __name__ == '__main__':
    main()

