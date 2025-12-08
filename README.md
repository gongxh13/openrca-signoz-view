# OpenRCA SigNoz 数据导入工具

将 OpenRCA 数据集的 metric、trace 和 log 数据导入到 SigNoz 进行可视化分析。支持将历史数据的时间戳映射到当前时间15天内，方便在 SigNoz 界面查看。

## 功能特性

- 📊 读取 OpenRCA 数据集中的 metric、trace 和 log 数据文件
  - 支持 `metric_app.csv`（应用指标）
  - 支持 `metric_container.csv`（容器指标）
  - 支持 `trace_span.csv`（追踪数据）
  - 支持 `log_service.csv`（日志数据）
- ⏰ 支持时间戳映射：将历史日期映射到当前时间15天内
- 🚀 通过 OpenTelemetry Protocol (OTLP) 将数据发送到 SigNoz
- 📦 支持批量处理以提高性能
- 💾 流式处理，适合处理大文件
- 🔧 自动构建数据目录路径（基于源日期）
- 🔄 导入顺序：先导入 log，再导入 metric，最后导入 trace

## 前置要求

1. **Python 3.8+**
2. **已部署的 SigNoz 实例**（本地 Docker 部署）
3. **OpenRCA 数据集**（位于 `datasets/OpenRCA/Bank/telemetry/` 目录）
4. **SigNoz 配置文件**（位于 `config/signoz/` 目录，可直接使用）

## 安装

```bash
pip install -r requirements.txt
```

## SigNoz 配置

项目已提供配置好的 SigNoz 配置文件，位于 `config/signoz/` 目录：

- `metric-app.json` - 应用指标配置
- `metric-container.json` - 容器指标配置

**使用方法：**
1. 将配置文件导入到你的 SigNoz 实例
2. 或直接使用 SigNoz 的配置管理功能加载这些配置

## 快速开始

### 1. 确保 SigNoz 正在运行

确保你的 SigNoz 实例正在运行，并且 OTLP gRPC 端点可访问（默认是 `localhost:4317`）。

```bash
# 检查 SigNoz 是否在运行
docker ps | grep signoz

# 或者检查端口是否开放
nc -zv localhost 4317
```

### 2. 运行导入脚本

**基本用法（最简单）：**

```bash
python main.py \
  --source-date 2021-03-04 \
  --target-date 2024-12-19
```

脚本会自动根据 `--source-date` 构建数据目录路径：`datasets/OpenRCA/Bank/telemetry/2021_03_04`

**完整示例（自定义所有参数）：**

```bash
python main.py \
  --source-date 2021-03-04 \
  --target-date 2024-12-19 \
  --signoz-endpoint localhost:4317 \
  --data-dir datasets/OpenRCA/Bank/telemetry/2021_03_04 \
  --service-name openrca-bank \
  --batch-size 1000
```

### 3. 查看结果

1. 打开 SigNoz Web 界面（通常是 `http://localhost:3301`）
2. 导航到相应的页面：
   - **Metrics 页面**：查看导入的 metric 数据（应用指标和容器指标）
   - **Traces 页面**：查看导入的 trace 数据
   - **Logs 页面**：查看导入的 log 数据
3. 选择时间范围（使用目标日期附近的时间范围，例如：2024-12-19 前后）
4. 查看导入的数据

## 参数说明

### 必需参数

- `--source-date YYYY-MM-DD`: 源日期，datasets 中的历史日期（例如: `2021-03-04`）
  - 用于时间映射计算
  - 如果不提供 `--data-dir`，会根据此参数自动构建数据目录路径（格式：`datasets/OpenRCA/Bank/telemetry/YYYY_MM_DD`）

- `--target-date YYYY-MM-DD`: 目标日期，映射到的日期（必须在当前时间15天内，例如: `2024-12-19`）
  - 用于时间映射计算
  - 必须不晚于当前时间
  - 必须在当前时间15天内

### 可选参数

- `--data-dir`: 数据目录路径（可选）
  - 如果不提供，会根据 `--source-date` 自动构建路径
  - 如果提供，使用指定的路径（例如: `datasets/OpenRCA/Bank/telemetry/2021_03_04`）
  - 仅用于定位数据加载路径，不影响时间映射逻辑

- `--signoz-endpoint`: SigNoz OTLP gRPC 端点（默认: `localhost:4317`）

- `--service-name`: 服务名称（默认: `openrca-bank`）

- `--batch-size`: 批处理大小（默认: `1000`）

## Container Metric 说明

Container metric 数据包含大量不同的 KPI 指标。为了便于在 SigNoz 中管理和查询，工具会按大类自动分组：

- `container_mysql_metric` - MySQL 相关指标
- `container_oslinux_metric` - 操作系统 Linux 相关指标
- `container_redis_metric` - Redis 相关指标
- `container_container_metric` - Docker 容器相关指标
- `container_tomcat_metric` - Tomcat 相关指标
- `container_jvm_metric` - JVM 相关指标

每个 metric 使用 `kpi_name` 和 `cmdb_id` 标签来区分不同的指标和容器。

## 时间戳映射说明

脚本会将源日期的数据时间戳映射到目标日期，这样可以在 SigNoz 的最近15天内查看历史数据。

**工作原理：**
- **源日期**（`--source-date`）：指定 datasets 中的历史日期（例如: `2021-03-04`）
- **目标日期**（`--target-date`）：将历史数据映射到的日期（必须在当前时间15天内，例如: `2024-12-19`）
- **时间偏移**：脚本会自动计算两个日期之间的时间差，并将所有时间戳进行相应调整

**示例：**
```bash
# 将 2021-03-04 的数据映射到 2024-12-19
python main.py \
  --source-date 2021-03-04 \
  --target-date 2024-12-19
```

这样，2021-03-04 的数据在 SigNoz 中会显示为 2024-12-19，方便在最近15天内查看。

## 数据格式

脚本会读取以下格式的 CSV 文件：

### Metric 文件

**metric_app.csv**（应用指标）：
```csv
timestamp,rr,sr,cnt,mrt,tc
1614787440,100.0,100.0,22,53.27,ServiceTest1
```
- `timestamp`: 时间戳（秒）
- `rr`: 请求率 (Request Rate)
- `sr`: 成功率 (Success Rate)
- `cnt`: 计数 (Count)
- `mrt`: 平均响应时间 (Mean Response Time)
- `tc`: 服务名称 (Service Name)

**metric_container.csv**（容器指标）：
```csv
timestamp,cmdb_id,kpi_name,value
1614787200,Tomcat04,OSLinux-CPU_CPU_CPUCpuUtil,26.2957
```
- `timestamp`: 时间戳（秒）
- `cmdb_id`: 组件ID
- `kpi_name`: KPI指标名称
- `value`: 指标值

### Trace 文件

**trace_span.csv**（追踪数据）：
```csv
timestamp,cmdb_id,parent_id,span_id,trace_id,duration
1614787199628,dockerA2,369-bcou-dle-way1-c514cf30-43410@0824-2f0e47a816-17492,21030300016145905763,gw0120210304000517192504,19
```
- `timestamp`: 时间戳（毫秒）
- `cmdb_id`: 组件/服务标识
- `parent_id`: 父 span ID
- `span_id`: 当前 span ID
- `trace_id`: trace ID
- `duration`: 持续时间（毫秒）

### Log 文件

**log_service.csv**（日志数据）：
```csv
timestamp,cmdb_id,log_name,value,log_id
1614787200,Tomcat04,error_log,Error message,log_001
```
- `timestamp`: 时间戳（秒）
- `cmdb_id`: 组件标识
- `log_name`: 日志名称
- `value`: 日志内容
- `log_id`: 日志ID

## 使用示例

### 示例1：使用自动构建的数据目录（推荐）

```bash
# 脚本会自动构建路径：datasets/OpenRCA/Bank/telemetry/2021_03_04
python main.py \
  --source-date 2021-03-04 \
  --target-date 2024-12-19
```

### 示例2：指定自定义数据目录

```bash
# 使用自定义的数据目录路径
python main.py \
  --source-date 2021-03-04 \
  --target-date 2024-12-19 \
  --data-dir /path/to/custom/data/dir
```

### 示例3：自定义 SigNoz 端点

```bash
python main.py \
  --source-date 2021-03-04 \
  --target-date 2024-12-19 \
  --signoz-endpoint your-signoz-host:4317
```

### 示例4：调整批处理大小

```bash
# 对于大数据集，可以增加批处理大小以提高性能
python main.py \
  --source-date 2021-03-04 \
  --target-date 2024-12-19 \
  --batch-size 2000
```

## 常见问题

### Q: 数据没有显示在 SigNoz 中？

**A:** 
- 等待几分钟让数据索引完成
- 检查时间范围设置（使用目标日期附近的时间范围，时区选择 UTC）
- 查看 SigNoz 日志确认数据是否成功接收：
  ```bash
  docker logs <signoz-otel-collector-container> --tail 50
  ```
- 确保服务名称过滤器选择了正确的服务（或清除所有过滤器）
- 对于 metric 数据，检查 Metrics 页面；对于 trace 数据，检查 Traces 页面；对于 log 数据，检查 Logs 页面

### Q: 连接错误？

**A:**
- 确认 SigNoz 正在运行：`docker ps | grep signoz`
- 检查 OTLP 端点是否正确（默认: `localhost:4317`）
- 如果使用 Docker，确保端口映射正确
- 检查防火墙设置是否允许连接

### Q: 目标日期验证失败？

**A:**
- 目标日期必须在当前时间15天内
- 目标日期不能晚于当前时间
- 检查日期格式是否正确：`YYYY-MM-DD`

### Q: 数据目录不存在？

**A:**
- 如果不提供 `--data-dir`，脚本会根据 `--source-date` 自动构建路径
- 确保 `--source-date` 格式正确（例如: `2021-03-04` 对应文件夹 `2021_03_04`）
- 如果使用 `--data-dir`，确保指定的路径存在且包含以下文件：
  - `log/log_service.csv`（可选）
  - `metric/metric_app.csv`（可选）
  - `metric/metric_container.csv`（可选）
  - `trace/trace_span.csv`（可选）

### Q: 处理速度慢？

**A:**
- 数据集可能很大，这是正常的
- 可以调整 `--batch-size` 参数来优化性能（默认: 1000）
- 对于非常大的数据集，考虑分批处理不同日期的数据

## 注意事项

1. **导入顺序**: 脚本会先导入 log 数据，然后导入 metric 数据（metric_app.csv 和 metric_container.csv），最后导入 trace 数据（trace_span.csv）
2. **时间戳映射**: 脚本会将源日期的数据时间戳映射到目标日期，确保数据在 SigNoz 中显示为目标日期的时间
3. **15天限制**: 目标日期必须在当前时间15天内，这是为了符合 SigNoz 的数据保留策略
4. **自动路径构建**: 如果不提供 `--data-dir`，脚本会根据 `--source-date` 自动构建数据目录路径（格式：`datasets/OpenRCA/Bank/telemetry/YYYY_MM_DD`）
5. **流式处理**: 脚本采用流式处理方式，边读边发送，不会一次性加载所有数据到内存，适合处理大文件
6. **网络连接**: 确保能够访问 SigNoz 的 OTLP 端点
7. **处理时间**: 如果数据集很大，处理可能需要一些时间，请耐心等待
8. **Metric 时间戳**: Metric 和 Log 文件使用秒级时间戳，Trace 文件使用毫秒级时间戳，脚本会自动处理
9. **配置文件**: 可以直接使用 `config/signoz/` 目录中的配置文件，或根据需要进行调整

## 许可证

本项目使用与 OpenRCA 数据集相同的许可证。
