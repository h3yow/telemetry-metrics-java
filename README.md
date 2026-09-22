# telemetry-metrics-java（bootstrap-a）

本地可运行的运行期指标采集与告警判定服务。全部状态保存在进程内存中，使用滚动窗口
（tumbling window）按事件时间聚合指标样本，并基于窗口聚合结果做带滞回的告警判定。
**不依赖任何外部服务、数据库或真实凭据。**

## 1. 快速开始

```bash
mvn spring-boot:run          # 启动服务（默认端口 8080）
mvn test                     # 运行全部测试（17 个，确定性、可复现）
```

## 2. 指标模型

| 概念 | 说明 |
| --- | --- |
| `MetricSample` | 一条样本：`name`（指标名）+ `tags`（标签键值）+ `value` + `timestampMs`（**事件时间**）+ `sampleId`（幂等键） |
| `SeriesKey` | 指标名 + 标签排序归一化后形成的时间序列标识；标签上报顺序不同也归入同一序列 |
| `WindowStats` | 窗口聚合：`count / sum / min / max / avg / lastValue`，以及窗口起止时间 |

样本上报结果 `IngestStatus` 是**可区分**的：

- `ACCEPTED`：已计入对应窗口；
- `DUPLICATE`：`sampleId` 已接收过，幂等跳过，**不重复计数**（HTTP 200）；
- `REJECTED_TOO_OLD`：事件时间落入已关闭/已淘汰窗口（HTTP 422，响应 `reason` 给出水位与窗口范围）；
- `REJECTED_TOO_FUTURE`：事件时间超过 `now + telemetry.max-future-skew-ms`（默认 60s，HTTP 422）；
- `REJECTED_WINDOW_FULL`：目标窗口样本数达到容量上界（HTTP 429）；
- `REJECTED_TOO_MANY_SERIES`：序列数达到基数上界（HTTP 422）。

## 3. 窗口语义

- **滚动窗口**：`windowStart = floor(timestampMs / windowSizeMs) * windowSizeMs`，
  窗口区间为 `[start, start + size)`，归属严格按**事件时间**，与到达顺序无关。
- **水位（watermark）**：`watermark = now - allowedLatenessMs`。
  每次 tick 推进水位时，`endMs <= watermark` 的窗口被一次性关闭（seal）。
  关闭前允许迟到样本进入，关闭后迟到样本明确返回 `REJECTED_TOO_OLD`。
- **乱序/迟到**：只要窗口未关闭，乱序样本按事件时间进入正确窗口；
  跨越窗口边界的样本（如 1999ms 与 2000ms）分别进入相邻窗口，边界不会造成重复计入。
- **去重**：单窗口内 `sampleId` 原子去重（`去重判定 + 容量判定 + 累加` 在同一把锁内完成），
  并发上报与重试不会重复计数，也不会丢样本。
- **窗口不可变**：关闭时固化聚合快照并释放去重集合；再次 tick 不会重复关闭，
  下游告警对每个窗口恰好判定一次。

### 并发正确性

- 每序列一把锁串行化窗口写入；窗口内“去重/容量/累加”为原子操作。
- 一把全局**滚动锁**串行化 `ingest` 的窗口创建决策与 `advanceWatermark` 的
  seal/淘汰/水位发布，消除两类竞态：
  1. “窗口已 seal 但水位尚未发布”时创建出本该关闭的窗口；
  2. 空序列在滚动淘汰后，并发写入落到游离对象导致样本丢失。
  已存在窗口的纯写入不经过滚动锁，上报吞吐不会被滚动串行化。

### 内存与查询上界

- 每条序列至多保留 `retained-windows-per-series`（默认 6）个窗口，超出在关闭时淘汰最旧窗口；
- 序列数受 `max-series`（默认 10000）约束，空序列在滚动后移除；
- 窗口样本数受 `window-capacity`（默认 10000）约束；
- 因此内存占用上界为
  `O(maxSeries × retainedWindowsPerSeries × windowCapacity)`；
- 窗口按起点有序存储，查询为 O(log 窗口数) 且窗口数有界；
- 告警状态条数 ≤ 规则数（≤ 1000）× 序列数；告警事件历史为有界环形缓冲（默认 1000 条）。

## 4. 告警语义

规则 `AlertRule`：匹配指标名 + 必须包含的标签键值，对聚合量
`AVG/SUM/MIN/MAX/COUNT` 做 `GT/GTE/LT/LTE` 阈值比较。

状态按 `(ruleId, series)` 独立维护，三态状态机带滞回：

```
(无记录 = OK) --越线--> FIRING      （产生 1 个 FIRING 事件）
FIRING        --持续越线--> FIRING   （状态不变，不重复告警）
FIRING        --恢复--> RECOVERED   （产生 1 个 RECOVERED 事件）
RECOVERED     --持续恢复--> 回到 OK  （不产生事件，状态条目被清理）
RECOVERED     --再次越线--> FIRING   （必须重新产生 FIRING 事件）
```

因此：阈值附近抖动不会重复告警；状态未变化不会反复触发；恢复后再次越线必然重新触发。
每个事件/状态都带可解释的判定依据字符串，例如：
`windowStart=10000 avg=12.0 GT threshold=10.0 => BREACHED`。
同一窗口或更旧窗口的重复判定不会翻转已有结论。

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/telemetry/samples` | 上报样本（`timestampMs` 缺省取服务端时间，`sampleId` 缺省自动生成） |
| POST | `/telemetry/tick?nowMs=` | 显式推进窗口；不传 `nowMs` 用墙钟。返回新水位与本次产生的告警事件 |
| GET  | `/telemetry/series/{name}/stats/latest?k=v` | 查询最新窗口聚合 |
| GET  | `/telemetry/alerts/status` | 查询全部规则×序列的当前状态与判定依据 |
| GET  | `/telemetry/alerts/events?limit=50` | 查询最近的状态迁移事件（新事件在前） |
| POST/GET/DELETE | `/telemetry/rules`、`/telemetry/rules/{ruleId}` | 规则管理 |

示例：

```bash
curl -s localhost:8080/telemetry/rules -H 'Content-Type: application/json' -d '{
  "ruleId":"hot-cpu","metricName":"cpu","requiredTags":{"host":"web1"},
  "function":"avg","op":"gt","threshold":10}'

curl -s localhost:8080/telemetry/samples -H 'Content-Type: application/json' -d '{
  "name":"cpu","tags":{"host":"web1"},"value":42,"timestampMs":1700000000100,"sampleId":"s1"}'

curl -s -XPOST 'localhost:8080/telemetry/tick?nowMs=1700000001000'
curl -s 'localhost:8080/telemetry/series/cpu/stats/latest?host=web1'
curl -s 'localhost:8080/telemetry/alerts/status'
```

生产模式默认每 `telemetry.tick-interval-ms`（1s）自动滚动一次；
测试/回放场景可设置 `telemetry.auto-tick-enabled=false`，改用显式 tick 时间驱动，
保证结果不依赖真实墙钟。

## 6. 配置项（application.properties 前缀 telemetry）

| 属性 | 默认值 | 含义 |
| --- | --- | --- |
| `window-size-ms` | 10000 | 窗口大小 |
| `window-capacity` | 10000 | 单窗口样本数上界 |
| `retained-windows-per-series` | 6 | 每序列保留窗口数 |
| `max-series` | 10000 | 序列基数上界 |
| `max-future-skew-ms` | 60000 | 未来事件时间容忍上界 |
| `allowed-lateness-ms` | 1000 | 水位延迟（乱序容忍） |
| `event-history-size` | 1000 | 告警事件历史条数 |
| `tick-interval-ms` | 1000 | 自动滚动周期 |
| `auto-tick-enabled` | true | 是否启用后台自动滚动 |

## 7. 测试与验证方法

`mvn test`，共 17 个用例。所有用例使用可手动拨动的时钟与**显式事件时间**，
不依赖 sleep 或墙钟，统计结果可复现；测试日志会打印每条输入样本、
状态（`ACCEPTED/DUPLICATE/REJECTED_*`）、窗口归属与告警判定依据。

| 测试类 | 覆盖场景 |
| --- | --- |
| `TumblingWindowTest` | 基础聚合、幂等去重、容量拒绝不占槽位、关闭后拒绝写入 |
| `WindowStoreTest` | 16 线程并发上报不重不丢；乱序/迟到样本按事件时间归窗、跨边界不重复；滚动后迟到样本拒绝原因可区分；未来/容量/序列基数三类拒绝；有界淘汰与查询 |
| `AlertEngineTest` | 持续越线只告警一次、恢复事件、恢复后再越线重新告警、阈值抖动不重复、过期/重复窗口不翻转结论、标签匹配与删规则清状态 |
| `TelemetryServiceTest` | tick 驱动“滚动→判定→事件历史”；事件历史有界；**并发上报与滚动交织**时 accepted 数与窗口 count 恒等、已关闭窗口统计不可变 |
| `TelemetryHttpE2ETest` | 全链路 HTTP：建规则、上报/重放/容量拒绝/迟到拒绝、滚动触发 FIRING、状态与事件查询、规则删除 |

## 8. 代码结构

```
model/    MetricSample、SeriesKey、WindowStats、IngestResult/Status
window/   TumblingWindow（单窗口聚合）、WindowStore（窗口生命周期/水位/淘汰）
alert/    AlertRule、AlertState、AlertEvent、AlertEngine（滞回状态机）
service/  TelemetryService（门面）、WindowTicker（后台滚动）
config/   TelemetryProperties、TelemetryConfig（装配）
web/      TelemetryController + dto（本地 HTTP 接口）
```
