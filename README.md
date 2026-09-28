# telemetry-metrics-java

本地可运行的**运行期指标采集与告警判定**服务：用内存中的固定滚动窗口（tumbling
window）聚合指标样本，并基于聚合结果做带滞回的告警判定。**不依赖任何外部服务、
消息中间件或真实凭据**，所有状态都在进程内存中，关停即清空。

- Java 21 + Spring Boot 3.4，仅使用仓库已有依赖（spring-web、spring-test）。
- 支持并发上报、窗口滚动、乱序/迟到样本，统计结果确定且可解释。
- 内存占用与查询延迟均有明确上界。

---

## 1. 指标模型

### 1.1 样本 `MetricSample`

| 字段 | 含义 |
| --- | --- |
| `name` | 指标名，非空 |
| `value` | 样本值，必须是有限数（NaN/Infinity 被拒绝） |
| `timestamp` | **事件时间**（epoch millis），窗口归属只看它，与到达时间无关 |
| `labels` | 标签键值对（不可变）；同一 `name` + 同一组标签构成一条时间序列 |
| `sampleId` | 幂等 ID，可空；非空时同一窗口内重复 ID 只计入一次（at-least-once 重传安全） |

序列身份由 `指标名 + 按 key 排序后的标签集` 决定（`SeriesKey`），因此标签
`Map` 的实现/迭代顺序不同不会产生重复序列。

### 1.2 窗口聚合 `WindowAggregation`

每条序列、每个窗口输出一份不可变快照：`count / sum / min / max / average /
lastValue`。其中 `lastValue` **按事件时间**定义（窗口内事件时间最大的样本），
乱序到达不会污染它。

### 1.3 上报结果 `IngestResult`

每次上报都有确定结论：

- 接受：`accepted=true`，并给出归入窗口的起点 `windowStartMs` 与说明；
- 拒绝：`accepted=false`，`rejectReason` 是**可区分的枚举**：

| 拒绝原因 | 触发条件 |
| --- | --- |
| `TOO_OLD` | 事件时间早于 `now - lateArrivalGraceMs`（迟到宽限已过） |
| `TOO_FAR_IN_FUTURE` | 事件时间晚于 `now + maxFutureSkewMs`（时钟异常/未来样本） |
| `WINDOW_CAPACITY_EXCEEDED` | 该序列该窗口样本数已达 `maxSamplesPerSeriesWindow` |
| `TOO_MANY_SERIES` | 全局序列数已达 `maxSeries` |
| `LABEL_CARDINALITY_EXCEEDED` | 某标签 key 的不同 value 数已达 `maxLabelValuesPerKey` |
| `DUPLICATE_SAMPLE` | 同 `sampleId` 已计入目标窗口（并发重传/跨边界重复） |
| `INVALID_ARGUMENT` | 指标名为空、值非有限数等 |

批量上报 `POST /metrics/samples` 逐条独立判定，结果数组与输入数组**位置一一对应**，
单条被拒不影响其它样本。

---

## 2. 窗口语义

### 2.1 固定窗口与边界

窗口长度 `windowSizeMs`，窗口边界按 epoch 对齐：

```
windowStart = floorDiv(timestamp, windowSizeMs) * windowSizeMs
区间为左闭右开 [windowStart, windowStart + windowSizeMs)
```

- 恰好落在边界点的样本属于**右侧**窗口；一条样本只可能归入一个窗口，
  **跨边界不会被重复计入**（`OutOfOrderSampleTest.samplesOnWindowBoundary...`）。
- 乱序样本按事件时间归入其本来的窗口，而不是到达时的“当前窗口”。

### 2.2 迟到、封定与淘汰

- **迟到宽限** `lateArrivalGraceMs`：窗口结束后的宽限期内，迟到样本仍可计入该窗口；
  超过宽限（`eventTs < now - grace`）的样本以 `TOO_OLD` 明确拒绝。
- **封定（seal）**：窗口满足 `windowStart + windowSize + grace <= now` 时封定。
  封定动作对每个「序列 × 窗口」**恰好执行一次**，此时把不可变聚合快照交给告警判定器，
  因此告警对每个窗口只评估一次，窗口滚动/并发 tick 不会重复判定。
  - 封定后、且窗口仍在保留范围内时，迟到样本仍可进入该窗口并体现在**查询**中；
    告警值取“封定时刻”的聚合（这是延迟与完整性之间的确定取舍，默认
    `grace=5s`，窗口 10s，迟到容忍充足）。
- **淘汰（evict）**：内存只保留最近 `retainedWindows` 个窗口，更早的窗口连同
  样本与幂等集合一起释放。

维护由两部分驱动：真实运行时 `MetricsMaintenanceLifecycle` 每 500ms 触发一次
（守护线程，随 Spring 生命周期启停）；每次 `ingest` 也会顺带维护一次。
测试中用可控时钟 `MutableClock` + 显式 `maintain()` 驱动，**完全确定、可复现**。

### 2.3 并发一致性

`MetricWindowStore` 用单一 `ReentrantLock` 串行化写入与维护，
「容量校验 → 序列/基数校验 → 幂等校验 → 计入样本 → 建窗口」是同一个原子临界区：

- 并发上报不会超卖容量、不会重复计数、不会丢样本；
- 上报与窗口封定/淘汰并发进行时，每个样本仍有**唯一确定去向**（计入唯一窗口或
  得到唯一拒绝原因）；
- 查询在锁内把目标窗口的不可变快照取出，锁外组装，查询只与命中窗口数有关。

### 2.4 内存与延迟上界

任一时刻存活窗口数有界，其余均由配置常量封顶：

```
窗口数 ≤ retainedWindows + ceil(maxFutureSkewMs / windowSizeMs)   （未来样本窗口）
序列数 ≤ maxSeries
单序列单窗口样本数 ≤ maxSamplesPerSeriesWindow
标签值集合 ≤ #labelKey × maxLabelValuesPerKey
幂等集合大小 ≤ 对应窗口样本数
告警状态条目 ≤ 规则数 × 序列数
分组状态条目 ≤ 分组规则数 × maxGroupCombinations（且受保留期淘汰约束）
单次分组查询：窗口数 ≤ maxQueryWindows、组合数 ≤ maxGroupCombinations、
组内序列数 ≤ maxSeriesPerGroup
告警事件队列 ≤ 10_000（超出丢弃最旧并计数，见 droppedEventCount）
```

总内存为 `O(maxSeries × 存活窗口数 × maxSamplesPerSeriesWindow)` 的确定上界；
上报为 O(log 窗口数) 的 TreeMap 操作 + O(1) 计数；查询为 O(命中窗口数)，
窗口数由 `retainedWindows` 封顶，**不会随运行时间无界增长**。

---

## 3. 告警语义

### 3.1 规则 `AlertRule`

```
ruleId / metricName / labelMatchers / aggregator / fireThreshold / recoverThreshold
```

- `labelMatchers`：精确相等匹配，空 map 表示该指标下所有序列；规则只需声明关心的标签，
  样本上的额外标签不影响匹配。
- `aggregator`：`AVG / SUM / MAX / MIN / COUNT / LAST`，决定用窗口聚合的哪个字段判定。
- 要求 `recoverThreshold <= fireThreshold`（构造期校验）。

### 3.2 滞回状态机（状态跨窗口保持稳定）

每个「规则 × 序列」维护一个状态，初始 `RESOLVED`：

```
RESOLVED -- observed >= fireThreshold    --> FIRING   （产生一次 FIRING 事件）
FIRING   -- observed <= recoverThreshold --> RESOLVED （产生一次 RESOLVED 事件）
其余情况（含 recover < observed < fire 死区、状态不变）--> 不产生任何事件
```

由此保证：

1. **阈值附近抖动不重复告警**：进入 FIRING 后，值在死区内（如 fire=80/recover=50
   时的 85、60）不会反复触发，也不会提前恢复；
2. **状态未变化不反复触发**：连续多个窗口越线，只有第一次产生 FIRING 事件；
3. **恢复后再次越线必须重新触发**：回到 RESOLVED 后再次 `>= fireThreshold`
   会产生新的 FIRING 事件；
4. 事件 `AlertEvent` 带有规则、序列、翻转后状态、窗口、观测值与**文字判定依据**；
   `AlertStatusView` 提供当前状态、最近观测值、最近判定窗口、上次翻转窗口，
   结论可解释、可复现。

---

## 3A. 按标签分组的跨序列汇总与分组告警

在单序列能力之上新增一层“按部分标签分组”：调用方只指定关心的几个标签键，
同一窗口内这些键取值相同的多条序列合并成一组，对“这一组的整体水位”查询与告警，
而不是对每台机器单独告警。

### 3A.1 分组查询 `queryGroups` / `POST /metrics/query-groups`

入参 `name + groupByKeys + fromEpochMs + toEpochMs`，输出
`GroupWindowAggregation` 列表，每行是“一个分组在一个窗口”的跨序列聚合：

| 字段 | 含义 |
| --- | --- |
| `groupLabels` | **仅含分组键**投影出的标签子集（按 key 排序）；未参与分组的标签不会出现 |
| `sampleCount` | 组内样本总数（跨序列求和） |
| `seriesCount` | 贡献样本的序列数 |
| `sum / min / max / average` | 跨序列合并的和、最小、最大、平均值 |
| `lastValue` | 组内**按事件时间**的最后值（事件时间相同按序列键字典序取确定值） |

分组语义要点：

- **只按调用方指定的键拆组**：`groupByKeys=["host"]` 时，同 host 的
  `service/instance` 等额外标签差异不会把一组拆成多组；`groupByKeys=[]`
  表示把该指标全部序列合并成一个无分组标签的**总组**。
- 分组键在某序列上缺失时按空串 `""` 取值，“无此标签”自成一组，不与任何实际值合并。
- **只输出有样本的（分组，窗口）**，空分组不会凭空出现；空结果返回空列表。
- **顺序确定可复现**：结果先按窗口起点升序，同一窗口内按分组标签
  `(key,value)` 字典序升序；序列侧用按 `SeriesKey` 排序的 TreeMap 遍历，
  因此相同输入（无论标签 Map 实现、传入键顺序、到达顺序如何）多次查询的
  顺序与内容完全一致。

### 3A.2 分组告警规则

`AlertRule` 新增可选字段 `groupByKeys`（空列表 = 原有单序列模式，完全兼容）。
非空时规则按这些键把封定窗口内、通过 `labelMatchers` 的序列分组，对每个分组的
聚合结果（`AVG/SUM/MAX/MIN/COUNT/LAST`，其中 `COUNT` 为组内样本总数）独立做滞回判定。

- **每个（规则 × 分组 × 窗口）只判定一次**：存储层在窗口越过“结束 + 迟到宽限”
  封定线时，把该（指标，窗口）的**全部**序列快照整体、恰好一次交给分组判定器；
  此后迟到样本会被 `TOO_OLD` 拒绝，不存在“先判一半再补齐”。
- **没有样本的窗口不会被当成 0 值或正常值推进状态**：分组在某窗口缺席时，
  该分组的滞回状态原样保留，不触发恢复。
- **消失再出现**：明确保留期（`group-alert-retention-windows`）内分组消失又回来，
  沿用原状态——状态没变不重复告警，回到 `RESOLVED` 后再次越线才重新触发；
  超过保留期仍未出现的分组状态条目被静默淘汰（不补发合成 `RESOLVED`），
  之后回来按全新组处理（可再次触发），从而保证状态表不随标签组合无限增长。
- **可解释**：分组事件 `AlertEvent` 带 `grouped=true`、`groupLabels`、
  `seriesCount`、`sampleCount` 与文字依据（分组、样本/序列数、观测值、阈值比较）；
  `groupAlertStatuses()` 提供每个分组的当前状态。

### 3A.3 容量边界与可区分反馈

分组能力不静默丢样本、不把不同组合合并，任何超限都返回**可区分状态**：

| 反馈（`GroupQueryStatus`） | 触发条件 | HTTP |
| --- | --- | --- |
| `OK` | 完整返回 | 200 |
| `TOO_MANY_WINDOWS` | 请求窗口数超 `max-query-windows` | 422 |
| `TOO_MANY_GROUP_COMBINATIONS` | 某窗口分组取值组合数超 `max-group-combinations` | 422 |
| `TOO_MANY_SERIES_IN_GROUP` | 某分组合并的序列数超 `max-series-per-group` | 422 |

分组告警路径超组合/组内序列上限时，该（规则，窗口）判定被跳过并计数，
通过 `AlertEvaluator.groupEvaluationSkipCount()` 与 `lastGroupSkipDetail()` 可观测。
分组状态条目数 ≤ `规则数 × 有界组合数`，且受保留期淘汰约束，
**不会随运行时间或新标签组合不断增长**；分组查询只遍历有界的存活窗口，
锁内取快照、锁外合并，查询延迟稳定不越跑越慢。

### 3A.4 并发一致性

分组遍历与聚合全部在存储的同一把 `ReentrantLock` 下取不可变快照后于锁外完成：
并发上报、查询、窗口滚动/封定同时发生时，分组 `sampleCount` 与接受样本
精确守恒（不重复计数、不漏样本），封定回调每窗口每组恰好一次。

---

## 4. HTTP 接口（本地可选）

| 方法 & 路径 | 作用 |
| --- | --- |
| `POST /metrics/samples` | 批量上报，返回每条样本的接受/拒绝结论 |
| `POST /metrics/query` | 按指标名+标签+时间范围查各窗口聚合（时间自动对齐窗口） |
| `POST /metrics/query-groups` | 按部分标签分组查跨序列聚合；超限返回 422 + 可区分状态 |
| `POST /metrics/alerts/rules` | 注册告警规则（重复 ruleId 返回 400；`groupByKeys` 非空即分组规则） |
| `GET  /metrics/alerts/status` | 当前全部规则×序列（及规则×分组）状态 |
| `POST /metrics/alerts/events/drain` | 取走并清空状态翻转事件 |

启动：

```bash
mvn spring-boot:run
# 上报
curl -s -X POST localhost:8080/metrics/samples -H 'Content-Type: application/json' \
  -d '[{"name":"cpu","value":95.0,"timestamp":'$(date +%s%3N)',"labels":{"host":"a"},"sampleId":"s1"}]'
```

也可以不经过 HTTP，直接在代码里用 `MetricsService`（核心单测均如此）。

---

## 5. 配置项（`application.properties`）

| 配置 | 默认 | 含义 |
| --- | --- | --- |
| `metrics.window-size-ms` | 10000 | 窗口长度 |
| `metrics.retained-windows` | 3 | 保留窗口数（内存上界） |
| `metrics.max-samples-per-series-window` | 10000 | 单序列单窗口样本容量 |
| `metrics.max-series` | 10000 | 全局最大序列数 |
| `metrics.max-label-values-per-key` | 2000 | 单标签 key 的值基数上限 |
| `metrics.max-future-skew-ms` | 60000 | 允许的未来时间偏移 |
| `metrics.late-arrival-grace-ms` | 5000 | 迟到宽限 / 告警封定延迟 |
| `metrics.max-group-combinations` | 1000 | 分组查询/封定允许的标签取值组合数上限 |
| `metrics.max-series-per-group` | 5000 | 单分组允许合并的序列数上限 |
| `metrics.max-query-windows` | 1000 | 一次查询允许的窗口数上限 |
| `metrics.group-alert-retention-windows` | 6 | 分组告警状态消失后保留窗口数，超期淘汰 |

---

## 6. 验证方法

```bash
mvn test            # 全部 26 个测试，纯内存、无外部依赖，可重复运行
```

测试与对应需求：

| 测试类 | 覆盖场景 |
| --- | --- |
| `WindowAggregationTest` | 按事件时间/标签聚合；各类拒绝原因可区分；窗口滚动淘汰与内存上界 |
| `OutOfOrderSampleTest` | 乱序归入正确窗口；宽限内迟到计入一次；超宽限 TOO_OLD；`sampleId` 幂等；边界点唯一归窗；`lastValue` 按事件时间 |
| `AlertEvaluationTest` | 滞回去抖、死区不触发/不提前恢复、恢复后重新触发、每窗口恰好一次事件 |
| `GroupAggregationQueryTest` | 按部分标签跨序列合并；全字段计算（含按事件时间 `lastValue`）；额外标签不拆组、缺键自成一组、无空分组；重复查询顺序内容一致；三类容量上界可区分反馈 |
| `GroupAlertEvaluationTest` | 分组独立滞回与事件依据；缺窗口不被当 0/正常值、保留期内回来不重触发；恢复后重新触发；超保留期按新组处理且不补发 RESOLVED；同组同窗口只判定一次 |
| `GroupedConcurrencyTest` | 并发上报+滚动下分组计数守恒、同组多序列不重复告警、重复查询结果一致与查询有界、超限可区分反馈 |
| `ConcurrentIngestTest` | 多线程并发上报无丢失无重复、容量不超卖、确定性两阶段的上报与窗口滚动并发一致、相同 `sampleId` 并发重传恰好一次 |
| `EndToEndIntegrationTest` | 经 HTTP 的上报→查询→分组查询→规则→状态全链路，400/422 错误区分 |

所有单测日志均打印**输入样本**（值/事件时间/标签/ID）与**判定依据**（归入窗口、
拒绝原因、聚合值、阈值比较），可用以下方式查看：

```bash
mvn test -Dtest=AlertEvaluationTest
# 或查看报告中的 system-out
grep '\[告警\]' target/surefire-reports/TEST-*AlertEvaluationTest.xml
```

并发测试断言每个样本“接受数 + 拒绝数 = 输入数”，且仍保留窗口内
“接受数 = 各窗口 count 之和”，因此失败时可区分是丢失、重复还是错误拒绝。
