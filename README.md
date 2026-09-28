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

### 1.3 按标签分组的跨序列汇总 `GroupWindowAggregation`

看板常需要按某几个维度看“一组机器的整体水位”。分组查询
`queryGrouped(name, groupBy, from, to)` 在每个窗口内把多条序列合并成组：

| 字段 | 含义 |
| --- | --- |
| `groupLabels` | **只包含调用方指定的分组键**及其取值（按 key 排序） |
| `sampleCount` | 组内全部序列的样本总数 |
| `seriesCount` | 组内不同序列（完整标签集）条数 |
| `sum / min / max / average` | 组内样本的求和/最小/最大/平均 |
| `lastValue` | 组内**按事件时间**的最后一个样本值 |

分组语义与确定性保证：

- **只按声明的键分组**：未出现在 `groupBy` 中的标签（如 `service`、`pod`）一律忽略，
  不会把同一组拆成多组；`groupBy` 为空表示该指标下所有序列聚成唯一一个全局组。
- **缺失键取空串**：某序列缺少某个分组键时，该键取值按空串 `""` 参与分组（确定、可解释）。
- **结果可复现**：先按窗口起点、再按分组标签渲染（`k=v,...`）稳定排序；
  `lastValue` 在事件时间并列时取序列键更小者；相同输入多次查询的顺序与内容完全一致。
- **空分组不凭空出现**：只有真正有样本的（窗口 × 分组）才出现在结果中；
  查无数据的时间区间或不存在的指标返回空列表。

### 1.4 上报结果 `IngestResult`

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
分组告警状态条目 ≤ 分组规则数 × maxGroupCombinations（超保留期回收）
单次分组聚合规模 ≤ maxGroupCombinations × maxSeriesPerGroup 个序列
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

### 3.3 分组告警 `GroupAlertRule`（按标签对“组整体水位”告警）

```
ruleId / metricName / groupBy / aggregator / fireThreshold / recoverThreshold
```

- `groupBy`：参与分组的标签键（规范化为有序去重）；同一窗口内这些键取值相同的多条序列
  合并成一组，对**每个分组独立**跑一遍与 3.2 相同的滞回状态机；未参与分组的标签不拆组。
- `aggregator`：`AVG / SUM / MAX / MIN / COUNT / LAST`，其中 `COUNT` 用组内**样本总数**，
  其余取自分组聚合字段。
- 每个窗口的**全部序列**在封定时一次性交给分组判定器（批量封定回调，每个窗口恰好一次），
  因此同一分组同一窗口**只判定一次**，窗口滚动/并发 tick 不会重复告警。

关键语义：

1. **缺样本窗口不被当 0 值/正常值推进**：入参只含真正有样本的序列窗口，没有样本的分组
   根本不参与判定，因此 FIRING 中的分组不会因为某窗口没数据就被“自动恢复”。
2. **消失再出现（保留期内不算新触发）**：状态按「规则 × 分组」保留
   `groupAlertRetentionWindows` 个窗口；保留期内分组消失又回来沿用原状态——仍 FIRING 就
   不重复告警；连续缺席超过保留期才回收状态，再次出现按**新分组从 RESOLVED 开始**。
3. **状态不变不反复告警，恢复后再次越线重新触发**：与单序列滞回完全一致。
4. **分组切换互不干扰**：不同分组（如 `host=a`、`host=b`）各自独立维护状态。
5. 事件 `GroupAlertEvent` 带规则、**分组标签/分组键**、翻转后状态、窗口、观测值、
   **当时的完整分组聚合快照**与文字判定依据；`GroupAlertStatusView` 提供每组当前状态、
   最近判定窗口、最近观测值、上次翻转窗口。

### 3.4 分组容量边界与可区分反馈

分组能力不随运行时间或新标签组合无限增长，触发任一上界都会**明确报错/计数**，
绝不悄悄丢样本或把不同组合合并：

| 越界类型 | 反馈 |
| --- | --- |
| 不同分组键取值组合数 &gt; `maxGroupCombinations` | `TOO_MANY_GROUP_COMBINATIONS` |
| 单个分组内序列条数 &gt; `maxSeriesPerGroup` | `TOO_MANY_SERIES_IN_GROUP` |
| 一次查询返回行数（分组数 × 窗口数）&gt; `maxGroupedQueryRows` | `TOO_MANY_RESULT_ROWS` |
| 查询覆盖窗口数 &gt; `maxQueryWindows` | `TOO_MANY_QUERY_WINDOWS` |

- **分组查询**：越界抛出 `GroupLimitException`（携带 `reason/limit/actual`），
  HTTP 层返回 `400` 与对应字段，调用方可区分是哪一类超限。
- **分组告警**：某规则某窗口越界时**只跳过该规则在该窗口的评估**（不造数、不合并），
  并累加 `groupAlertRejectedCount()`、在 `groupAlertLastRejections()` 中记录
  `ruleId#windowStart -> 原因/上限/实际值`；其它规则照常判定。
- **状态条目上界**：分组状态数 ≤ 规则数 × `maxGroupCombinations`，且超保留期的分组被回收。
- **延迟稳定**：分组聚合在封定时一次性完成，输入规模受上述上界约束；
  查询为 O(命中窗口内该指标序列数)，与存活窗口数、序列数相关但均有上界，不随运行时间变慢。

---

## 4. HTTP 接口（本地可选）

| 方法 & 路径 | 作用 |
| --- | --- |
| `POST /metrics/samples` | 批量上报，返回每条样本的接受/拒绝结论 |
| `POST /metrics/query` | 按指标名+标签+时间范围查各窗口聚合（时间自动对齐窗口） |
| `POST /metrics/query/grouped` | 按 `groupBy` 标签键查跨序列分组聚合（`groupBy=[]` 为整指标一个组） |
| `POST /metrics/alerts/rules` | 注册单序列告警规则（重复 ruleId 返回 400） |
| `POST /metrics/alerts/group-rules` | 注册分组告警规则（按组独立滞回判定） |
| `GET  /metrics/alerts/status` | 当前全部规则×序列状态 |
| `GET  /metrics/alerts/group-status` | 当前全部规则×分组状态 |
| `POST /metrics/alerts/events/drain` | 取走并清空单序列状态翻转事件 |
| `POST /metrics/alerts/group-events/drain` | 取走并清分组状态翻转事件 |

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
| `metrics.max-group-combinations` | 500 | 单次分组查询/单条分组规则允许的不同分组组合数上界 |
| `metrics.max-series-per-group` | 1000 | 单个分组内允许的不同序列条数上界 |
| `metrics.max-grouped-query-rows` | 5000 | 一次分组查询返回行数（分组数 × 窗口数）上界 |
| `metrics.max-query-windows` | 100 | 一次分组查询覆盖窗口数上界 |
| `metrics.group-alert-retention-windows` | 3 | 分组告警状态在分组无样本后保留的窗口数 |

---

## 6. 验证方法

```bash
mvn test            # 全部 28 个测试，纯内存、无外部依赖，可重复运行
```

测试与对应需求：

| 测试类 | 覆盖场景 |
| --- | --- |
| `WindowAggregationTest` | 按事件时间/标签聚合；各类拒绝原因可区分；窗口滚动淘汰与内存上界 |
| `OutOfOrderSampleTest` | 乱序归入正确窗口；宽限内迟到计入一次；超宽限 TOO_OLD；`sampleId` 幂等；边界点唯一归窗；`lastValue` 按事件时间 |
| `AlertEvaluationTest` | 滞回去抖、死区不触发/不提前恢复、恢复后重新触发、每窗口恰好一次事件 |
| `GroupQueryTest` | 按部分标签分组合并（样本数/序列数/sum/min/max/avg/按事件时间最后值）；未分组标签不拆组；空 groupBy 全局组；缺失键取空串；结果稳定排序可复现；空分组不出现；四类容量越界可区分 |
| `GroupAlertEvaluationTest` | 组整体水位触发一次；缺样本窗口不被当 0 恢复；保留期内消失再出现沿用状态、超期回来按新组；恢复后重新触发；分组独立、每窗一次；容量越界只跳过该规则并计数 |
| `GroupConcurrencyTest` | 多线程分组并发上报不重复不漏、组内序列不被未分组标签拆开；上报与窗口滚动并发下每分组每窗口至多一次事件；同批输入重复运行结果一致 |
| `ConcurrentIngestTest` | 8/16 线程并发上报无丢失无重复、容量不超卖、上报与窗口滚动并发一致（确定性、不依赖调度）、相同 `sampleId` 并发重传恰好一次 |
| `EndToEndIntegrationTest` | 经 HTTP 的上报→查询（含分组查询）→规则（含分组规则）→状态全链路，以及 400 错误区分 |

### 兼容性说明

- 分组能力是**新增层**，原有单序列上报、查询、单序列告警规则与事件接口签名、行为均不变；
  `WindowAggregation` 仅新增了一个内部字段（分组合并用的 `lastTimestamp`），
  保留原有构造方式，序列化输出字段不变。
- 单序列告警与分组告警可同时注册、并行工作：前者消费“每序列每窗口一次”的回调，
  后者消费“每窗口一次的全量序列”批量回调，两者互不影响。

所有单测日志均打印**输入样本**（值/事件时间/标签/ID）与**判定依据**（归入窗口、
拒绝原因、聚合值、阈值比较），可用以下方式查看：

```bash
mvn test -Dtest=GroupAlertEvaluationTest
# 或查看报告中的 system-out
grep '\[分组告警\]' target/surefire-reports/TEST-*GroupAlertEvaluationTest.xml
```

并发测试断言每个样本“接受数 + 拒绝数 = 输入数”，分组并发测试额外断言
“各组样本数之和 = 接受数 = 逐序列计数之和”以及“事件去重(分组×窗口)数 = 事件数”，
因此失败时可区分是丢失、重复还是重复判定。
