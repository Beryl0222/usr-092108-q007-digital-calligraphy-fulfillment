# 数字草书限量履约

限量数字作品发行的履约领域服务：保证各款式在任何时刻都**不超发一个编号、不让同一编号落到两位购买者手里**，并把支付回调、链上登记、实体仓配、物流签收等异步系统以幂等事件对齐。

## 领域口径

《草书诗帖》发行项目（见 `src/fulfillment/catalog.py`）：

- 四款付费数字作品，每款各 **1000** 份；
- 一款免费限领版本，**1000** 份；
- **1000** 套实体组合（仅随付费订单提供）。

## 不变量

- **序号守恒**：`available + owned + held + isolated = cap`，每款独立成立；实体池同理（另含退款后待退回桶）。所有占用都从最小可用号原子取得，并发下单由事件存储的单一事务边界串行化。
- **一人一名额**：同一用户在同一款式的并发请求只有一个能占用名额；免费、付费资格分别核验，免费领取资格一次性使用。
- **超时与迟到支付**：订单保留到期即释放数字号与未支付的实体预留号；已取消订单上迟到的成功支付被拒绝（`LATE_PAYMENT_REJECTED`），不会再次成交；登记接受回执在退款撤销后迟到同样不能复活登记。
- **幂等外部系统**：订单与支付、链端、物流、票据系统之间统一使用事件信封的 `causation_id`（外部请求标识）去重；同一请求并发或重放都返回首次结果，不产生第二次占用或第二次成交。
- **不可删除，只追加**：事件只能 append。登记更正通过 `REGISTRATION_CORRECTED` 追加（撤销、元数据勘误），原事件完整保留；存储没有删除/原地更新接口。
- **撤销与补发**：退款时未发出的实体件随撤销进入隔离号段（不再发给任何人）；在途件先计入"待退回"，退回后隔离；丢件补发开新物流批次但**沿用原收藏序号**。
- **悬空状态显式化**：数字权益仍有效而纸质件已退回时，购买者视图直接标出不一致并指向仲裁。

## 模块

| 路径 | 职责 |
| --- | --- |
| `src/fulfillment/envelope.py` | 事件信封：沿用 event_id/event_type/aggregate_type/aggregate_id/occurred_at/version/summary，扩展 payload、causation_id、correlation_id、seq |
| `src/fulfillment/event_store.py` | 仅追加存储：event_id 去重、聚合版本乐观屏障、causation 幂等索引、跨聚合原子提交、事务边界 |
| `src/fulfillment/model.py` | 事件流折叠出的快照（项目、款式、订单、序号台账、物流、登记、发票、仲裁） |
| `src/fulfillment/services.py` | 领域命令：项目/款式、资格、下单保留、支付回调、超时释放、登记、仓配、签收、发票、退款、仲裁 |
| `src/fulfillment/views.py` | 三类只读视图（见下） |
| `src/fulfillment/catalog.py` | 《草书诗帖》标准建档引导 |
| `contracts/domain.schema.json` | 事件契约，枚举与代码常量由测试强制同步 |

## 三类视图（最小知情）

- **购买者** `ReadModel.buyer_view(order_id)`：数字编号、登记状态、实体配号、运单与签收，附 `consistent / mismatches` 一致性核对。
- **客服** `ReadModel.support_view(order_id)`：按订单 `correlation_id` 汇总的时间线，每次占用/释放都带原因和外部请求标识，可解释超时释放、退款隔离、补发批次。
- **发行方** `ReadModel.issuer_report(project_id)`：只有分款计数与守恒等式（`held/owned/isolated/available`），**不含任何用户标识**，无法拼出用户画像。

## 本地检查

```bash
python3 -m unittest discover -s tests
```

21 个用例覆盖：建档口径、60 并发抢 10 个名额、同人并发只占一号、幂等并发、免费/付费资格分离、超时释放与迟到支付竞态、登记追加纠正、迟到登记回执、未发实体隔离、在途退款→退回隔离、丢件补发沿用原号、悬空状态、发票冲红、守恒等式、PII 不泄露。
