# 数字草书限量履约

《草书诗帖》限量发行的履约领域服务。覆盖出版项目、款式上限、序号池、用户资格、
下单保留、支付、链上登记、实体仓配、签收、发票、退款与仲裁的全过程。

## 要证明的事

- 四款各 1000 份付费数字作品、免费限领版、1000 套实体组合：任何时刻**不多发一个编号**，
  同一编号**不会落到两位购买者**手里；
- 支付回调、链上登记、实体配号、物流异步推进，取消 / 补寄 / 超时释放次序混乱时，
  不出现「数字仍有效、纸质已退回」的悬空结果；
- 免费领取与付费购买**分别核验资格**；并发请求**只能占一个名额**；
  超时释放后到达的**迟到支付不能再次成交**；
- 不可删除的登记以**追加状态纠正**留痕；未发实体随撤销**隔离**；
  丢件补发**沿用原收藏序号**。

## 设计纪律

| 纪律 | 落地方式 |
| --- | --- |
| 仅追加 | `EventStore` 无删除/改写接口；登记纠正、发票红冲都产生后继事件 |
| 幂等 | 每个外部命令/回调带 `request_id`（信封的 `causation_id`），重放返回首次结果 |
| 原子占号 | 全部决策在事件存储的全局提交锁内重放最新状态后一次提交；流内版本连续校验 |
| 序号唯一事实源 | 占号/释放/售出只围绕「款式 + 序号」，售出集合天然保证一号一人 |
| 最小知情 | 客服台账中的用户标识脱敏；发行方守恒报告只有计数，无用户画像 |

## 模块

- `src/events.py`：事件信封、聚合类型与事件类型（`SERIAL_HELD`、`PAYMENT_CONFIRMED`、
  `REGISTRATION_ACCEPTED`、`PHYSICAL_DISPATCHED`、`ORDER_REMEDIED` 等）。
- `src/store.py`：仅追加、按 `event_id` 与 `causation_id` 幂等、乐观并发、跨流原子提交。
- `src/domain.py`：各聚合状态与事件重放（项目/款式/资格/订单/登记账/仓配/发票/仲裁）。
- `src/service.py`：应用服务与状态机（`FulfilmentService`）。
- `src/projections.py`：三类读模型：
  - 购买者：数字编号、实体配号、物流是否一致（`OrderView.consistent`）；
  - 客服：每个序号的每次占用/释放/处置台账（`serial_history` / `order_history`）；
  - 发行方：各款式守恒报告（`conservation_report`，校验 `held + sold <= 发行量` 等）。
- `src/app.py`：装配存储、服务与同步投影。
- `src/validator.py`：事件信封基础字段与枚举校验。
- `contracts/domain.schema.json`：事件契约（枚举与各字段语义）。
- `tests/test_fulfilment.py`：覆盖并发占号、超时与迟到支付竞争、登记追加纠正、
  未发实体隔离、丢件/退回补发同号、回调幂等、守恒报告等。

## 典型流程

```python
from src.app import build_application
from src import domain as d

app = build_application()
svc = app.service

svc.create_project("P1", "草书诗帖", "req-project")
svc.open_variant("CAOSHU-1", "P1", "草书诗帖 第一款", d.KIND_PAID, 1000,
                 request_id="req-v1")
svc.place_order("O-1", "CAOSHU-1", "u-1", d.KIND_PAID, price=5000,
                request_id="req-order-1")
svc.confirm_payment("O-1", "PAY-20260921-1", "req-pay-callback-1")  # 回调可安全重放
svc.request_registration("O-1", "req-reg-1")
svc.registration_outcome("O-1", True, "chain://0x...", "", "req-reg-out-1")

print(app.reads.purchaser_view("O-1").consistent)
for vid, r in app.reads.conservation_report().items():
    assert r.conserved
```

免费领取在授予 `free` 资格后即时成交（占号即售出，无待支付窗口）；
实体组合付款后配号（配号必须等于收藏序号）、发货、签收；判丢或仲裁退回结论
都以 `PHYSICAL_RESENT` 沿用原序号补发。

## 本地检查

```bash
python3 -m unittest discover -s tests
```
