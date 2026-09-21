"""限量发行履约服务的端到端行为测试。"""

from __future__ import annotations

import threading
import unittest
from datetime import timedelta

from src.app import build_application
from src import domain as d
from src import events as ev
from src.errors import (
    AlreadyOccupiesQuota, IllegalTransition, LatePaymentRejected,
    NotEligible, SoldOut,
)
from src.events import utc_now
from src.service import FulfilmentService


class Clock:
    """可控时钟。"""

    def __init__(self) -> None:
        self.t = utc_now()

    def __call__(self):
        return self.t

    def advance(self, minutes: int) -> None:
        self.t += timedelta(minutes=minutes)


class StoryTest(unittest.TestCase):
    """《草书诗帖》四款各 1000 + 免费版 + 1000 实体套的发行故事。"""

    def setUp(self) -> None:
        self.clock = Clock()
        self.app = build_application(clock=self.clock)
        self.svc: FulfilmentService = self.app.service
        self.reads = self.app.reads
        self.svc.create_project("P1", "草书诗帖", "req-project")
        self.variants = []
        for i in range(1, 5):
            vid = f"CAOSHU-{i}"
            self.svc.open_variant(vid, "P1", f"草书诗帖 第{i}款", d.KIND_PAID,
                                  1000, request_id=f"req-variant-{i}")
            self.variants.append(vid)
        self.svc.open_variant("CAOSHU-FREE", "P1", "草书诗帖 免费限领",
                              d.KIND_FREE, 2000, requires_eligibility=True,
                              request_id="req-variant-free")
        self.svc.open_variant("CAOSHU-BOX", "P1", "草书诗帖 实体组合",
                              d.KIND_BUNDLE, 1000, request_id="req-variant-box")

    # --------------------------------------------------------------
    # 资格：免费与付费分别核验
    # --------------------------------------------------------------

    def test_free_claim_requires_free_eligibility(self) -> None:
        with self.assertRaises(NotEligible):
            self.svc.place_order("O-free-1", "CAOSHU-FREE", "u-free",
                                 d.KIND_FREE, request_id="r1")
        self.svc.grant_eligibility("CAOSHU-FREE", "u-free", d.KIND_FREE,
                                   "campaign", request_id="g1")
        events = self.svc.place_order("O-free-1", "CAOSHU-FREE", "u-free",
                                      d.KIND_FREE, request_id="r1")
        types = [e.event_type for e in events]
        # 免费领取：占号 + 即时成交
        self.assertEqual(types.count(ev.SERIAL_HELD), 1)
        self.assertEqual(types.count(ev.PAYMENT_CONFIRMED), 2)
        view = self.reads.purchaser_view("O-free-1")
        self.assertTrue(view.paid)
        self.assertEqual(view.serial, 1)

    def test_paid_variant_eligibility_gate(self) -> None:
        # 默认付费款式不设门槛；开启门槛的款式必须先授予购买资格
        self.svc.open_variant("VIP", "P1", "贵宾款", d.KIND_PAID, 10,
                              requires_eligibility=True, request_id="vip-open")
        with self.assertRaises(NotEligible):
            self.svc.place_order("O-vip", "VIP", "u-vip", d.KIND_PAID,
                                 price=9900, request_id="p-vip")
        self.svc.grant_eligibility("VIP", "u-vip", d.KIND_PAID, "whitelist",
                                   request_id="g-vip")
        self.svc.place_order("O-vip", "VIP", "u-vip", d.KIND_PAID,
                             price=9900, request_id="p-vip")
        self.assertEqual(self.reads.purchaser_view("O-vip").serial, 1)

    # --------------------------------------------------------------
    # 序号池：并发只能占一个名额
    # --------------------------------------------------------------

    def test_concurrent_holds_never_oversell(self) -> None:
        self.svc.open_variant("SMALL", "P1", "小发行量", d.KIND_PAID,
                              10, request_id="small-open")
        errors: list[Exception] = []
        start = threading.Event()

        def claim(idx: int) -> None:
            start.wait()
            try:
                self.svc.place_order(f"O-c{idx}", "SMALL", f"u-c{idx}",
                                     d.KIND_PAID, price=100,
                                     request_id=f"req-c{idx}")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=claim, args=(i,)) for i in range(50)]
        for t in threads:
            t.start()
        start.set()  # 同时放行，竞争全局提交锁
        for t in threads:
            t.join()

        variant = self.svc._state(ev.AGG_VARIANT, "SMALL")  # noqa: SLF001
        sold_or_held = list(variant.serial_status.keys())
        self.assertEqual(len(sold_or_held), 10)          # 恰好 10 个名额
        self.assertEqual(len(set(sold_or_held)), 10)     # 无重复编号
        self.assertEqual(len(errors), 40)
        self.assertIsInstance(errors[0], SoldOut)

        # 守恒：每个编号只对应一个订单
        order_serials = list(variant.order_serial.values())
        self.assertEqual(len(order_serials), len(set(order_serials)))

    def test_same_user_concurrent_requests_gets_one_serial(self) -> None:
        outcomes: list[str] = []

        def claim(tag: str) -> None:
            try:
                self.svc.place_order(f"O-dup-{tag}", "CAOSHU-1", "u-dup",
                                     d.KIND_PAID, price=100,
                                     request_id=f"req-dup-{tag}")
                outcomes.append("ok")
            except AlreadyOccupiesQuota:
                outcomes.append("rejected")

        threads = [threading.Thread(target=claim, args=(str(i),)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(outcomes), ["ok", "rejected"])

    # --------------------------------------------------------------
    # 保留、支付、超时释放、迟到支付
    # --------------------------------------------------------------

    def test_hold_timeout_then_late_payment_cannot_close(self) -> None:
        self.svc.place_order("O-late", "CAOSHU-1", "u-late", d.KIND_PAID,
                             price=5000, hold_minutes=15, request_id="place-late")
        self.clock.advance(16)
        released = self.svc.expire_holds()
        self.assertTrue(any(e.event_type == ev.HOLD_RELEASED for e in released))

        # 名额立刻可被新买家占用，且新买家与旧序号不冲突
        self.svc.place_order("O-new", "CAOSHU-1", "u-new", d.KIND_PAID,
                             price=5000, request_id="place-new")
        self.assertEqual(self.reads.purchaser_view("O-new").serial, 1)

        # 迟到支付：登记拒绝事实，不能再次成交，也不会把序号从新买家手里夺走
        with self.assertRaises(LatePaymentRejected):
            self.svc.confirm_payment("O-late", "PAY-LATE-1", "pay-late")
        order = self.svc._state(ev.AGG_ORDER, "O-late")  # noqa: SLF001
        self.assertEqual(order.status, d.ORDER_CANCELLED)
        self.assertIn("PAY-LATE-1", order.rejected_payments)
        new_view = self.reads.purchaser_view("O-new")
        self.assertEqual(new_view.serial, 1)

        # 迟到支付回调重放幂等：只留一条拒绝记录，重放返回该记录不重复成交
        before = len(self.app.store.stream("O-late"))
        replay = self.svc.confirm_payment("O-late", "PAY-LATE-1", "pay-late")
        self.assertEqual(len(self.app.store.stream("O-late")), before)
        self.assertEqual(replay[0].event_type, ev.PAYMENT_LATE_REJECTED)

    def test_payment_and_timeout_race_only_one_outcome(self) -> None:
        self.svc.place_order("O-race", "CAOSHU-2", "u-race", d.KIND_PAID,
                             price=5000, request_id="place-race")
        self.clock.advance(16)
        # 支付回调先到
        self.svc.confirm_payment("O-race", "PAY-RACE", "pay-race")
        # 超时扫描随后执行：已支付订单不受影响
        self.assertEqual(self.svc.expire_holds(), [])
        self.assertEqual(
            self.svc._state(ev.AGG_ORDER, "O-race").status,  # noqa: SLF001
            d.ORDER_PAID)

    def test_payment_callback_is_idempotent(self) -> None:
        self.svc.place_order("O-idem", "CAOSHU-1", "u-idem", d.KIND_PAID,
                             price=100, request_id="place-idem")
        first = self.svc.confirm_payment("O-idem", "PAY-1", "pay-idem")
        second = self.svc.confirm_payment("O-idem", "PAY-1", "pay-idem")
        self.assertEqual([e.event_id for e in second],
                         [e.event_id for e in first[:1]])

    def test_cancelled_hold_releases_serial_for_resale(self) -> None:
        self.svc.place_order("O-cancel", "CAOSHU-3", "u-a", d.KIND_PAID,
                             price=100, request_id="place-cancel")
        self.assertEqual(self.reads.purchaser_view("O-cancel").serial, 1)
        self.svc.cancel_order("O-cancel", "cancel-1")
        self.svc.place_order("O-again", "CAOSHU-3", "u-b", d.KIND_PAID,
                             price=100, request_id="place-again")
        self.assertEqual(self.reads.purchaser_view("O-again").serial, 1)
        history = self.reads.serial_history("CAOSHU-3", 1)
        self.assertEqual([h.action for h in history],
                         ["held", "released", "held"])

    # --------------------------------------------------------------
    # 链上登记：追加式，不可删除
    # --------------------------------------------------------------

    def _paid_order(self, oid: str, vid: str, user: str, price: int = 100):
        self.svc.place_order(oid, vid, user, d.KIND_PAID, price=price,
                             request_id=f"place-{oid}")
        self.svc.confirm_payment(oid, f"PAY-{oid}", f"pay-{oid}")

    def test_registration_is_append_only_and_corrected(self) -> None:
        self._paid_order("O-reg", "CAOSHU-1", "u-reg")
        self.svc.request_registration("O-reg", "reg-req-1")
        self.svc.registration_outcome("O-reg", True, "chain://0xabc", "",
                                      "reg-out-1")
        ledger_id = self.svc._ledger_id("CAOSHU-1")  # noqa: SLF001
        ledger = self.svc._state(ev.AGG_REGISTRY, ledger_id)  # noqa: SLF001
        original_ref = ledger.records[1].chain_ref

        # 争议后纠正：只能追加，原引用保留
        self.svc.correct_registration("CAOSHU-1", 1, "O-reg", "revoked",
                                      "仲裁撤销登记", "corr-1")
        ledger2 = self.svc._state(ev.AGG_REGISTRY, ledger_id)  # noqa: SLF001
        self.assertEqual(ledger2.records[1].chain_ref, original_ref)
        self.assertEqual(ledger2.records[1].status, "revoked")
        types = [e.event_type for e in self.app.store.stream(ledger_id)]
        self.assertEqual(types, [
            ev.REGISTRATION_REQUESTED, ev.REGISTRATION_ACCEPTED,
            ev.REGISTRATION_CORRECTED])

        # 外部回调重放幂等
        n = len(self.app.store.stream(ledger_id))
        self.svc.registration_outcome("O-reg", True, "chain://0xabc", "",
                                      "reg-out-1")
        self.assertEqual(len(self.app.store.stream(ledger_id)), n)

    # --------------------------------------------------------------
    # 实体仓配：配号一致、未发隔离、丢件补发沿用原号
    # --------------------------------------------------------------

    def test_bundle_fulfilment_consistency_and_lost_reship(self) -> None:
        self._paid_order("O-box", "CAOSHU-BOX", "u-box", price=20000)
        self.svc.allocate_physical("O-box", "BOX-UNIT-0001", "alloc-1")
        self.svc.dispatch_physical("O-box", "SF1", "顺丰", "dispatch-1")

        view = self.reads.purchaser_view("O-box")
        self.assertEqual(view.physical_serial, view.serial)
        self.assertTrue(view.consistent)
        self.assertEqual(len(view.shipments), 1)

        self.svc.report_lost("O-box", "派送遗失", "lost-1")
        self.svc.reship_lost("O-box", "SF2", "顺丰", "reship-1")
        self.svc.deliver_physical("O-box", "deliver-1")

        view = self.reads.purchaser_view("O-box")
        self.assertTrue(view.consistent)
        self.assertEqual(view.physical_serial, view.serial)  # 补发沿用原号
        self.assertEqual([s.kind for s in view.shipments],
                         ["original", "reshipment"])
        self.assertEqual(view.physical_status, d.PHY_DELIVERED)

        f = self.svc._state(  # noqa: SLF001
            ev.AGG_FULFILLMENT, self.svc._fulfilment_id("O-box"))
        self.assertEqual({s.tracking_no for s in f.shipments}, {"SF1", "SF2"})

        # 未丢件不能补发
        with self.assertRaises(IllegalTransition):
            self.svc.reship_lost("O-box", "SF3", "顺丰", "reship-2")

    def test_unshipped_physical_quarantined_on_refund(self) -> None:
        self._paid_order("O-q", "CAOSHU-BOX", "u-q", price=20000)
        self.svc.allocate_physical("O-q", "BOX-UNIT-0002", "alloc-q")
        # 已配货但未发出 → 退款时实体隔离，数字登记追加纠正
        self.svc.request_registration("O-q", "reg-q")
        self.svc.registration_outcome("O-q", True, "chain://0xq", "", "out-q")
        self.svc.grant_refund("O-q", "买家投诉", "refund-q",
                              revoke_digital=True)
        f = self.svc._state(  # noqa: SLF001
            ev.AGG_FULFILLMENT, self.svc._fulfilment_id("O-q"))
        self.assertEqual(f.status, d.PHY_QUARANTINED)
        self.assertIn("BOX-UNIT-0002", f.quarantine_reason + f.physical_unit)

        view = self.reads.purchaser_view("O-q")
        # 已隔离未发货，订单已退，一致性视图仍然成立（无悬空实体）
        self.assertTrue(view.consistent)

        # 隔离件不允许再直接发出
        with self.assertRaises(IllegalTransition):
            self.svc.dispatch_physical("O-q", "SFX", "顺丰", "dispatch-x")

    def test_allocated_physical_requires_payment(self) -> None:
        self.svc.place_order("O-unpaid", "CAOSHU-BOX", "u-unpaid",
                             d.KIND_PAID, price=20000, request_id="place-unpaid")
        with self.assertRaises(IllegalTransition):
            self.svc.allocate_physical("O-unpaid", "U1", "alloc-unpaid")

    # --------------------------------------------------------------
    # 发票与仲裁
    # --------------------------------------------------------------

    def test_invoice_lifecycle_with_credit_on_refund(self) -> None:
        self._paid_order("O-inv", "CAOSHU-1", "u-inv", price=8800)
        self.svc.request_invoice("INV-1", "O-inv", "某文化公司", "910000X",
                                 "inv-req")
        self.svc.issue_invoice("INV-1", "inv-issue")
        self.svc.void_invoice("INV-1", "inv-void")
        self.svc.grant_refund("O-inv", "退货", "refund-inv")
        self.svc.reissue_invoice("CR-1", "INV-1", "credit-1")
        inv = self.svc._state(ev.AGG_INVOICE, "CR-1")  # noqa: SLF001
        self.assertEqual(inv.related_invoice_id, "INV-1")
        self.assertEqual(inv.status, d.INVOICE_CREDIT)

    def test_arbitration_explains_dangling_state(self) -> None:
        # 数字有效但纸质退回的悬空状态：进入仲裁，结论为按原序号重发并闭环
        self._paid_order("O-arb", "CAOSHU-BOX", "u-arb", price=20000)
        self.svc.allocate_physical("O-arb", "BOX-UNIT-0003", "alloc-arb")
        self.svc.dispatch_physical("O-arb", "SF9", "顺丰", "dispatch-arb")
        self.svc.report_returned("O-arb", "收件人拒收退回", "return-arb")
        self.svc.open_arbitration("CASE-1", "O-arb", "纸质退回但数字仍有效",
                                  "case-open")
        self.svc.resolve_arbitration(
            "CASE-1", "reissue_physical", "数字编号保留，按原序号重新发出实体",
            "case-resolve")
        # 仲裁结论落地：沿用原收藏序号重发，新运单，最终签收
        self.svc.reship_returned("O-arb", "SF10", "顺丰", "reship-arb")
        self.svc.deliver_physical("O-arb", "deliver-arb")

        case = self.svc._state(ev.AGG_CASE, "CASE-1")  # noqa: SLF001
        self.assertEqual(case.status, d.ARBITRATION_RESOLVED)
        view = self.reads.purchaser_view("O-arb")
        self.assertTrue(view.consistent)
        self.assertEqual(view.physical_serial, view.serial)   # 编号从未改变
        self.assertEqual(view.physical_status, d.PHY_DELIVERED)
        self.assertEqual([s.tracking_no for s in view.shipments], ["SF9", "SF10"])
        # 客服能按订单解释每一步占用与处置，悬空状态有据可查
        actions = [h.action for h in self.reads.order_history("O-arb")]
        self.assertEqual(actions, [
            "held", "sold", "physical_allocated", "dispatched",
            "reshipped", "delivered",
        ])

        # 没有仲裁结论（未判丢/未退回）不能擅自重发
        with self.assertRaises(IllegalTransition):
            self.svc.reship_returned("O-arb", "SF11", "顺丰", "reship-again")

    # --------------------------------------------------------------
    # 三类读者视图
    # --------------------------------------------------------------

    def test_purchaser_sees_consistent_numbers(self) -> None:
        self._paid_order("O-view", "CAOSHU-2", "u-view", price=6000)
        view = self.reads.purchaser_view("O-view")
        self.assertEqual(view.serial, 1)
        self.assertTrue(view.consistent)
        # 只返回该用户自己的订单，读模型不为个人暴露他人视图
        self.assertEqual(view.user_id, "u-view")

    def test_customer_service_can_explain_every_hold_and_release(self) -> None:
        self.svc.place_order("O-cs1", "CAOSHU-4", "u-cs1", d.KIND_PAID,
                             price=100, request_id="place-cs1")
        self.clock.advance(16)
        self.svc.expire_holds()
        self.svc.place_order("O-cs2", "CAOSHU-4", "u-cs2", d.KIND_PAID,
                             price=100, request_id="place-cs2")
        self.svc.confirm_payment("O-cs2", "PAY-CS2", "pay-cs2")
        history = self.reads.serial_history("CAOSHU-4", 1)
        actions = [(h.action, h.order_id) for h in history]
        self.assertEqual(actions, [
            ("held", "O-cs1"),
            ("released", "O-cs1"),
            ("held", "O-cs2"),
            ("sold", "O-cs2"),
        ])
        # 台账里用户身份脱敏，不形成完整用户画像
        self.assertTrue(all(
            "***" in (h.detail.get("user_masked", "") or "***")
            or "user_masked" not in h.detail for h in history))

    def test_conservation_report_for_whole_edition(self) -> None:
        # 混合若干成交：付费、免费、实体套、一次超时释放、一次丢件补发
        for i in range(20):
            oid = f"O-p{i}"
            self.svc.place_order(oid, "CAOSHU-1", f"u-p{i}", d.KIND_PAID,
                                 price=100, request_id=f"rp{i}")
            self.svc.confirm_payment(oid, f"PAY-{i}", f"rpay{i}")
        self.svc.place_order("O-hold", "CAOSHU-1", "u-hold", d.KIND_PAID,
                             price=100, request_id="rhold")
        self.clock.advance(16)
        self.svc.expire_holds()
        self.svc.place_order("O-hold2", "CAOSHU-1", "u-hold2", d.KIND_PAID,
                             price=100, request_id="rhold2")
        self.svc.confirm_payment("O-hold2", "PAY-HOLD2", "pay-hold2")

        for i in range(5):
            self.svc.grant_eligibility("CAOSHU-FREE", f"u-f{i}", d.KIND_FREE,
                                       "campaign", request_id=f"gf{i}")
            self.svc.place_order(f"O-f{i}", "CAOSHU-FREE", f"u-f{i}",
                                 d.KIND_FREE, request_id=f"rf{i}")

        self._paid_order("O-boxr", "CAOSHU-BOX", "u-boxr", price=20000)
        self.svc.allocate_physical("O-boxr", "UNIT-9", "ar")
        self.svc.dispatch_physical("O-boxr", "T1", "顺丰", "dr")
        self.svc.report_lost("O-boxr", "丢", "lr")
        self.svc.reship_lost("O-boxr", "T2", "顺丰", "rr")

        reports = self.reads.conservation_report()
        r1 = reports["CAOSHU-1"]
        self.assertTrue(r1.conserved)
        self.assertEqual(r1.sold, 21)          # 20 + 超时后重新成交 1
        self.assertEqual(r1.held, 0)
        self.assertLessEqual(r1.accounted, r1.edition_size)
        box = reports["CAOSHU-BOX"]
        self.assertEqual(box.reshipments, 1)
        self.assertEqual(box.physical_allocated, 1)
        self.assertTrue(box.conserved)
        free = reports["CAOSHU-FREE"]
        self.assertEqual(free.sold, 5)
        self.assertTrue(free.conserved)
        # 报告不含任何用户字段
        for r in reports.values():
            self.assertFalse(any("user" in k for k in vars(r)))

    # --------------------------------------------------------------
    # 事件信封与存储纪律
    # --------------------------------------------------------------

    def test_events_are_append_only_and_continuously_versioned(self) -> None:
        self._paid_order("O-ver", "CAOSHU-1", "u-ver")
        for sid in ("CAOSHU-1", "O-ver"):
            versions = [e.version for e in self.app.store.stream(sid)]
            self.assertEqual(versions, list(range(1, len(versions) + 1)))

    def test_command_replay_returns_same_event(self) -> None:
        first = self.svc.open_variant("DUP-V", "P1", "重复建档", d.KIND_PAID,
                                      5, request_id="dup-open")
        stream_len = len(self.app.store.stream("DUP-V"))
        second = self.svc.open_variant("DUP-V", "P1", "重复建档", d.KIND_PAID,
                                       5, request_id="dup-open")
        # 重放不产生新事件，返回首次提交的主事件
        self.assertEqual(len(self.app.store.stream("DUP-V")), stream_len)
        self.assertEqual(second[0].event_id, first[0].event_id)


if __name__ == "__main__":
    unittest.main()
