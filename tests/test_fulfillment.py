"""限量发行履约的功能、并发与边界测试。"""

from __future__ import annotations

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from src.fulfillment import (
    Conflict,
    EligibilityRefused,
    EventStore,
    FulfillmentService,
    SoldOut,
    model,
)
from src.fulfillment.catalog import (
    FREE_VARIANT,
    PAID_VARIANTS,
    bootstrap_edition,
)
from src.fulfillment.services import LATE_PAYMENT_REJECTED
from src.fulfillment.views import ReadModel


class Clock:
    """可拨动的时钟，模拟超时与迟到回调。"""

    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 9, 21, 10, 0, tzinfo=timezone(timedelta(hours=8)))

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


def new_service(ttl: int = 900) -> tuple[FulfillmentService, ReadModel, Clock]:
    clock = Clock()
    service = FulfillmentService(EventStore(), hold_ttl_seconds=ttl, clock=clock)
    return service, ReadModel(service.store), clock


def pay(service: FulfillmentService, order_id: str, amount: int, req: str,
        *, pid: str | None = None, succeeded: bool = True) -> dict:
    return service.payment_callback(
        order_id, pid or f"PAY-{order_id}", amount,
        succeeded=succeeded, request_id=req,
    )


def buy(service: FulfillmentService, project: str, variant: str, user: str,
        req: str, *, amount: int, with_physical: bool = False) -> str:
    """付费下单并支付成交，返回订单号。"""
    result = service.place_order(
        project, variant, user, request_id=req, with_physical=with_physical
    )
    order_id = result["order_id"]
    pay(service, order_id, amount, f"cb-{req}")
    return order_id


class BootstrapTest(unittest.TestCase):
    def test_edition_has_four_paid_plus_free_and_thousand_physical(self) -> None:
        service, views, _ = new_service()
        bootstrap_edition(service)
        report = views.issuer_report("CAOSHU_SHITIE")
        self.assertEqual(len(report["digital_variants"]), 5)
        paid = [v for v in report["digital_variants"] if v["kind"] == "digital_paid"]
        free = [v for v in report["digital_variants"] if v["kind"] == "digital_free"]
        self.assertEqual(len(paid), 4)
        self.assertTrue(all(v["cap"] == 1000 for v in paid))
        self.assertEqual(len(free), 1)
        self.assertEqual(report["physical_bundles"]["cap"], 1000)
        self.assertTrue(report["conservation_ok"])


class ConcurrencyTest(unittest.TestCase):
    def test_concurrent_paid_orders_never_oversell_or_share_serial(self) -> None:
        service, views, _ = new_service()
        service.publish_project("P1", "并发测试项目", physical_cap=0)
        service.configure_variant("P1", "V1", "并发款", kind="digital_paid", cap=10, price=100)

        order_ids: list[str] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def attempt(i: int) -> None:
            try:
                result = service.place_order("P1", "V1", f"user-{i}", request_id=f"req-{i}")
                with lock:
                    order_ids.append(result["order_id"])
            except SoldOut:
                pass
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(attempt, range(60)))

        self.assertEqual(errors, [])
        self.assertEqual(len(order_ids), 10)  # 60 个并发请求，只有 10 个名额
        serials = []
        for oid in order_ids:
            order = service._load_order(oid)
            serials.append(order.serial_no)
            pay(service, oid, 100, f"cb-{oid}")
        self.assertEqual(sorted(serials), [f"{i:04d}" for i in range(1, 11)])
        self.assertEqual(len(set(serials)), 10)
        report = views.issuer_report("P1")
        self.assertTrue(report["conservation_ok"], report["discrepancies"])

    def test_same_user_concurrent_requests_hold_only_one_slot(self) -> None:
        service, _, _ = new_service()
        service.publish_project("P2", "同人一款项目", physical_cap=0)
        service.configure_variant("P2", "V2", "同人一款", kind="digital_paid", cap=100, price=50)

        outcomes: list[str] = []
        barrier = threading.Barrier(8)

        def attempt(i: int) -> None:
            barrier.wait()
            try:
                service.place_order("P2", "V2", "same-user", request_id=f"req-{i}")
                outcomes.append("won")
            except Conflict:
                outcomes.append("rejected")

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(attempt, range(8)))
        self.assertEqual(outcomes.count("won"), 1)
        self.assertEqual(outcomes.count("rejected"), 7)

    def test_same_request_id_concurrent_is_idempotent(self) -> None:
        service, _, _ = new_service()
        service.publish_project("P3", "幂等项目", physical_cap=0)
        service.configure_variant("P3", "V3", "幂等款", kind="digital_paid", cap=100, price=50)

        results: list[dict] = []
        barrier = threading.Barrier(8)

        def attempt() -> None:
            barrier.wait()
            results.append(service.place_order("P3", "V3", "user-x", request_id="idem-001"))

        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in range(8):
                pool.submit(attempt)
        order_ids = {r["order_id"] for r in results}
        self.assertEqual(len(order_ids), 1)
        self.assertEqual(sum(1 for r in results if r["idempotent"]), 7)


class EligibilityTest(unittest.TestCase):
    def test_free_and_paid_eligibility_checked_separately(self) -> None:
        service, _, _ = new_service()
        bootstrap_edition(service)
        free_id = FREE_VARIANT[0]
        paid_id = PAID_VARIANTS[0][0]

        # 免费领取没有资格 → 拒绝
        with self.assertRaises(EligibilityRefused):
            service.place_order("CAOSHU_SHITIE", free_id, "u-nogrant", request_id="r1")
        # 付费通道不能买免费款（走免费核验，缺资格）
        with self.assertRaises(EligibilityRefused):
            service.place_order("CAOSHU_SHITIE", free_id, "u-nogrant", request_id="r2")

        grant = service.grant_entitlement("u1", "CAOSHU_SHITIE", "airdrop-1")
        # 免费领取不能挂实体组合
        with self.assertRaises(EligibilityRefused):
            service.place_order("CAOSHU_SHITIE", free_id, "u1", request_id="r0",
                                grant_id=grant, with_physical=True)
        first = service.place_order("CAOSHU_SHITIE", free_id, "u1",
                                    request_id="r3", grant_id=grant)
        # 资格一次性：同人同资格第二次领取拒绝
        with self.assertRaises(EligibilityRefused):
            service.place_order("CAOSHU_SHITIE", free_id, "u1",
                                request_id="r4", grant_id=grant)
        # 免费单下单即成交，无需支付
        order = service._load_order(first["order_id"])
        self.assertEqual(order.status, model.PAID)

        # 免费款式不能走付费通道：付费下单免费款同样在资格处被拒（缺 grant）
        with self.assertRaises(EligibilityRefused):
            service.place_order("CAOSHU_SHITIE", free_id, "u2", request_id="r5")

    def test_paid_with_physical_sold_out(self) -> None:
        service, _, _ = new_service()
        service.publish_project("P4", "实体约满项目", physical_cap=1)
        service.configure_variant("P4", "V4", "实体款", kind="digital_paid", cap=100, price=10)
        service.place_order("P4", "V4", "u-a", request_id="ra", with_physical=True)
        with self.assertRaises(EligibilityRefused):
            service.place_order("P4", "V4", "u-b", request_id="rb", with_physical=True)


class TimeoutAndLatePaymentTest(unittest.TestCase):
    def test_timeout_releases_serial_and_late_payment_cannot_settle(self) -> None:
        service, views, clock = new_service(ttl=900)
        bootstrap_edition(service)
        variant = PAID_VARIANTS[0][0]

        result = service.place_order("CAOSHU_SHITIE", variant, "slow-user", request_id="slow-1")
        order_id = result["order_id"]
        self.assertEqual(result["serial_no"], "0001")

        # 超过保留窗口仍未支付 → 超时释放
        clock.advance(minutes=20)
        released = service.expire_holds()
        self.assertEqual(released, [order_id])
        self.assertEqual(service._load_order(order_id).status, model.CANCELLED)

        # 迟到的成功支付：绝不能再次成交
        with self.assertRaises(Conflict) as cm:
            pay(service, order_id, 19900, "late-cb")
        self.assertEqual(cm.exception.code, LATE_PAYMENT_REJECTED)
        # 支付回执没有落库，订单仍是取消态
        self.assertEqual(service._load_order(order_id).status, model.CANCELLED)

        # 释放出的序号可被后来者领走
        second = service.place_order("CAOSHU_SHITIE", variant, "fast-user", request_id="fast-1")
        self.assertEqual(second["serial_no"], "0001")
        pay(service, second["order_id"], 19900, "fast-cb")
        self.assertEqual(service._load_order(second["order_id"]).status, model.PAID)
        self.assertTrue(views.issuer_report("CAOSHU_SHITIE")["conservation_ok"])

    def test_settled_order_is_not_touched_by_expiry(self) -> None:
        service, _, clock = new_service(ttl=900)
        bootstrap_edition(service)
        variant = PAID_VARIANTS[1][0]
        order_id = buy(service, "CAOSHU_SHITIE", variant, "u1", "o1", amount=19900)
        clock.advance(minutes=30)
        self.assertEqual(service.expire_holds(), [])
        self.assertEqual(service._load_order(order_id).status, model.PAID)

    def test_payment_callback_idempotent_and_duplicate_serial_rejected(self) -> None:
        service, _, _ = new_service()
        bootstrap_edition(service)
        variant = PAID_VARIANTS[2][0]
        result = service.place_order("CAOSHU_SHITIE", variant, "u1", request_id="po")
        order_id = result["order_id"]
        first = pay(service, order_id, 19900, "cb-x")
        self.assertEqual(first["result"], "settled")
        # 同一外部请求重放：返回首次结果，不重复成交
        replay = pay(service, order_id, 19900, "cb-x")
        self.assertTrue(replay["idempotent"])
        # 同一渠道流水号换 request_id 再来：拒绝
        with self.assertRaises(Conflict):
            pay(service, order_id, 19900, "cb-other", pid="PAY-" + order_id)
        # 金额不符拒绝
        with self.assertRaises(Exception):
            service.payment_callback(order_id, "PAY-NEW", 1, succeeded=True, request_id="cb-bad")


class RegistrationTest(unittest.TestCase):
    def test_registration_is_append_only_and_correctable(self) -> None:
        service, _, _ = new_service()
        bootstrap_edition(service)
        variant = PAID_VARIANTS[0][0]
        order_id = buy(service, "CAOSHU_SHITIE", variant, "u1", "o1", amount=19900)
        reg_id = f"REG-{order_id}"

        service.request_registration(order_id, request_id="regreq-1")
        service.accept_registration(order_id, "TOKEN-abc", request_id="regacc-1")
        # 回调重复送达幂等
        service.accept_registration(order_id, "TOKEN-abc", request_id="regacc-1")

        history = service.store.get("registration", reg_id)
        types = [e.event_type for e in history]
        self.assertEqual(types.count("REGISTRATION_ACCEPTED"), 1)

        # 追加纠正：原事件保留，有效内容被后继记录更新
        service.correct_registration(order_id, {"metadata_uri": "ipfs://new"},
                                     reason="元数据勘误", request_id="corr-1")
        service.correct_registration(order_id, {"metadata_uri": "ipfs://newer"},
                                     reason="再次勘误", request_id="corr-2")
        reg = model.fold_registration(service.store.events_for("registration", reg_id))
        self.assertEqual(reg.effective["metadata_uri"], "ipfs://newer")
        self.assertEqual(len(reg.corrections), 2)
        # 不可删除：事件数只增不减
        self.assertGreaterEqual(len(service.store.events_for("registration", reg_id)), 4)
        # 没有任何删除/更新接口可供调用
        self.assertFalse(hasattr(service.store, "delete"))

    def test_late_registration_acceptance_after_refund_is_rejected(self) -> None:
        service, _, _ = new_service()
        bootstrap_edition(service)
        variant = PAID_VARIANTS[0][0]
        order_id = buy(service, "CAOSHU_SHITIE", variant, "u1", "o1", amount=19900)
        # 已请求登记但链上接受回执迟到；期间订单退款撤销
        service.request_registration(order_id, request_id="regreq-1")
        service.refund_order(order_id, external_payment_id=f"PAY-{order_id}",
                             reason="fraud_check", request_id="ref1")
        reg = model.fold_registration(service.store.get("registration", f"REG-{order_id}"))
        self.assertEqual(reg.state, model.REG_REVOKED)
        # 迟到的接受回调不能让撤销的登记复活
        with self.assertRaises(Conflict):
            service.accept_registration(order_id, "TOKEN-late", request_id="regacc-late")


class PhysicalLifecycleTest(unittest.TestCase):
    def _settled_physical_order(self, service: FulfillmentService, user: str, req: str) -> str:
        result = service.place_order("CAOSHU_SHITIE", PAID_VARIANTS[0][0], user,
                                     request_id=req, with_physical=True)
        order_id = result["order_id"]
        pay(service, order_id, 19900, f"cb-{req}")
        return order_id

    def test_lost_replacement_keeps_original_collectible_no(self) -> None:
        service, views, _ = new_service()
        bootstrap_edition(service)
        order_id = self._settled_physical_order(service, "u1", "o1")
        order = service._load_order(order_id)
        collectible_no = order.collectible_no

        service.pack_shipment(order_id)
        service.dispatch_shipment(order_id, carrier="SF", waybill_no="SF1", request_id="d1")
        service.report_lost(order_id, proof="carrier-confirmed", request_id="lost1")
        attempt = service.replace_lost(order_id, request_id="rep1")
        self.assertEqual(attempt, 2)
        shipment = model.fold_shipment(service.store.get("physical_shipment", f"SHP-{order_id}"))
        self.assertEqual(shipment.collectible_no, collectible_no)  # 沿用原收藏序号
        service.dispatch_shipment(order_id, carrier="SF", waybill_no="SF2", request_id="d2")
        service.deliver_shipment(order_id, request_id="recv1")

        buyer = views.buyer_view(order_id)
        self.assertTrue(buyer["consistent"], buyer["mismatches"])
        self.assertEqual(buyer["physical"]["attempts"], 2)
        self.assertEqual(buyer["physical"]["state"], "delivered")
        # 补发幂等：同一请求重放不新增批次
        again = service.replace_lost(order_id, request_id="rep1")
        self.assertEqual(again, 2)

    def test_unshipped_physical_isolated_when_order_revoked(self) -> None:
        service, views, _ = new_service()
        bootstrap_edition(service)
        order_id = self._settled_physical_order(service, "u1", "o1")
        collectible_no = service._load_order(order_id).collectible_no
        # 尚未打包即退款撤销
        service.refund_order(order_id, external_payment_id=f"PAY-{order_id}",
                             reason="buyer_cancelled", request_id="ref1")

        project = service._load_project("CAOSHU_SHITIE")
        pool = model.fold_physical_pool(
            service.store.events_for("serial_pool", project.physical_pool_id),
            project.physical_pool_id, project.physical_cap,
        )
        self.assertEqual(pool.status_of(collectible_no), model.ISOLATED)
        self.assertIsNone(service._shipment_state(order_id))
        report = views.issuer_report("CAOSHU_SHITIE")
        self.assertTrue(report["conservation_ok"])

    def test_dangling_state_digital_valid_but_paper_returned(self) -> None:
        service, views, _ = new_service()
        bootstrap_edition(service)
        order_id = self._settled_physical_order(service, "u1", "o1")
        service.pack_shipment(order_id)
        service.dispatch_shipment(order_id, carrier="SF", waybill_no="SF9", request_id="d9")
        # 纸质件退回，但数字权益仍有效 → 悬空状态必须被显式标出
        service.return_shipment(order_id, reason="address_unreachable", request_id="ret1")

        buyer = views.buyer_view(order_id)
        self.assertFalse(buyer["consistent"])
        self.assertTrue(any("悬空" in m for m in buyer["mismatches"]))
        # 客服可以据此开仲裁
        case_id = service.open_arbitration(order_id, reason="纸质退回数字仍有效")
        service.apply_remedy(case_id, action="resend", note="核实地址后补寄", request_id="rm1")
        service.resolve_arbitration(case_id, ruling="补寄并由发行方承担运费", request_id="res1")

    def test_return_after_refund_isolates_collectible_no(self) -> None:
        service, views, _ = new_service()
        bootstrap_edition(service)
        order_id = self._settled_physical_order(service, "u1", "o1")
        no = service._load_order(order_id).collectible_no
        service.pack_shipment(order_id)
        service.dispatch_shipment(order_id, carrier="SF", waybill_no="SF8", request_id="d8")
        # 先退款撤销（件在途）：编号保留、计入待退回桶，守恒仍成立
        service.refund_order(order_id, external_payment_id=f"PAY-{order_id}",
                             reason="dispute", request_id="ref8")
        mid_report = views.issuer_report("CAOSHU_SHITIE")
        self.assertTrue(mid_report["conservation_ok"], mid_report["discrepancies"])
        self.assertEqual(mid_report["physical_bundles"]["awaiting_return_after_refund"], 1)
        # 纸质件退回库房后转入隔离
        service.return_shipment(order_id, reason="refund_return", request_id="ret8")
        project = service._load_project("CAOSHU_SHITIE")
        pool = service._load_physical_pool(project)
        self.assertEqual(pool.status_of(no), model.ISOLATED)
        final = views.issuer_report("CAOSHU_SHITIE")
        self.assertTrue(final["conservation_ok"])
        self.assertEqual(final["physical_bundles"]["awaiting_return_after_refund"], 0)
        self.assertEqual(final["physical_bundles"]["isolated"], 1)


class InvoiceTest(unittest.TestCase):
    def test_invoice_issued_and_voided_with_refund(self) -> None:
        service, _, _ = new_service()
        bootstrap_edition(service)
        order_id = buy(service, "CAOSHU_SHITIE", PAID_VARIANTS[3][0], "u1", "o1", amount=19900)
        invoice_id = service.issue_invoice(order_id, request_id="inv1")
        self.assertTrue(service.issue_invoice(order_id, request_id="inv1") == invoice_id)
        invoice = model.fold_invoice(service.store.get("invoice", invoice_id))
        self.assertFalse(invoice.voided)
        service.refund_order(order_id, external_payment_id=f"PAY-{order_id}",
                             reason="requested", request_id="ref1")
        invoice = model.fold_invoice(service.store.events_for("invoice", invoice_id))
        self.assertTrue(invoice.voided)


class ViewTest(unittest.TestCase):
    def test_support_view_explains_every_hold_and_release(self) -> None:
        service, views, clock = new_service()
        bootstrap_edition(service)
        result = service.place_order("CAOSHU_SHITIE", PAID_VARIANTS[0][0], "u-slow",
                                     request_id="s1")
        order_id = result["order_id"]
        clock.advance(minutes=20)
        service.expire_holds()

        support = views.support_view(order_id)
        events = {m["event"] for m in support["occupancy_movements"]}
        self.assertIn("SERIAL_HELD", events)
        self.assertIn("HOLD_RELEASED", events)
        release = next(m for m in support["occupancy_movements"] if m["event"] == "HOLD_RELEASED")
        self.assertEqual(release["reason"], "hold_timeout")
        self.assertEqual(release["external_request_id"], "")  # 系统超时无外部请求
        hold = next(m for m in support["occupancy_movements"] if m["event"] == "SERIAL_HELD")
        self.assertEqual(hold["external_request_id"], "s1")

    def test_issuer_report_has_no_user_identifiers(self) -> None:
        service, views, _ = new_service()
        bootstrap_edition(service)
        users = ["buyer-zhang", "buyer-li", "buyer-wang", "pii-secret-user"]
        for i, user in enumerate(users):
            buy(service, "CAOSHU_SHITIE", PAID_VARIANTS[i % 4][0], user, f"o-{user}", amount=19900)
        # 一个免费领取用户
        grant = service.grant_entitlement("free-user-pii", "CAOSHU_SHITIE", "campaign-9")
        service.place_order("CAOSHU_SHITIE", FREE_VARIANT[0], "free-user-pii",
                            request_id="f1", grant_id=grant)

        import json

        report_text = json.dumps(views.issuer_report("CAOSHU_SHITIE"), ensure_ascii=False)
        for user in users + ["free-user-pii"]:
            self.assertNotIn(user, report_text)

    def test_conservation_holds_across_mixed_lifecycle(self) -> None:
        service, views, clock = new_service()
        bootstrap_edition(service)

        # 成交两单（其中一单含实体并完成签收）
        o1 = buy(service, "CAOSHU_SHITIE", PAID_VARIANTS[0][0], "u1", "o1",
                 amount=19900, with_physical=True)
        service.pack_shipment(o1)
        service.dispatch_shipment(o1, carrier="SF", waybill_no="W1", request_id="d1")
        service.deliver_shipment(o1, request_id="v1")
        service.request_registration(o1, request_id="q1")
        service.accept_registration(o1, "T1", request_id="a1")

        o2 = buy(service, "CAOSHU_SHITIE", PAID_VARIANTS[0][0], "u2", "o2", amount=19900)
        # 一单下单未支付、一单超时、一单退款
        o3 = service.place_order("CAOSHU_SHITIE", PAID_VARIANTS[1][0], "u3", request_id="o3")
        o4 = service.place_order("CAOSHU_SHITIE", PAID_VARIANTS[1][0], "u4",
                                 request_id="o4")["order_id"]
        clock.advance(minutes=20)
        service.expire_holds()
        service.refund_order(o2, external_payment_id=f"PAY-{o2}", reason="x", request_id="rf2")

        # 免费领取一单
        grant = service.grant_entitlement("u5", "CAOSHU_SHITIE", "c1")
        service.place_order("CAOSHU_SHITIE", FREE_VARIANT[0], "u5",
                            request_id="o5", grant_id=grant)

        report = views.issuer_report("CAOSHU_SHITIE")
        self.assertTrue(report["conservation_ok"], report["discrepancies"])
        totals = report["totals"]
        self.assertLessEqual(totals["digital_owned"] + totals["digital_held"]
                             + totals["digital_isolated"], totals["digital_cap"])
        self.assertEqual(totals["physical_committed"], 1)
        # 订单 o3/o4 超时后号可再售：最先释放的小号被后来者领取
        again = service.place_order("CAOSHU_SHITIE", PAID_VARIANTS[1][0], "u6", request_id="o6")
        self.assertEqual(again["serial_no"], o3["serial_no"])


if __name__ == "__main__":
    unittest.main()
