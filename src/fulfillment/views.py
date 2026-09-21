"""读模型：购买者、客服、发行方三种视图。

三类调用方只拿到完成职责所必需的字段：

* 购买者视图只呈现本人订单，核对数字编号、实体配号、物流是否一致；
* 客服视图按时间线解释每一次占用与释放（含原因与外部请求标识）；
* 发行方视图只有分款计数与守恒等式，不含任何用户标识，无法拼出用户画像。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from . import model
from .event_store import EventStore


class ReadModel:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    # ------------------------------------------------------------------ #
    # 购买者：数字编号 / 实体配号 / 物流一致性
    # ------------------------------------------------------------------ #

    def buyer_view(self, order_id: str) -> dict[str, Any]:
        order = model.fold_order(self.store.get("purchase_order", order_id))
        assert order is not None

        reg = model.fold_registration(self.store.events_for("registration", f"REG-{order_id}"))
        shipment = model.fold_shipment(self.store.events_for("physical_shipment", f"SHP-{order_id}"))
        variant = model.fold_variant(self.store.get("edition", order.variant_id))

        digital: dict[str, Any] = {
            "serial_no": order.serial_no,
            "order_status": order.status,
            "registration": None,
        }
        mismatches: list[str] = []
        if reg is not None:
            if reg.serial_no != order.serial_no:
                mismatches.append(f"登记序号 {reg.serial_no} 与订单序号 {order.serial_no} 不一致")
            digital["registration"] = {
                "state": reg.state,
                "token_id": reg.token_id if reg.state == model.REG_ACCEPTED else "",
                "corrected": bool(reg.corrections),
                "effective": reg.effective if reg.state == model.REG_ACCEPTED else None,
            }

        physical: dict[str, Any] | None = None
        if order.with_physical:
            if shipment is None:
                physical = {"collectible_no": order.collectible_no, "state": "awaiting_pack",
                            "carrier": "", "waybill_no": "", "attempts": 0}
                if order.status == model.PAID:
                    mismatches.append("订单已成交但实体件尚未进入仓配")
            else:
                attempt = shipment.current_attempt or {}
                physical = {
                    "collectible_no": shipment.collectible_no,
                    "state": shipment.state,
                    "carrier": attempt.get("carrier", ""),
                    "waybill_no": attempt.get("waybill_no", ""),
                    "attempts": len(shipment.attempts),
                }
                if shipment.collectible_no != order.collectible_no:
                    mismatches.append(
                        f"实体配号 {shipment.collectible_no} 与订单配号 {order.collectible_no} 不一致"
                    )
                packed = next(
                    (e for e in self.store.events_for("physical_shipment", f"SHP-{order_id}")
                     if e.event_type == "PHYSICAL_PACKED"),
                    None,
                )
                if packed is not None and packed.payload.get("serial_no") != order.serial_no:
                    mismatches.append("实体包裹绑定的数字序号与订单不一致")
                if order.status == model.REFUNDED and shipment.state not in ("returned", "lost"):
                    mismatches.append("订单已退款但实体件未退回/丢件，处于悬空状态，待仲裁")
                if order.status == model.PAID and shipment.state == "returned":
                    mismatches.append("数字权益仍有效而纸质件已退回，处于悬空状态，待仲裁")

        invoice = model.fold_invoice(self.store.events_for("invoice", f"INV-{order_id}"))
        invoice_view = (
            {"invoice_id": invoice.invoice_id, "voided": invoice.voided} if invoice else None
        )

        return {
            "order_id": order_id,
            "variant": variant.name if variant else order.variant_id,
            "channel": order.channel,
            "digital": digital,
            "physical": physical,
            "invoice": invoice_view,
            "consistent": not mismatches,
            "mismatches": mismatches,
        }

    # ------------------------------------------------------------------ #
    # 客服：每一次占用与释放都可解释
    # ------------------------------------------------------------------ #

    OCCUPANCY_EVENTS = {
        "SERIAL_HELD", "HOLD_RELEASED", "SERIAL_ISOLATED",
        "PHYSICAL_ALLOCATED", "PHYSICAL_ISOLATED", "PHYSICAL_REPLACEMENT_ALLOCATED",
    }

    def support_view(self, order_id: str) -> dict[str, Any]:
        order_events = self.store.get("purchase_order", order_id)
        order = model.fold_order(order_events)
        assert order is not None

        timeline = sorted(
            (e for e in self.store.stream() if e.correlation_id == order_id),
            key=lambda e: e.seq,
        )
        movements = [
            {
                "seq": e.seq,
                "at": e.occurred_at,
                "event": e.event_type,
                "serial_no": e.payload.get("serial_no") or e.payload.get("collectible_no"),
                "reason": e.payload.get("reason", ""),
                "external_request_id": e.causation_id,
            }
            for e in timeline
            if e.event_type in self.OCCUPANCY_EVENTS
        ]
        full_timeline = [
            {
                "seq": e.seq,
                "at": e.occurred_at,
                "event": e.event_type,
                "aggregate": f"{e.aggregate_type}/{e.aggregate_id}",
                "summary": e.summary,
                "external_request_id": e.causation_id,
            }
            for e in timeline
        ]
        return {
            "order_id": order_id,
            "user_id": order.user_id,
            "status": order.status,
            "digital_serial_no": order.serial_no,
            "physical_collectible_no": order.collectible_no,
            "occupancy_movements": movements,
            "timeline": full_timeline,
        }

    # ------------------------------------------------------------------ #
    # 发行方：分款守恒核对，无用户标识
    # ------------------------------------------------------------------ #

    def issuer_report(self, project_id: str) -> dict[str, Any]:
        project = model.fold_project(self.store.get("project", project_id))
        assert project is not None
        # 款式配置事件挂在 edition 聚合上，这里按项目装入。
        for ve in self.store.stream(event_type="VARIANT_CONFIGURED"):
            if ve.payload["project_id"] == project_id and ve.aggregate_id not in project.variants:
                variant = model.fold_variant([ve])
                if variant is not None:
                    project.variants[variant.variant_id] = variant

        variants_report: list[dict[str, Any]] = []
        for variant_id, variant in sorted(project.variants.items()):
            variants_report.append(self._digital_counts(project, variant))
        physical = self._physical_counts(project)

        conservation_ok = all(v["conservation_ok"] for v in variants_report) and physical["conservation_ok"]
        discrepancies: list[dict[str, Any]] = []
        for row in variants_report + [physical]:
            discrepancies.extend(row.get("discrepancies", []))

        return {
            "project_id": project_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "digital_variants": variants_report,
            "physical_bundles": physical,
            "totals": {
                "digital_cap": sum(v["cap"] for v in variants_report),
                "digital_owned": sum(v["owned_total"] for v in variants_report),
                "digital_held": sum(v["held_pending_payment"] for v in variants_report),
                "digital_isolated": sum(v["isolated"] for v in variants_report),
                "physical_cap": physical["cap"],
                "physical_committed": physical["committed"],
                "physical_reserved": physical["reserved_pending_payment"],
                "physical_awaiting_return": physical["awaiting_return_after_refund"],
                "physical_isolated": physical["isolated"],
            },
            "conservation_ok": conservation_ok,
            "discrepancies": discrepancies,
        }

    def _orders_by_variant(self, project_id: str, variant_id: str) -> list[model.Order]:
        result = []
        for e in self.store.stream(event_type="ORDER_PLACED"):
            if e.payload["project_id"] == project_id and e.payload["variant_id"] == variant_id:
                order = model.fold_order(self.store.events_for("purchase_order", e.aggregate_id))
                if order is not None:
                    result.append(order)
        return result

    def _shipment_state(self, order_id: str) -> str | None:
        history = self.store.events_for("physical_shipment", f"SHP-{order_id}")
        if not history:
            return None
        shipment = model.fold_shipment(history)
        return shipment.state if shipment else None

    def _digital_counts(self, project: model.Project, variant: model.Variant) -> dict[str, Any]:
        pool = model.fold_serial_pool(
            self.store.events_for("serial_pool", variant.pool_id), variant.pool_id, variant.cap
        )
        orders = self._orders_by_variant(project.project_id, variant.variant_id)
        orders_by_id = {o.order_id: o for o in orders}

        owned_registered = 0
        owned_pending_registration = 0
        held_pending = 0
        isolated = 0
        discrepancies: list[dict[str, Any]] = []

        for serial, entry in pool.serials.items():
            status = entry["status"]
            order = orders_by_id.get(entry.get("order_id")) if entry.get("order_id") else None
            if status == model.ISOLATED:
                isolated += 1
            elif status == model.HELD:
                if order is None:
                    discrepancies.append({"pool": variant.pool_id, "serial_no": serial,
                                          "problem": "占用中但找不到订单"})
                elif order.status == model.PAID:
                    reg = model.fold_registration(
                        self.store.events_for("registration", f"REG-{order.order_id}")
                    )
                    if reg is not None and reg.state == model.REG_ACCEPTED:
                        owned_registered += 1
                    else:
                        owned_pending_registration += 1
                elif order.status == model.PENDING:
                    held_pending += 1
                elif order.status == model.CANCELLED:
                    discrepancies.append({"pool": variant.pool_id, "serial_no": serial,
                                          "problem": "订单已取消但序号未释放"})
                elif order.status == model.REFUNDED:
                    discrepancies.append({"pool": variant.pool_id, "serial_no": serial,
                                          "problem": "订单已退款但序号未隔离"})

        # 反向校验：每个成交订单的序号都必须在池中且归它独有。
        paid_serials = [o.serial_no for o in orders if o.status in (model.PAID, model.REFUNDED)]
        if len(paid_serials) != len(set(paid_serials)):
            duplicates = sorted({s for s in paid_serials if paid_serials.count(s) > 1})
            discrepancies.append({"pool": variant.pool_id, "problem": "同一序号落到多张订单",
                                  "serial_nos": duplicates})

        owned_total = owned_registered + owned_pending_registration
        available = variant.cap - owned_total - held_pending - isolated
        conservation_ok = (
            available >= 0
            and available + owned_total + held_pending + isolated == variant.cap
            and not discrepancies
        )
        return {
            "variant_id": variant.variant_id,
            "name": variant.name,
            "kind": variant.kind,
            "cap": variant.cap,
            "owned_total": owned_total,
            "owned_registered": owned_registered,
            "owned_pending_registration": owned_pending_registration,
            "held_pending_payment": held_pending,
            "isolated": isolated,
            "available": available,
            "conservation_equation": f"available({available}) + owned({owned_total}) + "
                                     f"held({held_pending}) + isolated({isolated}) = cap({variant.cap})",
            "conservation_ok": conservation_ok,
            "discrepancies": discrepancies,
        }

    def _physical_counts(self, project: model.Project) -> dict[str, Any]:
        pool = model.fold_physical_pool(
            self.store.events_for("serial_pool", project.physical_pool_id),
            project.physical_pool_id, project.physical_cap,
        )
        orders = []
        for e in self.store.stream(event_type="ORDER_PLACED"):
            if e.payload["project_id"] == project.project_id and e.payload.get("with_physical"):
                order = model.fold_order(self.store.events_for("purchase_order", e.aggregate_id))
                if order is not None:
                    orders.append(order)
        orders_by_id = {o.order_id: o for o in orders}

        committed = 0
        reserved = 0
        isolated = 0
        awaiting_return = 0
        discrepancies: list[dict[str, Any]] = []
        for no, entry in pool.serials.items():
            order = orders_by_id.get(entry.get("order_id")) if entry.get("order_id") else None
            if entry["status"] == model.ISOLATED:
                isolated += 1
            elif entry["status"] == model.RESERVED:
                if order is None:
                    discrepancies.append({"pool": project.physical_pool_id, "collectible_no": no,
                                          "problem": "预留中但找不到订单"})
                elif order.status == model.PAID:
                    committed += 1
                elif order.status == model.PENDING:
                    reserved += 1
                elif order.status == model.CANCELLED:
                    discrepancies.append({"pool": project.physical_pool_id, "collectible_no": no,
                                          "problem": "订单已取消但实体号未释放"})
                elif order.status == model.REFUNDED:
                    # 件在途或已签收，尚未退回；退回事件到达后该号转为隔离。
                    state = self._shipment_state(order.order_id)
                    if state in ("dispatched", "delivered"):
                        awaiting_return += 1
                    else:
                        discrepancies.append({"pool": project.physical_pool_id, "collectible_no": no,
                                              "problem": "订单已退款但实体号未隔离"})

        paid_nos = [o.collectible_no for o in orders if o.status in (model.PAID, model.REFUNDED)]
        if len(paid_nos) != len(set(paid_nos)):
            duplicates = sorted({s for s in paid_nos if paid_nos.count(s) > 1})
            discrepancies.append({"pool": project.physical_pool_id, "problem": "同一实体配号落到多张订单",
                                  "collectible_nos": duplicates})

        available = project.physical_cap - committed - reserved - isolated - awaiting_return
        conservation_ok = (
            available >= 0
            and available + committed + reserved + isolated + awaiting_return == project.physical_cap
            and not discrepancies
        )
        return {
            "pool_id": project.physical_pool_id,
            "cap": project.physical_cap,
            "committed": committed,
            "reserved_pending_payment": reserved,
            "awaiting_return_after_refund": awaiting_return,
            "isolated": isolated,
            "available": available,
            "conservation_equation": f"available({available}) + committed({committed}) + "
                                     f"reserved({reserved}) + awaiting_return({awaiting_return}) + "
                                     f"isolated({isolated}) = cap({project.physical_cap})",
            "conservation_ok": conservation_ok,
            "discrepancies": discrepancies,
        }
