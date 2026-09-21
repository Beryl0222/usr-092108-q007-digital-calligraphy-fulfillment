"""领域错误。"""


class DomainError(Exception):
    """所有业务规则违反的基类。"""


class NotFound(DomainError):
    pass


class VersionConflict(DomainError):
    """事件流版本冲突（并发提交）。"""


class SoldOut(DomainError):
    """款式序号池已无可用名额。"""


class NotEligible(DomainError):
    """免费或付费资格核验未通过。"""


class AlreadyOccupiesQuota(DomainError):
    """同一用户对该款式已持有或拥有一个名额。"""


class IllegalTransition(DomainError):
    """订单/履约状态机不允许该迁移，例如超时后确认支付。"""


class PhysicalMismatch(DomainError):
    """仓配上报的配号与数字收藏序号不一致。"""


class LatePaymentRejected(DomainError):
    """超时/取消后到达的支付被拒绝（拒绝事实已追加留痕）。"""

    def __init__(self, order_id: str, payment_ref: str) -> None:
        super().__init__(f"订单 {order_id} 的迟到支付 {payment_ref} 已拒绝并退款待办")
        self.order_id = order_id
        self.payment_ref = payment_ref
