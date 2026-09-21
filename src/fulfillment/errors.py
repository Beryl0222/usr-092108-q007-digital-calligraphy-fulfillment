"""领域错误。

所有业务规则违例都以 :class:`DomainError` 表达；调用方据此决定是 4xx
返回还是重试，基础设施错误不混入其中。
"""

from __future__ import annotations


class DomainError(Exception):
    """业务规则错误，code 供调用方做稳定分支判断。"""

    code = "DOMAIN_ERROR"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class NotFound(DomainError):
    code = "NOT_FOUND"


class Conflict(DomainError):
    """并发冲突或状态机不允许的跃迁。"""

    code = "CONFLICT"


class SoldOut(Conflict):
    """序号池已无可占编号。"""

    code = "SOLD_OUT"


class EligibilityRefused(DomainError):
    """免费/付费资格核验未通过。"""

    code = "ELIGIBILITY_REFUSED"


class DuplicateRequest(Conflict):
    """幂等键重复且载荷与首次不一致。"""

    code = "DUPLICATE_REQUEST"
