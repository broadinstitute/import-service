from typing import NamedTuple


class UserInfo(NamedTuple):
    subject_id: str
    user_email: str
    enabled: bool
