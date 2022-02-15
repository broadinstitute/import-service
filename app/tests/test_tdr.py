import pytest
from app.tests import testutils
from app.external import tdr
from app.util import exceptions

def test_list_policies_for_resource():
    # tdr returns an false
    with testutils.patch_request("app.external.tdr", "post", 403):
        with pytest.raises(exceptions.AuthorizationException) as excinfo:
            tdr.add_snapshot_policy_member("snapshot_id", "a_tdr_policy", "test@y", "ya29.bearer_token")
            assert excinfo.value.http_status == 403

    # tdr returns true
    with testutils.patch_request("app.external.tdr", "post", 200):
        tdr.add_snapshot_policy_member("snapshot_id", "a_tdr_policy", "test@y", "ya29.bearer_token")
