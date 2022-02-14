from app.tests import testutils
from app.external import tdr

def test_list_policies_for_resource():
    # tdr returns an false
    with testutils.patch_request("app.external.tdr", "post", 403):
        response = tdr.add_snapshot_policy_member("snapshot_id", "a_tdr_policy", "test@y", "ya29.bearer_token")
        assert response == False

    # tdr returns true
    with testutils.patch_request("app.external.tdr", "post", 200):
        response = tdr.add_snapshot_policy_member("snapshot_id", "a_tdr_policy", "test@y", "ya29.bearer_token")
        assert response == True
