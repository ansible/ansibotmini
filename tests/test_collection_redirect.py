import json
import os

import pytest

from ansibotmini import is_in_collection, TriageContext


@pytest.fixture
def ctx():
    with open(os.path.join(os.path.dirname(__file__), "collections_list")) as f:
        collections_list = json.load(f)

    with open(os.path.join(os.path.dirname(__file__), "collections_file_map")) as f:
        collections_file_map = json.load(f)

    return TriageContext(
        collections_list=collections_list,
        collections_file_map=collections_file_map,
        committers=[],
        devel_file_list=[],
    )


@pytest.mark.parametrize(
    "in_component, out_component, expected",
    [
        (
            "community.vmware.vmware_guest_disk",
            "community.vmware.vmware_guest_disk",
            "community.vmware",
        ),
        (
            "lib/ansible/modules/cloud/vmware/vmware_guest_disk.py",
            "plugins/modules/vmware_guest_disk.py",
            "community.vmware",
        ),
        (
            "lib/ansible/plugins/action/patch.py",
            "plugins/action/patch.py",
            "ansible.posix",
        ),
        ("patch", "plugins/action/patch.py", "ansible.posix"),
    ],
)
def test_collection_redirect(ctx, in_component, out_component, expected):
    # FIXME more results than just expected
    assert expected in is_in_collection([], [in_component], ctx)[out_component]
