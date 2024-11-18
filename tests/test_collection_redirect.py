import json
import os

import pytest

from ansibotmini import is_in_collection, TriageContext


@pytest.fixture
def ctx():
    with open(os.path.join(os.path.dirname(__file__), "data/collections_list")) as f:
        collections_list = json.load(f)

    with open(
        os.path.join(os.path.dirname(__file__), "data/collections_file_map")
    ) as f:
        collections_file_map = json.load(f)

    return TriageContext(
        collections_list=collections_list,
        collections_file_map=collections_file_map,
        committers=[],
        devel_file_list=[],
        v29_file_list=[
            "lib/ansible/modules/cloud/amazon/ec2_instance.py",
            "lib/ansible/plugins/action/patch.py",
        ],
        v29_flatten_modules=[
            "lib/ansible/modules/ec2_instance.py",
            "lib/ansible/modules/vmware_guest_disk.py",
        ],
        collections_to_redirect=[
            "ansible.posix",
            "community.general",
            "community.vmware",
            "amazon.aws",
        ],
        oldest_supported_bugfix_version=[],
        labels_to_ids_map={},
        updated_at=None,
    )


@pytest.mark.parametrize(
    "in_component, out_component, expected",
    [
        (
            "community.vmware.vmware_guest_disk",
            "community.vmware.vmware_guest_disk",
            ["community.vmware"],
        ),
        (
            "lib/ansible/modules/cloud/vmware/vmware_guest_disk.py",
            "plugins/modules/vmware_guest_disk.py",
            ["community.vmware"],
        ),
        (
            "lib/ansible/plugins/action/patch.py",
            "plugins/action/patch.py",
            ["ansible.posix"],
        ),
        ("patch", "plugins/action/patch.py", ["ansible.posix"]),
        (
            "vmware_guest_disk",
            "plugins/modules/vmware_guest_disk.py",
            ["community.vmware"],
        ),
        ("role", "", []),
        ("host", "", []),
        (
            "ansible.posix",
            "ansible.posix",
            ["ansible.posix"],
        ),
        (
            "not.a.fq.cn",
            "not.a.fq.cn",
            [],
        ),
        (
            "ec2_instance",
            "plugins/modules/ec2_instance.py",
            ["amazon.aws"],
        ),
    ],
)
def test_collection_redirect(ctx, in_component, out_component, expected):
    assert sorted(is_in_collection([in_component], ctx)[out_component]) == sorted(
        expected
    )
