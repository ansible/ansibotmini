import os

import pytest

from ansibotmini import process_component, match_existing_components


@pytest.fixture
def existing_files():
    with open(os.path.join(os.path.dirname(__file__), "data/existing_files")) as f:
        rv = [line.strip() for line in f.readlines()]
    return rv


@pytest.mark.parametrize(
    "in_data, expected",
    [
        ("ansible-galaxy", ["bin/ansible-galaxy"]),
        (
            "Module: unarchive",
            [
                "lib/ansible/plugins/action/unarchive.py",
                "lib/ansible/modules/unarchive.py",
            ],
        ),
        (
            "template",
            [
                "lib/ansible/plugins/action/template.py",
                "lib/ansible/modules/template.py",
                "lib/ansible/plugins/lookup/template.py",
            ],
        ),
        (
            "unarchive",
            [
                "lib/ansible/plugins/action/unarchive.py",
                "lib/ansible/modules/unarchive.py",
            ],
        ),
        ("ansible.builtin.setup module", ["lib/ansible/modules/setup.py"]),
        ("File", ["lib/ansible/modules/file.py", "lib/ansible/plugins/lookup/file.py"]),
        ("yum", ["lib/ansible/plugins/action/yum.py", "lib/ansible/modules/yum.py"]),
        ("ansible-test", ["bin/ansible-test"]),
        (
            "ansible.builtin.file",
            ["lib/ansible/modules/file.py", "lib/ansible/plugins/lookup/file.py"],
        ),
        #  ("  - ansible_playbook", ["bin/ansible-playbook"]),
        (
            "lib/ansible/module_utils/powershell/Ansible.ModuleUtils.Legacy.psm1",
            ["lib/ansible/module_utils/powershell/Ansible.ModuleUtils.Legacy.psm1"],
        ),
        ("lib/ansible/cli/galaxy", ["lib/ansible/cli/galaxy.py", "lib/ansible/galaxy"]),
        #  ("module_utils", ["lib/ansible/module_utils"]),
        #  "role"
        #  "templar"
    ],
)
def test_component_matcher(existing_files, in_data, expected):
    assert sorted(
        match_existing_components(process_component(in_data), existing_files)
    ) == sorted(expected)
