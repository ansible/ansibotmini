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
        (
            "lib/ansible/module_utils/powershell/Ansible.ModuleUtils.Legacy.psm1",
            ["lib/ansible/module_utils/powershell/Ansible.ModuleUtils.Legacy.psm1"],
        ),
        ("lib/ansible/cli/galaxy", ["lib/ansible/cli/galaxy.py", "lib/ansible/galaxy"]),
        ("role", ["lib/ansible/playbook/role/__init__.py"]),
        ("tags", ["lib/ansible/playbook/block.py"]),
        ("discovery", ["lib/ansible/executor/interpreter_discovery.py"]),
        ("templar", ["lib/ansible/template/__init__.py"]),
        ("core", []),
        ("handler", ["lib/ansible/playbook/handler.py"]),
        ("block", ["lib/ansible/playbook/block.py"]),
        ("module_utils", ["lib/ansible/module_utils"]),
        ("task_executor", ["lib/ansible/executor/task_executor.py"]),
        ("play_iterator", ["lib/ansible/executor/play_iterator.py"]),
        ("hostvars", ["lib/ansible/vars/hostvars.py"]),
        ("become", ["lib/ansible/plugins/become/__init__.py"]),
        ("strategy", ["lib/ansible/plugins/strategy/__init__.py"]),
        ("any_errors_fatal", ["lib/ansible/plugins/strategy/linear.py"]),
        ("max_fail_percentage", ["lib/ansible/plugins/strategy/linear.py"]),
        ("register", ["lib/ansible/executor/task_executor.py"]),
        ("retries", ["lib/ansible/executor/task_executor.py"]),
        ("loop", ["lib/ansible/executor/task_executor.py"]),
        ("facts", ["lib/ansible/modules/setup.py"]),
        ("run_once", ["lib/ansible/plugins/strategy/__init__.py"]),
        ("force_handlers", ["lib/ansible/playbook/play.py"]),
        ("vars_prompt", ["lib/ansible/executor/playbook_executor.py"]),
    ],
)
def test_component_matcher(existing_files, in_data, expected):
    assert sorted(
        match_existing_components(process_component(in_data), existing_files)
    ) == sorted(expected)
