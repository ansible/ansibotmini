import datetime

import pytest

from ansibotmini import (
    Actions,
    Issue,
    Label,
    PR,
    LabeledEvent,
    STALE_PR_DAYS,
    WAITING_ON_CONTRIBUTOR_CLOSE_DAYS,
    CI,
    TriageContext,
)
from ansibotmini import (
    needs_triage,
    networking,
    stale_pr,
    match_version,
    waiting_on_contributor,
)


issue_kw = {
    "id": "id",
    "author": "author",
    "number": 1,
    "title": "title",
    "body": "body",
    "url": "https://github.com/ansible/ansible/1",
    "events": [],
    "labels": set(),
    "created_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    "components": [],
    "last_triaged_at": datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc),
    "has_pr": False,
}

pr_kw = {
    "id": "id",
    "author": "author",
    "number": 1,
    "title": "title",
    "body": "body",
    "url": "https://github.com/ansible/ansible/1",
    "events": [],
    "labels": set(),
    "components": [],
    "last_triaged_at": datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc),
    "branch": "devel",
    "files": [],
    "mergeable": "mergeable",
    "changes_requested": False,
    "last_reviewed_at": datetime.datetime(2026, 1, 3, tzinfo=datetime.timezone.utc),
    "last_committed_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    "ci": CI(),
    "from_repo": "ansible/ansible",
    "has_issue": True,
    "created_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    "pushed_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    "all_commits_signed": True,
}


def test_needs_triage_label_new_issue():
    issue = Issue(**issue_kw)
    actions = Actions()
    needs_triage(issue, actions)
    assert Label.NEEDS_TRIAGE in actions.to_label


def test_needs_triage_label_old_issue():
    issue = Issue(**issue_kw)
    issue.events = [
        LabeledEvent(
            created_at=issue.last_triaged_at, label=Label.NEEDS_TRIAGE, author="ansibot"
        )
    ]
    actions = Actions()
    needs_triage(issue, actions)
    assert Label.NEEDS_TRIAGE not in actions.to_label


@pytest.mark.parametrize(
    "component, is_networking",
    [
        ["lib/ansible/module_utils/connection.py", True],
        ["task_executor.py", False],
        ["lib/ansible/plugins/httpapi/__init__.py", True],
    ],
)
def test_networking_component(component, is_networking):
    issue = Issue(**issue_kw)
    issue.components = [component]
    actions = Actions()
    networking(issue, actions)

    if is_networking:
        assert Label.NETWORKING in actions.to_label
        assert Label.NETWORKING not in actions.to_unlabel
    else:
        assert Label.NETWORKING in actions.to_unlabel
        assert Label.NETWORKING not in actions.to_label


def test_stale_pr_stale(monkeypatch):
    monkeypatch.setattr("ansibotmini.days_since", lambda _: STALE_PR_DAYS + 1)
    pr = PR(**pr_kw)
    actions = Actions()
    stale_pr(pr, actions)
    assert Label.STALE_PR in actions.to_label
    assert Label.STALE_PR not in actions.to_unlabel


def test_stale_pr_not_stale(monkeypatch):
    monkeypatch.setattr("ansibotmini.days_since", lambda _: STALE_PR_DAYS - 1)
    pr = PR(**pr_kw)
    actions = Actions()
    stale_pr(pr, actions)
    assert Label.STALE_PR not in actions.to_label
    assert Label.STALE_PR in actions.to_unlabel


def test_match_version(monkeypatch):
    TriageContext._current = TriageContext(
        oldest_supported_bugfix_version=(2, 19),
        updated_at=datetime.datetime.now(tz=datetime.timezone.utc),
    )
    issue = Issue(**issue_kw)
    issue.body = "### Ansible Version\nansible [core 2.21.0.dev0] (devel 51d5a456ef) last updated 2026/03/13 10:36:46 (GMT +200)\n### Configuration"
    actions = Actions()
    actions.to_label = [Label.BUG]
    match_version(issue, actions)
    assert Label.AFFECTS_2_21 in actions.to_label
    assert len(actions.comments) == 0


def test_match_version_unsupported(monkeypatch):
    TriageContext._current = TriageContext(
        oldest_supported_bugfix_version=(2, 22),
        updated_at=datetime.datetime.now(tz=datetime.timezone.utc),
    )
    issue = Issue(**issue_kw)
    issue.body = "### Ansible Version\nansible [core 2.21.0.dev0] (devel 51d5a456ef) last updated 2026/03/13 10:36:46 (GMT +200)\n### Configuration"
    actions = Actions()
    actions.to_label = [Label.BUG]
    match_version(issue, actions)
    assert Label.AFFECTS_2_21 in actions.to_label
    assert len(actions.comments) == 1


def test_waiting_on_contributor(monkeypatch):
    monkeypatch.setattr(
        "ansibotmini.days_since", lambda _: WAITING_ON_CONTRIBUTOR_CLOSE_DAYS + 1
    )
    issue = Issue(**issue_kw)
    actions = Actions()
    issue.events = [
        LabeledEvent(
            created_at=issue.last_triaged_at,
            label=Label.WAITING_ON_CONTRIBUTOR,
            author="core",
        )
    ]
    waiting_on_contributor(issue, actions)
    assert actions.close
    assert Label.WAITING_ON_CONTRIBUTOR in actions.to_unlabel
    assert len(actions.comments) == 1
