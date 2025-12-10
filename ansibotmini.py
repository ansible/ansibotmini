#!/usr/bin/env python
# Copyright 2022 Martin Krizek <martin.krizek@gmail.com>
# GNU General Public License v3.0+ (see LICENSE or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import annotations

import argparse
import base64
import collections
import concurrent.futures
import configparser
import dataclasses
import datetime
import difflib
import hashlib
import io
import itertools
import json
import logging
import logging.handlers
import os.path
import pickle
import pprint
import queue
import re
import string
import subprocess
import sys
import tempfile
import threading
import time
import typing as t
import urllib.error
import urllib.parse
import urllib.request
import zipfile

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None

try:
    __version__ = subprocess.check_output(
        ("git", "rev-parse", "HEAD"), text=True
    ).strip()
except subprocess.CalledProcessError:
    __version__ = "unknown"

minimal_required_python_version = (3, 13)
if sys.version_info < minimal_required_python_version:
    raise SystemExit(
        f"ansibotmini requires Python {'.'.join((str(e) for e in minimal_required_python_version))} or newer. "
        f"Python version detected: {sys.version.split(' ', maxsplit=1)[0]}"
    )


BOT_ACCOUNT = "ansibot"

AZP_ARTIFACTS_URL_FMT = "https://dev.azure.com/ansible/ansible/_apis/build/builds/%d/artifacts?api-version=7.0"
AZP_TIMELINE_URL_FMT = "https://dev.azure.com/ansible/ansible/_apis/build/builds/%d/timeline/?api-version=7.0"
AZP_BUILD_URL_FMT = (
    "https://dev.azure.com/ansible/ansible/_apis/build/builds/%d?api-version=7.0"
)
AZP_BUILD_ID_RE = re.compile(
    r"https://dev\.azure\.com/(?P<organization>[^/]+)/(?P<project>[^/]+)/_build/results\?buildId=(?P<buildId>[0-9]+)",
)

ANSIBLE_CORE_PYPI_URL = "https://pypi.org/pypi/ansible-core/json"
GALAXY_URL = "https://galaxy.ansible.com/"
GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
COLLECTIONS_LIST_ENDPOINT = "https://sivel.eng.ansible.com/api/v1/collections/list"
COLLECTIONS_FILEMAP_ENDPOINT = (
    "https://sivel.eng.ansible.com/api/v1/collections/file_map"
)
COLLECTIONS_TO_REDIRECT_ENDPOINT = "https://raw.githubusercontent.com/ansible-community/ansible-build-data/main/12/ansible.in"
DEVEL_FILE_LIST = (
    "https://api.github.com/repos/ansible/ansible/git/trees/devel?recursive=1"
)
V29_FILE_LIST = (
    "https://api.github.com/repos/ansible/ansible/git/trees/v2.9.0?recursive=1"
)

STALE_CI_DAYS = 7
STALE_PR_DAYS = 365
STALE_ISSUE_DAYS = 7
NEEDS_INFO_WARN_DAYS = 14
NEEDS_INFO_CLOSE_DAYS = 28
WAITING_ON_CONTRIBUTOR_CLOSE_DAYS = 365
SLEEP_SECONDS = 300

CONFIG_FILENAME = os.path.expanduser("~/.ansibotmini.cfg")
CACHE_FILENAME = os.path.expanduser("~/.ansibotmini_cache.pickle")
BYFILE_PAGE_FILENAME = os.path.expanduser("~/byfile.html")
LOG_FILENAME = os.path.expanduser("~/ansibotmini.log")

COMPONENT_RE = re.compile(
    r"#{3,5}\scomponent\sname(.+?)(?=#{3,5}|$)", flags=re.IGNORECASE | re.DOTALL
)
OBJ_TYPE_RE = re.compile(
    r"#{3,5}\sissue\stype(.+?)(?=#{3,5}|$)", flags=re.IGNORECASE | re.DOTALL
)
VERSION_RE = re.compile(
    r"#{3,5}\sansible\sversion(.+?)(?=#{3,5}|$)", flags=re.IGNORECASE | re.DOTALL
)
VERSION_OUTPUT_RE = re.compile(r"ansible\s(?:\[core\s)?(\d+.\d+\.\d+)")
COMPONENT_COMMAND_RE = re.compile(
    r"^(?:@ansibot\s)?!?component\s([=+-]\S+)\s*$", flags=re.MULTILINE
)
FLATTEN_MODULES_RE = re.compile(r"(lib/ansible/modules)/(.*)(/.+\.(?:py|ps1))")

VALID_COMMANDS = (
    "bot_skip",
    "!bot_skip",
    "bot_broken",
    "!bot_broken",
    "!needs_collection_redirect",
)

VALID_LABELS = {
    # label ids of previous versions of affects_* would be downloaded on demand
    "affects_2.9",
    "affects_2.10",
    "affects_2.11",
    "affects_2.12",
    "affects_2.13",
    "affects_2.14",
    "affects_2.15",
    "affects_2.16",
    "affects_2.17",
    "affects_2.18",
    "affects_2.19",
    "affects_2.20",
    "backport",
    "bot_broken",
    "bot_closed",
    "bug",
    "ci_verified",
    "feature",
    "has_issue",
    "has_pr",
    "module",
    "needs_ci",
    "needs_info",
    "needs_rebase",
    "needs_revision",
    "needs_template",
    "needs_triage",
    "networking",
    "pending_ci",
    "pre_azp",
    "stale_ci",
    "stale_review",
    "test",
    "waiting_on_contributor",
}

COMMANDS_RE = re.compile(
    rf"^(?:@ansibot\s)?({'|'.join(VALID_COMMANDS)})\s*$", flags=re.MULTILINE
)

ANSIBLE_PLUGINS = frozenset(
    (
        "action",
        "become",
        "cache",
        "callback",
        "cliconf",
        "connection",
        "doc_fragments",
        "filter",
        "httpapi",
        "inventory",
        "lookup",
        "netconf",
        "shell",
        "strategy",
        "terminal",
        "test",
        "vars",
    )
)

NETWORK_PLUGIN_TYPE_DIRS = frozenset(
    (
        "lib/ansible/plugins/cliconf",
        "lib/ansible/plugins/httpapi",
        "lib/ansible/plugins/netconf",
        "lib/ansible/plugins/terminal",
    )
)

NETWORK_FILES = frozenset(
    (
        "lib/ansible/module_utils/connection.py",
        "lib/ansible/cli/scripts/ansible_connection_cli_stub.py",
    )
)

ISSUE_FEATURE_TEMPLATE_SECTIONS = frozenset(
    (
        "Summary",
        "Issue Type",
        "Component Name",
    )
)

ISSUE_BUG_TEMPLATE_SECTIONS = frozenset(
    itertools.chain(
        ISSUE_FEATURE_TEMPLATE_SECTIONS,
        (
            "Ansible Version",
            "Configuration",
            "OS / Environment",
        ),
    )
)

LABELS_DO_NOT_OVERRIDE = {
    "bug",
    "feature",
    "has_pr",
    "module",
    "needs_revision",
    "needs_template",
    "networking",
    "stale_review",
    "test",
}

QUERY_NUMBERS_TMPL = """
query ($after: String) {
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
  repository(owner: "ansible", name: "ansible") {
    %s(states: OPEN, first: 100, after: $after) {
      pageInfo {
          hasNextPage
          endCursor
      }
      nodes {
        number
        updatedAt
        timelineItems(last: 1, itemTypes: [CROSS_REFERENCED_EVENT]) {
          updatedAt
        }
        %s
      }
    }
  }
}
"""

QUERY_ISSUE_NUMBERS = QUERY_NUMBERS_TMPL % ("issues", "")

QUERY_PR_NUMBERS = QUERY_NUMBERS_TMPL % (
    "pullRequests",
    """
commits(last:1) {
  nodes {
    commit {
      checkSuites(last:1) {
        nodes {
          updatedAt
        }
      }
    }
  }
}
""",
)

QUERY_SINGLE_TMPL = """
query($number: Int!)
{
  repository(owner: "ansible", name: "ansible") {
    %s(number: $number) {
      id
      author {
        login
      }
      number
      title
      body
      url
      labels (first: 40) {
        nodes {
          id
          name
        }
      }
      timelineItems(first: 200, itemTypes: [ISSUE_COMMENT, LABELED_EVENT, UNLABELED_EVENT]) {
        pageInfo {
            endCursor
            hasNextPage
        }
        nodes {
          __typename
          ... on IssueComment {
            createdAt
            updatedAt
            author {
              login
            }
            body
          }
          ... on LabeledEvent {
            createdAt
            actor {
              login
            }
            label {
              name
            }
          }
          ... on UnlabeledEvent {
            createdAt
            actor {
              login
            }
            label {
              name
            }
          }
        }
      }
      %s
    }
  }
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
}
"""

QUERY_SINGLE_ISSUE = QUERY_SINGLE_TMPL % (
    "issue",
    """
closedByPullRequestsReferences(last: 1) {
  nodes {
    number
  }
}
    """,
)

QUERY_SINGLE_PR = QUERY_SINGLE_TMPL % (
    "pullRequest",
    """
createdAt
baseRef {
  name
}
files(first: 50) {
  nodes {
    path
  }
}
last_commit: commits(last: 1) {
  nodes {
    commit {
      statusCheckRollup {
        state
      }
      committedDate
      checkSuites(first: 5) {
        nodes {
          app {
            name
          }
          checkRuns(last: 1) {
            nodes {
              name
              detailsUrl
              completedAt
              conclusion
              status
              startedAt
            }
          }
        }
      }
    }
  }
}
mergeable
reviews(last: 10, states: [APPROVED, CHANGES_REQUESTED, DISMISSED]) {
  nodes {
    author {
      login
    }
    state
    updatedAt
    isMinimized
  }
}
headRepository {
  name
  owner {
    login
  }
}
closingIssuesReferences(last: 1) {
  nodes {
    number
  }
}
""",
)

_http_request_lock = threading.Lock()
_http_request_counter = 0


class TriageNextTime(Exception):
    """Skip triaging an issue/PR due to the bot not receiving complete data to continue. Try next time."""


@dataclasses.dataclass(frozen=True, slots=True)
class Response:
    status_code: int | None
    reason: str
    raw_data: bytes

    def json(self) -> t.Any:
        return json.loads(self.raw_data or b"{}")


@dataclasses.dataclass(slots=True)
class IssueBase:
    id: str
    author: str
    number: int
    title: str
    body: str
    url: str
    events: list[Event]
    labels: dict[str, str]
    updated_at: datetime.datetime
    components: list[str]
    last_triaged_at: datetime.datetime | None

    def is_new(self) -> bool:
        return not any(
            e
            for e in self.events
            if isinstance(e, LabeledEvent) and e.label in ("needs_triage", "triage")
        )


@dataclasses.dataclass(slots=True)
class Issue(IssueBase):
    has_pr: bool


@dataclasses.dataclass(slots=True)
class PR(IssueBase):
    branch: str
    files: list[str]
    mergeable: str
    changes_requested: bool
    last_reviewed_at: datetime.datetime
    last_committed_at: datetime.datetime
    ci: CI
    from_repo: str
    has_issue: bool
    created_at: datetime.datetime
    pushed_at: datetime.datetime


@dataclasses.dataclass(slots=True)
class CI:
    build_id: int | None = None
    completed: bool = False
    passed: bool = False
    cancelled: bool = False
    completed_at: datetime.datetime | None = None
    started_at: datetime.datetime | None = None
    non_azp_failures: bool = False
    pending: bool = False

    def is_running(self) -> bool:
        return self.build_id is not None and not self.completed


@dataclasses.dataclass(frozen=True, slots=True)
class Command:
    updated_at: datetime.datetime
    arg: str | None = None


@dataclasses.dataclass(slots=True)
class Actions:
    to_label: list[str] = dataclasses.field(default_factory=list)
    to_unlabel: list[str] = dataclasses.field(default_factory=list)
    comments: list[str] = dataclasses.field(default_factory=list)
    cancel_ci: bool = False
    close: bool = False
    close_reason: str | None = None

    def __bool__(self):
        return bool(
            self.to_label
            or self.to_unlabel
            or self.comments
            or self.cancel_ci
            or self.close
        )


GH_OBJ = t.TypeVar("GH_OBJ", Issue, PR)


@dataclasses.dataclass(slots=True)
class TriageContext:
    collections_list: dict[str, t.Any] | None
    collections_file_map: dict[str, t.Any] | None
    committers: list[str]
    devel_file_list: list[str]
    v29_file_list: list[str]
    v29_flatten_modules: list[str]
    collections_to_redirect: list[str]
    labels_to_ids_map: dict[str, str]
    oldest_supported_bugfix_version: tuple[int, int]
    updated_at: datetime.datetime
    commands_found: dict[str, list[Command]] = dataclasses.field(default_factory=dict)

    @classmethod
    def fetch(cls) -> t.Self:
        devel_file_list = [
            e["path"]
            for e in http_request(
                DEVEL_FILE_LIST, headers={"Authorization": f"Bearer {gh_token}"}
            ).json()["tree"]
        ]
        v29_file_list = [
            e["path"]
            for e in http_request(
                V29_FILE_LIST, headers={"Authorization": f"Bearer {gh_token}"}
            ).json()["tree"]
        ]
        v29_flatten_modules = []
        for f in v29_file_list:
            if f.startswith("lib/ansible/modules") and f.endswith((".py", ".ps1")):
                if (possibly_flatten := flatten_module_path(f)) not in v29_file_list:
                    v29_flatten_modules.append(possibly_flatten)

        collections_list = None
        collections_file_map = None
        try:
            collections_list = http_request(COLLECTIONS_LIST_ENDPOINT).json()
            collections_file_map = http_request(COLLECTIONS_FILEMAP_ENDPOINT).json()
        except urllib.error.HTTPError as e:
            logging.error("%s: %d %s", e.url, e.status, e.reason)
        except (TimeoutError, urllib.error.URLError) as e:
            logging.error("%s: %s", COLLECTIONS_LIST_ENDPOINT, e)

        return cls(
            collections_list=collections_list,
            collections_file_map=collections_file_map,
            committers=get_committers(),
            devel_file_list=devel_file_list,
            v29_file_list=v29_file_list,
            v29_flatten_modules=v29_flatten_modules,
            collections_to_redirect=(
                http_request(COLLECTIONS_TO_REDIRECT_ENDPOINT)
                .raw_data.decode()
                .splitlines()
            ),
            oldest_supported_bugfix_version=get_oldest_supported_bugfix_version(),
            labels_to_ids_map={n: get_label_id(n) for n in VALID_LABELS},
            updated_at=datetime.datetime.now(datetime.timezone.utc),
        )


@dataclasses.dataclass(frozen=True, slots=True)
class CacheEntry:
    title: str
    url: str
    components: list[str]
    updated_at: datetime.datetime
    last_triaged_at: datetime.datetime
    last_committed_at: datetime.datetime | None
    pushed_at: datetime.datetime | None

    @classmethod
    def from_obj(cls, obj: GH_OBJ):
        return cls(
            title=obj.title,
            url=obj.url,
            components=obj.components,
            updated_at=obj.updated_at,
            last_triaged_at=obj.last_triaged_at,
            last_committed_at=getattr(obj, "last_committed_at", None),
            pushed_at=getattr(obj, "pushed_at", None),
        )


def http_request(
    url: str,
    data: str = "",
    headers: t.MutableMapping[str, str] | None = None,
    method: t.Literal["GET", "POST", "PATCH"] = "GET",
    retries: int = 3,
) -> Response:
    if headers is None:
        headers = {}

    wait_seconds = 10
    for i in range(retries):
        try:
            global _http_request_counter
            with _http_request_lock:
                _http_request_counter += 1
                counter_value = _http_request_counter
            logging.info(
                "http request no. %d: %s %s",
                counter_value,
                method,
                url,
            )
            with urllib.request.urlopen(
                urllib.request.Request(
                    url,
                    data=data.encode("ascii"),
                    headers=headers,
                    method=method.upper(),
                ),
            ) as response:
                logging.info(
                    "response: %d, %s",
                    response.status,
                    response.reason,
                )
                return Response(
                    status_code=response.status,
                    reason=response.reason,
                    raw_data=response.read(),
                )
        except urllib.error.HTTPError as e:
            logging.info(e)
            if e.status is not None and e.status >= 500:
                if i < retries - 1:
                    logging.info(
                        "Waiting for %d seconds and retrying the request...",
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                else:
                    raise
            else:
                return Response(
                    status_code=e.status,
                    reason=e.reason,
                    raw_data=b"",
                )
        except (TimeoutError, urllib.error.URLError) as e:
            logging.info(e)
            if i < retries - 1:
                logging.info(
                    "Waiting for %d seconds and retrying the request...", wait_seconds
                )
                time.sleep(wait_seconds)
            else:
                raise


def send_query(data: dict[str, t.Any]) -> Response:
    resp = http_request(
        GITHUB_GRAPHQL_URL,
        method="POST",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {gh_token}",
        },
        data=json.dumps(data),
    )

    if errors := resp.json().get("errors"):
        raise ValueError(errors)

    return resp


def get_label_id(name: str) -> str:
    query = """
    query ($name: String!){
      repository(owner: "ansible", name: "ansible") {
        label(name: $name) {
          id
        }
      }
    }
    """
    resp = send_query({"query": query, "variables": {"name": name}})
    if label := resp.json()["data"]["repository"]["label"]:
        return label["id"]
    else:
        raise ValueError(f"Label {name!r} does not exist.")


def add_labels(obj: GH_OBJ, labels: list[str], ctx: TriageContext) -> None:
    if not (
        label_ids := [
            ctx.labels_to_ids_map.get(label, get_label_id(label)) for label in labels
        ]
    ):
        return

    query = """
    mutation($input: AddLabelsToLabelableInput!) {
      addLabelsToLabelable(input:$input) {
        clientMutationId
      }
    }
    """
    send_query(
        {
            "query": query,
            "variables": {
                "input": {
                    "labelIds": label_ids,
                    "labelableId": obj.id,
                },
            },
        }
    )


def remove_labels(obj: GH_OBJ, labels: list[str]) -> None:
    if not (
        label_ids := [obj.labels[label] for label in labels if label in obj.labels]
    ):
        return

    query = """
    mutation($input: RemoveLabelsFromLabelableInput!) {
      removeLabelsFromLabelable(input:$input) {
        clientMutationId
      }
    }
    """
    send_query(
        {
            "query": query,
            "variables": {
                "input": {
                    "labelIds": label_ids,
                    "labelableId": obj.id,
                },
            },
        }
    )


def add_comment(obj: GH_OBJ, body: str) -> None:
    query = """
    mutation($input: AddCommentInput!) {
      addComment(input:$input) {
        clientMutationId
      }
    }
    """
    send_query(
        {
            "query": query,
            "variables": {
                "input": {
                    "body": body,
                    "subjectId": obj.id,
                },
            },
        }
    )


def close_issue(obj_id: str, reason: str) -> None:
    query = """
    mutation($input: CloseIssueInput!) {
      closeIssue(input:$input) {
        clientMutationId
      }
    }
    """
    send_query(
        {
            "query": query,
            "variables": {
                "input": {
                    "issueId": obj_id,
                    "stateReason": reason,
                },
            },
        }
    )


def close_pr(obj_id: str) -> None:
    query = """
    mutation($input: ClosePullRequestInput!) {
      closePullRequest(input:$input) {
        clientMutationId
      }
    }
    """
    send_query(
        {
            "query": query,
            "variables": {
                "input": {
                    "pullRequestId": obj_id,
                },
            },
        }
    )


def get_committers() -> list[str]:
    query = """
    query {
      organization(login: "ansible") {
        team(slug: "ansible-commit") {
          members {
            nodes {
              login
            }
          }
        }
      }
    }
    """
    resp = send_query({"query": query})
    return [
        n["login"]
        for n in resp.json()["data"]["organization"]["team"]["members"]["nodes"]
    ]


def process_component(data):
    rv = []
    for line in (
        l
        for l in data.strip("\t\n\r").splitlines()
        if l and not ("<!--" in l or "-->" in l)
    ):
        for comma_split in line.split(","):
            space_split = comma_split.split(" ")
            if len(space_split) > 5:
                continue
            for c in space_split:
                c = c.strip().strip("`").lstrip("/")
                if "/" in c:
                    if "#" in c:
                        c = c.split("#")[0]
                    c.replace("\\", "").strip()
                else:
                    c = (
                        c.lower()
                        .removeprefix("the ")
                        .removeprefix("module ")
                        .removeprefix("plugin ")
                        .removesuffix(" module")
                        .removesuffix(" plugin")
                        .replace("ansible.builtin.", "")
                        .replace("ansible.legacy.", "")
                        .replace(".py", "")
                        .replace(".ps1", "")
                    )

                if c := re.sub(r"[^0-9a-zA-Z/._-]", "", c):
                    if (flatten := flatten_module_path(c)) != c:
                        rv.append(flatten)
                    if len(c) > 1:
                        rv.append(c)

    return rv


def flatten_module_path(c: str) -> str:
    return re.sub(FLATTEN_MODULES_RE, r"\1\3", c)


def template_comment(template_name: str, sub_map: dict | None = None) -> str:
    if sub_map is None:
        sub_map = {}
    with open(
        os.path.join(os.path.dirname(__file__), "templates", f"{template_name}.tmpl")
    ) as f:
        rv = string.Template(f.read()).substitute(sub_map)
    return rv


COMPONENT_TO_FILENAME = {
    "role": "lib/ansible/playbook/role/__init__.py",
    "tags": "lib/ansible/playbook/block.py",
    "discovery": "lib/ansible/executor/interpreter_discovery.py",
    "module_utils": "lib/ansible/module_utils",
    "become": "lib/ansible/plugins/become/__init__.py",
    "strategy": "lib/ansible/plugins/strategy/__init__.py",
    "any_errors_fatal": "lib/ansible/plugins/strategy/linear.py",
    "max_fail_percentage": "lib/ansible/plugins/strategy/linear.py",
    "templar": "lib/ansible/template/__init__.py",
    "register": "lib/ansible/executor/task_executor.py",
    "retries": "lib/ansible/executor/task_executor.py",
    "loop": "lib/ansible/executor/task_executor.py",
    "facts": "lib/ansible/modules/setup.py",
    "run_once": "lib/ansible/plugins/strategy/__init__.py",
    "force_handlers": "lib/ansible/playbook/play.py",
    "vars_prompt": "lib/ansible/executor/playbook_executor.py",
}


def match_existing_components(
    filenames: list[str], existing_files: list[str]
) -> list[str]:
    if not filenames:
        return []
    paths = [
        "lib/ansible/modules/",
        "bin/",
        "lib/ansible/cli/",
        "lib/ansible/playbook/",
        "lib/ansible/executor/",
        "lib/ansible/vars/",
    ]
    paths.extend((f"lib/ansible/plugins/{name}/" for name in ANSIBLE_PLUGINS))
    files = set()
    for filename in filenames:
        if filename == "core":
            continue
        if matched_filename := COMPONENT_TO_FILENAME.get(filename):
            files.add(matched_filename)
            continue

        files.add(filename)
        if "/" not in filename:
            for path in paths:
                files.add(f"{path}{filename}.py")
                files.add(f"{path}{filename}")

    components = files.intersection(existing_files)
    if not components:
        for file in filenames:
            components.update(
                difflib.get_close_matches(file, existing_files, cutoff=0.85)
            )
    return list(components)


def was_labeled_by_human(obj: GH_OBJ, label_name: str) -> bool:
    return any(
        e
        for e in obj.events
        if isinstance(e, LabeledEvent)
        and e.label == label_name
        and e.author != BOT_ACCOUNT
    )


def was_unlabeled_by_human(obj: GH_OBJ, label_name: str) -> bool:
    return any(
        e
        for e in obj.events
        if isinstance(e, UnlabeledEvent)
        and e.label == label_name
        and e.author != BOT_ACCOUNT
    )


def last_labeled(obj: GH_OBJ, name: str) -> datetime.datetime | None:
    return max(
        (
            e.created_at
            for e in obj.events
            if isinstance(e, LabeledEvent) and e.label == name
        ),
        default=None,
    )


def last_unlabeled(obj: GH_OBJ, name: str) -> datetime.datetime | None:
    return max(
        (
            e.created_at
            for e in obj.events
            if isinstance(e, UnlabeledEvent) and e.label == name
        ),
        default=None,
    )


def last_commented_by(obj: GH_OBJ, name: str) -> datetime.datetime | None:
    return max(
        (
            e.created_at
            for e in obj.events
            if isinstance(e, IssueCommentEvent) and e.author == name
        ),
        default=None,
    )


def last_boilerplate(obj: GH_OBJ, name: str) -> IssueCommentEvent | None:
    return max(
        (
            e
            for e in obj.events
            if isinstance(e, IssueCommentEvent)
            and e.author == BOT_ACCOUNT
            and f"<!--- boilerplate: {name} --->" in e.body
        ),
        key=lambda x: x.created_at,
        default=None,
    )


def days_since(when: datetime.datetime) -> int:
    return (datetime.datetime.now(datetime.timezone.utc) - when).days


def match_components(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    existing_components = []
    if isinstance(obj, PR):
        # for old PRs that still touch unflatten modules
        # FIXME remove this once such PRs are merged/closed/rebased
        files = []
        for fn in obj.files:
            if fn not in ctx.devel_file_list and fn.startswith("lib/ansible/modules"):
                files.append(flatten_module_path(fn))
            else:
                files.append(fn)
        existing_components = files
    elif isinstance(obj, Issue):
        processed_components = []
        if match := COMPONENT_RE.search(obj.body):
            processed_components = process_component(match.group(1))
            existing_components = match_existing_components(
                processed_components, ctx.devel_file_list
            )

        for command in ctx.commands_found.get("component", []):
            op = command.arg[0]
            path = command.arg[1:].replace("`", "").replace(r"\_", "_")
            if path not in ctx.devel_file_list:
                processed_components.append(path)
                continue
            match op:
                case "=":
                    existing_components = [path]
                case "+":
                    if path not in existing_components:
                        existing_components.append(path)
                case "-":
                    if path in existing_components:
                        existing_components.remove(path)
                case _:
                    logging.info(
                        "Incorrect operation for the component command: %s", op
                    )

        post_comments_banner = True
        if (
            not existing_components
            and "!needs_collection_redirect" not in ctx.commands_found
        ):
            if entries := is_in_collection(processed_components, ctx):
                assembled_entries = []
                for component, fqcns in entries.items():
                    for fqcn in fqcns:
                        collection_info = ctx.collections_list[fqcn]["manifest"][
                            "collection_info"
                        ]
                        assembled_entries.append(
                            f"* {component} -> {collection_info['repository']} ({GALAXY_URL}{collection_info['namespace']}.{collection_info['name']})"
                        )
                actions.comments.append(
                    template_comment(
                        "collection_redirect",
                        {"components": "\n".join(assembled_entries)},
                    )
                )
                actions.to_label.append("bot_closed")
                actions.close = True
                actions.close_reason = "NOT_PLANNED"
                post_comments_banner = False

        if post_comments_banner:
            last_comment = last_boilerplate(obj, "components_banner")
            if last_comment:
                last_components = [
                    re.sub(r"[*`\[\]]", "", re.sub(r"\([^)]+\)", "", line)).strip()
                    for line in last_comment.body.splitlines()
                    if line.startswith("*")
                ]
                post_comments_banner = sorted(existing_components) != sorted(
                    last_components
                )

            last_command = ctx.commands_found.get("component", [])[-1:]
            if post_comments_banner and (
                obj.is_new()
                or (
                    last_comment
                    and last_command
                    and last_comment.created_at < last_command[0].updated_at
                )
            ):
                actions.comments.append(
                    template_comment(
                        "components_banner",
                        {
                            "components": (
                                "\n".join(
                                    f"* [`{component}`](https://github.com/ansible/ansible/blob/devel/{component})"
                                    for component in existing_components
                                )
                                if existing_components
                                else None
                            )
                        },
                    )
                )

    obj.components = existing_components

    logging.info(
        "%s #%d: identified components: %s",
        obj.__class__.__name__,
        obj.number,
        ", ".join(obj.components),
    )


def is_in_collection(components: list[str], ctx: TriageContext) -> dict[str, set[str]]:
    if ctx.collections_list is None or ctx.collections_file_map is None:
        return {}

    entries = collections.defaultdict(set)

    for component in components:
        parts = component.split(".")
        match len(parts):
            case 2:
                fqcn = component
            case 3:
                fqcn = ".".join(parts[:2])
            case _:
                continue
        if fqcn in ctx.collections_list and fqcn in ctx.collections_to_redirect:
            entries[component].add(fqcn)

    for component in components:
        if "/" not in component:
            continue
        flatten = re.sub(
            rf"lib/ansible/(?:plugins/)?({'|'.join(itertools.chain(ANSIBLE_PLUGINS, ['modules']))})(.*)/(.+\.(?:py|ps1))",
            r"plugins/\1/\3",
            component,
        )
        for fqcn in ctx.collections_file_map.get(flatten, []):
            if fqcn in ctx.collections_to_redirect:
                entries[flatten].add(fqcn)
        if entries:
            break
    else:
        for component, plugin_type, ext in itertools.product(
            (c for c in components if "/" not in c),
            (itertools.chain(ANSIBLE_PLUGINS, ["modules"])),
            ("py", "ps1"),
        ):
            candidate = f"plugins/{plugin_type}/{component}.{ext}"
            for fqcn in ctx.collections_file_map.get(candidate, []):
                if fqcn in ctx.collections_to_redirect:
                    if plugin_type == "modules":
                        if (
                            f"lib/ansible/modules/{component}.{ext}"
                            in ctx.v29_flatten_modules
                        ):
                            entries[candidate].add(fqcn)
                    else:
                        if f"lib/ansible/{candidate}" in ctx.v29_file_list:
                            entries[candidate].add(fqcn)
    return entries


def needs_triage(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if obj.is_new():
        actions.to_label.append("needs_triage")


def waiting_on_contributor(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if (
        "waiting_on_contributor" in obj.labels
        and days_since(last_labeled(obj, "waiting_on_contributor"))
        > WAITING_ON_CONTRIBUTOR_CLOSE_DAYS
    ):
        actions.close = True
        actions.close_reason = "NOT_PLANNED"
        actions.to_label.append("bot_closed")
        actions.to_unlabel.append("waiting_on_contributor")
        actions.comments.append(template_comment("waiting_on_contributor"))


def needs_info(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if needs_info_labeled_date := last_labeled(obj, "needs_info"):
        needs_info_unlabeled_date = last_unlabeled(obj, "needs_info")
        commented_datetime = last_commented_by(obj, obj.author)
        if (
            commented_datetime is None or needs_info_labeled_date > commented_datetime
        ) and (
            needs_info_unlabeled_date is None
            or needs_info_labeled_date > needs_info_unlabeled_date
        ):
            days_labeled = days_since(needs_info_labeled_date)
            if days_labeled > NEEDS_INFO_CLOSE_DAYS:
                actions.close = True
                actions.close_reason = "NOT_PLANNED"
                actions.to_label.append("bot_closed")
                actions.comments.append(
                    template_comment(
                        "needs_info_close",
                        {"author": obj.author, "object_type": obj.__class__.__name__},
                    )
                )
            elif days_labeled > NEEDS_INFO_WARN_DAYS:
                last_warned = last_boilerplate(obj, "needs_info_warn")
                if last_warned is None:
                    last_warned = last_boilerplate(obj, "needs_info_base")
                if (
                    last_warned is None
                    or last_warned.created_at < needs_info_labeled_date
                ):
                    actions.comments.append(
                        template_comment(
                            "needs_info_warn",
                            {
                                "author": obj.author,
                                "object_type": obj.__class__.__name__,
                            },
                        )
                    )
        else:
            if "needs_info" in actions.to_label:
                actions.to_label.remove("needs_info")
            actions.to_unlabel.append("needs_info")


def match_object_type(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if match := OBJ_TYPE_RE.search(obj.body):
        data = re.sub(r"~[^~]+~", "", match.group(1).lower()).lower()
        for m in re.findall(r"\b(feature|bug|test|bugfix)\b", data, flags=re.MULTILINE):
            if m == "bugfix":
                m = "bug"
            actions.to_label.append(m)


def match_version(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if isinstance(obj, PR):
        return
    if match := VERSION_RE.search(obj.body):
        if match := VERSION_OUTPUT_RE.search(match.group(1)):
            version = tuple(int(c) for c in match.group(1).split(".")[:2])
            version_s = f"{version[0]}.{version[1]}"
            actions.to_label.append(f"affects_{version_s}")
            if (
                obj.is_new()  # prevent spamming half the repo
                and "bug" in actions.to_label
                and version < ctx.oldest_supported_bugfix_version
            ):
                actions.comments.append(
                    template_comment(
                        "unsupported_version",
                        {
                            "author": obj.author,
                            "version_reported": version_s,
                        },
                    )
                )


def _sanitize_ci_comment(body: str) -> str:
    # GitHub allows up to 65536 but leave room for metadata/etc
    max_comment_len = 60000
    if len(body) <= max_comment_len:
        return body
    ommited_msg = "... [omitted, message too long]"
    return f"{body[:max_comment_len - len(ommited_msg)]}{ommited_msg}"


def ci_comments(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR) or not obj.ci.completed:
        return
    resp = http_request(AZP_TIMELINE_URL_FMT % obj.ci.build_id)
    if resp.status_code == 404:
        # not available anymore
        if obj.ci.passed and not obj.ci.non_azp_failures:
            actions.to_unlabel.append("ci_verified")
        return
    failed_job_ids = []
    for r in resp.json().get("records", []):
        if r["type"] == "Job":
            if r["result"] == "failed":
                failed_job_ids.append(r["id"])
            elif r["result"] == "canceled":
                obj.ci.cancelled = True
    if not failed_job_ids:
        if not obj.ci.non_azp_failures:
            actions.to_unlabel.append("ci_verified")
        return
    ci_comment = []
    ci_verifieds = []
    for url in (
        a["resource"]["downloadUrl"]
        for a in http_request(AZP_ARTIFACTS_URL_FMT % obj.ci.build_id).json()["value"]
        if a["name"].startswith("Bot ") and a["source"] in failed_job_ids
    ):
        zfile = zipfile.ZipFile(io.BytesIO(http_request(url).raw_data))
        for filename in zfile.namelist():
            if "ansible-test-" not in filename:
                continue
            with zfile.open(filename) as f:
                artifact_data = json.load(f)
                ci_verifieds.append(artifact_data["verified"])
                for r in artifact_data["results"]:
                    ci_comment.append(f"{r['message']}\n```\n{r['output']}\n```\n")
    if ci_comment:
        results = _sanitize_ci_comment("\n".join(ci_comment))
        r_hash = hashlib.md5(results.encode()).hexdigest()
        if not any(
            e
            for e in obj.events
            if isinstance(e, IssueCommentEvent)
            and e.author == BOT_ACCOUNT
            and "<!--- boilerplate: ci_test_result --->" in e.body
            and f"<!-- r_hash: {r_hash} -->" in e.body
        ):
            actions.comments.append(
                template_comment(
                    "ci_test_results", {"results": results, "r_hash": r_hash}
                )
            )
    if (all(ci_verifieds) and len(ci_verifieds) == len(failed_job_ids)) or (
        "ci_verified" in obj.labels
        and last_labeled(obj, "ci_verified") >= obj.ci.completed_at
    ):
        actions.to_label.append("ci_verified")
    else:
        actions.to_unlabel.append("ci_verified")


def needs_revision(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR) or not obj.ci.completed:
        return
    if (
        obj.changes_requested
        and obj.last_reviewed_at is not None
        and obj.last_reviewed_at > obj.last_committed_at
    ) or not obj.ci.passed:
        actions.to_label.append("needs_revision")
    else:
        actions.to_unlabel.append("needs_revision")


def stale_pr(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR):
        return

    if days_since(obj.pushed_at) > STALE_PR_DAYS:
        actions.to_label.append("stale_pr")
    else:
        actions.to_unlabel.append("stale_pr")


def needs_ci(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if "stale_pr" in actions.to_label:
        actions.to_unlabel.append("needs_ci")
        return

    if (
        not isinstance(obj, PR)
        or "needs_rebase" in actions.to_label
        or (datetime.datetime.now(datetime.timezone.utc) - obj.created_at).seconds
        < 5 * 60
    ):
        actions.to_unlabel.append("needs_ci")
        return

    if (
        (
            obj.ci.build_id is None
            and (datetime.datetime.now(datetime.timezone.utc) - obj.pushed_at).seconds
            > 5 * 60
        )
        or obj.ci.cancelled
        or (
            obj.ci.is_running()
            and (
                datetime.datetime.now(datetime.timezone.utc) - obj.ci.started_at
            ).seconds
            > 2 * 60 * 60
        )
    ):
        if "pre_azp" not in obj.labels:
            actions.to_label.append("needs_ci")
            logging.info(
                "Adding needs_ci: PR created_at: '%s', PR pushed at: '%s', PR CI: '%s'",
                obj.created_at,
                obj.pushed_at,
                obj.ci,
            )
    else:
        actions.to_unlabel.append("needs_ci")
        actions.to_unlabel.append("pre_azp")


def stale_ci(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR) or not obj.ci.completed:
        return
    if days_since(obj.ci.completed_at) > STALE_CI_DAYS:
        actions.to_label.append("stale_ci")
    else:
        actions.to_unlabel.append("stale_ci")


def pending_ci(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR):
        return

    if (
        obj.ci.pending
        and (datetime.datetime.now(datetime.timezone.utc) - obj.pushed_at).seconds
        > 60 * 60  # wait one hour after last push to the PR before declaring "stuck CI"
    ):
        actions.to_label.append("pending_ci")
    else:
        actions.to_unlabel.append("pending_ci")


def backport(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR):
        return
    if obj.branch.startswith("stable-"):
        actions.to_label.append("backport")
    else:
        actions.to_unlabel.append("backport")


def is_module(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if any(c.startswith("lib/ansible/modules/") for c in obj.components):
        actions.to_label.append("module")
    else:
        actions.to_unlabel.append("module")


def needs_rebase(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR):
        return
    # https://docs.github.com/en/graphql/reference/enums#mergeablestate
    match obj.mergeable:
        case "mergeable":
            actions.to_unlabel.append("needs_rebase")
        case "conflicting":
            actions.to_label.append("needs_rebase")
        case "unknown":
            raise TriageNextTime("Skipping due to the mergeable state being unknown")
        case _:
            raise AssertionError(f"Unexpected mergeable value: '{obj.mergeable}'")


def stale_review(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR) or obj.last_reviewed_at is None:
        return
    if obj.last_reviewed_at < obj.last_committed_at:
        actions.to_label.append("stale_review")
    else:
        actions.to_unlabel.append("stale_review")


def pr_from_upstream(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR) or obj.from_repo != "ansible/ansible":
        return
    actions.close = True
    actions.close_reason = "NOT_PLANNED"
    actions.comments.append(
        template_comment("pr_from_upstream", {"author": obj.author})
    )
    if obj.ci.is_running():
        actions.cancel_ci = True


def cancel_ci(build_id: int) -> None:
    logging.info("Cancelling CI buildId %d", build_id)
    resp = http_request(
        url=AZP_BUILD_URL_FMT % build_id,
        method="PATCH",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic {}".format(
                base64.b64encode(f":{azp_token}".encode()).decode()
            ),
        },
        data=json.dumps({"status": "Cancelling"}),
    )
    logging.info("Cancelled with status_code: %d", resp.status_code)


def linked_objs(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if isinstance(obj, PR):
        if obj.has_issue:
            actions.to_label.append("has_issue")
        else:
            actions.to_unlabel.append("has_issue")
    elif isinstance(obj, Issue):
        if obj.has_pr:
            actions.to_label.append("has_pr")
        else:
            actions.to_unlabel.append("has_pr")


def needs_template(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if isinstance(obj, PR) or obj.author in ctx.committers:
        return
    missing = []
    if "bug" in actions.to_label:
        sections = ISSUE_BUG_TEMPLATE_SECTIONS
    else:
        sections = ISSUE_FEATURE_TEMPLATE_SECTIONS
    for section in sections:
        if (
            re.search(
                r"^#{3,5}\s*%s\s*$" % section,
                obj.body,
                flags=re.IGNORECASE | re.MULTILINE,
            )
            is None
        ):
            missing.append(section)

    if "Component Name" in missing and "component" in ctx.commands_found:
        missing.remove("Component Name")

    if missing:
        if obj.is_new():  # do not spam old issues
            actions.to_label.append("needs_template")
            actions.to_label.append("needs_info")
            if last_boilerplate(obj, "issue_missing_data") is None:
                actions.comments.append(
                    template_comment(
                        "issue_missing_data",
                        {
                            "author": obj.author,
                            "obj_type": obj.__class__.__name__,
                            "missing_sections": "\n".join((f"- {s}" for s in missing)),
                        },
                    )
                )
    else:
        actions.to_unlabel.append("needs_template")
        if (
            not was_labeled_by_human(obj, "needs_info")
            and "needs_info" not in ctx.commands_found
        ):
            actions.to_unlabel.append("needs_info")


def test_support_plugin(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR) or ctx.collections_file_map is None:
        return

    data = []
    for fn in obj.components:
        if not fn.startswith("test/support/"):
            continue

        try:
            plugin_path = fn.split("plugins/")[1]
        except IndexError:
            # not a plugin
            continue

        data.append(f"* `{fn}`")
        if collection_names := [
            cn
            for cn in ctx.collections_file_map.get(f"plugins/{plugin_path}", [])
            if cn in ctx.collections_to_redirect
        ]:
            data.append(
                f"\t* Possible match in the following collections: {', '.join(collection_names)}"
            )
        else:
            data.append("Could not find a match in collections.")

    if data and last_boilerplate(obj, "test_support_plugins") is None:
        actions.comments.append(
            template_comment(
                "test_support_plugins", {"author": obj.author, "data": "\n".join(data)}
            )
        )


def networking(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if any(
        any(c.startswith(n) for n in NETWORK_PLUGIN_TYPE_DIRS) or c in NETWORK_FILES
        for c in obj.components
    ):
        actions.to_label.append("networking")
    else:
        actions.to_unlabel.append("networking")


bot_funcs = [
    match_components,  # order matters, other funcs use detected components
    match_object_type,  # order matters, other funcs use detected object type
    needs_triage,
    waiting_on_contributor,
    needs_info,
    match_version,
    ci_comments,  # order matters, must be before needs_ci
    needs_revision,
    needs_rebase,  # order matters, must be before needs_ci
    stale_pr,  # order matters, must be before needs_ci
    needs_ci,
    stale_ci,
    pending_ci,
    backport,
    is_module,
    stale_review,
    pr_from_upstream,
    linked_objs,
    needs_template,
    test_support_plugin,
    networking,
]


def is_command_applied(name: str, obj: GH_OBJ, ctx: TriageContext) -> bool:
    applied = []
    if name in ctx.commands_found:
        applied.append(ctx.commands_found[name][-1].updated_at)
    if d := last_labeled(obj, name):
        applied.append(d)

    removed = []
    if f"!{name}" in ctx.commands_found:
        removed.append(ctx.commands_found[f"!{name}"][-1].updated_at)
    if d := last_unlabeled(obj, name):
        removed.append(d)

    last_applied = max(applied, default=None)
    last_removed = max(removed, default=None)

    if last_applied and last_removed is None:
        return True
    elif last_applied is None and last_removed:
        logging.warning("'%s' removed without being applied?", name)
    elif last_applied is None and last_removed is None:
        return False
    elif last_applied > last_removed:
        return True

    return False


# https://packaging.python.org/en/latest/specifications/version-specifiers/#version-specifiers-regex
VERSION_PATTERN = r"""
    v?
    (?:
        (?:(?P<epoch>[0-9]+)!)?                           # epoch
        (?P<release>[0-9]+(?:\.[0-9]+)*)                  # release segment
        (?P<pre>                                          # pre-release
            [-_\.]?
            (?P<pre_l>(a|b|c|rc|alpha|beta|pre|preview))
            [-_\.]?
            (?P<pre_n>[0-9]+)?
        )?
        (?P<post>                                         # post release
            (?:-(?P<post_n1>[0-9]+))
            |
            (?:
                [-_\.]?
                (?P<post_l>post|rev|r)
                [-_\.]?
                (?P<post_n2>[0-9]+)?
            )
        )?
        (?P<dev>                                          # dev release
            [-_\.]?
            (?P<dev_l>dev)
            [-_\.]?
            (?P<dev_n>[0-9]+)?
        )?
    )
    (?:\+(?P<local>[a-z0-9]+(?:[-_\.][a-z0-9]+)*))?       # local version
"""

_version_re = re.compile(
    r"^\s*" + VERSION_PATTERN + r"\s*$", re.VERBOSE | re.IGNORECASE
)


def get_oldest_supported_bugfix_version() -> tuple[int, int]:
    versions = set()
    for release in http_request(ANSIBLE_CORE_PYPI_URL).json().get("releases", []):
        if (m := _version_re.match(release)) is not None and not m.group("pre"):
            versions.add(
                t.cast(
                    tuple[int, int],
                    tuple(int(c) for c in m.group("release").split(".")[:2]),
                )
            )
    # latest 3 core versions are supported, the last one is security only though
    return sorted(versions, reverse=True)[1]


def triage(
    obj: GH_OBJ,
    ctx: TriageContext,
    dry_run: bool = False,
    ask: bool = False,
    ignore_bot_skip: bool = False,
) -> None:
    logging.info("Triaging %s %s (#%d)", obj.__class__.__name__, obj.title, obj.number)
    logging.info(obj.url)

    # commands
    bodies = itertools.chain(
        ((obj.author, obj.body, obj.updated_at),),
        (
            (e.author, e.body, e.updated_at)
            for e in obj.events
            if isinstance(e, IssueCommentEvent)
        ),
    )
    ctx.commands_found = collections.defaultdict(list)
    for author, body, updated_at in bodies:
        for command in COMMANDS_RE.findall(body):
            if command in {"bot_skip", "!bot_skip"} and author not in ctx.committers:
                continue
            ctx.commands_found[command].append(Command(updated_at=updated_at))
        if isinstance(obj, PR):
            continue
        for component in COMPONENT_COMMAND_RE.findall(body):
            ctx.commands_found["component"].append(
                Command(updated_at=updated_at, arg=component)
            )

    if not ignore_bot_skip:
        if is_command_applied("bot_broken", obj, ctx):
            obj.last_triaged_at = datetime.datetime.now(datetime.timezone.utc)
            logging.info(
                "Skipping %s %s (#%d) due to bot_broken",
                obj.__class__.__name__,
                obj.title,
                obj.number,
            )
            if not dry_run:
                add_labels(obj, ["bot_broken"], ctx)
            return
        if not dry_run:
            remove_labels(obj, ["bot_broken"])
        if is_command_applied("bot_skip", obj, ctx):
            obj.last_triaged_at = datetime.datetime.now(datetime.timezone.utc)
            logging.info(
                "Skipping %s %s (#%d) due to bot_skip",
                obj.__class__.__name__,
                obj.title,
                obj.number,
            )
            return

    # triage
    actions = Actions()
    for f in bot_funcs:
        f(obj, actions, ctx)
        if not dry_run and actions.close:
            # short-circuit the rest of the triage to avoid posting
            # information that would be irrelevant after closing
            break

    # remove bot_closed for re-opened issues/prs
    if "bot_closed" not in actions.to_label:
        actions.to_unlabel.append("bot_closed")

    logging.info("All potential actions:")
    logging.info(pprint.pformat(actions))

    actions.to_label = [
        l
        for l in actions.to_label
        if l not in obj.labels
        and not (
            (l in LABELS_DO_NOT_OVERRIDE or l.startswith("affects_"))
            and was_unlabeled_by_human(obj, l)
        )
    ]
    actions.to_unlabel = [
        l
        for l in actions.to_unlabel
        if l in obj.labels
        and not (
            (l in LABELS_DO_NOT_OVERRIDE or l.startswith("affects_"))
            and was_labeled_by_human(obj, l)
        )
    ]

    if common_labels := set(actions.to_label).intersection(actions.to_unlabel):
        raise AssertionError(
            f"The following labels were scheduled to be both added and removed {', '.join(common_labels)}"
        )

    if dry_run:
        logging.info("Skipping taking actions due to --dry-run")
    else:
        if actions:
            logging.info("Summary of actions to take:")
            logging.info(pprint.pformat(actions))

            if ask:
                user_input = input("Take actions? (y/n): ")
                take_actions = user_input.strip() == "y"
            else:
                take_actions = True

            if take_actions:
                if actions.to_label:
                    add_labels(obj, actions.to_label, ctx)
                if actions.to_unlabel:
                    remove_labels(obj, actions.to_unlabel)

                for comment in actions.comments:
                    add_comment(obj, comment)

                if actions.cancel_ci:
                    cancel_ci(obj.ci.build_id)

                if actions.close:
                    logging.info("%s #%d: closing", obj.__class__.__name__, obj.number)
                    if isinstance(obj, PR):
                        close_pr(obj.id)
                    else:
                        close_issue(obj.id, actions.close_reason)
            else:
                logging.info("Skipping taking actions")
        else:
            logging.info("No actions to take")

    obj.last_triaged_at = datetime.datetime.now(datetime.timezone.utc)
    logging.info(
        "Done triaging %s %s (#%d)", obj.__class__.__name__, obj.title, obj.number
    )


@dataclasses.dataclass(frozen=True, slots=True)
class Event:
    created_at: datetime.datetime


@dataclasses.dataclass(frozen=True, slots=True)
class LabeledEvent(Event):
    label: str
    author: str


@dataclasses.dataclass(frozen=True, slots=True)
class UnlabeledEvent(Event):
    label: str
    author: str


@dataclasses.dataclass(frozen=True, slots=True)
class IssueCommentEvent(Event):
    body: str
    updated_at: datetime.datetime
    author: str


def process_events(issue: dict[str, t.Any]) -> list[Event]:
    rv: list[Event] = []
    for node in issue["timelineItems"]["nodes"]:
        if node is None:
            continue

        created_at = datetime.datetime.fromisoformat(node["createdAt"])
        match node["__typename"]:
            case "LabeledEvent":
                rv.append(
                    LabeledEvent(
                        created_at=created_at,
                        label=node["label"]["name"],
                        author=node["actor"]["login"],
                    )
                )
            case "UnlabeledEvent":
                rv.append(
                    UnlabeledEvent(
                        created_at=created_at,
                        label=node["label"]["name"],
                        author=node["actor"]["login"],
                    )
                )
            case "IssueComment":
                rv.append(
                    IssueCommentEvent(
                        created_at=created_at,
                        body=node["body"],
                        updated_at=datetime.datetime.fromisoformat(node["updatedAt"]),
                        author=(
                            node["author"]["login"]
                            if node["author"] is not None
                            else ""
                        ),
                    )
                )

    return rv


def ratelimit_to_str(rate_limit: dict[str, t.Any]) -> str:
    return f"cost: {rate_limit['cost']}, {rate_limit['remaining']}/{rate_limit['limit']} until {rate_limit['resetAt']}"


def get_gh_objects(
    obj_name: str, query: str
) -> t.Generator[tuple[int, datetime.datetime], None, None]:
    variables: dict[str, t.Any] = {}
    while True:
        logging.info("Getting open %s", obj_name)
        resp = send_query({"query": query, "variables": variables})
        data = resp.json()["data"]
        logging.info(ratelimit_to_str(data["rateLimit"]))

        objs = data["repository"][obj_name]
        for node in objs["nodes"]:
            updated_ats = [
                node["updatedAt"],
                node["timelineItems"]["updatedAt"],
            ]
            if obj_name == "pullRequests":
                last_commit = node["commits"]["nodes"][0]["commit"]
                if ci_results := last_commit["checkSuites"]["nodes"]:
                    updated_ats.append(ci_results[0]["updatedAt"])
            yield (
                node["number"],
                max(map(datetime.datetime.fromisoformat, updated_ats)),
            )

        if objs["pageInfo"]["hasNextPage"]:
            variables["after"] = objs["pageInfo"]["endCursor"]
        else:
            break


def get_issues(q: queue.SimpleQueue):
    try:
        for issue in get_gh_objects("issues", QUERY_ISSUE_NUMBERS):
            q.put((issue, fetch_issue))
    except Exception as e:
        logging.exception(e)
    finally:
        q.put(...)


def get_prs(q: queue.SimpleQueue):
    try:
        for pr in get_gh_objects("pullRequests", QUERY_PR_NUMBERS):
            q.put((pr, fetch_pr))
    except Exception as e:
        logging.exception(e)
    finally:
        q.put(...)


def fetch_object(
    number: int,
    obj: t.Type[GH_OBJ],
    object_name: t.Literal["issue", "pullRequest"],
    query: str,
    updated_at: datetime.datetime | None = None,
) -> GH_OBJ:
    logging.info("Getting %s #%d", object_name, number)
    resp = send_query({"query": query, "variables": {"number": number}})
    data = resp.json()["data"]
    logging.info(ratelimit_to_str(data["rateLimit"]))
    o = data["repository"][object_name]
    if o is None:
        raise ValueError(f"{number} not found")

    kwargs = {
        "id": o["id"],
        "author": o["author"]["login"] if o["author"] else "ghost",
        "number": o["number"],
        "title": o["title"],
        "body": o["body"],
        "url": o["url"],
        "events": process_events(o),
        "labels": {node["name"]: node["id"] for node in o["labels"].get("nodes", [])},
        "updated_at": updated_at,
        "components": [],
        "last_triaged_at": None,
    }
    if object_name == "issue":
        kwargs["has_pr"] = bool(len(o["closedByPullRequestsReferences"]["nodes"]))
    elif object_name == "pullRequest":
        kwargs["created_at"] = datetime.datetime.fromisoformat(o["createdAt"])
        kwargs["branch"] = o["baseRef"]["name"]
        kwargs["files"] = [f["path"] for f in o["files"]["nodes"]]
        kwargs["mergeable"] = o["mergeable"].lower()
        reviews = {}
        for review in reversed(o["reviews"]["nodes"]):
            if review["isMinimized"]:
                continue
            if (author := review["author"]["login"]) not in reviews:
                reviews[author] = review["state"].lower()
        kwargs["changes_requested"] = "changes_requested" in reviews.values()
        kwargs["last_reviewed_at"] = max(
            (r["updatedAt"] for r in o["reviews"]["nodes"]), default=None
        )
        if kwargs["last_reviewed_at"]:
            kwargs["last_reviewed_at"] = datetime.datetime.fromisoformat(
                kwargs["last_reviewed_at"]
            )
        last_commit = o["last_commit"]["nodes"][0]["commit"]
        kwargs["last_committed_at"] = kwargs["pushed_at"] = (
            datetime.datetime.fromisoformat(last_commit["committedDate"])
        )

        check_run = None
        non_azp_failures = False
        for cs in last_commit["checkSuites"]["nodes"]:
            if cs.get("app", {}).get("name") == "Azure Pipelines":
                check_run = cs["checkRuns"]["nodes"][0]
            else:
                non_azp_failures |= (
                    cs["checkRuns"]["nodes"][0]["conclusion"].lower() != "success"
                )

        if (
            check_run
            and (
                (conclusion := (check_run["conclusion"] or "").lower())
                != "action_required"
            )
            and (azp_match := AZP_BUILD_ID_RE.search(check_run["detailsUrl"]))
        ):
            build_id = int(azp_match.group("buildId"))
            started_at = datetime.datetime.fromisoformat(check_run["startedAt"])
            if check_run["name"] == "CI":
                try:
                    completed_at = datetime.datetime.fromisoformat(
                        check_run["completedAt"]
                    )
                except TypeError:
                    completed_at = None
                kwargs["ci"] = CI(
                    build_id=build_id,
                    completed=check_run["status"].lower() == "completed",
                    passed=conclusion == "success",
                    cancelled=conclusion == "canceled",
                    completed_at=completed_at,
                    started_at=started_at,
                    non_azp_failures=non_azp_failures,
                )
            else:
                kwargs["ci"] = CI(
                    build_id=build_id,
                    started_at=started_at,
                )
        else:
            kwargs["ci"] = CI()

        if status_check := last_commit.get("statusCheckRollup", {}):
            kwargs["ci"].pending = status_check.get("state", "").lower() == "pending"

        repo = o["headRepository"]
        kwargs["from_repo"] = (
            f"{repo['owner']['login']}/{repo['name']}" if repo else "ghost/ghost"
        )
        kwargs["has_issue"] = len(o["closingIssuesReferences"]["nodes"]) > 0

    return obj(**kwargs)


def fetch_issue(number: int, updated_at: datetime.datetime | None = None) -> Issue:
    return fetch_object(number, Issue, "issue", QUERY_SINGLE_ISSUE, updated_at)


def fetch_pr(number: int, updated_at: datetime.datetime | None = None) -> PR:
    return fetch_object(number, PR, "pullRequest", QUERY_SINGLE_PR, updated_at)


def fetch_object_by_number(number: int) -> Issue | PR:
    try:
        return fetch_issue(number)
    except ValueError:
        return fetch_pr(number)


def fetch_objects(cache: dict[int, CacheEntry]) -> t.Generator[GH_OBJ, None, None]:
    q: queue.SimpleQueue = queue.SimpleQueue()
    workers = 2
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="WorkerThread"
    ) as executor:
        executor.submit(get_issues, q)
        executor.submit(get_prs, q)
        done = 0
        open_numbers = []
        while done < workers:
            v = q.get()
            if v is ...:
                done += 1
            else:
                (number, updated_at), fetch_func = v
                open_numbers.append(number)
                if (
                    (o := cache.get(number, None)) is None
                    or o.last_triaged_at is None
                    or o.last_triaged_at < updated_at
                    or days_since(o.last_triaged_at) >= STALE_ISSUE_DAYS
                ):
                    yield fetch_func(number, updated_at)

        for number in cache.keys() - open_numbers:
            cache.pop(number)


def daemon(
    dry_run: bool = False,
    generate_byfile: bool = False,
    ask: bool = False,
    ignore_bot_skip: bool = False,
) -> None:
    cache: dict[int, CacheEntry] = {}
    try:
        with open(CACHE_FILENAME, "rb") as cf:
            cache = pickle.load(cf)
    except (OSError, EOFError) as e:
        logging.info("Could not use cache: '%s'", e)

    ctx = TriageContext.fetch()
    while True:
        logging.info("Starting triage")
        global _http_request_counter
        _http_request_counter = 0
        start = time.time()
        if days_since(ctx.updated_at) >= 2:
            ctx = TriageContext.fetch()

        n = 0
        try:
            for n, obj in enumerate(fetch_objects(cache), 1):
                if (
                    isinstance(obj, PR)
                    and (cached_obj := cache.get(obj.number)) is not None
                ):
                    if obj.last_committed_at != cached_obj.last_committed_at:
                        obj.pushed_at = datetime.datetime.now(datetime.timezone.utc)
                    else:
                        obj.pushed_at = cached_obj.pushed_at

                try:
                    triage(obj, ctx, dry_run, ask, ignore_bot_skip)
                except TriageNextTime as e:
                    logging.warning(e)
                else:
                    cache[obj.number] = CacheEntry.from_obj(obj)
        finally:
            if n:
                with tempfile.NamedTemporaryFile(dir=".", delete=False) as f:
                    pickle.dump(cache, f)

                try:
                    os.chmod(f.name, 0o644)
                    os.rename(f.name, CACHE_FILENAME)
                except OSError:
                    os.unlink(f.name)
                    raise
                if generate_byfile:
                    generate_byfile_page(cache)

            logging.info(
                f"Took {time.time() - start:.2f} seconds and {_http_request_counter} HTTP requests to check for new/stale "
                f"issues/PRs{f' and triage {n} of them.' if n else '.'}",
            )
        logging.info("Sleeping for %d minutes", SLEEP_SECONDS // 60)
        time.sleep(SLEEP_SECONDS)


gh_token = azp_token = None


def main() -> None:
    global gh_token, azp_token

    parser = argparse.ArgumentParser(
        prog="ansibotmini",
        description=(
            "triages ansible/ansible GitHub repository issues and pull requests "
            "based on the Ansible Core Engineering team workflow"
        ),
    )
    parser.add_argument(
        "--number", type=int, help="GitHub issue or pull request number"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="do not take any actions, just print what would have been done",
    )
    parser.add_argument(
        "--generate-byfile-page",
        action="store_true",
        help=(
            f"generate {BYFILE_PAGE_FILENAME} file that contains summary of all "
            "issues and pull requests per a file in ansible/ansible repository"
        ),
    )
    parser.add_argument(
        "--ask",
        action="store_true",
        help="stop and ask user before applying actions",
    )
    parser.add_argument(
        "--ignore-bot-skip",
        action="store_true",
        help="ignore bot_skip and bot_broken commands",
    )
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s:%(threadName)s: %(message)s",
        level=logging.INFO,
        handlers=[
            logging.handlers.RotatingFileHandler(
                LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=3
            ),
            logging.StreamHandler(),
        ],
    )

    config = configparser.ConfigParser()
    config.read(CONFIG_FILENAME)

    if sentry_sdk is not None:
        try:
            sentry_dsn = config.get("default", "sentry_dsn")
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            logging.warning(
                "Option 'sentry_dsn' in the configuration file is required to integrate sentry, "
                "original error: %s",
                e,
            )
        else:
            sentry_sdk.init(
                dsn=sentry_dsn,
                attach_stacktrace=True,
                release=__version__,
            )

    try:
        gh_token = config.get("default", "gh_token")
        azp_token = config.get("default", "azp_token")
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logging.error(
            "Options 'gh_token' and 'azp_token' in the default section of the configuration file are required, "
            "original error: %s",
            e,
        )
        sys.exit(1)

    if args.number:
        triage(
            fetch_object_by_number(args.number),
            TriageContext.fetch(),
            dry_run=args.dry_run,
            ask=args.ask,
            ignore_bot_skip=args.ignore_bot_skip,
        )
    else:
        try:
            daemon(
                dry_run=args.dry_run,
                generate_byfile=args.generate_byfile_page,
                ask=args.ask,
                ignore_bot_skip=args.ignore_bot_skip,
            )
        except KeyboardInterrupt:
            print("Bye")
            sys.exit(0)
        except Exception as e:
            logging.exception(e)
            sys.exit(1)


def generate_byfile_page(cache: dict[int, CacheEntry]):
    logging.info("Generating %s", BYFILE_PAGE_FILENAME)
    component_to_numbers = collections.defaultdict(list)
    for number, entry in cache.items():
        for component in entry.components:
            component_to_numbers[component].append((number, entry))

    data = []
    for idx, (component, issues) in enumerate(
        sorted(component_to_numbers.items(), key=lambda x: len(x[1]), reverse=True),
        start=1,
    ):
        data.append(
            f'<div style="background-color: #cfc; padding: 10px; border: 1px solid green;" id="{component}">\n'
            f'{idx}. <a href="https://github.com/ansible/ansible/blob/devel/{component}">{component}</a> '
            f'{len(issues)} total <a href="#{component}">&para;</a>\n'
            "</div><br />\n"
        )
        for number, entry in sorted(issues, key=lambda x: x[0]):
            data.append(
                f'<a href="{entry.url}">#{number}</a>&emsp;{entry.title}<br />\n'
            )
        data.append("<br />\n")

    with tempfile.NamedTemporaryFile(mode="w", dir=".", delete=False) as f:
        f.write("".join(data))

    try:
        os.chmod(f.name, 0o644)
        os.rename(f.name, BYFILE_PAGE_FILENAME)
    except OSError:
        os.unlink(f.name)
        raise

    logging.info("%s generated", BYFILE_PAGE_FILENAME)


if __name__ == "__main__":
    main()
