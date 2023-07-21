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
import pprint
import re
import shelve
import string
import sys
import time
import typing as t
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None

__version__ = "0.0.1"

minimal_required_python_version = (3, 11)
if sys.version_info < minimal_required_python_version:
    raise SystemExit(
        f"ansibotmini requires Python {'.'.join((str(e) for e in minimal_required_python_version))} or newer. "
        f"Python version detected: {sys.version.split(' ', maxsplit=1)[0]}"
    )


BOT_ACCOUNT = "ansibot"

AZP_ARTIFACTS_URL_FMT = "https://dev.azure.com/ansible/ansible/_apis/build/builds/%s/artifacts?api-version=7.0"
AZP_TIMELINE_URL_FMT = "https://dev.azure.com/ansible/ansible/_apis/build/builds/%s/timeline/?api-version=7.0"
AZP_BUILD_URL_FMT = (
    "https://dev.azure.com/ansible/ansible/_apis/build/builds/%s?api-version=7.0"
)
AZP_BUILD_ID_RE = re.compile(
    r"https://dev\.azure\.com/(?P<organization>[^/]+)/(?P<project>[^/]+)/_build/results\?buildId=(?P<buildId>[0-9]+)",
)

GALAXY_URL = "https://galaxy.ansible.com/"
GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
COLLECTIONS_LIST_ENDPOINT = "https://sivel.eng.ansible.com/api/v1/collections/list"
COLLECTIONS_FILEMAP_ENDPOINT = (
    "https://sivel.eng.ansible.com/api/v1/collections/file_map"
)
COLLECTIONS_TO_REDIRECT_ENDPOINT = "https://raw.githubusercontent.com/ansible-community/ansible-build-data/main/7/ansible.in"
DEVEL_FILE_LIST = (
    "https://api.github.com/repos/ansible/ansible/git/trees/devel?recursive=1"
)
V29_FILE_LIST = (
    "https://api.github.com/repos/ansible/ansible/git/trees/v2.9.0?recursive=1"
)

STALE_CI_DAYS = 7
STALE_ISSUE_DAYS = 7
NEEDS_INFO_WARN_DAYS = 14
NEEDS_INFO_CLOSE_DAYS = 28
WAITING_ON_CONTRIBUTOR_CLOSE_DAYS = 365
SLEEP_SECONDS = 300

CONFIG_FILENAME = os.path.expanduser("~/.ansibotmini.cfg")
CACHE_FILENAME = os.path.expanduser("~/.ansibotmini_cache")
BYFILE_PAGE_FILENAME = os.path.expanduser("~/byfile.html")
LOG_FILENAME = os.path.expanduser("~/ansibotmini.log")

COMPONENT_RE = re.compile(
    r"#{3,5}\scomponent\sname(.+?)(?=#{3,5}|$)", flags=re.IGNORECASE | re.DOTALL
)
OBJ_TYPE_RE = re.compile(
    r"#{3,5}\sissue\stype(.+?)(?=#{3,5}|$)", flags=re.IGNORECASE | re.DOTALL
)
VERSION_RE = re.compile(r"ansible\s\[core\s([^]]+)]")
COMPONENT_COMMAND_RE = re.compile(
    r"^(?:@ansibot\s)?!?component\s([=+-]\S+)$", flags=re.MULTILINE
)
FLATTEN_MODULES_RE = re.compile(r"lib/ansible/modules/(.*)/(.+\.(?:py|ps1))")

VALID_COMMANDS = (
    "bot_skip",
    "!bot_skip",
    "bot_broken",
    "!bot_broken",
    "!needs_collection_redirect",
)
COMMANDS_RE = re.compile(
    f"^(?:@ansibot\s)?({'|'.join(VALID_COMMANDS)})\s*$", flags=re.MULTILINE
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


# TODO fetch from the actual template
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

LABLES_DO_NOT_OVERRIDE = {
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
      timelineItems(first: 200, itemTypes: [ISSUE_COMMENT, LABELED_EVENT, UNLABELED_EVENT, CROSS_REFERENCED_EVENT]) {
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
          ... on CrossReferencedEvent {
            createdAt
            source {
              ... on PullRequest {
                number
                repository {
                  name
                  owner {
                    ... on Organization {
                      name
                    }
                  }
                }
              }
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

QUERY_SINGLE_ISSUE = QUERY_SINGLE_TMPL % ("issue", "")

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
      committedDate
      checkSuites(last: 1) {
        nodes {
          createdAt
          checkRuns(last: 1) {
            nodes {
              name
              detailsUrl
              completedAt
              conclusion
              status
            }
          }
        }
      }
    }
  }
}
commits(last: 50) {
  nodes {
    commit {
      oid
      parents(last: 2) {
        nodes {
          id
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


@dataclass
class Response:
    status_code: int
    reason: str
    raw_data: bytes

    def json(self) -> t.Any:
        return json.loads((self.raw_data or b"{}").decode())


@dataclass
class Issue:
    id: str
    author: str
    number: int
    title: str
    body: str
    url: str
    events: list[dict]
    labels: dict[str, str]
    updated_at: datetime.datetime
    components: list[str]
    last_triaged: datetime.datetime | None


@dataclass
class PR(Issue):
    branch: str
    files: list[str]
    mergeable: str
    changes_requested: bool
    last_review: datetime.datetime
    last_commit: datetime.datetime
    ci: CI
    from_repo: str
    merge_commits: list[str]
    has_issue: bool
    created_at: datetime.datetime


@dataclass
class CI:
    build_id: t.Optional[int] = None
    completed: bool = False
    passed: bool = False
    cancelled: bool = False
    completed_at: t.Optional[datetime.datetime] = None
    created_at: t.Optional[datetime.datetime] = None

    def is_running(self) -> bool:
        return self.build_id is not None and not self.completed


@dataclass
class Command:
    updated_at: datetime.datetime
    arg: t.Optional[str] = None


@dataclass
class Actions:
    to_label: list[str] = dataclasses.field(default_factory=list)
    to_unlabel: list[str] = dataclasses.field(default_factory=list)
    comments: list[str] = dataclasses.field(default_factory=list)
    cancel_ci: bool = False
    close: bool = False
    close_reason: t.Optional[str] = None

    def __bool__(self):
        return bool(
            self.to_label
            or self.to_unlabel
            or self.comments
            or self.cancel_ci
            or self.close
        )


@dataclass
class TriageContext:
    collections_list: dict[str, t.Any]
    collections_file_map: dict[str, t.Any]
    committers: list[str]
    devel_file_list: list[str]
    v29_file_list: list[str]
    v29_flatten_modules: list[str]
    collections_to_redirect: list[str]
    commands_found: dict[str, list[Command]] = dataclasses.field(default_factory=dict)


GH_OBJ = t.TypeVar("GH_OBJ", Issue, PR)
GH_OBJ_T = t.TypeVar("GH_OBJ_T", t.Type[Issue], t.Type[PR])

request_counter = 0


def http_request(
    url: str,
    data: str = "",
    headers: t.Optional[t.MutableMapping[str, str]] = None,
    method: str = "GET",
    retries: int = 2,
) -> Response:
    global request_counter
    if headers is None:
        headers = {}

    for i in range(retries):
        try:
            with urllib.request.urlopen(
                urllib.request.Request(
                    url,
                    data=data.encode("ascii"),
                    headers=headers,
                    method=method.upper(),
                ),
            ) as response:
                request_counter += 1
                logging.info(
                    "http request no. %d: %s %s: %d, %s",
                    request_counter,
                    method,
                    url,
                    response.status,
                    response.reason,
                )
                return Response(
                    status_code=response.status,
                    reason=response.reason,
                    raw_data=response.read(),
                )
        except urllib.error.HTTPError as e:
            logging.info("%s %s %s", method, url, e)
            if e.status >= 500 and i < retries - 1:
                logging.info("Retrying the request...")
                continue
            return Response(
                status_code=e.status,
                reason=e.reason,
                raw_data=b"",
            )


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
    return resp.json()["data"]["repository"]["label"]["id"]


def add_labels(obj: GH_OBJ, labels: list[str]) -> None:
    # TODO gather label IDs globally from processed issues to limit API calls to get IDs
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
                    "labelIds": [get_label_id(label) for label in labels],
                    "labelableId": obj.id,
                },
            },
        }
    )


def remove_labels(obj: GH_OBJ, labels: list[str]) -> None:
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
                    "labelIds": [
                        obj.labels[label] for label in labels if label in obj.labels
                    ],
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
                c = c.strip()
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

                if c := re.sub(r"[^1a-zA-Z/._-]", "", c):
                    if (flatten := flatten_module_path(c)) != c:
                        rv.append(flatten)
                    if len(c) > 1:
                        rv.append(c)

    return rv


def flatten_module_path(c: str) -> str:
    return re.sub(r"(lib/ansible/modules)/(.*)(/.+\.(?:py|ps1))", r"\1\3", c)


def template_comment(template_name: str, sub_map: t.Optional[t.Mapping] = None) -> str:
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
    files = []
    for filename in filenames:
        if filename == "core":
            continue
        if matched_filename := COMPONENT_TO_FILENAME.get(filename):
            files.append(matched_filename)
            continue

        files.append(filename)
        if "/" not in filename:
            for path in paths:
                files.append(f"{path}{filename}.py")
                files.append(f"{path}{filename}")

    components = [f for f in files if f in existing_files]

    if not components:
        for file in filenames:
            components.extend(
                difflib.get_close_matches(file, existing_files, cutoff=0.85)
            )

    return list(set(components))


def was_labeled_by_human(obj: GH_OBJ, label_name: str) -> bool:
    return any(
        e
        for e in obj.events
        if e["name"] == "LabeledEvent"
        and e["label"] == label_name
        and e["author"] != BOT_ACCOUNT
    )


def was_unlabeled_by_human(obj: GH_OBJ, label_name: str) -> bool:
    return any(
        e
        for e in obj.events
        if e["name"] == "UnlabeledEvent"
        and e["label"] == label_name
        and e["author"] != BOT_ACCOUNT
    )


def last_labeled(obj: GH_OBJ, name: str) -> datetime.datetime:
    return max(
        (
            e["created_at"]
            for e in obj.events
            if e["name"] == "LabeledEvent" and e["label"] == name
        ),
        default=None,
    )


def last_unlabeled(obj: GH_OBJ, name: str) -> datetime.datetime:
    return max(
        (
            e["created_at"]
            for e in obj.events
            if e["name"] == "UnlabeledEvent" and e["label"] == name
        ),
        default=None,
    )


def last_commented_by(obj: GH_OBJ, name: str) -> datetime.datetime | None:
    return max(
        (
            e["created_at"]
            for e in obj.events
            if e["name"] == "IssueComment" and e["author"] == name
        ),
        default=None,
    )


def last_boilerplate(obj: GH_OBJ, name: str) -> dict[str, t.Any] | None:
    return max(
        (
            e
            for e in obj.events
            if e["name"] == "IssueComment"
            and e["author"] == BOT_ACCOUNT
            and f"<!--- boilerplate: {name} --->" in e["body"]
        ),
        key=lambda x: x["created_at"],
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

        command_components = []
        for command in ctx.commands_found.get("component", []):
            op, path = command.arg[0], command.arg[1:]
            command_components.append(path)
            if path not in ctx.devel_file_list and not is_in_collection(
                [path], [], ctx
            ):
                continue
            match op:
                case "=":
                    existing_components = [path]
                case "+":
                    existing_components.append(path)
                case "-":
                    if path in existing_components:
                        existing_components.remove(path)
                case _:
                    raise ValueError(
                        f"Incorrect operation for the component command: {op}"
                    )

        post_comments_banner = True

        if (
            not existing_components
            and "!needs_collection_redirect" not in ctx.commands_found
        ):
            if entries := is_in_collection(
                command_components, processed_components, ctx
            ):
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
                    for line in last_comment["body"].splitlines()
                    if line.startswith("*")
                ]
                post_comments_banner = sorted(existing_components) != sorted(
                    last_components
                )

            last_command = ctx.commands_found.get("component", [])[-1:]
            if post_comments_banner and (
                is_new_issue(obj)
                or (
                    last_comment
                    and last_command
                    and last_comment["created_at"] < last_command[0].updated_at
                )
            ):
                entries = [
                    f"* [`{component}`](https://github.com/ansible/ansible/blob/devel/{component})"
                    for component in existing_components
                ]
                actions.comments.append(
                    template_comment(
                        "components_banner",
                        {"components": "\n".join(entries) if entries else None},
                    )
                )

    obj.components = existing_components

    logging.info(
        "%s #%d: identified components: %s",
        obj.__class__.__name__,
        obj.number,
        ", ".join(obj.components),
    )


def is_in_collection(
    command_components: list[str], processed_components: list[str], ctx: TriageContext
) -> dict[str, list[str]]:
    entries = collections.defaultdict(list)

    for component in processed_components:
        fqcn = component.split(".")
        if len(fqcn) != 3:
            continue
        fqcn = ".".join(fqcn[:2])
        if fqcn in ctx.collections_list and fqcn in ctx.collections_to_redirect:
            entries[component].append(fqcn)

    for component in itertools.chain(processed_components, command_components):
        if "/" not in component:
            continue
        flatten = re.sub(
            rf"lib/ansible/(?:plugins/)?({'|'.join(itertools.chain(ANSIBLE_PLUGINS, ['modules']))})(.*)/(.+\.(?:py|ps1))",
            r"plugins/\1/\3",
            component,
        )
        for fqcn in ctx.collections_file_map.get(flatten, []):
            if fqcn in ctx.collections_to_redirect:
                entries[flatten].append(fqcn)
        if entries:
            break
    else:
        for component, plugin_type, ext in itertools.product(
            (c for c in processed_components if "/" not in c),
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
                            entries[candidate].append(fqcn)
                    else:
                        if f"lib/ansible/{candidate}" in ctx.v29_file_list:
                            entries[candidate].append(fqcn)
    return entries


def needs_triage(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if is_new_issue(obj):
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
                    or last_warned["created_at"] < needs_info_labeled_date
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
        for m in re.findall(r"\b(feature|bug|test)\b", data, flags=re.MULTILINE):
            actions.to_label.append(m)


def match_version(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if isinstance(obj, PR):
        return
    if match := VERSION_RE.search(obj.body):
        actions.to_label.append(f"affects_{'.'.join(match.group(1).split('.')[:2])}")


def ci_comments(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR) or not obj.ci.completed:
        return
    resp = http_request(AZP_TIMELINE_URL_FMT % obj.ci.build_id)
    if resp.status_code == 404:
        # not available anymore
        if obj.ci.passed:
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
        results = "\n".join(ci_comment)
        r_hash = hashlib.md5(results.encode()).hexdigest()
        if not any(
            e
            for e in obj.events
            if e["name"] == "IssueComment"
            and e["author"] == BOT_ACCOUNT
            and "<!--- boilerplate: ci_test_result --->" in e["body"]
            and f"<!-- r_hash: {r_hash} -->" in e["body"]
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
        and obj.last_review is not None
        and obj.last_review > obj.last_commit
    ) or not obj.ci.passed:
        actions.to_label.append("needs_revision")
    else:
        actions.to_unlabel.append("needs_revision")


def needs_ci(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if (
        not isinstance(obj, PR)
        or (datetime.datetime.now(datetime.timezone.utc) - obj.created_at).seconds
        < 5 * 60
    ):
        return

    if (
        obj.ci.build_id is None
        or (obj.ci.cancelled and "merge_commit" not in actions.to_label)
        or (
            obj.ci.is_running()
            and (
                datetime.datetime.now(datetime.timezone.utc) - obj.ci.created_at
            ).seconds
            > 2 * 60 * 60
        )
    ):
        if "pre_azp" not in obj.labels:
            actions.to_label.append("needs_ci")
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
    if obj.mergeable == "conflicting":
        actions.to_label.append("needs_rebase")
    else:
        actions.to_unlabel.append("needs_rebase")


def stale_review(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR) or obj.last_review is None:
        return
    if obj.last_review < obj.last_commit:
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
        method="patch",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic {}".format(
                base64.b64encode(f":{azp_token}".encode()).decode()
            ),
        },
        data=json.dumps({"status": "Cancelling"}),
    )
    logging.info("Cancelled with status_code: %d", resp.status_code)


def merge_commits(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR):
        return
    if obj.merge_commits:
        if obj.ci.is_running():
            actions.cancel_ci = True
        if not last_boilerplate(obj, "merge_commit_notify"):
            actions.comments.append(
                template_comment(
                    "merge_commit_notify",
                    {
                        "author": obj.author,
                        "commits": "\n".join((f"* {c}" for c in obj.merge_commits)),
                    },
                )
            )
        actions.to_label.append("merge_commit")
    else:
        actions.to_unlabel.append("merge_commit")


def linked_objs(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if isinstance(obj, PR):
        if obj.has_issue:
            actions.to_label.append("has_issue")
        else:
            actions.to_unlabel.append("has_issue")
    elif isinstance(obj, Issue):
        if [e for e in obj.events if e["name"] == "CrossReferencedEvent"]:
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
        if is_new_issue(obj):  # do not spam old issues
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


def is_new_issue(obj: GH_OBJ) -> bool:
    return not any(
        e
        for e in obj.events
        if e["name"] == "LabeledEvent" and e["label"] in ("needs_triage", "triage")
    )


def test_support_plugin(obj: GH_OBJ, actions: Actions, ctx: TriageContext) -> None:
    if not isinstance(obj, PR):
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
    merge_commits,  # order matters, must be before needs_ci
    ci_comments,  # order matters, must be before needs_ci
    needs_revision,
    needs_ci,
    stale_ci,
    backport,
    is_module,
    needs_rebase,
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
    if last_applied is None and last_removed:
        raise AssertionError("Removed without being applied?")
    if last_applied is None and last_removed is None:
        return False
    if last_applied > last_removed:
        return True

    return False


def triage(
    objects: dict[str, GH_OBJ],
    dry_run: bool = False,
    ask: bool = False,
    ignore_bot_skip: bool = False,
) -> None:
    # FIXME cache TriageContext
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
            v29_flatten_modules.append(
                re.sub(FLATTEN_MODULES_RE, r"plugins/modules/\2", f)
            )

    ctx = TriageContext(
        collections_list=http_request(COLLECTIONS_LIST_ENDPOINT).json(),
        collections_file_map=http_request(COLLECTIONS_FILEMAP_ENDPOINT).json(),
        committers=get_committers(),
        collections_to_redirect=http_request(COLLECTIONS_TO_REDIRECT_ENDPOINT)
        .raw_data.decode()
        .splitlines(),
        devel_file_list=devel_file_list,
        v29_file_list=v29_file_list,
        v29_flatten_modules=v29_flatten_modules,
    )
    for obj in objects.values():
        logging.info(
            "Triaging %s %s (#%d)", obj.__class__.__name__, obj.title, obj.number
        )
        logging.info(obj.url)
        # commands
        bodies = itertools.chain(
            ((obj.author, obj.body, obj.updated_at),),
            (
                (e["author"], e["body"], e["updated_at"])
                for e in obj.events
                if e["name"] == "IssueComment"
            ),
        )
        ctx.commands_found = collections.defaultdict(list)
        for author, body, updated_at in bodies:
            for command in COMMANDS_RE.findall(body):
                if (
                    command in {"bot_skip", "!bot_skip"}
                    and author not in ctx.committers
                ):
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
                obj.last_triaged = datetime.datetime.now(datetime.timezone.utc)
                logging.info(
                    "Skipping %s %s (#%d) due to bot_broken",
                    obj.__class__.__name__,
                    obj.title,
                    obj.number,
                )
                if not dry_run:
                    add_labels(obj, ["bot_broken"])
                continue
            if not dry_run:
                remove_labels(obj, ["bot_broken"])
            if is_command_applied("bot_skip", obj, ctx):
                obj.last_triaged = datetime.datetime.now(datetime.timezone.utc)
                logging.info(
                    "Skipping %s %s (#%d) due to bot_skip",
                    obj.__class__.__name__,
                    obj.title,
                    obj.number,
                )
                continue

        # triage
        actions = Actions()
        for f in bot_funcs:
            f(obj, actions, ctx)

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
                (l in LABLES_DO_NOT_OVERRIDE or l.startswith("affects_"))
                and was_unlabeled_by_human(obj, l)
            )
        ]
        actions.to_unlabel = [
            l
            for l in actions.to_unlabel
            if l in obj.labels
            and not (
                (l in LABLES_DO_NOT_OVERRIDE or l.startswith("affects_"))
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
                        add_labels(obj, actions.to_label)
                    if actions.to_unlabel:
                        remove_labels(obj, actions.to_unlabel)

                    for comment in actions.comments:
                        add_comment(obj, comment)

                    if actions.cancel_ci:
                        cancel_ci(obj.ci.build_id)

                    if actions.close:
                        logging.info(
                            "%s #%d: closing", obj.__class__.__name__, obj.number
                        )
                        if isinstance(obj, PR):
                            close_pr(obj.id)
                        elif isinstance(obj, Issue):
                            close_issue(obj.id, actions.close_reason)
                else:
                    logging.info("Skipping taking actions")
            else:
                logging.info("No actions to take")

        obj.last_triaged = datetime.datetime.now(datetime.timezone.utc)
        logging.info(
            "Done triaging %s %s (#%d)", obj.__class__.__name__, obj.title, obj.number
        )


def process_events(issue: dict[str, t.Any]) -> list[dict[str, str]]:
    rv = []
    for node in issue["timelineItems"]["nodes"]:
        if node is None:
            continue
        event = dict(
            name=node["__typename"],
            created_at=datetime.datetime.fromisoformat(node["createdAt"]),
        )
        if node["__typename"] in ["LabeledEvent", "UnlabeledEvent"]:
            event["label"] = node["label"]["name"]
            event["author"] = node["actor"]["login"]
        elif node["__typename"] == "IssueComment":
            event["body"] = node["body"]
            event["updated_at"] = datetime.datetime.fromisoformat(node["updatedAt"])
            event["author"] = (
                node["author"]["login"] if node["author"] is not None else ""
            )
        elif node["__typename"] == "CrossReferencedEvent" and node["source"]:
            event["number"] = node["source"]["number"]
            event["repo"] = node["source"]["repository"]
            event["owner"] = (
                node["source"]["repository"].get("owner", {}).get("name", "")
            )
        else:
            continue
        rv.append(event)

    return rv


def ratelimit_to_str(rate_limit: dict[str, t.Any]) -> str:
    return f"cost: {rate_limit['cost']}, {rate_limit['remaining']}/{rate_limit['limit']} until {rate_limit['resetAt']}"


def get_gh_objects(obj_name: str) -> list[tuple[str, datetime.datetime]]:
    logging.info("Getting open %s", obj_name)
    query = QUERY_ISSUE_NUMBERS if obj_name == "issues" else QUERY_PR_NUMBERS
    rv = []
    variables = {}
    while True:
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
            rv.append(
                (
                    str(node["number"]),
                    max(map(datetime.datetime.fromisoformat, updated_ats)),
                )
            )

        if objs["pageInfo"]["hasNextPage"]:
            variables["after"] = objs["pageInfo"]["endCursor"]
        else:
            break

    return rv


def fetch_object(
    number: str,
    obj: GH_OBJ_T,
    object_name: str,
    updated_at: t.Optional[datetime.datetime] = None,
) -> GH_OBJ:
    logging.info("Getting %s #%s", object_name, number)
    query = QUERY_SINGLE_ISSUE if object_name == "issue" else QUERY_SINGLE_PR
    resp = send_query({"query": query, "variables": {"number": int(number)}})
    data = resp.json()["data"]
    logging.info(ratelimit_to_str(data["rateLimit"]))
    o = data["repository"][object_name]
    if o is None:
        raise ValueError(f"{number} not found")

    kwargs = dict(
        id=o["id"],
        author=o["author"]["login"] if o["author"] else "ghost",
        number=o["number"],
        title=o["title"],
        body=o["body"],
        url=o["url"],
        events=process_events(o),
        labels={node["name"]: node["id"] for node in o["labels"].get("nodes", [])},
        updated_at=updated_at,
        components=[],
        last_triaged=None,
    )
    if object_name == "pullRequest":
        kwargs["created_at"] = datetime.datetime.fromisoformat(o["createdAt"])
        kwargs["branch"] = o["baseRef"]["name"]
        kwargs["files"] = [f["path"] for f in o["files"]["nodes"]]
        kwargs["mergeable"] = o["mergeable"].lower()
        reviews = {}
        for review in reversed(o["reviews"]["nodes"]):
            state = review["state"].lower()
            if state not in ("changes_requested", "dismissed"):
                continue
            author = review["author"]["login"]
            if author not in reviews:
                reviews[author] = state
        kwargs["changes_requested"] = "changes_requested" in reviews.values()
        kwargs["last_review"] = max(
            (r["updatedAt"] for r in o["reviews"]["nodes"]), default=None
        )
        if kwargs["last_review"]:
            kwargs["last_review"] = datetime.datetime.fromisoformat(
                kwargs["last_review"]
            )
        kwargs["last_commit"] = datetime.datetime.fromisoformat(
            o["last_commit"]["nodes"][0]["commit"]["committedDate"]
        )
        if check_suite := o["last_commit"]["nodes"][0]["commit"]["checkSuites"][
            "nodes"
        ]:
            check_run = check_suite[0]["checkRuns"]["nodes"][0]
            build_id = AZP_BUILD_ID_RE.search(check_run["detailsUrl"]).group("buildId")
            created_at = datetime.datetime.fromisoformat(check_suite[0]["createdAt"])
            if check_run["name"] == "CI":
                try:
                    completed_at = datetime.datetime.fromisoformat(
                        check_run["completedAt"]
                    )
                except TypeError:
                    completed_at = None
                conclusion = (check_run["conclusion"] or "").lower()
                kwargs["ci"] = CI(
                    build_id=build_id,
                    completed=check_run["status"].lower() == "completed",
                    passed=conclusion == "success",
                    cancelled=conclusion == "canceled",
                    completed_at=completed_at,
                    created_at=created_at,
                )
            else:
                kwargs["ci"] = CI(
                    build_id=build_id,
                    created_at=created_at,
                )
        else:
            kwargs["ci"] = CI()

        repo = o["headRepository"]
        kwargs["from_repo"] = (
            f"{repo['owner']['login']}/{repo['name']}" if repo else "ghost/ghost"
        )
        kwargs["merge_commits"] = [
            n["commit"]["oid"]
            for n in o["commits"]["nodes"]
            if len(n["commit"]["parents"]["nodes"]) > 1
        ]
        kwargs["has_issue"] = len(o["closingIssuesReferences"]["nodes"]) > 0

    return obj(**kwargs)


def fetch_object_by_number(number: str) -> GH_OBJ:
    try:
        obj = fetch_object(number, Issue, "issue")
    except ValueError:
        obj = fetch_object(number, PR, "pullRequest")

    return obj


def fetch_objects(force_all_from_cache: bool = False) -> dict[str, GH_OBJ]:
    if force_all_from_cache:
        with shelve.open(CACHE_FILENAME) as cache:
            if data := dict(cache):
                return data
            raise RuntimeError("Empty cache")

    with shelve.open(CACHE_FILENAME) as cache:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(get_gh_objects, "issues"): "issues",
                executor.submit(get_gh_objects, "pullRequests"): "prs",
            }
            number_map = collections.defaultdict(list)
            for future in concurrent.futures.as_completed(futures):
                issue_type = futures[future]
                number_map[issue_type] = [
                    (number, updated_at)
                    for number, updated_at in future.result()
                    if number not in cache
                    or cache[str(number)].last_triaged is None
                    or cache[str(number)].last_triaged < updated_at
                    or days_since(cache[str(number)].last_triaged) >= STALE_ISSUE_DAYS
                ]

    data = {}
    for number, updated_at in number_map["issues"]:
        obj = fetch_object(number, Issue, "issue", updated_at)
        data[str(obj.number)] = obj
    for number, updated_at in number_map["prs"]:
        obj = fetch_object(number, PR, "pullRequest", updated_at)
        data[str(obj.number)] = obj

    if data:
        with shelve.open(CACHE_FILENAME) as cache:
            cache.update(data)
    return data


def daemon(
    dry_run: bool = False,
    generate_byfile: bool = False,
    ask: bool = False,
    ignore_bot_skip: bool = False,
    force_all_from_cache: bool = False,
) -> None:
    global request_counter
    while True:
        logging.info("Starting triage")
        request_counter = 0
        start = time.time()
        if objs := fetch_objects(force_all_from_cache):
            try:
                triage(objs, dry_run, ask, ignore_bot_skip)
            finally:
                logging.info("Caching triaged issues")
                with shelve.open(CACHE_FILENAME) as cache:
                    for number, obj in objs.items():
                        cache[str(number)] = obj
                logging.info(
                    "Took %.2f seconds to triage %d issues/PRs and %d HTTP requests",
                    time.time() - start,
                    len(objs),
                    request_counter,
                )
                if generate_byfile:
                    logging.info("Generating %s", BYFILE_PAGE_FILENAME)
                    generate_byfile_page()
                    logging.info("Done generating %s", BYFILE_PAGE_FILENAME)
        else:
            logging.info("No new issues/PRs")
            logging.info(
                "Took %.2f seconds to check for new issues/PRs and %d HTTP requests",
                time.time() - start,
                request_counter,
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
    parser.add_argument("--number", help="GitHub issue or pull request number")
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
    parser.add_argument(
        "--force-all-from-cache",
        action="store_true",
        help=(
            "force triaging all issues and pull requests from cache, "
            "for testing purposes, not applicable with --number"
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
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
        obj = fetch_object_by_number(args.number)
        triage(
            {args.number: obj},
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
                force_all_from_cache=args.force_all_from_cache,
            )
        except KeyboardInterrupt:
            print("Bye")
            sys.exit(0)
        except Exception as e:
            logging.exception(e)
            sys.exit(1)


def generate_byfile_page():
    with shelve.open(CACHE_FILENAME) as objs:
        component_to_numbers = collections.defaultdict(list)
        for obj in objs.values():
            for component in obj.components:
                component_to_numbers[component].append(obj)

    data = []
    for idx, (component, issues) in enumerate(
        sorted(component_to_numbers.items(), key=lambda x: len(x[1]), reverse=True),
        start=1,
    ):
        component_url = f"https://github.com/ansible/ansible/blob/devel/{component}"
        data.append(
            '<div style="background-color: #cfc; padding: 10px; border: 1px solid green;">\n'
            f'{idx}. <a href="{component_url}">{component_url}</a> {len(issues)} total\n'
            "</div><br />\n"
        )
        for obj in sorted(issues, key=lambda x: x.number):
            otype = "pull" if isinstance(obj, PR) else "issues"
            issue_url = f"https://github.com/ansible/ansible/{otype}/{obj.number}"
            data.append(f'<a href="{issue_url}">{issue_url}</a>\t{obj.title}<br />\n')
        data.append("<br />\n")

    with open(BYFILE_PAGE_FILENAME, "w") as f:
        f.write("".join(data))


if __name__ == "__main__":
    main()
