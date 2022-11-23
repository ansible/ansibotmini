#!/usr/bin/env python3
# Copyright 2022 Martin Krizek <martin.krizek@gmail.com>
# GNU General Public License v3.0+ (see LICENSE or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import configparser
import datetime
import itertools
import json
import logging
import os.path
import re
import shelve
import string
import sys
import time
import typing as t
import urllib.parse
import urllib.request
from dataclasses import dataclass

STALE_ISSUE_DAYS = 7
NEEDS_INFO_WARN_DAYS = 14
NEEDS_INFO_CLOSE_DAYS = 28
WAITING_ON_CONTRIBUTOR_CLOSE_DAYS = 365
SLEEP_SECONDS = 300
CONFIG_FILENAME = os.path.expanduser("~/.ansibotmini.cfg")
CACHE_FILENAME = os.path.expanduser("~/.ansibotmini_cache")
GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
config = configparser.ConfigParser()
config.read(CONFIG_FILENAME)
gh_token = config.get("default", "gh_token")

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {gh_token}",
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
          app {
            slug
          }
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
      labels (first: 20) {
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
              ... on Issue {
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
baseRef {
  name
}
files(first: 50) {
  nodes {
    path
  }
}
""",
)


@dataclass
class Response:
    status_code: int
    reason: str
    text: str
    ok: bool

    def json(self) -> t.Any:
        return json.loads(self.text)


@dataclass
class Issue:
    id: str
    author: str
    number: int
    title: str
    body: str
    events: list[dict]
    labels: dict[str, str]
    updated_at: datetime.datetime
    components: list[str]
    last_triaged: datetime.datetime


@dataclass
class PR(Issue):
    branch: str
    files: list[str]


GH_OBJ = t.TypeVar("GH_OBJ", Issue, PR)
GH_OBJ_T = t.TypeVar("GH_OBJ_T", t.Type[Issue], t.Type[PR])

request_counter = 0


def http_request(
    url: str,
    data: str = "",
    headers: t.Optional[t.MutableMapping[str, str]] = None,
    method: str = "GET",
    encoding: str = "utf-8",
) -> Response:
    global request_counter
    if headers is None:
        headers = {}

    with urllib.request.urlopen(
        urllib.request.Request(
            url, data=data.encode("ascii"), headers=headers, method=method.upper()
        ),
    ) as response:
        request_counter += 1
        logging.info(
            f"http request no. {request_counter}: {method} {url}: {response.status}, {response.reason}"
        )
        return Response(
            status_code=response.status,
            reason=response.reason,
            text=response.read().decode(encoding),
            ok=response.status == 200,
        )


def send_query(data: str) -> Response:
    return http_request(
        GITHUB_GRAPHQL_URL,
        method="POST",
        headers=HEADERS,
        data=data,
    )


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
    resp = send_query(
        json.dumps(
            {
                "query": query,
                "variables": {"name": name},
            }
        )
    )

    data = resp.json()["data"]
    return data["repository"]["label"]["id"]


def add_labels(obj: GH_OBJ, labels: list[str]) -> None:
    # TODO gather label IDs globally from processed issues to limit API calls to get IDs
    label_id_to_name_map = {
        get_label_id(label): label for label in labels if label not in obj.labels
    }
    if not label_id_to_name_map:
        return

    logging.info(
        f"{obj.__class__.__name__} #{obj.number}: adding {', '.join(label_id_to_name_map.values())} labels"
    )

    query = """
    mutation($input: AddLabelsToLabelableInput!) {
      addLabelsToLabelable(input:$input) {
        clientMutationId
      }
    }
    """
    send_query(
        json.dumps(
            {
                "query": query,
                "variables": {
                    "input": {
                        "labelIds": list(label_id_to_name_map.keys()),
                        "labelableId": obj.id,
                    },
                },
            }
        )
    )


def remove_labels(obj: GH_OBJ, labels: list[str]) -> None:
    label_id_to_name_map = {
        obj.labels[label]: label for label in labels if label in obj.labels
    }
    if not label_id_to_name_map:
        return

    logging.info(
        f"{obj.__class__.__name__} #{obj.number}: removing {', '.join(label_id_to_name_map.values())} labels"
    )
    query = """
    mutation($input: RemoveLabelsFromLabelableInput!) {
      removeLabelsFromLabelable(input:$input) {
        clientMutationId
      }
    }
    """
    send_query(
        json.dumps(
            {
                "query": query,
                "variables": {
                    "input": {
                        "labelIds": list(label_id_to_name_map.keys()),
                        "labelableId": obj.id,
                    },
                },
            }
        )
    )


def add_comment(obj: GH_OBJ, body: str) -> None:
    logging.info(f"{obj.__class__.__name__} #{obj.number}: adding a comment: '{body}'")
    query = """
    mutation($input: AddCommentInput!) {
      addComment(input:$input) {
        clientMutationId
      }
    }
    """
    send_query(
        json.dumps(
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
    )


def close_issue(obj_id: str) -> None:
    query = """
    mutation($input: CloseIssueInput!) {
      closeIssue(input:$input) {
        clientMutationId
      }
    }
    """
    send_query(
        json.dumps(
            {
                "query": query,
                "variables": {
                    "input": {
                        "issueId": obj_id,
                    },
                },
            }
        )
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
        json.dumps(
            {
                "query": query,
                "variables": {
                    "input": {
                        "pullRequestId": obj_id,
                    },
                },
            }
        )
    )


def close_object(obj: GH_OBJ) -> None:
    logging.info(f"{obj.__class__.__name__} #{obj.number}: closing")
    if isinstance(obj, Issue):
        close_issue(obj.id)
    elif isinstance(obj, PR):
        close_pr(obj.id)


def process_events(issue: dict[str, t.Any]) -> list[dict[str, str]]:
    rv = []
    for node in issue["timelineItems"]["nodes"]:
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
        elif node["__typename"] == "CrossReferencedEvent":
            event["number"] = node["source"]["number"]
            event["repo"] = node["source"]["repository"]
            event["owner"] = (
                node["source"]["repository"].get("owner", {}).get("name", "")
            )
        rv.append(event)

    return rv


def get_gh_objects(obj_name: str) -> list[tuple[str, datetime.datetime]]:
    query = QUERY_ISSUE_NUMBERS if obj_name == "issues" else QUERY_PR_NUMBERS
    rv = []
    variables = {}
    while True:
        resp = send_query(
            json.dumps(
                {
                    "query": query,
                    "variables": variables,
                }
            )
        )
        data = resp.json()["data"]
        logging.info(data["rateLimit"])

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
    query = QUERY_SINGLE_ISSUE if object_name == "issue" else QUERY_SINGLE_PR
    resp = send_query(
        json.dumps(
            {
                "query": query,
                "variables": {"number": int(number)},
            }
        )
    )
    data = resp.json()["data"]
    logging.info(data["rateLimit"])
    o = data["repository"][object_name]
    if o is None:
        raise ValueError(f"{number} not found")

    kwargs = dict(
        id=o["id"],
        author=o["author"]["login"] if o["author"] else "ghost",
        number=o["number"],
        title=o["title"],
        body=o["body"],
        events=process_events(o),
        labels={node["name"]: node["id"] for node in o["labels"].get("nodes", [])},
        updated_at=updated_at,
        components=[],
        last_triaged=datetime.datetime.now(datetime.timezone.utc),
    )
    if object_name == "pullRequest":
        kwargs["branch"] = o["baseRef"]["name"]
        kwargs["files"] = [f["path"] for f in o["files"]["nodes"]]

    return obj(**kwargs)


def fetch_objects() -> dict[str, GH_OBJ]:
    with shelve.open(CACHE_FILENAME) as cache:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(get_gh_objects, "issues"): "issues",
                executor.submit(get_gh_objects, "pullRequests"): "prs",
            }
            number_map = collections.defaultdict(list)
            for future in concurrent.futures.as_completed(futures):
                issue_type = futures[future]
                now = datetime.datetime.now(datetime.timezone.utc)
                number_map[issue_type] = [
                    (number, updated_at)
                    for number, updated_at in future.result()
                    if number not in cache
                    or cache[str(number)].updated_at < updated_at
                    or (now - cache[str(number)].last_triaged).days >= STALE_ISSUE_DAYS
                ]

        if not number_map["issues"] and not number_map["prs"]:
            return {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for object_name, obj, data in (
                ("issue", Issue, number_map["issues"]),
                ("pullRequest", PR, number_map["prs"]),
            ):
                for number, updated_at in data:
                    futures.append(
                        executor.submit(
                            fetch_object,
                            number,
                            obj,
                            object_name,
                            updated_at,
                        )
                    )

            data = {}
            for future in concurrent.futures.as_completed(futures):
                obj = future.result()
                data[str(obj.number)] = obj

            cache.update(data)
            return data


component_re = re.compile(
    r"#{3,5}\scomponent\sname(.+?)(?=#{3,5})", flags=re.IGNORECASE | re.DOTALL
)
obj_type_re = re.compile(
    r"#{3,5}\sissue\stype(.+?)(?=#{3,5})", flags=re.IGNORECASE | re.DOTALL
)
valid_commands = (
    "bot_skip",
    "bot_broken",
    "needs_info",
    "waiting_on_contributor",
)
# TODO '/' prefix?
commands_re = re.compile(f"^{'|'.join(valid_commands)}$", flags=re.MULTILINE)
component_command_re = re.compile(r"^[!/]component\s([=+-]\S+)$", flags=re.MULTILINE)
version_re = re.compile(r"ansible\s\[core\s([^]]+)]")


def triage(objects: dict[str, GH_OBJ], dry_run: t.Optional[bool] = None) -> None:
    for obj in objects.values():
        to_label = []
        to_unlabel = []
        comments = []
        close = False
        logging.info(f"Triaging {obj.__class__.__name__} {obj.title} (#{obj.number})")

        # commands
        # TODO negate commands
        comment_able = "\n".join(
            itertools.chain(
                (obj.body,),
                (e["body"] for e in obj.events if e["name"] == "IssueComment"),
            )
        )
        commands_found = commands_re.findall(comment_able)

        # bot_skip/bot_broken
        skip_this = False
        for command in ("bot_skip", "bot_broken"):
            if command in commands_found:
                logging.info(
                    f"Skipping {obj.__class__.__name__} {obj.title} (#{obj.number}) due to {command}"
                )
                skip_this = True
                break
        if skip_this:
            continue

        # label commands
        for command in ("needs_info", "waiting_on_contributor"):
            if command in commands_found:
                to_label.append(command)

        # needs_triage
        if not any(
            e
            for e in obj.events
            if e["name"] == "LabeledEvent" and e["label"] == "needs_triage"
        ):
            to_label.append("needs_triage")

        # waiting_on_contributor
        if (
            "waiting_on_contributor" in obj.labels
            and (
                datetime.datetime.now(datetime.timezone.utc)
                - last_labeled(obj, "waiting_on_contributor")
            ).days
            > WAITING_ON_CONTRIBUTOR_CLOSE_DAYS
        ):
            close = True
            to_label.append("bot_closed")
            to_unlabel.append("waiting_on_contributor")
            with open("templates/waiting_on_contributor.tmpl") as f:
                comments.append(f.read())

        # needs_info
        if "needs_info" in obj.labels:
            labeled_datetime = last_labeled(obj, "needs_info")
            commented_datetime = last_commented_by(obj, obj.author)
            if commented_datetime is None or labeled_datetime > commented_datetime:
                days_since = (
                    datetime.datetime.now(datetime.timezone.utc) - labeled_datetime
                ).days
                if days_since > NEEDS_INFO_CLOSE_DAYS:
                    close = True
                    with open("templates/needs_info_close.tmpl") as f:
                        comments.append(
                            string.Template(f.read()).substitute(
                                author=obj.author, object_type=obj.__class__.__name__
                            )
                        )
                elif days_since > NEEDS_INFO_WARN_DAYS:
                    # FIXME commented before?
                    with open("templates/needs_info_warn.tmpl") as f:
                        comments.append(
                            string.Template(f.read()).substitute(
                                author=obj.author, object_type=obj.__class__.__name__
                            )
                        )
            else:
                to_unlabel.append("needs_info")

        def process_component(data):
            rv = []
            for line in data:
                for c in line.split(","):
                    if "<!--" in c or "-->" in c or " " in c:
                        continue
                    if "#" in c:
                        c = c.split("#")[0]
                    if c := (
                        c.strip("\t\n\r ")
                        .lower()
                        .removeprefix("the ")
                        .removeprefix("ansible.builtin.")
                        .removeprefix("module ")
                        .removesuffix(" module")
                        .replace("\\", "")
                    ):
                        rv.append(c)

            return rv

        # components
        components = []
        if isinstance(obj, PR):
            components = obj.files
        elif isinstance(obj, Issue):
            if match := component_re.search(obj.body):
                components = match_existing_components(
                    process_component(match.group(1).splitlines())
                )

        for component_command in component_command_re.findall(comment_able):
            path = component_command[1:]
            if component_command.startswith("="):
                components = [path]
            elif component_command.startswith("+"):
                components.append(path)
            elif component_command.startswith("-"):
                if path in components:
                    components.remove(path)

        obj.components = components

        logging.info(
            f"{obj.__class__.__name__} #{obj.number}: indentified components: {', '.join(obj.components)}"
        )

        # object type
        if match := obj_type_re.search(obj.body):
            data = re.sub(r"~[^~]+~", "", match.group(1).lower())
            if "feature" in data:
                to_label.append("feature")
            if "bug" in data:
                to_label.append("bug")
            if "documentation" in data or "docs" in data:
                to_label.append("docs")
            if "test" in data:
                to_label.append("test")

        # version matcher
        if match := version_re.search(obj.body):
            # TODO do not add if a maintainer manually removed the label
            to_label.append(f"affects_{'.'.join(match.group(1).split('.')[:2])}")

        # PRs
        if isinstance(obj, PR):
            # backport
            if obj.branch.startswith("stable-"):
                to_label.append("backport")
            else:
                to_unlabel.append("backport")
            # docs only
            if all(c.startswith("docs/") for c in components):
                to_label.append("docs_only")
                if not any(
                    e
                    for e in obj.events
                    if e["name"] == "IssueComment" and "<!--- boilerplate: docs_only --->" in e["body"]
                ):
                    with open("templates/docs_only.tmpl") as f:
                        comments.append(f.read())

        # TODO conflicting actions
        if common_labels := set(to_label).intersection(to_unlabel):
            raise AssertionError(
                f"The following labels were scheduled to be both added and removed {', '.join(common_labels)}"
            )

        if dry_run:
            logging.info(f"add labels: {', '.join(to_label)}")
            logging.info(f"remove labels: {', '.join(to_unlabel)}")
            logging.info(f"comments: {', '.join(comments)}")
            logging.info(f"close: {close}")
        else:
            if to_label:
                add_labels(obj, to_label)
            if to_unlabel:
                remove_labels(obj, to_unlabel)

            for comment in comments:
                add_comment(obj, comment)

            if close:
                close_object(obj)

        logging.info(
            f"Done triaging {obj.__class__.__name__} {obj.title} (#{obj.number})"
        )


def match_existing_components(filenames: list[str]) -> list[str]:
    if not filenames:
        return []
    query_fmt = """
        {
          repository(owner: "ansible", name: "ansible") {
            %s
          }
        }
    """

    plugins = [
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
    ]
    paths = ["lib/ansible/modules/"]
    paths.extend((f"lib/ansible/plugins/{name}/" for name in plugins))
    files = []
    component_to_path = {}
    # TODO simplify
    for i, filename in enumerate(filenames):
        if "/" in filename:
            files.append(
                """
                    file%s: object(expression: "HEAD:%s") {
                      ... on Blob {
                        byteSize
                      }
                    }
                """
                % (i, filename)
            )
            component_to_path[f"file{i}"] = filename
        else:
            tried_paths = paths
            for idx, path in enumerate(tried_paths):
                files.append(
                    """
                        file%s: object(expression: "HEAD:%s%s.py") {
                          ... on Blob {
                            byteSize
                          }
                        }
                    """
                    % (f"{i}{idx}", path, filename)
                )
                component_to_path[f"file{i}{idx}"] = path + filename + ".py"

    resp = send_query(json.dumps({"query": query_fmt % " ".join(files)}))
    return [
        component_to_path[file]
        for file, res in resp.json()["data"]["repository"].items()
        if res is not None
    ]


def last_labeled(obj: GH_OBJ, name: str) -> datetime.datetime:
    return max(
        (
            e["created_at"]
            for e in obj.events
            if e["name"] == "LabeledEvent" and e["label"] == name
        )
    )


def last_commented_by(obj: GH_OBJ, name: str) -> datetime.datetime:
    return max(
        (
            e["created_at"]
            for e in obj.events
            if e["name"] == "IssueComment" and e["author"] == name
        ),
        default=None,
    )


def fetch_object_by_number(number: str) -> GH_OBJ:
    try:
        obj = fetch_object(number, Issue, "issue")
    except ValueError:
        obj = fetch_object(number, PR, "pullRequest")

    return obj


def daemon(dry_run: t.Optional = None) -> None:
    global request_counter
    while True:
        request_counter = 0
        start = time.time()
        objs = fetch_objects()
        if objs:
            # TODO multiprocess?
            triage(objs, dry_run)
            with shelve.open(CACHE_FILENAME) as cache:
                for number, obj in objs.items():
                    obj.last_triaged = datetime.datetime.now(datetime.timezone.utc)
                    cache[str(number)] = obj
            logging.info(
                f"Took {time.time() - start:.2f} seconds to triage {len(objs)} issues/PRs"
                f" and {request_counter} HTTP requests"
            )
        else:
            logging.info("No new issues/PRs")
            logging.info(
                f"Took {time.time() - start:.2f} seconds to check for new issues/PRs"
                f" and {request_counter} HTTP requests"
            )
        logging.info(f"Sleeping for {SLEEP_SECONDS // 60} minutes")
        time.sleep(SLEEP_SECONDS)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ansibotmini",
        description="Triages github.com/ansible/ansible issues and PRs",
    )
    parser.add_argument("--number", help="Github issue or pull request number")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=logging.DEBUG if args.debug else logging.INFO,
        stream=sys.stderr,
    )
    if args.number:
        obj = fetch_object_by_number(args.number)
        triage({args.number: obj}, dry_run=args.dry_run)
    else:
        daemon(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
