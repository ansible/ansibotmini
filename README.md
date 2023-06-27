# ansibotmini

* is a Python 3.11+ program that acts as a bot for processing issues and pull requests in the `ansible/ansible` GitHub repository
* implements custom developer workflow of the Ansible Core Engineering team
* only uses https://github.com/ansibot GitHub account for all communication on the issues and pull requests


#### Table of contents
* [Functionality](#functionality)
  * [Labels](#labels)
  * [Commands](#commands)
* [Installation](#installation)
* [Configuration](#configuration)
* [Running the bot](#running-the-bot)
* [Testing](#testing)
* [FAQ](#faq)
* [Have a question?](#have-a-question)
* [Found a bug?](#found-a-bug)


## Functionality
* `ansibotmini` is a pull-based bot, it repeatedly pulls new and/or updated since the last run issues/PRs in a loop and triages them in approximately 5 minute intervals, consequently it might take a few minutes for the bot to process a new issue/PR
* issues/PRs that have not seen an update in 7 days are re-processed
* closed issues are NOT processed
* adds [Labels](#labels)
* supports [Commands](#commands)
* automatically identifies components (files in `ansible/ansible`) that relate to an issue/PR
* a listing of all issues/PRs per a file of the `ansible/ansible` repository is periodically generated and can be accessed at https://ansibullbot.eng.ansible.com/byfile
* for pull requests changing plugins in `test/support` directory (containing plugins moved to collections but still used in CI to support testing, generally such plugins should not be changed), the bot posts a comment on such  pull requests explaining the intent of the directory
* posts comments about sanity tests failures from the CI on pull requests
* auto-cancels CI runs on Azure Pipelines for pull requests that have merge commits and/or are not based on a branch from a fork
* closes issues/PRs that relate to components that have been moved to collections, posts a comment with information about the new repository


### Labels
The table below provides a listing of all labels `ansibotmini` adds to and/or removes from issues and pull requests:

| Label                    | Applied to               | Description                                                                                                                                                                                                                                                                                                                                              |
|--------------------------|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `affects_X.Y`            | Issues                   | Indicates which version (e.g. `affects_2.14`) a bug is reported against according to the `Ansible Version` section in the issue description provided in the issue template.                                                                                                                                                                              |
| `backport`               | Pull requests            | This pull request does not target the `devel` branch. Indicates a backport to a released (or soon to be) version. The PR must target stable-*X* branch, such as `stable-2.14`.                                                                                                                                                                           |
| `bot_closed`             | Issues and pull requests | Indicates an issue/PR has been closed by the bot, see `needs_info` and `waiting_on_contributor` [Labels](#labels) and `!needs_collection_redirect` [Command](#commands).                                                                                                                                                                                 |
| `bot_broken`             | Issues and pull requests | The bot is misbehaving. NOT for failing CI. An Ansible Core team member will investigate. Also see the [Command](#commands) of the same name.                                                                                                                                                                                                            |
| `bug`                    | Issues and pull requests | This issue/PR relates to a bug.                                                                                                                                                                                                                                                                                                                          |
| `ci_verified`            | Pull requests            | Changes made in this PR are causing tests to fail.                                                                                                                                                                                                                                                                                                       |
| `feature`                | Issues and pull requests | This issue/PR relates to a feature request.                                                                                                                                                                                                                                                                                                              |
| `has_issue`              | Pull requests            | This PR once merged will close an associated issue(s).                                                                                                                                                                                                                                                                                                   |
| `has_pr`                 | Issues                   | This issue has an associated PR.                                                                                                                                                                                                                                                                                                                         |
| `merge_commit`           | Pull requests            | This PR contains at least one merge commit. Please resolve. Pull requests must not contain a merge commit.                                                                                                                                                                                                                                               |
| `module`                 | Issues and pull requests | This issue/PR relates to a module.                                                                                                                                                                                                                                                                                                                       |
| `needs_ci`               | Pull requests            | This PR requires CI testing to be performed. Please close and re-open this PR to trigger CI.                                                                                                                                                                                                                                                             |
| `needs_info`             | Issues and pull requests | This issue requires further information. Please answer any outstanding questions. If the info is not provided in 28 days, the issue will be auto-closed by the bot. The submitter will be reminded by the bot about the ask after 14 days the label being applied. Once the submitter provides an answer, the label is automatically removed by the bot. |
| `needs_rebase`           | Pull requests            | A pull request needs to be rebased. See https://docs.ansible.com/ansible/devel/dev_guide/developing_rebasing.html                                                                                                                                                                                                                                        |
| `needs_revision`         | Pull requests            | This PR fails CI tests or a maintainer has requested a review/revision of the PR.                                                                                                                                                                                                                                                                        |
| `needs_template`         | Issues and pull requests | This issue/PR has an incomplete description. Please fill in the proposed template correctly.                                                                                                                                                                                                                                                             |
| `needs_triage`           | Issues and pull requests | Needs an Ansible Core team member triage before being processed. Given to all new issues/PRs.                                                                                                                                                                                                                                                            |
| `networking`             | Issues and pull requests | This issue/PR relates to networking.                                                                                                                                                                                                                                                                                                                     |
| `pre_azp`                | Pull requests            | This PR was last tested before migration to Azure Pipelines.                                                                                                                                                                                                                                                                                             |
| `stale_ci`               | Pull requests            | The CI result is older than a week and needs to be re-run. Close and re-open this PR to get it retested.                                                                                                                                                                                                                                                 |
| `stale_review`           | Pull requests            | Updates were made after the last review and the last review is more than 7 days old.                                                                                                                                                                                                                                                                     |
| `test`                   | Issues and pull requests | This issue/PR relates to tests.                                                                                                                                                                                                                                                                                                                          |
| `waiting_on_contributor` | Issues and pull requests | This would be accepted but the Ansible Core team has no plans to actively work on it. Once the label has been applied for one year and nobody volunteered to work on it the issue/PR is auto-closed. Note the ticket can still be implemented should someone decides to create a pull request for it even after the issue is closed.                     |


### Commands
To use a command leave a comment on an issue/PR containing the correct form of the command (see the table below) on a separate line:

| Command                               | Scope                    | Allowed   | Description                                                                                                                                                                                                                                                                         |
|---------------------------------------|--------------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `@ansibot bot_broken`                 | Issues and pull requests | Anyone    | Use this command if you think the bot is misbehaving (not for test failures), and an Ansible Core team member will investigate. Also see the [Label](#Labels) of the same name.                                                                                                     |
| `@ansibot !bot_broken`                | Issues and pull requests | Anyone    | Clear `bot_broken` command.                                                                                                                                                                                                                                                         |
| `@ansibot bot_skip`                   | Issues and pull requests | Core team | Ansible Core team members use this to have the bot skip triaging an issue/PR.                                                                                                                                                                                                       |
| `@ansibot !bot_skip`                  | Issues and pull requests | Core team | Clear `bot_skip` command.                                                                                                                                                                                                                                                           |
| `@ansibot component [=+-]FILEPATH`    | Issues                   | Anyone    | Set, append or remove a file from the matched components. To set, use `@ansibot component =lib/ansible/foo/bar`. To add, use `@ansibot component +lib/ansible/foo/bar`. To remove, use `@ansibot component -lib/ansible/foo/bar`.                                                   |
| `@ansibot !needs_collection_redirect` | Issues and pull requests | Anyone    | Use this command if bot made a mistake in deciding an issue or pull request was for a file in a collection. Ansible Core team member will need to re-open the issue/PR. Contact a Core team member to review the issue/PR on IRC: `#ansible-devel` on Libera.chat IRC.              |
| `/azp run`                            | Pull requests            | Anyone    | This is not a command operated by the bot but by Azure Pipelines used to (re-)run the CI                                                                                                                                                                                            |


## Installation
```
$ git clone https://github.com/mkrizek/ansibotmini.git
$ cd ansibotmini
```


## Configuration
```
$ cat ~/.ansibotmini.cfg
[default]
gh_token=XXX
azp_token=XXX
```

Issues and pull requests data are cached in `~/.ansibotmini_cache`.


## Running the bot
```
# print help
$ python ansibotmini.py --help

# triage specific issue or pull request
$ python ansibotmini.py --number 10000

# run the bot in a loop and triage all issues
$ python ansibotmini.py
```


## Testing
```
$ pip install pytest
$ python -m pytest
```


## FAQ
### Why did the bot close my issue or pull request?
See the `bot_closed` label in [Labels](#labels).


## Have a question?
Please reach out via the typical communication channels: https://docs.ansible.com/ansible/latest/community/communication.html


## Found a bug?
Please create an issue via https://github.com/mkrizek/ansibotmini/issues/new.
