<!-- markdownlint-disable MD013 -->
# Contributing

Thank you for investing your time in contributing to KubeRay project!

Read our [Code of Conduct](./CODE_OF_CONDUCT.md) to keep our community approachable and respectable.

This guide details how to use issues and pull requests to improve KubeRay project.

## General Guidelines

### Getting involved

Kuberay has an active community of developers. Here’s how to get involved with the Kuberay community:

Join our community: Join [Ray community slack](https://forms.gle/9TSdDYUgxYs8SA9e8) (search for Kuberay channel) or use our [discussion board](https://discuss.ray.io/c/ray-clusters/ray-kubernetes) to ask questions and get answers.

Please see Github Issue section below to file bug & feature requests.

### Pull Requests

Make sure to keep Pull Requests small and functional to make them easier to review, understand, and look up in commit history. This repository uses "Squash and Commit" to keep our history clean and make it easier to revert changes based on PR.

Adding the appropriate documentation, unit tests and e2e tests as part of a feature is the responsibility of the feature owner, whether it is done in the same Pull Request or not.

Pull Requests should follow the "subject: message" format, where the subject describes what part of the code is being modified.

Refer to the template for more information on what goes into a PR description.

### Updating your Pull Request

To make reviews easier, we suggest merging `master` into your branch instead of rebasing when it falls behind, and adding new commits instead of squashing or force-pushing during review. This lets reviewers see what changed since their last review. PRs will be squashed when merged.

### Design Docs

A contributor proposes a design with a PR on the repository to allow for revisions and discussions. If a design needs to be discussed before formulating a document for it, make use of Google doc and GitHub issue to involve the community on the discussion.

### GitHub Issues

GitHub Issues are used to file bugs, work items, and feature requests with actionable items/issues (Please refer to the "Reporting Bugs/Feature Requests" section below for more information).

### Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features that have actionable items/issues (as opposed to introducing a feature request on GitHub Discussions).

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

- A reproducible test case or series of steps
- The version of the code being used
- Any modifications you've made relevant to the bug
- Anything unusual about your environment or deployment

## Contributing via Pull Requests

### Find interesting issue

If you spot a problem with the problem, [search if an issue already exists](https://github.com/ray-project/kuberay/issues). If a related issue doesn't exist, you can open a new issue using [issue template](https://github.com/ray-project/kuberay/issues/new/choose).

### Solve an issue

KubeRay has subproject and each of them may have different development and testing procedure. Please check `DEVELOPMENT.md` in sub folder to get familiar with running and testing codes.

### Open a Pull request

When you're done making the changes, open a pull request and fill PR template so we can better review your PR. The template helps reviewers understand your changes and the purpose of your pull request.

Don't forget to link PR to issue if you are solving one.

If you run into any merge issues, checkout this [git tutorial](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/addressing-merge-conflicts/resolving-a-merge-conflict-on-github) to help you resolve merge conflicts and other issues.

### Release Acknowledgements

Once your PR is merged, your contributions will be acknowledged in every release, see [Release Notes](https://github.com/ray-project/kuberay/releases) for more details.

## AI-assisted Contributions

> These guidelines apply to **all** AI-assisted contributions to KubeRay.
> PRs that do not follow them may be closed without review.

AI tools are welcome, but you are responsible for every line you submit.

- **Disclose it:** Answer the "AI assistance" section in the PR template, and briefly describe how AI was used if it was.
- **Review and test it yourself:** Read every changed line and run the relevant tests locally before requesting review. Reviewers should not be the first to read AI-generated code.
- **Keep the PR description concise:** We suggest writing the PR description mostly yourself and keeping it short, so reviewers can quickly understand what changed and why.
- **Be able to explain it:** Understand why each change was made, and be ready to explain it when reviewers ask.
- **Reply to reviews yourself:** Do not use AI tools to write responses to review comments.
- **Avoid duplicate and mass-generated PRs:** Search open issues and PRs before starting. Group small fixes (typos, style) into one PR instead of opening many.

## Finding contributions to work on

Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any 'help wanted' and 'good first issue' issues are a great place to start.
