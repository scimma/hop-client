---
name: Release checklist
about: Standard release checklist template
title: Release version <version>
labels: ''
assignees: ''

---

**version**: <insert version here>

**git ref**: <insert git commit of the updated CHANGELOG>

## Release Checklist

### Pre-release:

* [ ] Create a new branch named `release-vX.Y.Z` from master, substituting the new version for `X.Y.Z`
* [ ] Update `CHANGELOG.md` with major changes since that last previous release
* [ ] `make test` runs without errors.
* [ ] `make lint` doesn't give any warnings.
* [ ] `make format` doesn't give any code formatting suggestions.
* [ ] `make doc` runs without errors and generated docs render correctly.
* [ ] Ensure that `CHANGELOG.md` has been updated with the current version via `make changelog VERSION=X.Y.Z`
* [ ] Check that `CHANGELOG.md` isn't missing any feature additions/changes/removals for the proposed version, and commit.
* [ ] Update this issue with the commit ref # of the CHANGELOG update in the previous step.
* [ ] Make PR from the `release-vX.Y.Z` branch created above and reference this issue in the description.
* [ ] Check that the CI pipeline run on this PR passes all stages.

### Release:

* [ ] Review signoff on the PR to create a new version tag.
* [ ] Make a tag based on the proposed git reference via `git tag vX.Y.Z`
* [ ] Running `python setup.py --version` shows the correct version.
* [ ] `make pypi-dist` creates both source and binary packages for PyPI without error.
* [ ] `make pypi-dist-check` shows no issues with the built packages.
* [ ] Push the tag via `git push --tags`.
* [ ] Ensure the release CI pipeline passes all stages.
* [ ] Check that PyPI package is available for the proposed version: https://pypi.org/project/hop-client/#files
* [ ] Create and merge version release PR on conda-forge feedstocks: https://github.com/conda-forge/hop-client-feedstock
* [ ] Check that conda-forge package is available for the proposed version: https://anaconda.org/conda-forge/hop-client
* [ ] Merge the branch using the "Rebase and merge option".
