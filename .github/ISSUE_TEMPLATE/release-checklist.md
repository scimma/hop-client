---
name: Release checklist
about: Standard release checklist template
title: Release version <version>
labels: ''
assignees: ''

---

**version**: <insert version here>

**git ref**: <insert git reference for release>

## Release Checklist

### Pre-release:

* [ ] `make test` runs without errors.
* [ ] `make lint` doesn't give any warnings.
* [ ] `make format` doesn't give any code formatting suggestions.
* [ ] `make doc` runs without errors and generated docs render correctly.
* [ ] Ensure that `CHANGELOG.md` has been updated with the current version via `make changelog VERSION=X.Y.Z`
* [ ] Check that `CHANGELOG.md` isn't missing any feature additions/changes/removals for the proposed version.
* [ ] Check that CI pipeline run on this PR passes all stages.

### Release:

* [ ] Review signoff on this PR to create a tag.
* [ ] Make a tag based on the proposed git reference via `git tag vX.Y.Z`
* [ ] Running `python setup.py --version` shows the correct version.
* [ ] `make dist` creates both source and binary packages without error.
* [ ] `make dist-check` shows no issues with the built packages.
* [ ] Push the tag via `git push --tags`.
* [ ] Ensure the release CI pipeline passes all stages.
* [ ] Check that PyPI/conda packages are available for the proposed version.
* [ ] Merge the branch using the "Rebase and merge option".
