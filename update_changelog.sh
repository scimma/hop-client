#!/bin/sh
TARGET="$1"

sed -e 's@## \[Unreleased]@## \[Unreleased]\
\
## \['"${VERSION}"'] - '"$(date +'%Y-%m-%d')"'@' \
-e 's@.*\[Unreleased]:.*@\[Unreleased]: '${REPO_URL}'/compare/v'${VERSION}'...HEAD\
['${VERSION}']: '${REPO_URL}'/releases/tag/v'${VERSION}'@' "$TARGET" > "${TARGET}.new"
