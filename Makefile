.PHONY: help
help :
	@echo
	@echo 'Commands:'
	@echo
	@echo '  make test                  run unit tests'
	@echo '  make lint                  run linter'
	@echo '  make format                run code formatter, giving a diff for recommended changes'
	@echo '  make doc                   make documentation'
	@echo '  make changelog             update changelog based on version'
	@echo '  make dist                  make binary and source packages'
	@echo '  make dist-check            verify binary and source packages'
	@echo '  make upload                upload to PyPI'
	@echo

VERSION ?= $(shell python setup.py --version)
REPO_URL = https://github.com/scimma/hop-client

.PHONY: test
test :
	python setup.py test

.PHONY: lint
lint :
	# stop the build if there are Python syntax errors or undefined names
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	# exit-zero treats all errors as warnings
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=100 --statistics

.PHONY: format
format :
	# show diff via black
	black tests --diff
	black hop --diff

.PHONY: doc
doc :
	cd doc && make html

.PHONY: changelog
changelog :
	sed -i 's@## \[Unreleased]@## \[Unreleased]\n\n## \[$(VERSION)] - $(shell date +'%Y-%m-%d')@' CHANGELOG.md
	sed -i 's@.*\[Unreleased]:.*@\[Unreleased]: $(REPO_URL)/compare/v$(VERSION)...HEAD\n[$(VERSION)]: $(REPO_URL)/releases/tag/v$(VERSION)@' CHANGELOG.md

.PHONY: dist
dist :
	python setup.py sdist bdist_wheel	

.PHONY: dist-check
dist-check:
	twine check dist/*

.PHONY: upload
upload:
	twine upload dist/*
