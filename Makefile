.PHONY: help
help :
	@echo
	@echo 'Commands:'
	@echo
	@echo '  make test                  run unit tests'
	@echo '  make lint                  run linter'
	@echo '  make format                run code formatter'
	@echo '  make doc                   make documentation'
	@echo '  make changelog             update changelog based on version'
	@echo '  make pypi-dist             make binary and source packages for PyPI'
	@echo '  make pypi-dist-check       verify binary and source packages for PyPI'
	@echo '  make conda-build           make binary and source packages for conda-forge'
	@echo

VERSION ?= $(shell python setup.py --version)
REPO_URL = https://github.com/scimma/hop-client

.PHONY: test
test :
	python -m pytest -v --cov=hop tests

.PHONY: lint
lint :
	# stop the build if there are Python syntax errors or undefined names
	flake8 hop tests setup.py --count --select=E9,F63,F7,F82 --show-source --statistics
	# exit-zero treats all errors as warnings
	flake8 hop tests setup.py --count --exit-zero --max-complexity=15 --max-line-length=100 --statistics

.PHONY: format
format :
	autopep8 --recursive --in-place hop tests setup.py

.PHONY: doc
doc :
	cd doc && make html

.PHONY: changelog
changelog :
	sed -i 's@## \[Unreleased]@## \[Unreleased]\n\n## \[$(VERSION)] - $(shell date +'%Y-%m-%d')@' CHANGELOG.md
	sed -i 's@.*\[Unreleased]:.*@\[Unreleased]: $(REPO_URL)/compare/v$(VERSION)...HEAD\n[$(VERSION)]: $(REPO_URL)/releases/tag/v$(VERSION)@' CHANGELOG.md

.PHONY: pypi-dist
pypi-dist :
	python setup.py sdist bdist_wheel

.PHONY: pypi-dist-check
pypi-dist-check:
	twine check dist/*

.PHONY: conda-build
conda-build:
	conda build -c defaults -c conda-forge ./recipe
