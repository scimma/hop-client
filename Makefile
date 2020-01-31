.PHONY: help
help :
	@echo
	@echo 'Commands:'
	@echo
	@echo '  make test                  run unit tests'
	@echo '  make doc                   make documentation'
	@echo '  make dist                  make binary and source packages'
	@echo '  make dist-check            verify binary and source packages'
	@echo '  make upload                upload to PyPI'
	@echo

.PHONY: test
test :
	python -m pytest -v --cov=scimma.client

.PHONY: doc
doc :
	cd doc && make html

.PHONY: dist
dist :
	python setup.py sdist bdist_wheel	

.PHONY: dist-check
dist-check:
	twine check dist/*

.PHONY: upload
upload:
	twine upload dist/*
