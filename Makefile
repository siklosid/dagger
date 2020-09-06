.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

include .common.env
export

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-venv clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-venv: ## remove virtualenv
	$(shell if command -v deactivate ; then deactivate ; fi)
	rm -fr venv

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -rf {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint: ## check style with flake8
	flake8 dagger tests

test: ## run tests quickly with the default Python
	AIRFLOW_HOME=$(shell pwd)/tests/fixtures/config_finder/root/ \
	python setup.py test

coverage: ## check code coverage quickly with the default Python
	coverage run --source dagger setup.py test
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/dagger.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ dagger
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

servedocs: docs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release: dist ## package and upload a release
	twine upload dist/*

dist: clean ## builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist

install: clean ## install the package to the active Python's site-packages
	python setup.py install


install-dev: clean ## install the package to the active Python's site-packages
	virtualenv -p python3 venv; \
	source venv/bin/activate; \
	python setup.py install; \
	pip install -e . ; \
	pip install -r reqs/dev.txt -r reqs/test.txt

build-airflow:  ## Build airflow image
build-airflow: PROJ_NAME="airflow"
build-airflow:
	docker build --build-arg AIRFLOW_VERSION=${AIRFLOW_VERSION} -t ${DOCKER_REGISTRY}/${PROJ_NAME}:${AIRFLOW_VERSION} ./dockers/airflow

test-airflow: ## Run airflow image locally | args: services
test-airflow: export ARGS=$(shell if [ "${logs}" != "true" ]; then echo "-d"; fi)
test-airflow: build-airflow
	AIRFLOW_DAGS_DIR=$(shell pwd)/tests/fixtures/config_finder/root/dags \
	DAGGER_DIR=$(shell pwd)/dagger \
	DOCKERS_DIR=$(shell pwd)/dockers \
	docker-compose -f dockers/docker-compose.local.yml up ${ARGS} ${services}

stop-airflow: ## Stopping airflow
stop-airflow:
	docker-compose -f dockers/docker-compose.local.yml down

airflow-scheduler: ## Log in to scheduler
airflow-scheduler:
	docker exec -it dockers_scheduler_1 sh -c 'cd ~/dags && bash'
