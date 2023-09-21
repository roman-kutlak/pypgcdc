SHELL := /bin/bash

.EXPORT_ALL_VARIABLES:
PIP_REQUIRE_VIRTUALENV = true

# INSTALL_STAMP is from here
# https://blog.mathieu-leplatre.info/tips-for-your-makefile-with-python.html

VENV?=dev-venv
INSTALL_STAMP := $(VENV)/.install.stamp
PYTHON=${VENV}/bin/python3
ROOT_PATH := $(shell git rev-parse --show-toplevel)

export PGHOST := localhost
export PGPORT := 5433
export PGDATABASE := testdb
export PGUSER := postgres
export PGPASSWORD := postgrespw


venv: $(INSTALL_STAMP)

$(INSTALL_STAMP): setup.py tests/requirements-dev.txt
	if [ ! -d $(VENV) ]; then python3 -m venv $(VENV); fi
	${PYTHON} -m pip install --upgrade pip
	${PYTHON} -m pip install -r tests/requirements-dev.txt
	${PYTHON} -m pip install -e .
	touch $(INSTALL_STAMP)

.PHONY: test
test: venv
	${PYTHON} -m coverage run --omit="tests/*" -m pytest -vv tests/
	${PYTHON} -m coverage report -m

.PHONY: test-local
test-local: venv
	./tests/run_tests_locally.sh

.PHONY: mypy
mypy: venv
	${PYTHON} -m mypy src/ tests/

.PHONY: format
format: venv
	${PYTHON} -m isort src/ tests/
	${PYTHON} -m black --config=pyproject.toml src/ tests/

.PHONY: lint
lint: venv
	(! find ${ROOT_PATH}/src -name '*.py' | xargs  grep -F 'print') || echo "Print statement(s) found"
	${PYTHON} -m flake8 --ignore=W503,E501 src/ tests/
	${PYTHON} -m isort src/ tests/ --check-only
	${PYTHON} -m black --config=pyproject.toml src/ tests/ --check


.PHONY: infra-up
infra-up:
	docker-compose up -d

.PHONY: infra-down
infra-down:
	docker-compose down

.PHONY: clean
clean:
	rm -rf $(VENV)
	find . -type f -name *.pyc -delete
	find . -type d -name __pycache__ -delete
