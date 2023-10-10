.DEFAULT_GOAL=lint
SHELL := /bin/bash

SOURCE_FOLDERS=src tests
PYTEST_ARGS=--durations=0 tests
RUN_TIMESTAMP := $(shell /bin/date "+%Y-%m-%d-%H%M%S")

release: ready guard-clean-working-repository bump.patch tag publish

ready: clean-dev tidy test lint requirements.txt build

build: requirements.txt-to-git
	@poetry build

publish:
	@poetry publish

lint: tidy pylint

tidy: black isort

tidy-to-git: guard-clean-working-repository tidy
	@status="$$(git status --porcelain)"
	@if [[ "$$status" != "" ]]; then
		@git add .
		@git commit -m "📌 make tidy"
		@git push
	fi

test: output-dir
	@poetry run pytest $(PYTEST_ARGS) tests
	@rm -rf ./tests/output/*

output-dir:
	@mkdir -p ./tests/output

init: tools
	@poetry install

.ONESHELL: guard-clean-working-repository
guard-clean-working-repository:
	@status="$$(git status --porcelain)"
	@if [[ "$$status" != "" ]]; then
		echo "error: changes exists, please commit or stash them: "
		echo "$$status"
		exit 65
	fi

version:
	@echo $(shell grep "^version \= " pyproject.toml | sed "s/version = //" | sed "s/\"//g")

bump.patch: requirements.txt
	@poetry version patch
	@git add pyproject.toml requirements.txt
	@git commit -m "📌 bump version patch"
	@git push

tag:
	@poetry build
	@git push
	@git tag $(shell grep "^version \= " pyproject.toml | sed "s/version = //" | sed "s/\"//g") -a
	@git push origin --tags

pytest:
	@mkdir -p ./tests/output
	@poetry run pytest --quiet tests

pylint:
	@poetry run pylint $(SOURCE_FOLDERS)

mypy:
	@poetry run mypy --version
	@poetry run mypy .

flake8:
	@poetry run flake8 --version
	-poetry run flake8

isort:
	@poetry run isort --profile black --float-to-top --line-length 120 --py 38 $(SOURCE_FOLDERS)

black: clean-dev
	@poetry run black --version
	@poetry run black --line-length 120 --target-version py38 --skip-string-normalization $(SOURCE_FOLDERS)

clean-dev:
	@rm -rf .pytest_cache build dist .eggs *.egg-info
	@rm -rf .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@rm -rf tests/output

update:
	@poetry update

requirements.txt: poetry.lock
	@poetry export --without-hashes -f requirements.txt --output requirements.txt

requirements.txt-to-git: requirements.txt
	@git add requirements.txt
	@git commit -m "📌 updated requirements.txt"
	@git push

tools:
	@npm install -g pg-diff-cli
	
.PHONY: help check init version
.PHONY: lint flake8 pylint mypy black isort tidy
.PHONY: test retest test-coverage pytest
.PHONY: ready build tag bump.patch release fast-release