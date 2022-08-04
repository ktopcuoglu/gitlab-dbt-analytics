.PHONY: build

SHELL:=/bin/zsh
TEST_FOLDERS_PATH := $(shell eval find . -name "test" -type d)
PATH := $(PATH):$(PWD):$(TEST_FOLDERS_PATH)
GIT_BRANCH = $$(git symbolic-ref --short HEAD)
DOCKER_UP = "export GIT_BRANCH=$(GIT_BRANCH) && docker-compose up"
DOCKER_DOWN = "export GIT_BRANCH=$(GIT_BRANCH) && docker-compose down"
DOCKER_RUN = "export GIT_BRANCH=$(GIT_BRANCH) && docker-compose run"
DBT_DEPS = "cd transform/snowflake-dbt/ && poetry run dbt clean && poetry run dbt deps"

.EXPORT_ALL_VARIABLES:
DATA_TEST_BRANCH=main
DATA_SIREN_BRANCH=master
SNOWFLAKE_SNAPSHOT_DATABASE=SNOWFLAKE
SNOWFLAKE_LOAD_DATABASE=RAW
SNOWFLAKE_PREP_DATABASE=PREP
SNOWFLAKE_PREP_SCHEMA=preparation
SNOWFLAKE_PROD_DATABASE=PROD
SNOWFLAKE_TRANSFORM_WAREHOUSE=ANALYST_XS
SALT=pizza
SALT_IP=pie
SALT_NAME=pepperoni
SALT_EMAIL=cheese
SALT_PASSWORD=416C736F4E6F745365637265FFFFFFAB

VENV_NAME?=dbt

.DEFAULT: help

help:
	@echo "\n \
	------------------------------------------------------------ \n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
	++ Airflow Related ++ \n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
    - airflow: attaches a shell to the airflow deployment in docker-compose.yml. Access the webserver at localhost:8080\n \
    - init-airflow: initializes a new Airflow db and creates a generic admin user, required on a fresh db. \n \
	\n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
	++ Utilities ++ \n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
    - cleanup: WARNING: DELETES DB VOLUME, frees up space and gets rid of old containers/images. \n \
    - update-containers: Pulls fresh versions of all of the containers used in the repo. \n \
    - data-image: Attaches to a shell in the data-image and mounts the repo for testing. \n \
	\n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
	++ dbt Related ++ \n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
    - run-dbt: attaches a shell to the dbt virtual environment and changes to the dbt directory. \n \
    - run-dbt-docs: spins up a webserver with the dbt docs. Access the docs server at localhost:8081 \n \
    - clean-dbt: deletes all virtual environment artifacts \n \
    - pip-dbt-shell: opens the poetry environment in the dbt folder. Primarily for use with sql fluff. \n \
	\n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
	++ Python Related ++ \n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
    - black: Runs a linter (Black) over the whole repo. \n \
    - yq-lint: Runs a linter against a YAML file. Pass in the file with the variable YAML. Example: make yq-lint YAML=extract/postgres_pipeline/manifests/gitlab_com_db_manifest.yaml \n \
    - mypy: Runs a type-checker in the extract dir. \n \
    - pylint: Runs the pylint checker over the whole repo. Does not check for code formatting, only errors/warnings. \n \
    - complexity: Runs a cyclomatic complexity checker that will throw a non-zero exit code if the criteria aren't met. \n \
    - flake8: Run flake8 library to check Python code \n \
    - vulture: Run vulture library to check unused code \n \
    - pytest: run all pytest cases \n \
    - python_code_quality: integrate one command to run ALL python check (from the previous commands) \n \
    - clean-python: clean up python code and delete cache directories/files \n \
	------------------------------------------------------------"
########################################################################################################################
# Airflow
########################################################################################################################
airflow:
	@if [ "$(GIT_BRANCH)" = "master" ]; then echo "GIT_BRANCH must not be master" && exit 1; fi
	@echo "Attaching to the Webserver container..."
	@"$(DOCKER_DOWN)"
	@"$(DOCKER_UP)" -d airflow_webserver
	@sleep 5
	@docker-compose exec airflow_scheduler gcloud auth activate-service-account --key-file=/root/gcp_service_creds.json --project=gitlab-analysis
	@docker-compose exec airflow_webserver bash

init-airflow:
	@echo "Initializing the Airflow DB..."
	@"$(DOCKER_UP)" -d airflow_db
	@sleep 5
	@"$(DOCKER_RUN)" airflow_scheduler airflow db init
	@"$(DOCKER_RUN)" airflow_scheduler airflow users create --role Admin -u admin -p admin -e datateam@gitlab.com -f admin -l admin
	@"$(DOCKER_DOWN)"

########################################################################################################################
# Utilities
########################################################################################################################
cleanup:
	@echo "Cleaning things up..."
	@"$(DOCKER_DOWN)" -v
	@docker system prune -f

data-image:
	@echo "Attaching to data-image and mounting repo..."
	@"$(DOCKER_RUN)" data_image bash

update-containers:
	@echo "Pulling latest containers for airflow-image, analyst-image, data-image and dbt-image..."
	@docker pull registry.gitlab.com/gitlab-data/data-image/airflow-image:latest
	@docker pull registry.gitlab.com/gitlab-data/data-image/analyst-image:latest
	@docker pull registry.gitlab.com/gitlab-data/data-image/data-image:latest
	@docker pull registry.gitlab.com/gitlab-data/data-image/dbt-image:latest

########################################################################################################################
# DBT
########################################################################################################################
prepare-dbt:
	curl -k -sSL https://install.python-poetry.org/ | python3 -
	cd transform/snowflake-dbt/ && poetry install
	"$(DBT_DEPS)"

prepare-dbt-fix:
	python3 -m pip install poetry
	cd transform/snowflake-dbt/ && poetry install
	"$(DBT_DEPS)"

run-dbt-no-deps:
	cd transform/snowflake-dbt/ && poetry shell;

dbt-deps:
	"$(DBT_DEPS)"

run-dbt:
	"$(DBT_DEPS)"
	cd transform/snowflake-dbt/ && poetry shell;

run-dbt-docs:
	"$(DBT_DEPS)"
	cd transform/snowflake-dbt/ && poetry run dbt docs generate --target docs && poetry run dbt docs serve --port 8081;

clean-dbt:
	cd transform/snowflake-dbt/ && poetry run dbt clean && poetry env remove python

########################################################################################################################
# Python
########################################################################################################################
prepare-python:
	which poetry || python3 -m pip install poetry
	poetry install

black:
	@echo "Running lint (black)..."
	@poetry run black --check .

flake8:
	@echo "Running flake8..."
	@poetry run flake8 . --ignore=E203,E501,W503,W605

mypy:
	@echo "Running mypy..."
	@poetry run mypy extract/ --ignore-missing-imports

pylint:
	@echo "Running pylint..."
	@poetry run pylint . --disable=line-too-long,E0401,E0611,W1203,W1202

complexity:
	@echo "Running complexity (Xenon)..."
	@poetry run xenon --max-absolute B --max-modules B --max-average A . -i transform,shared_modules

yq-lint:
ifdef YAML
	@echo "Linting the YAML file... "
	@echo "Running: yq eval 'sortKeys(..)' $(YAML)"
	@"$(DOCKER_RUN)" data_image bash -c "yq eval 'sortKeys(..)' $(YAML)"
else
	@echo "No file. Exiting."
endif

vulture:
	@echo "Running vulture..."
	@poetry run vulture . --min-confidence 100

pytest:
	@echo "Running pytest..."
	@poetry run python3 -m pytest -vv -x

python_code_quality: black mypy pylint complexity flake8 vulture pytest
	@echo "Running python_code_quality..."

clean-python:
	@echo "Running clean-python..."
	@poetry env remove python
