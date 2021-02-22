.PHONY: build

GIT_BRANCH = $$(git symbolic-ref --short HEAD)
DOCKER_UP = "export GIT_BRANCH=$(GIT_BRANCH) && docker-compose up"
DOCKER_DOWN = "export GIT_BRANCH=$(GIT_BRANCH) && docker-compose down"
DOCKER_RUN = "export GIT_BRANCH=$(GIT_BRANCH) && docker-compose run"

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
VENV_ACTIVATE=. $(VENV_NAME)/bin/activate
PYTHON=${VENV_NAME}/bin/python3

.DEFAULT: help
help:
	@echo "\n \
	------------------------------ \n \
	++ Airflow Related ++ \n \
	airflow: attaches a shell to the airflow deployment in docker-compose.yml. Access the webserver at localhost:8080\n \
	init-airflow: initializes a new Airflow db and creates a generic admin user, required on a fresh db. \n \
	\n \
	++ dbt Related ++ \n \
	run-dbt: attaches a shell to the dbt virtual environment and changes to the dbt directory. \n \
	run-dbt-docs: spins up a webserver with the dbt docs. Access the docs server at localhost:8081 \n \
	clean-dbt: deletes all virtual environment artifacts \n \
	\n \
	++ Python Related ++ \n \
	data-image: attaches to a shell in the data-image and mounts the repo for testing. \n \
	lint: Runs a linter (Black) over the whole repo. \n \
	mypy: Runs a type-checker in the extract dir. \n \
	pylint: Runs the pylint checker over the whole repo. Does not check for code formatting, only errors/warnings. \n \
	radon: Runs a cyclomatic complexity checker and shows anything with less than an A rating. \n \
	xenon: Runs a cyclomatic complexity checker that will throw a non-zero exit code if the criteria aren't met. \n \
	\n \
	++ Utilities ++ \n \
	cleanup: WARNING: DELETES DB VOLUME, frees up space and gets rid of old containers/images. \n \
	update-containers: Pulls fresh versions of all of the containers used in the repo. \n \
	------------------------------ \n"

airflow:
	@if [ "$(GIT_BRANCH)" = "master" ]; then echo "GIT_BRANCH must not be master" && exit 1; fi
	@echo "Attaching to the Webserver container..."
	@"$(DOCKER_DOWN)"
	@"$(DOCKER_UP)" -d airflow_webserver
	@sleep 5
	@docker-compose exec airflow_scheduler gcloud auth activate-service-account --key-file=/root/gcp_service_creds.json --project=gitlab-analysis
	@docker-compose exec airflow_webserver bash

cleanup:
	@echo "Cleaning things up..."
	@"$(DOCKER_DOWN)" -v
	@docker system prune -f

data-image:
	@echo "Attaching to data-image and mounting repo..."
	@"$(DOCKER_RUN)" data_image bash

dbt-docs:
	@echo "Generating docs and spinning up the a webserver on port 8081..."
	@"$(DOCKER_RUN)" -p "8081:8081" dbt_image bash -c "dbt clean && dbt deps && dbt docs generate --target docs && dbt docs serve --port 8081"

dbt-image:
	@echo "Attaching to dbt-image and mounting repo..."
	@"$(DOCKER_RUN)" dbt_image bash -c "dbt clean && dbt deps && /bin/bash"

prepare-dbt:
	which python3 || apt install -y python3 python3-pip
	which virtualenv || python3 -m pip install virtualenv
	make check-venv

check-venv: $(VENV_NAME)/bin/activate
$(VENV_NAME)/bin/activate: setup.py
	test -d $(VENV_NAME) || virtualenv -p python3 $(VENV_NAME)
	${PYTHON} -m pip install -U pip
	${PYTHON} -m pip install -e .
	touch $(VENV_NAME)/bin/activate

run-dbt: check-venv
	$(VENV_ACTIVATE) && cd transform/snowflake-dbt/; dbt clean && dbt deps; exec bash;

run-dbt-docs: check-venv
	$(VENV_ACTIVATE) && cd transform/snowflake-dbt/; dbt clean && dbt deps && dbt docs generate --target docs && dbt docs serve --port 8081;

clean-dbt:
	find . -name '*.pyc' -delete
	rm -rf $(VENV_NAME) *.eggs *.egg-info

init-airflow:
	@echo "Initializing the Airflow DB..."
	@"$(DOCKER_UP)" -d airflow_db
	@sleep 5
	@"$(DOCKER_RUN)" airflow_scheduler airflow initdb
	@"$(DOCKER_RUN)" airflow_scheduler airflow create_user --role Admin -u admin -p admin -e datateam@gitlab.com -f admin -l admin
	@"$(DOCKER_DOWN)"

lint:
	@echo "Linting the repo..."
	@black .

mypy:
	@echo "Running mypy..."
	@mypy extract/ --ignore-missing-imports

pylint:
	@echo "Running pylint..."
	@pylint ../analytics/ --ignore=dags --disable=C --disable=W1203 --disable=W1202 --reports=y

radon:
	@echo "Run Radon to compute complexity..."
	@radon cc . --total-average -nb

update-containers:
	@echo "Pulling latest containers for airflow-image, data-image and dbt-image..."
	@docker pull registry.gitlab.com/gitlab-data/data-image/airflow-image:latest
	@docker pull registry.gitlab.com/gitlab-data/data-image/data-image:latest
	@docker pull registry.gitlab.com/gitlab-data/data-image/dbt-image:latest

xenon:
	@echo "Running Xenon..."
	@xenon --max-absolute B --max-modules A --max-average A . -i transform,shared_modules
