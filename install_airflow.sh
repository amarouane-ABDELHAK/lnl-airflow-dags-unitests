#! /bin/bash
shopt -s expand_aliases
alias local_pip='./venv/bin/pip'
export AIRFLOW_HOME=~/airflow_lnl
AIRFLOW_VERSION=2.4.3

PYTHON_VERSION=3.10

CONSTRAINT_URL="https://raw.githubusercontent.com/NASA-IMPACT/veda-data-airflow/dev/dags/requirements-constraints.txt"

local_pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"