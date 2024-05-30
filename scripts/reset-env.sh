#!/usr/bin/env sh

BASEDIR="$(cd "$(dirname -- "$0")/.." >/dev/null; pwd -P)"

ENV_NAME=snowflake-tools
PYTHON_VERSION=3.11

poetry cache clear PYPI --all
poetry cache clear _default_cach --all

conda run -n base conda env remove -n $ENV_NAME
conda run -n base conda create -n $ENV_NAME python=$PYTHON_VERSION -y

poetry install
