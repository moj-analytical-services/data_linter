FROM python:3.7-slim-buster

WORKDIR /validator

RUN apt-get update && apt-get install -y git
RUN python -m pip install --upgrade pip==20.3.3
RUN pip install poetry

COPY . /validator

RUN poetry config virtualenvs.create false \
  && poetry install --extras "ge frictionless" --no-interaction --no-ansi

ENTRYPOINT pytest tests/ -vv
