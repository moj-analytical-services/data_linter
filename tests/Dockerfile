FROM python:3.7-slim-buster

WORKDIR /validator

RUN python -m pip install --upgrade pip
RUN pip install poetry

COPY . /validator

RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi

ENTRYPOINT pytest tests/ -vv
