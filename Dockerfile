FROM python:3.7-slim-buster

RUN pip install poetry
COPY poetry.lock pyproject.toml /
RUN poetry install --no-root
COPY . /validator
WORKDIR /validator
ENTRYPOINT pytest
