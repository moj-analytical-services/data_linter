FROM python:3.7-slim-buster
RUN pip install poetry
COPY poetry.lock pyproject.toml /
RUN poetry config virtualenvs.create false \
&& poetry install --no-interaction --no-ansi
COPY . /validator
WORKDIR /validator
ENTRYPOINT pytest /tests -vv
