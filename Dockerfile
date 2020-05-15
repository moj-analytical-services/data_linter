FROM python:3.7-slim-buster
COPY requirements.txt /
RUN pip install -r /requirements.txt
COPY . /validator
WORKDIR /validator
ENTRYPOINT pytest
