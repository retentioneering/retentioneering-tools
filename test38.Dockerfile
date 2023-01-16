FROM python:3.8
RUN apt-get update && apt-get upgrade -y
#RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -
RUN pip install poetry
COPY pyproject.toml /app/pyproject.toml
WORKDIR /app
RUN poetry export --only main --without-hashes -f requirements.txt --output requirements.txt
RUN python -m pip install -r requirements.txt
RUN python -m pip install pytest
RUN python -m pip install pre-commit
