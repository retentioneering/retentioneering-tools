FROM python:3.9
RUN apt-get update && apt-get upgrade -y
RUN pip install poetry
COPY pyproject.toml /app/pyproject.toml
WORKDIR /app
RUN poetry export --only main --without-hashes -f requirements.txt --output requirements.txt
RUN python -m pip install -r requirements.txt
RUN python -m pip install pytest
RUN python -m pip install pre-commit
