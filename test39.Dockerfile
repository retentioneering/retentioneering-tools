FROM python:3.9
RUN apt-get update && apt-get upgrade -y
#RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -
RUN pip install poetry
COPY pyproject.toml /app/pyproject.toml
WORKDIR /app
RUN poetry export -f requirements.txt --output /app/requirements.txt
RUN python -m pip install -r requirements.txt
RUN python -m pip install pytest
RUN python -m pip install pre-commit
