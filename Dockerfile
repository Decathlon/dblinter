FROM python:3.12-slim-bullseye AS base

# Setup env
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    # pip:
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # poetry:
    POETRY_VERSION=1.2.1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/pypoetry' \
    POETRY_HOME='/usr/local'

# Install shared libraries needed a build and runtime
RUN apt-get update && apt-get install -y libpq-dev

# Install poetry and compilation dependencies
RUN pip install poetry

WORKDIR /app

# Create and switch to a new user
RUN groupadd -g 1000 -r app \
  && useradd -d '/app' -g app -l -r -u 1000 app \
  && chown app:app -R '/app'

# Install python dependencies in /.venv
COPY --chown=app:app poetry.lock pyproject.toml /app/

RUN poetry config virtualenvs.create false \
    && poetry install --only main

USER app

# Install application into container
COPY --chown=app:app . /app/

# Run the Python executable
ENTRYPOINT ["python", "-m", "dblinter"]
CMD ["-f", "dblinter/default_config.yaml"]

# Entrypoint for debugging
#ENTRYPOINT ["/bin/bash", "-c", "--"]
#CMD [ "while true; do sleep 30; done;" ]
