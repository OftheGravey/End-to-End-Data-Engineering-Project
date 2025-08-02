FROM python:3.10-slim

RUN pip install \
    dagster \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker \
    psycopg2-binary

# Set $DAGSTER_HOME and copy dagster.yaml and workspace.yaml there
ENV DAGSTER_HOME=/opt/dagster/app/

RUN mkdir -p $DAGSTER_HOME

# Add code location code
COPY dagster_code/ops/ /opt/dagster/app/ops/
COPY dagster_code/definitions.py /opt/dagster/app/definitions.py
COPY dagster_webserver/dagster.yaml dagster_webserver/workspace.yaml $DAGSTER_HOME
COPY dagster_webserver/init.sh /opt/dagster/app

WORKDIR $DAGSTER_HOME