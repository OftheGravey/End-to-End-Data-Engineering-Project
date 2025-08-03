FROM python:3.10-slim

RUN pip install \
    dagster \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker \
    psycopg2-binary

ENV DAGSTER_HOME=/opt/dagster/app/

RUN mkdir -p $DAGSTER_HOME

COPY dagster/ops/ $DAGSTER_HOME/ops
COPY dagster/definitions.py $DAGSTER_HOME/definitions.py
COPY dagster/dagster.yaml dagster/workspace.yaml $DAGSTER_HOME
COPY dagster/init.sh $DAGSTER_HOME

WORKDIR $DAGSTER_HOME
EXPOSE 4000

CMD ["bash", "init.sh"]