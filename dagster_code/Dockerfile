FROM python:3.10-slim

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker

# Add code location code
COPY dagster_code/ops/ /opt/dagster/app/ops/
COPY dagster_code/definitions.py /opt/dagster/app/definitions.py
WORKDIR /opt/dagster/app

# Run dagster code server on port 4000
EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors to execute runs and steps
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-f", "definitions.py"]