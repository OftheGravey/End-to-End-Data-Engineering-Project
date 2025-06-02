# Start from the official Flink image
FROM flink:2.0.0-scala_2.12-java21

# Install Python and PyFlink
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s $(which python3) /usr/bin/python && \
    pip install uv

COPY pyproject.toml uv.lock /app/
WORKDIR /app

RUN uv pip install --system -r pyproject.toml

COPY flink_jobs/ /app/flink_jobs/
COPY lib/ /opt/flink/lib/ 

# Set environment variable for PyFlink
ENV PATH="/app/.venv/bin:$PATH"
ENV PYFLINK_PYTHON_EXECUTABLE=/usr/bin/python3
ENV PYTHONPATH=/app/flink_jobs

WORKDIR /