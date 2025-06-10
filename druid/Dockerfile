FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    openjdk-17-jdk \
    netcat-openbsd \
    gnupg \
    coreutils \
    procps \
    perl-modules-5.36 \
    && apt-get clean

# Set environment variables
ENV DRUID_VERSION=33.0.0
ENV DRUID_HOME=/opt/druid
ENV PATH="$DRUID_HOME/bin:$PATH"

# Download and extract Druid
RUN curl -L https://downloads.apache.org/druid/${DRUID_VERSION}/apache-druid-${DRUID_VERSION}-bin.tar.gz \
    | tar -xz -C /opt && \
    mv /opt/apache-druid-${DRUID_VERSION} ${DRUID_HOME}

# Create necessary folders for Druid to run
RUN mkdir -p /opt/druid/var

# Workdir inside Druid
WORKDIR /opt/druid

# Expose default Druid UI port
EXPOSE 8081
