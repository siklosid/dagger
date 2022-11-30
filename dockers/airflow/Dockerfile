FROM python:3.7-slim

# Image arguments and variables
ARG AIRFLOW_VERSION
ENV AIRFLOW_HOME /usr/local/airflow
ENV AIRFLOW_GPL_UNIDECODE yes

ARG PYTHON_VERSION
ENV PYTHON_VERSION 3.7

ARG BUILD_DEPS="python3-dev libc-dev-bin libffi-dev libkrb5-dev libpq-dev libsasl2-dev libssl-dev"
ARG AIRFLOW_CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Install build dependencies & utilities
RUN apt-get update -yq \
    && apt-get upgrade -yq \
    && apt-get install -yq --no-install-recommends \
        $BUILD_DEPS \
        curl \
        freetds-bin \
        git \
        groff \
        htop \
        jq \
        less \
        libpq5 \
        libsasl2-modules \
        locales \
        lsb-release \
        net-tools \
        netcat \
        procps \
        python3-pip \
        sasl2-bin \
        sqlite3 \
        ssh \
        sudo \
        unixodbc \
        unzip \
        vim \
        zip

# Install AWS CLI
RUN curl -Ls "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip -q awscliv2.zip && \
    ./aws/install && \
    rm -rf aws*

# Install Airflow, extra Python libraries, then cleanup
RUN pip install -U --progress-bar off --no-cache-dir pip setuptools wheel

COPY requirements.txt requirements.txt
RUN pip install --progress-bar off --no-cache-dir apache-airflow[amazon,postgres,s3,statsd]==$AIRFLOW_VERSION --constraint $AIRFLOW_CONSTRAINTS && \
    pip install --progress-bar off --no-cache-dir -r requirements.txt && \
    apt-get purge --auto-remove -yq $BUILD_DEPS && \
    apt-get autoremove --purge -yq && \
    apt-get clean -yq && \
    rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# Setup Airflow user
RUN useradd --user-group --no-log-init -m -d $AIRFLOW_HOME --shell /bin/bash airflow \
    && chown -R airflow: $AIRFLOW_HOME \
    && echo airflow:airflow | chpasswd \
    && echo "airflow ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/airflow \
    && chmod 0440 /etc/sudoers.d/airflow

USER airflow

# Add configuration files
COPY docker-entrypoint.sh $AIRFLOW_HOME/docker-entrypoint.sh
COPY airflow.cfg $AIRFLOW_HOME/airflow.cfg

WORKDIR $AIRFLOW_HOME
ENTRYPOINT ["./docker-entrypoint.sh"]
