FROM python:3.7-slim

ARG AIRFLOW_VERSION
ENV AIRFLOW_HOME /usr/local/airflow

# Configure environment
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV AIRFLOW_GPL_UNIDECODE yes
ENV BUILD_DEPS='python3-dev libkrb5-dev libsasl2-dev libssl-dev libffi-dev build-essential liblapack-dev libpq-dev'

# Install build dependencies & utilities
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        apt-utils \
        $BUILD_DEPS \
        python3-pip \
        python3-requests \
        curl \
        netcat \
        net-tools \
        sudo \
        vim \
        htop \
        procps \
        less \
        jq \
        ssh \
        git

# Install python libraries & cleanup
RUN pip install -U pip setuptools wheel pytest-runner
RUN pip install \
    pytz \
    pyOpenSSL \
    ndg-httpsclient \
    apache-airflow[crypto,slack,druid,postgres,s3,statsd]==$AIRFLOW_VERSION \
    psycopg2-binary \
    -U awscli==1.16 \
    && apt-get purge --auto-remove -yqq $BUILD_DEPS \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# Setup airflow user
RUN useradd --user-group --no-log-init -m -d $AIRFLOW_HOME --shell /bin/bash airflow \
    && chown -R airflow: $AIRFLOW_HOME \
    && echo airflow:airflow | chpasswd \
    && echo "airflow ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/airflow \
    && chmod 0440 /etc/sudoers.d/airflow

USER airflow

# Install additional python libraries
COPY requirements.txt $AIRFLOW_HOME/requirements.txt
RUN /usr/local/bin/pip install --user -r  $AIRFLOW_HOME/requirements.txt

# Add files
COPY docker-entrypoint.sh $AIRFLOW_HOME/docker-entrypoint.sh
COPY airflow.cfg $AIRFLOW_HOME/airflow.cfg

WORKDIR $AIRFLOW_HOME
ENTRYPOINT ["./docker-entrypoint.sh"]
