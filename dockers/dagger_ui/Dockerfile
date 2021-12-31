FROM python:3.8-slim

ENV DAGGER_HOME /usr/local/dagger
ENV PYTHONPATH /usr/local/dagger


# Configure environment
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
#ENV AIRFLOW_GPL_UNIDECODE yes
#ENV BUILD_DEPS='python3-dev libkrb5-dev libsasl2-dev libssl-dev libffi-dev build-essential liblapack-dev libpq-dev'
#ENV CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install build dependencies & utilities
#RUN apt-get update -yqq \
#    && apt-get upgrade -yqq \
#    && apt-get install -yqq --no-install-recommends \
#        apt-utils \
#        freetds-bin \
#        libsasl2-modules \
#        locales \
#        lsb-release \
#        sasl2-bin \
#        sqlite3 \
#        unixodbc \
#        $BUILD_DEPS \
#        python3-pip \
#        python3-requests \
#        curl \
#        netcat \
#        net-tools \
#        sudo \
#        vim \
#        htop \
#        procps \
#        less \
#        jq \
#        ssh \
#        git \
#        zip unzip

# Install python libraries & cleanup
COPY requirements.txt $DAGGER_HOME/requirements.txt
RUN pip install -r $DAGGER_HOME/requirements.txt


# Add files
COPY docker-entrypoint.sh $DAGGER_HOME/docker-entrypoint.sh

WORKDIR $DAGGER_HOME
ENTRYPOINT ["./docker-entrypoint.sh"]
