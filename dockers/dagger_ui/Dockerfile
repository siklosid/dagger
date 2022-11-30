FROM python:3.8-slim

ENV DAGGER_HOME /usr/local/dagger
ENV PYTHONPATH /usr/local/dagger

#ARG BUILD_DEPS="python3-dev libc-dev-bin libffi-dev libkrb5-dev libpq-dev libsasl2-dev libssl-dev"
#ARG AIRFLOW_CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
#ENV AIRFLOW_GPL_UNIDECODE yes
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Install build dependencies & utilities
#RUN apt-get update -yq \
#    && apt-get upgrade -yq \
#    && apt-get install -yq --no-install-recommends \
#        $BUILD_DEPS \
#        curl \
#        freetds-bin \
#        git \
#        groff \
#        htop \
#        jq \
#        less \
#        libpq5 \
#        libsasl2-modules \
#        locales \
#        lsb-release \
#        net-tools \
#        netcat \
#        procps \
#        python3-pip \
#        sasl2-bin \
#        sqlite3 \
#        ssh \
#        sudo \
#        unixodbc \
#        unzip \
#        vim \
#        zip

# Install python libraries & cleanup
COPY requirements.txt $DAGGER_HOME/requirements.txt
RUN pip install -r $DAGGER_HOME/requirements.txt


# Add files
COPY docker-entrypoint.sh $DAGGER_HOME/docker-entrypoint.sh

WORKDIR $DAGGER_HOME
ENTRYPOINT ["./docker-entrypoint.sh"]
