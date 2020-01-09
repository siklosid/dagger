import logging
from acirc.alerts.alert import SlackAlert

import os

BASE_PATH = os.path.join(os.getcwd(), '..')
EXTRAS_DIR = os.path.join(BASE_PATH, 'extras')

# App parameters
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/usr/local/airflow/')
DAGS_DIR = os.path.join(AIRFLOW_HOME, 'dags')
ENV = os.environ.get('ENV', 'local')
ENV_SUFFIX = "dev" if ENV == "local" else ""
WITH_DATA_NODES = True
DEFAULT_ALERT = SlackAlert('conf.py', {
    'channel': '#airflow-jobs',
    'mentions': None
})

## Logging config
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# ConfigFinder
_logger = logging.getLogger('configFinder')
_logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
_logger.addHandler(ch)

_logger = logging.getLogger('graph')
_logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
_logger.addHandler(ch)

# Default task parameters
REDSHIFT_CONN_ID = 'redshift_default'
REDSHIFT_IAM_ROLE = "arn:aws:iam::120444018371:role/redshift"

SPARK_S3_FILES_BUCKET = "circdata-files"
SPARK_EMR_MASTER = "spark-jobs.data.circ"
SPARK_DEFAULT_ENGINE = "emr"

SQOOP_DEFAULT_FORMAT = "avro"
