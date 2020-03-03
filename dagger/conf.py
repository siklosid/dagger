import logging
import os

BASE_PATH = os.path.join(os.getcwd(), "..")
EXTRAS_DIR = os.path.join(BASE_PATH, "extras")

# App parameters
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow/")
DAGS_DIR = os.path.join(AIRFLOW_HOME, "dags")
ENV = os.environ.get("ENV", "local")
ENV_SUFFIX = "dev" if ENV == "local" else ""
DEFAULT_ALERT = {"type": "slack", "channel": "#airflow-jobs", "mentions": None}

# Airflow parameters
WITH_DATA_NODES = True


## Logging config
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# ConfigFinder
_logger = logging.getLogger("configFinder")
_logger.setLevel(logging.ERROR)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
_logger.addHandler(ch)

_logger = logging.getLogger("graph")
_logger.setLevel(logging.ERROR)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
_logger.addHandler(ch)

_logger = logging.getLogger("alerts")
_logger.setLevel(logging.ERROR)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
_logger.addHandler(ch)

# Default task parameters
REDSHIFT_CONN_ID = "redshift_default"
REDSHIFT_IAM_ROLE = "arn:aws:iam::120444018371:role/redshift"

SPARK_S3_FILES_BUCKET = "circdata-files"
SPARK_EMR_MASTER = "spark-jobs.data.circ"
SPARK_DEFAULT_ENGINE = "emr"
SPARK_OVERHEAD_MULTIPLIER = 1.5

SQOOP_DEFAULT_FORMAT = "avro"
SQOOP_DEFAULT_PROPERTIES = {"mapreduce.job.user.classpath.first": "true"}
