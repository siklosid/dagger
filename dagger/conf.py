import logging
import os
from envyaml import EnvYAML
from pathlib import Path

# BASE_PATH = os.path.join(os.getcwd(), "..")
# EXTRAS_DIR = os.path.join(BASE_PATH, "extras")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow/")
config_file = Path(AIRFLOW_HOME) / "dagger_config.yaml"
if config_file.is_file():
    config = EnvYAML(config_file)
else:
    config = {}

# App parameters
DAGS_DIR = os.path.join(AIRFLOW_HOME, "dags")
ENV = os.environ.get("ENV", "local")
ENV_SUFFIX = "dev" if ENV == "local" else ""

# Airflow parameters
airflow_config = config.get('airflow', {})
WITH_DATA_NODES = airflow_config.get('with_data_nodes', False)

# Neo4j parameters
neo4j_config = config.get('neo4j', {})
NE4J_HOST = neo4j_config.get('host', "localhost")
NE4J_PORT = neo4j_config.get('port', 7687)

# Elastic Search Parameters
es_config = config.get('elastic_search', {})
ES_HOST = es_config.get('host', "localhost")
ES_PORT = es_config.get('port', 9201)
ES_INDEX = es_config.get('index', None)


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

## Default task parameters
# Redshift
redshift_config = config.get('redshift', {})
REDSHIFT_CONN_ID = redshift_config.get('conn_id', None)
REDSHIFT_IAM_ROLE = redshift_config.get('iam_role', None)

# Spark
spark_config = config.get('spark', {})
SPARK_S3_FILES_BUCKET = spark_config.get('files_s3_bucket', None)
SPARK_S3_LIBS_SUFFIX = spark_config.get('libs_s3_path', None)
SPARK_EMR_MASTER = spark_config.get('emr_master', None)
SPARK_DEFAULT_ENGINE = spark_config.get('default_engine', 'emr')
SPARK_OVERHEAD_MULTIPLIER = spark_config.get('overhead_multiplier', 1.5)

# Sqoop
sqoop_config = config.get('sqoop', {})
SQOOP_DEFAULT_FORMAT = sqoop_config.get('default_file_format', "avro")
SQOOP_DEFAULT_PROPERTIES = sqoop_config.get('default_properties', {"mapreduce.job.user.classpath.first": "true"})

# Alert parameters
alert_config = config.get('alert', {})
SLACK_TOKEN = alert_config.get('slack_token', None)
DEFAULT_ALERT = alert_config.get('default_alert', {"type": "slack", "channel": "#airflow-jobs", "mentions": None})