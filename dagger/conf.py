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
airflow_config = config.get('airflow', None) or {}
WITH_DATA_NODES = airflow_config.get('with_data_nodes', False)
EXTERNAL_SENSOR_POKE_INTERVAL = airflow_config.get('external_sensor_poke_interval', 600)
EXTERNAL_SENSOR_TIMEOUT = airflow_config.get('external_sensor_timeout', 28800)
EXTERNAL_SENSOR_MODE = airflow_config.get('external_sensor_mode', 'reschedule')
IS_DUMMY_OPERATOR_SHORT_CIRCUIT = airflow_config.get('is_dummy_operator_short_circuit', False)

# Neo4j parameters
neo4j_config = config.get('neo4j', None) or {}
NE4J_HOST = neo4j_config.get('host', "localhost")
NE4J_PORT = neo4j_config.get('port', 7687)

# Elastic Search Parameters
es_config = config.get('elastic_search', None) or {}
ES_HOST = es_config.get('host', "localhost")
ES_PORT = es_config.get('port', 9200)
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
redshift_config = config.get('redshift', None) or {}
REDSHIFT_CONN_ID = redshift_config.get('conn_id', None)
REDSHIFT_IAM_ROLE = redshift_config.get('iam_role', None)

# Spark
spark_config = config.get('spark', None) or {}
SPARK_JOB_BUCKET = spark_config.get('job_bucket', None)
SPARK_CLUSTER_NAME = spark_config.get('cluster_name', None)
SPARK_DEFAULT_QUEUE = spark_config.get('default_queue', None)
SPARK_OVERHEAD_MULTIPLIER = spark_config.get('overhead_multiplier', 1.5)

# Batch
batch_config = config.get('batch', None) or {}
BATCH_AWS_REGION = batch_config.get('aws_region', None)
BATCH_CLUSTER_NAME = batch_config.get('cluster_name', None)
BATCH_AWS_CONN_ID = batch_config.get('aws_conn_id', None)
BATCH_DEFAULT_QUEUE = batch_config.get('default_queue', None)

# Athena
athena_config = config.get('athena', None) or {}
ATHENA_AWS_CONN_ID = athena_config.get('aws_conn_id', None)
ATHENA_DEFAULT_S3_OUTPUT_BUCKET = athena_config.get('default_s3_output_location', None)
ATHENA_DEFAULT_S3_OUTPUT_PATH = athena_config.get('default_s3_output_path', None)
ATHENA_S3_TMP_RESULTS_LOCATION = athena_config.get('s3_tmp_results_location', None)
ATHENA_DEFAULT_WORKGROUP = athena_config.get('default_workgroup', None)
ATHENA_DEFAULT_OUTPUT_FORMAT = athena_config.get('default_output_format', None)

# Sqoop
sqoop_config = config.get('sqoop', None) or {}
SQOOP_DEFAULT_FORMAT = sqoop_config.get('default_file_format', "avro")
SQOOP_DEFAULT_PROPERTIES = sqoop_config.get('default_properties', {"mapreduce.job.user.classpath.first": "true"})

# Alert parameters
alert_config = config.get('alert', None) or {}
SLACK_TOKEN = alert_config.get('slack_token', None)
DEFAULT_ALERT = alert_config.get('default_alert', {"type": "slack", "channel": "#airflow-jobs", "mentions": None})