import logging

import os

BASE_PATH = os.path.join(os.getcwd(), '..')
EXTRAS_DIR = os.path.join(BASE_PATH, 'extras')


DAGS_DIR = os.path.join(os.environ['AIRFLOW_HOME'], 'dags')
ENV = os.environ.get('ENV', 'local')

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
