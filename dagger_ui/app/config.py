import os


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    ES_HOST = os.environ.get('ES_HOST') or 'localhost'
    ES_PORT = os.environ.get('ES_PORT') or 9200
    NEO4J_HOST = os.environ.get('NEO4J_HOST') or 'localhost'
    NEO4J_PORT = os.environ.get('NEO4J_PORT') or 7687

