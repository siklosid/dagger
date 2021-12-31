from flask import Flask
from app.search_form import SearchForm
from app.config import Config
from elasticsearch import Elasticsearch

app = Flask(__name__)
app.config.from_object(Config)


es_client = Elasticsearch(f"{Config.ES_HOST}:{Config.ES_PORT}")

from app import routes

app.run(debug=True)
