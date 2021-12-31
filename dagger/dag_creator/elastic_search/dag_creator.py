from dagger import conf
from dagger.dag_creator.graph_traverser_base import GraphTraverserBase
from dagger.graph.task_graph import Graph
from dagger.utilities import uid
from elasticsearch import Elasticsearch

ES_TYPE_DAG = "dag"
ES_TYPE_TASK = "task"
ES_TYPE_DATASET = "dataset"


class DagCreator(GraphTraverserBase):
    def __init__(self, task_graph: Graph):
        super().__init__(task_graph, True)

        self.es = Elasticsearch(
            hosts=[f"{conf.ES_HOST}:{conf.ES_PORT}"]
        )

        self._reset_index()

    def _reset_index(self):
        self.es.indices.delete(index='index', ignore=[400, 404])
        self.es.indices.create(
            index='index',
            body={
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "my_analyzer": {
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "my_stemmer"
                                ]
                            }
                        },
                        "filter": {
                            "my_stemmer": {
                                "type": "stemmer",
                                "language": "english"
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "name": {
                            "type": "text",
                            "analyzer": "my_analyzer",
                            "search_analyzer": "my_analyzer",
                        },
                        "description": {
                            "type": "text",
                            "analyzer": "my_analyzer",
                            "search_analyzer": "my_analyzer",
                        },
                        "type": {
                            "type": "text"
                        },
                    }
                }
            }
        )

    def _index_doc(self, doc, id):
        return self.es.index(index=conf.ES_INDEX, document=doc, id=id)

    def _create_dag(self, pipe_id, node):
        uuid = uid.get_pipeline_uid(node.obj)
        es_doc = {
            "name": node.obj.name,
            "description": node.obj.description,
            "type": ES_TYPE_DAG
        }
        result = self._index_doc(es_doc, uuid)
        return result['_id']

    def _create_job_task(self, node):
        uuid = uid.get_task_uid(node.obj)
        es_doc = {
            "name": node.obj.name,
            "description": node.obj.description,
            "type": ES_TYPE_TASK
        }
        result = self._index_doc(es_doc, uuid)
        return result['_id']

    def _create_data_task(self, pipe_id, node):
        uuid = uid.get_dataset_uid(node.obj)
        if uuid in self._data_tasks:
            return uuid

        es_doc = {
            "name": node.obj.alias(),
            "description": node.obj.name,
            "type": ES_TYPE_DATASET
        }
        result = self._index_doc(es_doc, uuid)
        return result['_id']

    def _create_edge_without_data(self, from_task_id, to_task_ids, node):
        raise NotImplemented

    def _create_edge_with_data(self, from_task_id, to_task_ids, node):
        pass
