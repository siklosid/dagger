from dagger import conf
from dagger.dag_creator.graph_traverser_base import GraphTraverserBase
from dagger.graph.task_graph import Graph
from dagger.utilities import uid
from neo4j import GraphDatabase


class DagCreator(GraphTraverserBase):
    def __init__(self, task_graph: Graph):
        super().__init__(task_graph, True)
        neo4j_uri = "bolt://{host}:{port}".format(
            host=conf.NE4J_HOST, port=conf.NE4J_PORT
        )
        self._neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=("neo4j", "test"))

        with self._neo4j_driver.session() as session:
            session.write_transaction(self._reset_graph)

    @staticmethod
    def _reset_graph(tx):
        tx.run("MATCH ()-[r]->() DELETE r")
        tx.run("MATCH (n) DELETE n")

    @staticmethod
    def _add_node(tx, node_type: str, **kwargs):
        node_args = ", ".join([f'{key}:"{value}"' for key, value in kwargs.items()])
        create_cmd = f"CREATE (node:{node_type} {{{node_args}}}) RETURN node"
        result = tx.run(create_cmd)
        return result.single()['node'].id

    @staticmethod
    def _add_edge(tx, from_id: int, to_id: int, relationship_type: str, **kwargs):
        relationship_args = ", ".join(
            [f'{key}:"{value}"' for key, value in kwargs.items()]
        )
        create_cmd = f"""
            MATCH (from_node), (to_node)
            WHERE ID(from_node)={from_id} AND ID(to_node)={to_id}
            CREATE (from_node)-[:{relationship_type} {{{relationship_args}}}]->(to_node)
        """
        tx.run(create_cmd)

    def _create_dag(self, pipe_id, node):
        with self._neo4j_driver.session() as session:
            node_id = session.write_transaction(
                self._add_node,
                "Dag",
                name=node.obj.name,
                description=node.obj.description,
                uid=uid.get_pipeline_uid(node.obj)
            )
        return node_id

    def _create_job_task(self, node):
        with self._neo4j_driver.session() as session:
            node_id = session.write_transaction(
                self._add_node,
                "Job",
                name=node.obj.name,
                description=node.obj.description,
                uid=uid.get_task_uid(node.obj)
            )

        pipe_id = node.obj.pipeline_name
        with self._neo4j_driver.session() as session:
            session.write_transaction(
                self._add_edge, node_id, self._dags[pipe_id], "TASK_OF"
            )

        return node_id

    def _create_data_task(self, pipe_id, node):
        # if pipe_id not in self._data_tasks:
        #     self._data_tasks[pipe_id] = {}

        dataset_id = node.obj.airflow_name
        if dataset_id not in self._data_tasks:
            with self._neo4j_driver.session() as session:
                self._data_tasks[dataset_id] = session.write_transaction(
                    self._add_node,
                    "Dataset",
                    name=node.obj.alias(),
                    description=node.obj.name,
                    uid=uid.get_dataset_uid(node.obj)
                )

    def _create_edge_without_data(self, from_task_id, to_task_ids, node):
        raise NotImplemented

    def _create_edge_with_data(self, from_task_id, to_task_ids, node):
        from_pipe = (
            self._task_graph.get_node(from_task_id).obj.pipeline_name
            if from_task_id
            else None
        )
        data_id = node.obj.airflow_name
        if from_pipe:
            with self._neo4j_driver.session() as session:
                session.write_transaction(
                    self._add_edge,
                    self._tasks[from_task_id],
                    self._data_tasks[data_id],
                    "GENERATED_BY",
                )

        for to_task_id in to_task_ids:
            to_pipe = self._task_graph.get_node(to_task_id).obj.pipeline_name
            with self._neo4j_driver.session() as session:
                session.write_transaction(
                    self._add_edge,
                    self._data_tasks[data_id],
                    self._tasks[to_task_id],
                    "DEPENDS_ON",
                )
