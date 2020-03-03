from dagger.config_finder.config_finder import ConfigFinder
from dagger.config_finder.config_processor import ConfigProcessor
from dagger.graph.task_graph import TaskGraph
from dagger.dag_creator.airflow.dag_creator import DagCreator
from dagger import conf


def collect_dags():
    cf = ConfigFinder(conf.DAGS_DIR)
    cp = ConfigProcessor(cf)

    pipelines = cp.process_pipeline_configs()

    g = TaskGraph()
    for pipeline in pipelines:
        g.add_pipeline(pipeline)

    dc = DagCreator(g._graph)
    dags = dc.create_dags()
    return dags
