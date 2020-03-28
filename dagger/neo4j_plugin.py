from dagger import conf
from dagger.config_finder.config_finder import ConfigFinder
from dagger.config_finder.config_processor import ConfigProcessor
from dagger.dag_creator.neo4j.dag_creator import DagCreator
from dagger.graph.task_graph import TaskGraph

cf = ConfigFinder(conf.DAGS_DIR)
cp = ConfigProcessor(cf)

pipelines = cp.process_pipeline_configs()

g = TaskGraph()
for pipeline in pipelines:
    g.add_pipeline(pipeline)

dc = DagCreator(g._graph)
dags = dc.traverse_graph()
