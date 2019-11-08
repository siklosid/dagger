from config_finder.config_finder import ConfigFinder
from config_finder.config_processor import ConfigProcessor
from graph.task_graph import TaskGraph

import conf

cf = ConfigFinder(conf.DAGS_DIR)
cp = ConfigProcessor(cf)
pipelines = cp.process_pipeline_configs()

g = TaskGraph()

for pipeline in pipelines:
    g.add_pipeline(pipeline)

g.print_graph()



