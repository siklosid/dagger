# Dagger
Dagger is a light framework that can convert simple yaml files into complex Airflow dags. It makes your pipelines more re-usable and more structured. Based on matching the inputs and outputs of your etl jobs it can visualise the full dependency graph of your workflows including your datasets as well.


# Folder structure

```bash
.
├── dagger
│   ├── alerts
│   ├── cli
│   ├── config_finder
│   ├── dag_creator # takes the graph object and outputs it into the respective format
│   │   ├── airflow # generates dag definitions based on graph object
│   │   │   ├── hooks
│   │   │   ├── operator_creators # contains modules to create task specific operator
│   │   │   ├── operators # custom airflow creators that can be used in the operator creator
│   │   │   └── utils 
│   │   ├── elastic_search
│   │   └── neo4j
│   ├── graph # module that builds up a graph by matching inputs and outputs different task definitions.
│   ├── pipeline 
│   │   ├── ios # contains definition of the specific inputs and outputs of a task
│   │   └── tasks # contains definition of the task configurations  
│   └── utilities
├── dagger_ui
│   └── app
├── dockers
│   ├── airflow
│   └── dagger_ui
├── docs
├── extras
├── reqs
├── tests
│   ├── dag_creator
│   ├── fixtures
│   ├── graph
│   ├── pipeline
│   └── utilities
```

How to install
--------
* `virtualenv -p python3 venv`
* `. venv/bin/activate`
* `make install`
* `dagger --help`

How to install for development
--------
* `make install-dev`
* `. venv/bin/activate`

How to test locally
--------
* Build and start airflow in docker: `make test-airflow`
* Go to `localhost:8080` in your browser to see airflow UI.
    * User: dev_user
    * Password: dev_user
* Example Dagger dags are defined at tests/fixtures/config_finder/root/dags/ and mounted as the dags directory in the container


How to use it
--------
* Install it where airflow is running
* Put the dagger/collect_dags.py into your airflow dags folder
* Create a directory for your new airflow pipeline
* With the help of dagger cli create a pipeline.yaml file in the directory: `dagger init-pipeline`
* With the help of dagger cli add task yaml configurations:
    * `dagger list-tasks`
    * `dagger init-task --type=<task_type>`
* With the help of dagger cli add your inputs and outputs to the task configuration file:
    * dagger list-ios
    * dagger init-io --type=<io_type>
* Check your airflow UI. Airflow dag is generated automatically and dependencies are set up based on matching inputs/outputs of tasks

How to add new Airflow task
-------

```mermaid
flowchart TD;
  A[Add new task definition in pipeline/tasks] --> B{Do new inputs and output need to be created}
  B -->|yes| C[create them in pipeline/ios]
  B -->|no| D[Use the existing inputs and outputs defined in pipeline/ios]
  C --> E[Create a new operator creator in dag_creator/airflow/operator_creators]
  D --> E
   
```


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
