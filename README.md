=====
dagger
=====


How to install
--------

* `virtualenv -p python3 venv`
* `. venv/bin/activate`
* `make install`
* `dagger --help`

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


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
