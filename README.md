=====
acirc
=====


How to install
--------

* `virtualenv -p python3 venv`
* `. venv/bin/activate`
* `make install`
* `acirc --help`

How to use it
--------
* Install it where airflow is running
* Put the acirc/collect_dags.py into your airflow dags folder
* Create a directory for your new airflow pipeline
* With the help of acirc cli create a pipeline.yaml file in the directory: `acirc init-pipeline`
* With the help of acirc cli add task yaml configurations:
    * `acirc list-tasks`
    * `acirc init-task --type=<task_type>`
* With the help of acirc cli add your inputs and outputs to the task configuration file:
    * acirc list-ios
    * acirc init-io --type=<io_type>
* Check your airflow UI. Airflow dag is generated automatically and dependencies are set up based on matching inputs/outputs of tasks


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
