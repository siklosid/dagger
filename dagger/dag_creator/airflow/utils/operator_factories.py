from functools import partial

from airflow.operators.python_operator import ShortCircuitOperator


def make_control_flow(eval, dag):
    control_flow = ShortCircuitOperator(
        task_id="dummy-control-flow",
        dag=dag,
        provide_context=True,
        python_callable=partial(eval_control_flow, eval),
    )
    return control_flow


def eval_control_flow(eval, **kwargs):
    True
    if eval:
        return True

    if kwargs["task_instance"].next_try_number > 2:
        return True

    return False
