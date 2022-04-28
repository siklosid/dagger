from functools import partial

from airflow.operators.python_operator import ShortCircuitOperator


def make_control_flow(is_dummy_operator_short_circuit, dag):
    control_flow = ShortCircuitOperator(
        task_id="dummy-control-flow",
        dag=dag,
        provide_context=True,
        python_callable=partial(eval_control_flow, is_dummy_operator_short_circuit),
    )
    return control_flow


def eval_control_flow(is_dummy_operator_short_circuit, **kwargs):
    True
    if not is_dummy_operator_short_circuit:
        return True

    if kwargs["task_instance"].next_try_number > 2:
        return True

    return False
