from datetime import datetime

import croniter
from airflow.sensors.external_task import ExternalTaskSensor


class CronAwareExternalTaskSensor(ExternalTaskSensor):
    """
    ExternalTaskSensor that derives the upstream logical_date from two cron schedules.

    Stores ``from_schedule`` and ``to_schedule`` as serializable string attributes so
    behavior survives Airflow 3.x DAG serialization, where ``execution_date_fn`` closures
    are stringified and lose their captured variables.
    """

    template_fields = ExternalTaskSensor.template_fields + ["from_schedule", "to_schedule"]

    def __init__(self, *, from_schedule: str, to_schedule: str, **kwargs):
        super().__init__(**kwargs)
        self.from_schedule = from_schedule
        self.to_schedule = to_schedule

    def _get_dttm_filter(self, context):
        logical_date = context["logical_date"]

        to_dag_cron = croniter.croniter(self.to_schedule, logical_date)
        to_dag_next_schedule = to_dag_cron.get_next(datetime)

        from_dag_cron = croniter.croniter(self.from_schedule, to_dag_next_schedule)
        from_dag_cron.get_next(datetime)
        from_dag_cron.get_prev(datetime)
        from_dag_target_schedule = from_dag_cron.get_prev(datetime)

        return [from_dag_target_schedule]
