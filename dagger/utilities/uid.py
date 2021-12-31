from hashlib import md5
from dagger.pipeline import io, task, pipeline


def get_uid(*args):
    return md5("".join(args).encode()).hexdigest()


def get_pipeline_uid(pipeline_object: pipeline.Pipeline) -> str:
    return get_uid(pipeline_object.name)


def get_task_uid(task_object: task.Task) -> str:
    return get_uid(task_object.pipeline_name, task_object.name)


def get_dataset_uid(dataset_object: io.IO) -> str:
    return get_uid(dataset_object.alias())
