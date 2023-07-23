from dagger.pipeline.io import IO
from os import path
from dagger.pipeline.ios import (
    athena_io,
    db_io,
    dummy_io,
    gdrive_io,
    redshift_io,
    s3_io
)
from dagger.utilities.classes import get_deep_obj_subclasses


class IOFactory:
    def __init__(self):
        self.factory = dict()

        for cls in get_deep_obj_subclasses(IO):
            self.factory[cls.ref_name] = cls

    def create_io(self, ref_name, io_config, task):
        config_location = path.join(task.pipeline.directory, task.name + ".yaml")
        return self.factory[ref_name](io_config=io_config, config_location=config_location)
