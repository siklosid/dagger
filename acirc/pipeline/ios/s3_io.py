from acirc.utilities.config_validator import Attribute
from acirc.pipeline.io import IO
from os.path import join


class S3IO(IO):
    ref_name = "s3"

    @classmethod
    def init_attributes(cls):
        cls.add_config_attributes([
            Attribute(attribute_name='bucket'),
            Attribute(attribute_name='path')
        ])

    def __init__(self, io_config):
        S3IO.init_attributes_once()
        super().__init__(io_config)

        self._bucket = self.parse_attribute('bucket')
        self._path = self.parse_attribute('path')

    def alias(self):
        return "s3://{path}".format(path=join(self._bucket, self._path))

    @property
    def rendered_name(self):
        return self.alias()
