from dagger.utilities.config_validator import Attribute
from dagger.pipeline.io import IO
from os.path import join, normpath


class S3IO(IO):
    ref_name = "s3"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='s3_protocol', required=False, comment="S3 protocol: s3a/s3/s3n"),
            Attribute(attribute_name='bucket'),
            Attribute(attribute_name='path')
        ])

    def __init__(self, io_config, task):
        super().__init__(io_config, task)

        self._s3_protocol = self.parse_attribute('s3_protocol') or "s3"
        self._bucket = normpath(self.parse_attribute('bucket'))
        self._path = normpath(self.parse_attribute('path'))

    def alias(self):
        return "s3://{path}".format(path=join(self._bucket, self._path))

    @property
    def rendered_name(self):
        return "{protocol}://{path}".format(protocol=self._s3_protocol, path=join(self._bucket, self._path))

    @property
    def airflow_name(self):
        return "s3-{}".format(join(self._bucket, self._path).replace('/', '-'))
