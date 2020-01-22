from acirc.utilities.config_validator import Attribute
from acirc.pipeline.io import IO


class GDriveIO(IO):
    ref_name = "gdrive"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='folder', format_help='ID shown at the URL address of the Google Drive folder'),
            Attribute(attribute_name='file_name')
        ])

    def __init__(self, io_config, task):
        super().__init__(io_config, task)

        self._folder = self.parse_attribute('folder')
        self._file_name = self.parse_attribute('file_name')

    def alias(self):
        return "gdrive-{}-{}".format(self._folder, self._file_name)

    @property
    def rendered_name(self):
        return f"{self._folder}/{self._file_name}"

    @property
    def airflow_name(self):
        return "gdrive-{}-{}".format(self._folder, self._file_name)
