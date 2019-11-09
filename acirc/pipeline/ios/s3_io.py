from acirc.pipeline.io import IO


class S3IO(IO):
    ref_name = "s3"

    def __init__(self, io_config):
        super().__init__(io_config)

        self._bucket = io_config['bucket']
        self._path = io_config['path']

    def alias(self):
        return "s3://{bucket}/{path}".format(bucket=self._bucket, path=self._path)

