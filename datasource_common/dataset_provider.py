from tempfile import TemporaryDirectory


class DatasetProvider:
    def __enter__(self):
        self.tmpdir = TemporaryDirectory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tmpdir.cleanup()

    def get_dataset(self):
        raise NotImplemented
