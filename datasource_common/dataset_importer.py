from .log import log


class DatasetImporter:
    dataset_provider_class = None

    def __init__(self, connection_string, connection):
        self.connection_string = connection_string
        self.connection = connection
        self.cur = None
        self.dataset = None

    def setup(self):
        pass

    def run(self):
        log.info('---- %s ----', self.__class__.__name__)
        with self.dataset_provider_class() as provider:
            self.dataset = provider.get_dataset()
            self.import_dataset()
        log.info('------------')

    def import_dataset(self):
        try:
            self.cur = self.connection.cursor()
            self.setup()
            self.populate_table()
        except:
            self.connection.rollback()
            raise
        finally:
            self.cur.close()
            self.cur = None

    def populate_table(self):
        raise NotImplemented


class DatasetTemporaryTableImporter(DatasetImporter):
    table_name = None

    @property
    def temporary_table_name(self):
        return f'_tmp_{self.table_name}'

    def drop_temporary_table(self):
        self.cur.execute(f'DROP TABLE IF EXISTS {self.temporary_table_name} CASCADE;')

    def create_temporary_table(self):
        pass

    def drop_original_table(self):
        self.cur.execute(f'DROP TABLE IF EXISTS {self.table_name} CASCADE;')

    def create_indexes(self):
        pass

    def analyze_table(self):
        self.cur.execute(f'ANALYZE {self.temporary_table_name};')

    def rename_to_target_table(self):
        self.cur.execute(f'ALTER TABLE {self.temporary_table_name} RENAME TO {self.table_name};')

    def import_dataset(self):
        try:
            self.cur = self.connection.cursor()
            self.setup()
            self.drop_temporary_table()
            self.create_temporary_table()
            self.populate_table()
            self.drop_original_table()
            self.create_indexes()
            self.analyze_table()
            self.rename_to_target_table()
            self.connection.commit()
        except:
            self.connection.rollback()
            self.drop_temporary_table()
            self.connection.commit()
            raise
        finally:
            self.cur.close()
            self.cur = None
