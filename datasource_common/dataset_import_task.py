from .log import log


class DatasetImportTask:
    TABLE_NAME = None
    DOWNLOADER_CLASS = None

    def __init__(self, connection_string, connection):
        self.connection_string = connection_string
        self.connection = connection
        self.cur = None
        self.downloads = None

    @property
    def table_name(self):
        return self.TABLE_NAME

    @property
    def temporary_table_name(self):
        return f'_tmp_{self.table_name}'

    def drop_temporary_table(self):
        self.cur.execute(f'DROP TABLE IF EXISTS {self.temporary_table_name} CASCADE;')

    def create_temporary_table(self):
        pass

    def populate_table(self):
        raise NotImplemented

    def drop_original_table(self):
        self.cur.execute(f'DROP TABLE IF EXISTS {self.table_name} CASCADE;')

    def create_indexes(self):
        pass

    def analyze_table(self):
        self.cur.execute(f'ANALYZE {self.temporary_table_name};')

    def rename_to_target_table(self):
        self.cur.execute(f'ALTER TABLE {self.temporary_table_name} RENAME TO {self.table_name};')

    def import_data(self):
        try:
            self.cur = self.connection.cursor()
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

    def run(self):
        log.info('---- %s ----', self.__class__.__name__)
        with self.DOWNLOADER_CLASS() as downloader:
            self.downloads = downloader.download()
            self.import_data()
