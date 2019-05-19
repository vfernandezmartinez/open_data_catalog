import os
from tempfile import TemporaryDirectory

import psycopg2.extras
import xlrd

from datasource_common.dataset_import_task import DatasetImportTask
from datasource_common.downloads import download_file
from datasource_common.log import log


PROVINCES_FILE_URL = 'http://www.ine.es/daco/daco42/clasificaciones/codprov.xls'


class ProvincesDownloader:
    def __enter__(self):
        self._tmpdir = TemporaryDirectory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmpdir.cleanup()

    def download(self):
        provinces_file = os.path.join(self._tmpdir.name, 'codprov.xls')
        log.info('Downloading provinces file')
        download_file(PROVINCES_FILE_URL, provinces_file)

        return {
            'provinces_file': provinces_file
        }


class ProvincesImporter(DatasetImportTask):
    TABLE_NAME = 'provinces_spain'
    DOWNLOADER_CLASS = ProvincesDownloader

    def create_temporary_table(self):
        log.info('Creating province table')
        self.cur.execute(
            'CREATE TABLE {} ('
            '  cpro CHAR(2) NOT NULL,'
            '  province TEXT NOT NULL);'
                .format(self.temporary_table_name))

    def populate_table(self):
        provinces = self._get_provinces()
        query = f'INSERT INTO {self.temporary_table_name} (cpro, province) VALUES %s;'
        psycopg2.extras.execute_values(self.cur, query, provinces)

    def _get_provinces(self):
        log.info('Importing provinces')
        book = xlrd.open_workbook(self.downloads['provinces_file'])

        provinces = []
        for sheet in book.sheets():
            for row in sheet.get_rows():
                try:
                    province_code = format(int(row[0].value), '02')
                    province_name = row[1].value
                    province = province_code, province_name,
                    provinces.append(province)
                except ValueError:
                    pass

        return provinces

    def create_indexes(self):
        log.info('Creating province index')
        self.cur.execute(
            'ALTER TABLE {} '
            'ADD PRIMARY KEY(cpro);'
                .format(self.temporary_table_name))
