import re
import os
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import xlrd

from datasource_common.dataset_import_task import DatasetImportTask
from datasource_common.downloads import download_file
from datasource_common.log import log


DESCRIPTION_FILE_URL = 'http://www.ine.es/censos2011_datos/indicadores_seccen_rejilla.xls'
CENSUS_DATA_URL = 'http://www.ine.es/censos2011_datos/indicadores_seccion_censal_csv.zip'


class CensusDownloader:
    def __enter__(self):
        self._tmpdir = TemporaryDirectory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmpdir.cleanup()

    def download(self):
        description_file = os.path.join(self._tmpdir.name, 'indicators.xls')
        log.info('Downloading indicators file')
        download_file(DESCRIPTION_FILE_URL, description_file)

        census_zip_file = os.path.join(self._tmpdir.name, 'census_csv.zip')
        log.info('Downloading census data')
        download_file(CENSUS_DATA_URL, census_zip_file)

        log.info('Extracting census data')
        with ZipFile(census_zip_file, 'r') as zip:
            csv_files = [
                os.path.join(self._tmpdir.name, filename)
                for filename in zip.namelist()
                if filename.endswith('.csv')
            ]
            zip.extractall(self._tmpdir.name)

        return {
            'description_file': description_file,
            'csv_files': csv_files,
        }


class CensusImporter(DatasetImportTask):
    TABLE_NAME = 'census_spain'
    DOWNLOADER_CLASS = CensusDownloader

    def create_temporary_table(self):
            indicators = self._get_indicators()

            table_fields = [
                ('ccaa', 'CHAR(2) NOT NULL'),
                ('cpro', 'CHAR(2) NOT NULL'),
                ('cmun', 'CHAR(3) NOT NULL'),
                ('dist', 'CHAR(2) NOT NULL'),
                ('secc', 'CHAR(3) NOT NULL'),
            ]
            table_fields.extend([
                (indicator_code, 'BIGINT',)
                for indicator_code, _ in indicators
            ])

            field_sql = ',\n'.join([
                f'{field_name} {field_type}'
                for field_name, field_type in table_fields
            ])
            log.info('Creating census table')
            self.cur.execute(
                f'CREATE TABLE {self.temporary_table_name} ({field_sql});'
            )
            for indicator_code, indicator_label in indicators:
                self.cur.execute(
                    f'COMMENT ON COLUMN {self.temporary_table_name}.{indicator_code} IS %s;',
                    (indicator_label,)
                )

    def _get_indicators(self):
        book = xlrd.open_workbook(self.downloads['description_file'])
        indicator_rx = re.compile(r't\d+_\d+')
        indicators = []

        for sheet in book.sheets():
            for row in sheet.get_rows():
                indicator_code = row[0].value
                if indicator_rx.fullmatch(indicator_code):
                    indicator_label = row[1].value
                    indicator = indicator_code, indicator_label,
                    indicators.append(indicator)

        return indicators

    def populate_table(self):
        for csv_file in self.downloads['csv_files']:
            log.info('Importing data from: %s', os.path.basename(csv_file))
            self._import_census_csv(csv_file)

    def _import_census_csv(self, filepath):
        with open(filepath, 'rb') as f:
            f.readline()  # skip the header
            self.cur.copy_from(f, self.temporary_table_name, sep=',', null='')

    def create_indexes(self):
        log.info('Creating census table indexes')
        self.cur.execute(
            'ALTER TABLE {} '
            'ADD PRIMARY KEY(cpro, cmun, dist, secc);'
                .format(self.temporary_table_name))
        self.cur.execute(
            'CREATE INDEX province_municipality_idx ON {} '
            '(cpro, cmun);'
                .format(self.temporary_table_name))
