import os
import subprocess
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from datasource_common.log import log
from datasource_common.dataset_import_task import DatasetImportTask
from datasource_common.downloads import download_file


ZIPPED_SHAPEFILES_URL = (
    'http://centrodedescargas.cnig.es/CentroDescargas/descargaDir?'
    'secDescDirLA=114023&pagActual=1&numTotReg=5&codSerieSel=CAANE'
)
MUNICIPALITIES_PATTERN = '_muni_'
YEAR_PATTERN = '20110101'


class MunicipalityGeometryDownloader:
    def __enter__(self):
        self._tmpdir = TemporaryDirectory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmpdir.cleanup()

    def download(self):
        zip_filepath = os.path.join(self._tmpdir.name, 'shapefiles.zip')
        log.info('Downloading zipped shapefiles')
        download_file(ZIPPED_SHAPEFILES_URL, zip_filepath)

        log.info('Extracting zip with shapefiles')
        with ZipFile(zip_filepath, 'r') as zip:
            shapefiles = []
            for filename in zip.namelist():
                if MUNICIPALITIES_PATTERN in filename and YEAR_PATTERN in filename:
                    zip.extract(filename, path=self._tmpdir.name)
                    if filename.endswith('.shp'):
                        shapefiles.append(os.path.join(self._tmpdir.name, filename))

        return {
            'shapefiles': shapefiles
        }


class MunicipalityGeometryImporter(DatasetImportTask):
    TABLE_NAME = 'municipalities_spain'
    DOWNLOADER_CLASS = MunicipalityGeometryDownloader

    def create_temporary_table(self):
        self.cur.execute('CREATE EXTENSION IF NOT EXISTS postgis;')
        self.cur.execute('CREATE EXTENSION IF NOT EXISTS postgis_topology;')

    def populate_table(self):
        self.connection.commit()

        append = False
        for shapefile in self.downloads['shapefiles']:
            log.info('Importing shapefile: %s', os.path.basename(shapefile))

            args = ['ogr2ogr']
            if append:
                args.append('-append')
            args.extend([
                '-f', 'PostgreSQL',
                f'PG:{self.connection_string}',
                '-nln', self.temporary_table_name,
                '-nlt', 'MultiPolygon',
                '-lco', 'PRECISION=no',
                shapefile
            ])
            # PG_USE_COPY is used by default
            subprocess.run(args)
            append = True
