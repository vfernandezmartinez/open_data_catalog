import os
import subprocess
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from datasource_common.log import log
from datasource_common.dataset_importer import DatasetImporter
from datasource_common.downloads import download_file


ZIPPED_SHAPEFILES_URL = (
    'http://centrodedescargas.cnig.es/CentroDescargas/descargaDir?'
    'secDescDirLA=114023&pagActual=1&numTotReg=5&codSerieSel=CAANE'
)
MUNICIPALITIES_PATTERN = '_muni_'
TABLE_NAME = 'municipalities_spain'
YEAR_PATTERN = '20110101'


class MunicipalityGeometryProvider:
    def __enter__(self):
        self._tmpdir = TemporaryDirectory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmpdir.cleanup()

    def get_dataset(self):
        zip_filepath = self.download_zip()
        shapefiles = self.extract_shapefiles(zip_filepath)
        merged_shapefile = self.merge_shapefiles(shapefiles)
        return {'shapefile': merged_shapefile}

    def download_zip(self):
        log.info('Downloading zipped shapefiles')
        zip_filepath = os.path.join(self._tmpdir.name, 'shapefiles.zip')
        download_file(ZIPPED_SHAPEFILES_URL, zip_filepath)
        return zip_filepath

    def extract_shapefiles(self, zip_filepath):
        log.info('Extracting zip with shapefiles')
        with ZipFile(zip_filepath, 'r') as zip:
            shapefiles = []
            for filename in zip.namelist():
                if MUNICIPALITIES_PATTERN in filename and YEAR_PATTERN in filename:
                    zip.extract(filename, path=self._tmpdir.name)
                    if filename.endswith('.shp'):
                        shapefiles.append(os.path.join(self._tmpdir.name, filename))
        return shapefiles

    def merge_shapefiles(self, shapefiles):
        log.info('Merging shapefiles')
        merged_shapefile = os.path.join(self._tmpdir.name, f'{TABLE_NAME}.shp')
        for shapefile in shapefiles:
            subprocess.run([
                'ogr2ogr',
                '-f', 'ESRI Shapefile',
                '-append',
                '-update',
                merged_shapefile,
                shapefile,
            ])

        return merged_shapefile


class MunicipalityGeometryImporter(DatasetImporter):
    dataset_provider_class = MunicipalityGeometryProvider

    def setup(self):
        self.cur.execute('CREATE EXTENSION IF NOT EXISTS postgis;')
        self.cur.execute('CREATE EXTENSION IF NOT EXISTS postgis_topology;')
        self.connection.commit()

    def populate_table(self):
        log.info('Importing shapefile')
        subprocess.run([
            'ogr2ogr',
            '-overwrite',
            '-f', 'PostgreSQL',
            f'PG:{self.connection_string}',
            '-nlt', 'MultiPolygon',
            self.dataset['shapefile'],
        ])
