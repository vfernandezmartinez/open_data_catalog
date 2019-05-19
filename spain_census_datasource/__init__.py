from .census import CensusImporter
from .municipalities import MunicipalityGeometryImporter
from .provinces import ProvincesImporter


IMPORTER_CLASSES = (
    CensusImporter,
    MunicipalityGeometryImporter,
    ProvincesImporter,
)


def import_datasets(connection_string, conn):
    for importer_class in IMPORTER_CLASSES:
        importer = importer_class(connection_string, conn)
        importer.run()
