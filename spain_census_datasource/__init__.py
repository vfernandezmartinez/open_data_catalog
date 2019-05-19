from .census import CensusImporter
from .municipalities import MunicipalityGeometryImporter


IMPORTER_CLASSES = (
    CensusImporter,
    MunicipalityGeometryImporter,
)


def import_datasets(connection_string, conn):
    for importer_class in IMPORTER_CLASSES:
        importer = importer_class(connection_string, conn)
        importer.run()
