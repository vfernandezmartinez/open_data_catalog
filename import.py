import psycopg2

import settings
import spain_census_datasource


connection_string = ' '.join([
    f'{key}={value}'
    for key, value in settings.POSTGRESQL.items()
    if value
])

with psycopg2.connect(connection_string) as conn:
    spain_census_datasource.import_datasets(connection_string, conn)
