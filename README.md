# Setup

 * Create a virtualenv and activate it:
```
    python3 -m venv venv
    source venv/bin/activate
```

 * Install dependencies:
```
    pip install -r requirements.txt
```
 * Edit settings.py and adjust PostgreSQL database connection settings.

Note: This package uses psycopg2 for connecting to PostgreSQL databases. In case the installation of psycopg2 fails, you might need to install postgresql10-dev or postgresql-devel package (depending on your distribution).


# Execute

    python3 import.py

Any tables and required PostgreSQL extensions are automatically created.


# Queries

  * Percentage of people with university degrees (third-level studies) in a location, given by a point (latitude and longitude).

    For this example, the point used was latitude 39.63N and longitude 0.59W, which corresponds to Llíria (Valencia). In the query, the point appears as _POINT(-0.59 39.63)_

```
WITH municipality AS (
  SELECT substr(id_ine, 1, 2) AS cpro,
         substr(id_ine, 3, 3) AS cmun
  FROM municipalities_spain
  WHERE ST_Contains(wkb_geometry, ST_GeometryFromText('POINT(-0.59 39.63)', 4258))
    AND fecha_alta <= '2011-01-01'::date
    AND (fecha_baja IS NULL OR fecha_baja > '2011-01-01'::date)
  LIMIT 1
)
SELECT (100.0 * SUM(t12_5)) / SUM(t1_1) AS percentage_population_with_university_studies
FROM census_spain
WHERE cpro=ANY( (SELECT ARRAY(SELECT cpro FROM municipality LIMIT 1))::varchar[] )
  AND cmun=ANY( (SELECT ARRAY(SELECT cmun FROM municipality LIMIT 1))::varchar[] )
  AND t12_5 IS NOT NULL;
```

  * Listing of all the measures/indicators available in our database, with human-readable names
```
SELECT table_name,
       column_name as indicator_code,
       col_description((table_schema||'.'||table_name)::regclass::oid, ordinal_position) as indicator_label
FROM information_schema.columns
WHERE col_description((table_schema||'.'||table_name)::regclass::oid, ordinal_position) IS NOT NULL
ORDER BY table_name, ordinal_position;
```

  * List provinces with their population.

```
SELECT province_population.cpro, province, population
FROM provinces_spain
  JOIN (
    SELECT cpro, SUM(t1_1) AS population
    FROM census_spain
    GROUP BY cpro
  ) AS province_population
  ON provinces_spain.cpro=province_population.cpro
ORDER BY province;
```



# Remarks

## Table import optimizations

### Dropping tables to avoid dead rows

Any data might be queried anytime, even while an import is in progress. Queries run during the import should still be able to see the old data. Once the import is complete, the new data should be visible to new queries immediately, avoiding any service disruptions or data inconsistencies. In order to achieve this, there are several approaches that could be taken.

A first attempt might be to start a transaction, in which the table is first truncated and new rows are then inserted. However, this is not optimal because rows would not really be deleted from the table. Due to the MVCC model, while the import is in progress and the transaction hasn't been commited yet, PostgreSQL needs to be able to show these rows to other transactions. For that reason, PostgreSQL marks these rows as deleted for the import transaction but they are not really deleted until a VACUUM is executed (either manually or via autovacuum). As a result, space is wasted, since the table size on disk is bigger, which also has an impact on performance. Besides, depending on how often a VACUUM is performed and how often the import is run, a lot of disk might be used and the disk might even get full.

The most efficient approach for importing a whole large table is usually to drop the table and create a new one. This results in no dead rows and queries can be executed faster. PostgreSQL still needs to keep the old rows while the import transaction is in progress, but as soon as it's commited, it gets rid of the table completely so it can delete all the previous data immediately.

The downside of this approach is that in order to be able to drop the table, any foreign keys from other tables that point to this table need to be cascaded as well. Hence, if we want to use foreign keys, they would need to be recreated and there could potentially be integrity issues (broken foreign keys). We don't need foreign keys for this challenge, so this downside doesn't impact the current solution.

### Avoiding unnecessary index updates

The next thing to consider are indexes. Given our use case, where data is imported unfrequently and it is queried frequently, indexes can certainly help improve query performance.

When a table has indexes, PostgreSQL needs to update them whenever a new row is inserted. This means if we first create indexes and then insert new rows, PostgreSQL has to do a lot of extra work to keep the indexes up to date. This work is a waste of time. It is more efficient to insert all rows first and then create the index only once the table has been populated. This way, PostgreSQL only needs to generate the index once.

### Using COPY to increase import performance

INSERT is the statement defined by the SQL standard to add new rows to a table. However, PostgreSQL also supports COPY, which is not part of the standard. COPY is faster because it allows PostgreSQL to receive and insert a lot of rows at once. Besides, COPY also supports importing from CSV directly. Given that we receive the data in a CSV format, we can directly feed the CSV to PostgreSQL, making the insertion very fast.


## Concurrent imports

For simplicity, the code does not actively prevent two imports from being executed at the same time in parallel (e.g. by running the script from two terminal windows). It is assumed that whatever job scheduler is used will prevent the same import from running in parallel.

If needed, preventing concurrent imports could be made for example by using PostgreSQL advisory locks. When the import script starts, it would try to acquire an advisory lock using pg_try_advisory_lock(). If the lock is acquired successfully, the import could proceed. If the lock failed, it means the lock is currently held by another running import process. Hence, the code could just fail with an error message. The lock held by the running import proccess would automatically be released by PostgreSQL once its connection is closed.

## Importing shapefiles

Municipality geometry data is provided in two separate shapefiles: one for the Iberian peninsula and another one for the Canary Islands. In order to be able to run queries to find the municipality that contains a point, we need a way to merge these two.

It is possible to import these 2 shapefiles separately and create a PostgreSQL view. However, this is less efficient than having a single table.

The approach used here was to merge both shapefiles in advance using ogr2ogr. The resulting shapefile is then imported, thus resulting in a single table in the database.

## Query optimizations

### Percentage of people with university degrees

Municipalities that are completely enclosed within others are still detected correctly with a single match only. For example, Domeño (39.66N 0.67W) which is completely surrounded by Llíria.

Queries for points in the Canary Islands also work, since both shapefiles are merged in the same table. For example, _POINT(-13.502 29.232)_ corresponds to the small town of Caleta de Sebo. Queries correctly detect that it belongs to the municipality of Teguise.

Some rows in the census data contain NULL values for column t12_5. This also happens in other columns except t1_1. Given that data is unavailable for those districts/sections, such rows cannot be used for computing the percentage, as the results would be otherwise incorrect. This is the reason why the query includes `AND 12_5 IS NOT NULL`.

Initially, I wrote this query:
```
WITH municipality AS (
  SELECT substr(id_ine, 1, 2) AS cpro,
         substr(id_ine, 3, 3) AS cmun
  FROM municipalities_spain
  WHERE ST_Contains(wkb_geometry, ST_GeometryFromText('POINT(-0.55 39.65)', 4258))
    AND fecha_alta <= '2011-01-01'::date
    AND (fecha_baja IS NULL OR fecha_baja > '2011-01-01'::date)
  LIMIT 1
)
SELECT (100.0 * SUM(t12_5)) / SUM(t1_1) AS percentage_population_with_university_studies
FROM census_spain
WHERE cpro=(SELECT cpro FROM municipality)
  AND cmun=(SELECT cmun FROM municipality)
  AND t12_5 IS NOT NULL;
```

However, it had bad performance, as the query planner was using a sequential scan on census_spain even though this was not really necessary:
```
                                                                                      QUERY PLAN                                                                                      
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=5210.27..5210.29 rows=1 width=32) (actual time=103.938..103.938 rows=1 loops=1)
   CTE municipality
     ->  Limit  (cost=0.15..12.88 rows=1 width=64) (actual time=0.300..0.302 rows=1 loops=1)
           ->  Index Scan using municipalities_spain_wkb_geometry_geom_idx on municipalities_spain  (cost=0.15..38.34 rows=3 width=64) (actual time=0.298..0.299 rows=1 loops=1)
                 Index Cond: (wkb_geometry ~ '0101000020A21000009A9999999999E1BF3333333333D34340'::geometry)
                 Filter: ((fecha_alta <= '2011-01-01'::date) AND ((fecha_baja IS NULL) OR (fecha_baja > '2011-01-01'::date)) AND _st_contains(wkb_geometry, '0101000020A21000009A9999999999E1BF3333333333D34340'::geometry))
                 Rows Removed by Filter: 2
   InitPlan 2 (returns $1)
     ->  CTE Scan on municipality  (cost=0.00..0.02 rows=1 width=32) (actual time=0.306..0.308 rows=1 loops=1)
   InitPlan 3 (returns $2)
     ->  CTE Scan on municipality municipality_1  (cost=0.00..0.02 rows=1 width=32) (actual time=0.004..0.005 rows=1 loops=1)
   ->  Seq Scan on census_spain  (cost=0.00..5197.34 rows=1 width=16) (actual time=70.204..103.904 rows=15 loops=1)
         Filter: ((t12_5 IS NOT NULL) AND ((cpro)::text = $1) AND ((cmun)::text = $2))
         Rows Removed by Filter: 35902
 Planning time: 0.911 ms
 Execution time: 104.198 ms
(16 rows)
```

Notice the 35902 rows being discarded by the filter.

I found this weird because there is an index on census_spain that PostgreSQL could use. With very small tables, PostgreSQL usually ignores indexes because it's just faster to fetch all rows and store them in RAM. This table is not so small, though.

After some research and some trial/error cycles, I found the query planner would make use of the index if I use ANY():
```
WHERE cpro=ANY( (SELECT ARRAY(SELECT cpro FROM municipality LIMIT 1))::varchar[] )
  AND cmun=ANY( (SELECT ARRAY(SELECT cmun FROM municipality LIMIT 1))::varchar[] )
```

This is less readable but leads to a better query plan:
```
                                                                                     QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=792.65..792.66 rows=1 width=32) (actual time=0.184..0.184 rows=1 loops=1)
   CTE municipality
     ->  Limit  (cost=0.15..12.88 rows=1 width=64) (actual time=0.116..0.116 rows=1 loops=1)
           ->  Index Scan using municipalities_spain_wkb_geometry_geom_idx on municipalities_spain  (cost=0.15..38.34 rows=3 width=64) (actual time=0.116..0.116 rows=1 loops=1)
                 Index Cond: (wkb_geometry ~ '0101000020A2100000E17A14AE47E1E2BF713D0AD7A3D04340'::geometry)
                 Filter: ((fecha_alta <= '2011-01-01'::date) AND ((fecha_baja IS NULL) OR (fecha_baja > '2011-01-01'::date)) AND _st_contains(wkb_geometry, '0101000020A2100000E17A14AE47E1E2BF713D0AD7A3D04340'::geometry))
   InitPlan 3 (returns $2)
     ->  Result  (cost=0.02..0.03 rows=1 width=32) (actual time=0.124..0.124 rows=1 loops=1)
           InitPlan 2 (returns $1)
             ->  Limit  (cost=0.00..0.02 rows=1 width=32) (actual time=0.119..0.120 rows=1 loops=1)
                   ->  CTE Scan on municipality  (cost=0.00..0.02 rows=1 width=32) (actual time=0.118..0.119 rows=1 loops=1)
   InitPlan 5 (returns $4)
     ->  Result  (cost=0.02..0.03 rows=1 width=32) (actual time=0.003..0.003 rows=1 loops=1)
           InitPlan 4 (returns $3)
             ->  Limit  (cost=0.00..0.02 rows=1 width=32) (actual time=0.001..0.001 rows=1 loops=1)
                   ->  CTE Scan on municipality municipality_1  (cost=0.00..0.02 rows=1 width=32) (actual time=0.001..0.001 rows=1 loops=1)
   ->  Bitmap Heap Scan on census_spain  (cost=298.03..779.02 rows=136 width=16) (actual time=0.156..0.171 rows=15 loops=1)
         Recheck Cond: ((cpro = ANY ((($2)::character varying[])::bpchar[])) AND (cmun = ANY ((($4)::character varying[])::bpchar[])))
         Filter: (t12_5 IS NOT NULL)
         Heap Blocks: exact=2
         ->  Bitmap Index Scan on province_municipality_idx  (cost=0.00..298.00 rows=139 width=0) (actual time=0.151..0.151 rows=15 loops=1)
               Index Cond: ((cpro = ANY ((($2)::character varying[])::bpchar[])) AND (cmun = ANY ((($4)::character varying[])::bpchar[])))
 Planning time: 0.679 ms
 Execution time: 0.306 ms
(25 rows)
```

Now PostgreSQL is using the index, doing a bitmap index scan. This means PostgreSQL uses the index to decide which pages it needs to fetch from disk. Therefore, it doesn't need to fetch the whole table, only the pages corresponding to the rows that match the filter. You can also see the much smaller execution time.

### List of provinces with their population

A simple approach could be:
```
SELECT provinces_spain.cpro, province, SUM(t1_1) AS population
FROM provinces_spain
  JOIN census_spain
  ON provinces_spain.cpro=census_spain.cpro
GROUP BY provinces_spain.cpro
ORDER BY province;
```
This does a JOIN on census_spain and provinces_spain tables. However, this means PostgreSQL would first join both tables and then compute the aggregation.

It is possible to slightly reduce the amount of work PostgreSQL needs to do by letting it compute the aggregation on census_spain first, then join the aggregated data with provinces_spain. PostgreSQL will still need to scan the whole census_spain table, which is what takes most of the time. This means the execution time cannot be much lower with the second approach. However, in my tests the first approach usually takes 70-110ms, whereas the second approach takes 40-70ms so there is some obvious benefit on computing the aggregation first. For that reason, the proposed query uses the second approach.

# Error handling

For simplicity, most of the time, errors are not handled. For example, if a file download fails, the application will abort with an uncatched exception. The backtrace will be printed by python. However, everything is cleaned up. Temporary directories are deleted, temporary tables are dropped, transactions are rolled back, etc.