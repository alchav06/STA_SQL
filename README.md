Urban Drainage Data SQL Scripts
This repository contains SQL scripts used for processing, inserting, and structuring Urban Drainage Systems (UDS) data. These queries facilitate data harmonization and integration with SensorThings API (STA) and other geospatial standards.

1. Data Insertion Queries
The SQL scripts in the Insert_data.sql file handle bulk data uploads and structured insertion of sensor data into a PostgreSQL/PostGIS database. These queries ensure:

Proper data formatting and referential integrity
Handling of sensor observations, including timestamps, locations, and observed properties
Integration with OGC standards such as Observations & Measurements (O&M)
Example:
INSERT INTO PUBLIC."OBSERVATIONS" (
"PHENOMENON_TIME_START",
"PHENOMENON_TIME_END",
"RESULT_TIME",
"RESULT_NUMBER",
"FEATURE_ID",
"DATASTREAM_ID",
"ID"
)
SELECT 
	OV.TIMESTAMP::TIMESTAMPTZ AS PHENOMENON_TIME_START,
	OV.TIMESTAMP::TIMESTAMPTZ AS PHENOMENON_TIME_END,
	OV.TIMESTAMP::TIMESTAMPTZ AS RESULT_TIME,
	OV.VALUE_NUMERIC AS RESULT_NUMBER,
	OV.SITE_ID AS FEATURE_ID,
	DENSE_RANK() OVER (
    ORDER BY OV.SITE_ID, OV.SOURCE_ID, OV.VARIABLE_ID
	) AS DATASTREAM_ID,
    OV.SIGNAL_ID
FROM DATAPOOL_REMOTE.OBSERVATION_VIEW OV

For full details, see Insert_data.sql.

2. Materialized Views for Optimized Queries
The SQL scripts in Materialized_views.sql create precomputed views to optimize data access and analysis. Materialized views allow for:

Faster queries on large sensor datasets
Aggregated time-series data for easier visualization
Pre-filtered datasets for integration with Helgoland and QGIS
Example:
CREATE MATERIALIZED VIEW datapool_clone.procedure_view
TABLESPACE pg_default
AS SELECT s.source_id::bigint AS source_id,
    (('http://www.example.org/'::text || s.name))::character varying(255) AS identifier,
    (('http://www.sta.org/'::text || s.name))::character varying(255) AS sta_identifier,
    s.name::character varying(255) AS name,
    s.description,
    jsonb_build_object('sensorType', st.name, 'sensorDescription', st.description, 'manufacturer', st.manufacturer) AS properties
   FROM datapool_clone.source s
     LEFT JOIN datapool_clone.source_type st ON s.source_type_id = st.source_type_id
WITH DATA;
