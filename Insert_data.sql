-- SENSORS
INSERT INTO "SENSORS"  ("ID", "NAME", "DESCRIPTION", "PROPERTIES")
SELECT 
    s.source_id, 
    s.name, 
    s.description, 
    jsonb_build_object(
        'sensorType', st.name, 
        'sensorDescription', st.description, 
        'manufacturer', st.manufacturer
    ) AS properties
FROM 
    datapool_remote.source s
LEFT JOIN 
    datapool_remote.source_type st 
    ON s.source_type_id = st.source_type_id;

-- OBS_PROPERTIES
INSERT INTO "OBS_PROPERTIES"  ("ID", "NAME", "DESCRIPTION", "PROPERTIES")
SELECT 
    v.variable_id, 
    v.name, 
    v.description, 
    jsonb_build_object(
        'unit', v.unit
    ) AS properties
FROM 
    datapool_remote.variable v

-- THINGS
INSERT INTO "THINGS"  ("ID", "NAME", "DESCRIPTION")
SELECT 
    v.feature_id, 
    v.name, 
    v.description
FROM 
    datapool_remote.feature v

-- LOCATIONS
INSERT INTO "LOCATIONS"  ("ID", "NAME", "DESCRIPTION","PROPERTIES", "LOCATION", "GEOM")
SELECT 
    v.site_id, 
    v.name, 
    v.description,
    jsonb_build_object(
        'street', v.street,
        'postcode', REGEXP_REPLACE(v.postcode, '^''|''$', '', 'g'),  -- Removes surrounding quotes
        'city', v.city,
        'country', v.country
    ) AS properties,
    ST_AsGeoJSON(v.geom)::jsonb AS location,
    v.geom
FROM
    datapool_remote.feature_view v

-- THINGS_LOCATIONS
INSERT INTO public."THINGS_LOCATIONS" ("THING_ID", "LOCATION_ID")
SELECT t."ID", l."ID" 
FROM public."THINGS" t
JOIN public."LOCATIONS" l
ON t."NAME" = l."NAME";

-- DATASTREAMS
INSERT INTO public."DATASTREAMS"  (
    "NAME", 
    "DESCRIPTION", 
    "OBSERVATION_TYPE", 
    "SENSOR_ID", 
    "OBS_PROPERTY_ID", 
    "THING_ID", 
    "UNIT_NAME", 
    "UNIT_SYMBOL", 
    "UNIT_DEFINITION", 
    "ID"
    )
SELECT
	s.name || ' - ' || so.name || ' - ' || v.name AS NAME,
	s.description || ' - ' || so.description || ' - ' || v.description AS DESCRIPTION,
	'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement' AS OBSERVATION_TYPE,
    so.source_id AS sensor_id, 
    v.variable_id AS observed_property_id,  
    s.site_id AS thing_id, 
    v.name as UNIT_NAME,
    v.unit as UNIT_SYMBOL,
    op."DEFINITION"  AS UNIT_DEFINITION,  -- Added from OBS_PROPERTIES
    DENSE_RANK() OVER (
    ORDER BY s.site_id, so.source_id, v.variable_id
	) AS datastream_id
FROM datapool_remote.signal sig
JOIN datapool_remote.site s ON sig.site_id = s.site_id
JOIN datapool_remote.source so ON sig.source_id = so.source_id
JOIN datapool_remote.variable v ON sig.variable_id = v.variable_id
JOIN public."OBS_PROPERTIES" op ON op."ID" = v.variable_id  -- Join OBS_PROPERTIES
GROUP BY s.site_id, so.source_id, v.variable_id, s.name, so.name, v.name, s.description, so.description, v.description, v.unit, v.name, op."DEFINITION";

-- FEATURES
INSERT INTO PUBLIC."FEATURES" (
"NAME",
"DESCRIPTION",
"ENCODING_TYPE",
"FEATURE",
"GEOM",
"ID"
)
SELECT
F.NAME,
F.DESCRIPTION,
'APPLICATION/VND.GEO+JSON' AS ENCODINGTYPE,
JSONB_BUILD_OBJECT(
    'TYPE', 'POINT',
    'COORDINATES', ARRAY[ST_X(F.GEOM), ST_Y(F.GEOM)]
) AS FEATURE,
F.GEOM,
F.FEATURE_ID
FROM
DATAPOOL_REMOTE.FEATURE F

-- OBSERVATIONS
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