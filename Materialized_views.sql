-- dataset_view
CREATE MATERIALIZED VIEW datapool_clone.dataset_view
TABLESPACE pg_default
AS SELECT o.dataset_id,
    o.source_id::bigint AS procedure_id,
    o.variable_id::bigint AS phenomenon_id,
    p.id AS offering_id,
    '1'::bigint AS category_id,
    o.site_id::bigint AS feature_id
   FROM datapool_clone.observation_view o
     LEFT JOIN datapool_clone.offering_view p ON p.old_phenomenon_id = o.variable_id AND p.source_table = 'phenomenon'::text
  GROUP BY o.dataset_id, o.source_id, o.variable_id, p.id, o.site_id
  ORDER BY o.dataset_id
WITH DATA;

-- feature_view
CREATE MATERIALIZED VIEW datapool_clone.feature_view
TABLESPACE pg_default
AS SELECT sfv.site_id,
    (('http://www.example.org/'::text || s.name))::character varying(255) AS identifier,
    (('http://www.sta.org/'::text || s.name))::character varying(255) AS sta_identifier,
    s.name::character varying(255) AS name,
    s.description,
    max(
        CASE
            WHEN sfv.site_field_id = 1 THEN sfv.value
            ELSE NULL::text
        END) AS street,
    max(
        CASE
            WHEN sfv.site_field_id = 2 THEN sfv.value
            ELSE NULL::text
        END) AS postcode,
    max(
        CASE
            WHEN sfv.site_field_id = 3 THEN sfv.value
            ELSE NULL::text
        END) AS city,
    max(
        CASE
            WHEN sfv.site_field_id = 4 THEN sfv.value
            ELSE NULL::text
        END) AS country,
    max(
        CASE
            WHEN sfv.site_field_id = 5 THEN TRIM(BOTH FROM sfv.value)::double precision
            ELSE NULL::double precision
        END) AS coord_x,
    max(
        CASE
            WHEN sfv.site_field_id = 6 THEN TRIM(BOTH FROM sfv.value)::double precision
            ELSE NULL::double precision
        END) AS coord_y,
    st_setsrid(st_makepoint(max(
        CASE
            WHEN sfv.site_field_id = 5 THEN TRIM(BOTH FROM sfv.value)::double precision
            ELSE NULL::double precision
        END), max(
        CASE
            WHEN sfv.site_field_id = 6 THEN TRIM(BOTH FROM sfv.value)::double precision
            ELSE NULL::double precision
        END)), 4326) AS geom
   FROM datapool_clone.site_field_value sfv
     JOIN datapool_clone.site s ON sfv.site_id = s.site_id
  GROUP BY sfv.site_id, s.name, s.description
WITH DATA;

-- observation_view
CREATE MATERIALIZED VIEW datapool_clone.observation_view
TABLESPACE pg_default
AS WITH datasetgrouping AS (
         SELECT signal.variable_id,
            signal.source_id,
            signal.site_id,
            row_number() OVER (ORDER BY signal.variable_id, signal.source_id, signal.site_id) AS dataset_id
           FROM datapool_clone.signal
          GROUP BY signal.variable_id, signal.source_id, signal.site_id
        )
 SELECT s.signal_id,
    (s."timestamp" AT TIME ZONE 'CET'::text) AS "timestamp",
    'quantity'::character varying(255) AS value_type,
    s.value::numeric(20,10) AS value_numeric,
    s.variable_id,
    s.source_id,
    s.site_id,
    d.dataset_id,
    st_transform(st_setsrid(st_makepoint(s.coord_x::double precision, s.coord_y::double precision), 2056), 4326) AS geom_wgs84
   FROM datapool_clone.signal s
     JOIN datasetgrouping d ON s.variable_id = d.variable_id AND s.source_id = d.source_id AND s.site_id = d.site_id
  ORDER BY d.dataset_id
WITH DATA;

-- offering_view
CREATE MATERIALIZED VIEW datapool_clone.offering_view
TABLESPACE pg_default
AS WITH orderedunion AS (
         SELECT 'http://www.example.org/'::text || phenomenon_view.name::text AS identifier,
            phenomenon_view.name,
            phenomenon_view.description,
            'phenomenon'::text AS source_table,
            phenomenon_view.variable_id AS old_phenomenon_id
           FROM datapool_clone.phenomenon_view
        )
 SELECT row_number() OVER (ORDER BY source_table, name) AS id,
    identifier,
    name,
    description,
    source_table,
    old_phenomenon_id
   FROM orderedunion
WITH DATA;

-- phenomenon_view
CREATE MATERIALIZED VIEW datapool_clone.phenomenon_view
TABLESPACE pg_default
AS SELECT variable_id::bigint AS variable_id,
    (('http://www.example.org/'::text || name))::character varying(255) AS identifier,
    (('http://www.sta.org/'::text || name))::character varying(255) AS sta_identifier,
    name::character varying(255) AS name,
    description
   FROM datapool_clone.variable
WITH DATA;

-- platform_view
CREATE MATERIALIZED VIEW datapool_clone.platform_view
TABLESPACE pg_default
AS SELECT source_type_id::bigint AS source_type_id,
    (('http://www.example.org/'::text || name))::character varying(255) AS identifier,
    (('http://www.sta.org/'::text || name))::character varying(255) AS sta_identifier,
    name::character varying(255) AS name,
    description
   FROM datapool_clone.source_type
WITH DATA;

-- procedure_view
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

-- unit_view
CREATE MATERIALIZED VIEW datapool_clone.unit_view
TABLESPACE pg_default
AS SELECT variable_id::bigint AS variable_id,
    unit::character varying(255) AS unit
   FROM datapool_clone.variable
WITH DATA;