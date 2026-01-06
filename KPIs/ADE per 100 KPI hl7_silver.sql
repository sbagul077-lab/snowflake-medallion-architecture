
/*==============================================================================
  SILVER LAYER (HL7 + CSV + CCDA) — robust, cross-DB, keyword-safe
  - Fixes invalid identifier errors by creating CSV wrapper via dynamic SQL
    depending on actual columns present in CSV.ALLERGIES.
==============================================================================*/

-- 0) Context
USE DATABASE HL7_FINAL_ASSIGNMENT;
CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;

-- 0a) Optional MPI mapping (fallback to generated IDs if empty)
CREATE TABLE IF NOT EXISTS MAP_PATIENT_ID (
  MPI_ID           STRING,
  HL7_PATIENT_ID   STRING,
  CSV_PATIENT_ID   STRING,
  CCDA_PATIENT_ID  STRING
);

/* ---------------------------------------------------------------------------
   A) Diagnose CSV columns (optional helper)
--------------------------------------------------------------------------- */
-- Run this to see what columns actually exist in your CSV.ALLERGIES
SELECT COLUMN_NAME
FROM CSV_FINAL_ASSIGNMENT.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'CSV' AND TABLE_NAME = 'ALLERGIES'
ORDER BY ORDINAL_POSITION;

/* ---------------------------------------------------------------------------
   B) Create a robust CSV wrapper view via dynamic SQL
      - Quotes reserved keywords
      - Uses NULL when a column is missing
--------------------------------------------------------------------------- */
BEGIN
  LET has_patient  BOOLEAN := EXISTS(SELECT 1 FROM CSV_FINAL_ASSIGNMENT.INFORMATION_SCHEMA.COLUMNS
                                     WHERE TABLE_SCHEMA='CSV' AND TABLE_NAME='ALLERGIES' AND COLUMN_NAME='PATIENT');
  LET has_start    BOOLEAN := EXISTS(SELECT 1 FROM CSV_FINAL_ASSIGNMENT.INFORMATION_SCHEMA.COLUMNS
                                     WHERE TABLE_SCHEMA='CSV' AND TABLE_NAME='ALLERGIES' AND COLUMN_NAME='START');
  LET has_code     BOOLEAN := EXISTS(SELECT 1 FROM CSV_FINAL_ASSIGNMENT.INFORMATION_SCHEMA.COLUMNS
                                     WHERE TABLE_SCHEMA='CSV' AND TABLE_NAME='ALLERGIES' AND COLUMN_NAME='CODE');
  LET has_desc     BOOLEAN := EXISTS(SELECT 1 FROM CSV_FINAL_ASSIGNMENT.INFORMATION_SCHEMA.COLUMNS
                                     WHERE TABLE_SCHEMA='CSV' AND TABLE_NAME='ALLERGIES' AND COLUMN_NAME='DESCRIPTION');
  LET has_type     BOOLEAN := EXISTS(SELECT 1 FROM CSV_FINAL_ASSIGNMENT.INFORMATION_SCHEMA.COLUMNS
                                     WHERE TABLE_SCHEMA='CSV' AND TABLE_NAME='ALLERGIES' AND COLUMN_NAME='TYPE');
  LET has_category BOOLEAN := EXISTS(SELECT 1 FROM CSV_FINAL_ASSIGNMENT.INFORMATION_SCHEMA.COLUMNS
                                     WHERE TABLE_SCHEMA='CSV' AND TABLE_NAME='ALLERGIES' AND COLUMN_NAME='CATEGORY');
  LET has_severity BOOLEAN := EXISTS(SELECT 1 FROM CSV_FINAL_ASSIGNMENT.INFORMATION_SCHEMA.COLUMNS
                                     WHERE TABLE_SCHEMA='CSV' AND TABLE_NAME='ALLERGIES' AND COLUMN_NAME='SEVERITY');
  LET has_system   BOOLEAN := EXISTS(SELECT 1 FROM CSV_FINAL_ASSIGNMENT.INFORMATION_SCHEMA.COLUMNS
                                     WHERE TABLE_SCHEMA='CSV' AND TABLE_NAME='ALLERGIES' AND COLUMN_NAME='SYSTEM');

  LET sql STRING :=
    'CREATE OR REPLACE VIEW CSV_FINAL_ASSIGNMENT.CSV.V_ALLERGIES_SAFE AS ' ||
    'SELECT ' ||
      CASE WHEN has_patient  THEN 'a."PATIENT"     AS PATIENT'      ELSE 'CAST(NULL AS VARCHAR) AS PATIENT'      END || ', ' ||
      CASE WHEN has_start    THEN 'a."START"       AS START_RAW'     ELSE 'CAST(NULL AS VARCHAR) AS START_RAW'    END || ', ' ||
      CASE WHEN has_code     THEN 'a."CODE"        AS CODE'          ELSE 'CAST(NULL AS VARCHAR) AS CODE'         END || ', ' ||
      CASE WHEN has_desc     THEN 'a."DESCRIPTION" AS DESCRIPTION'   ELSE 'CAST(NULL AS VARCHAR) AS DESCRIPTION'  END || ', ' ||
      CASE WHEN has_type     THEN 'a."TYPE"        AS TYPE'          ELSE 'CAST(NULL AS VARCHAR) AS TYPE'         END || ', ' ||
      CASE WHEN has_category THEN 'a."CATEGORY"    AS CATEGORY'      ELSE 'CAST(NULL AS VARCHAR) AS CATEGORY'     END || ', ' ||
      CASE WHEN has_severity THEN 'a."SEVERITY"    AS SEVERITY'      ELSE 'CAST(NULL AS VARCHAR) AS SEVERITY'     END || ', ' ||
      CASE WHEN has_system   THEN 'TRY_CAST(a."SYSTEM" AS VARCHAR) AS SYSTEM' ELSE 'CAST(NULL AS VARCHAR) AS SYSTEM' END ||
    ' FROM CSV_FINAL_ASSIGNMENT.CSV.ALLERGIES a';

  EXECUTE IMMEDIATE :sql;
END;

/* ---------------------------------------------------------------------------
   C) HL7 canonical view (reads your Bronze AL1)
--------------------------------------------------------------------------- */
CREATE OR REPLACE VIEW V_ALLERGY_EVENT_HL7_CANON AS
SELECT
  COALESCE(mpi.MPI_ID,
           CASE WHEN s.PATIENT_ID IS NOT NULL THEN CONCAT('MPI-HL7-', s.PATIENT_ID)
                ELSE 'MPI-UNKNOWN' END)                                   AS MPI_ID,
  s.PATIENT_ID                                                           AS PATIENT_SOURCE_ID,

  /* Robust timestamp cascade: NTZ → cast TZ → parse RAW → DATE */
  COALESCE(
    s.ID_DT_NTZ,
    CAST(s.ID_DT_TZ AS TIMESTAMP_NTZ),
    TRY_TO_TIMESTAMP_NTZ(REGEXP_REPLACE(s.ID_DT_RAW, '[^0-9+-]', ''), 'YYYYMMDDHH24MISS+TZHTZM'),
    TO_TIMESTAMP_NTZ(s.ID_DT_DATE)
  )                                                                      AS EVENT_DT,

  NULLIF(TRIM(s.ALLERGEN_CODE), '')                                      AS CODE,
  COALESCE(NULLIF(TRIM(s.ALLERGEN_TEXT), ''), NULLIF(TRIM(s.ALLERGEN_CODE), '')) AS DISPLAY_RAW,
  LOWER(NULLIF(TRIM(s.ALLERGY_TYPE_DESC), ''))                           AS CATEGORY_RAW,
  NULLIF(TRIM(s.SEVERITY_DESC), '')                                      AS SEVERITY_RAW,
  COALESCE(NULLIF(TRIM(s.REACTION_FIRST), ''), NULLIF(TRIM(s.REACTION_RAW), '')) AS REACTION_TEXT,
  'HL7'                                                                  AS SOURCE_SYSTEM
FROM HL7_FINAL_ASSIGNMENT.HL7.HL7_BRONZE_AL1 s
LEFT JOIN SILVER.MAP_PATIENT_ID mpi
  ON mpi.HL7_PATIENT_ID = TO_VARCHAR(s.PATIENT_ID);

/* ---------------------------------------------------------------------------
   D) CSV canonical view (uses wrapper with safe aliases; no parse errors)
--------------------------------------------------------------------------- */
CREATE OR REPLACE VIEW V_ALLERGY_EVENT_CSV_CANON AS
SELECT
  COALESCE(mpi.MPI_ID, CONCAT('MPI-CSV-', s.PATIENT))                    AS MPI_ID,
  s.PATIENT                                                              AS PATIENT_SOURCE_ID,

  COALESCE(
    TRY_TO_TIMESTAMP_NTZ(s.START_RAW),
    TRY_TO_TIMESTAMP_NTZ(REGEXP_REPLACE(s.START_RAW, '[^0-9]', ''), 'YYYYMMDDHH24MISS'),
    TO_TIMESTAMP_NTZ(TRY_TO_DATE(s.START_RAW))
  )                                                                      AS EVENT_DT,

  NULLIF(TRIM(TO_VARCHAR(s.CODE)), '')                                   AS CODE,
  NULLIF(TRIM(s.DESCRIPTION), '')                                        AS DISPLAY_RAW,
  /* Prefer TYPE; fallback to CATEGORY when TYPE blank */
  LOWER(COALESCE(NULLIF(TRIM(s.TYPE), ''), NULLIF(TRIM(s.CATEGORY), ''))) AS CATEGORY_RAW,
  NULLIF(TRIM(s.SEVERITY), '')                                           AS SEVERITY_RAW,
  CAST(NULL AS VARCHAR)                                                  AS REACTION_TEXT,
  'CSV'                                                                  AS SOURCE_SYSTEM
FROM CSV_FINAL_ASSIGNMENT.CSV.V_ALLERGIES_SAFE s
LEFT JOIN SILVER.MAP_PATIENT_ID mpi
  ON mpi.CSV_PATIENT_ID = TO_VARCHAR(s.PATIENT);

/* ---------------------------------------------------------------------------
   E) CCDA canonical view
--------------------------------------------------------------------------- */
CREATE OR REPLACE VIEW V_ALLERGY_EVENT_CCDA_CANON AS
SELECT
  COALESCE(mpi.MPI_ID, COALESCE(NULLIF(TRIM(c.PATIENT_ID), ''), 'MPI-CCDA-UNKNOWN')) AS MPI_ID,
  c.DOCUMENT_ID                                                          AS PATIENT_SOURCE_ID,

  TRY_TO_TIMESTAMP_NTZ(REGEXP_REPLACE(c.START_RAW, '[^0-9]', ''), 'YYYYMMDDHH24MISS') AS EVENT_DT,
  TO_VARCHAR(c.SUBSTANCE_CODE)                                           AS CODE,
  NULLIF(TRIM(c.SUBSTANCE_DESC), '')                                     AS STANDARD_DISPLAY,
  COALESCE(NULLIF(TRIM(c.SUBSTANCE_CODE_SYSTEM), ''), 'SUBSTANCE')       AS DISPLAY_GROUP,
  'UNKNOWN'                                                              AS CATEGORY,
  COALESCE(NULLIF(UPPER(TRIM(c.SEVERITY)), ''), 'UNKNOWN')               AS SEVERITY,
  NULLIF(TRIM(c.REACTION_DESC), '')                                      AS REACTION_TEXT,
  'CCDA'                                                                 AS SOURCE_SYSTEM
FROM CCDA_FINAL_ASSIGNMENT.CCDA.CCDA_ALLERGIES c
LEFT JOIN SILVER.MAP_PATIENT_ID mpi
  ON mpi.CCDA_PATIENT_ID = TO_VARCHAR(c.PATIENT_ID);

/* ---------------------------------------------------------------------------
   F) Normalized union (HL7 + CSV) — cleaning, grouping, category/severity
--------------------------------------------------------------------------- */
CREATE OR REPLACE VIEW V_ALLERGY_UNION_NORMALIZED AS
SELECT
  u.MPI_ID,
  u.PATIENT_SOURCE_ID,
  u.EVENT_DT,
  u.CODE,
  u.SOURCE_SYSTEM,

  REGEXP_REPLACE(LOWER(TRIM(u.DISPLAY_RAW)),
                 '\\s*\\((substance|organism|finding)\\)\\s*', '') AS DISPLAY_NORM,

  CASE
    WHEN REGEXP_LIKE(u.DISPLAY_RAW, '^Der\\s?[fp]\\s?\\d', 'i') THEN 'house dust mite'
    WHEN REGEXP_LIKE(u.DISPLAY_RAW, '^Ara\\s?h\\s?\\d',   'i') THEN 'peanut'
    WHEN REGEXP_LIKE(u.DISPLAY_RAW, '^Can\\s?f\\s?\\d',  'i') THEN 'animal dander'
    WHEN REGEXP_LIKE(u.DISPLAY_RAW, '^Fel\\s?d\\s?\\d',  'i') THEN 'animal dander'
    WHEN REGEXP_LIKE(u.DISPLAY_RAW, '^Jug\\s?r\\s?\\d',  'i') THEN 'tree nut'
    WHEN REGEXP_LIKE(u.DISPLAY_RAW, '^Che\\s?a\\s?\\d',  'i') THEN 'tree nut'
    WHEN REGEXP_LIKE(u.DISPLAY_RAW, '^Pen\\s?n\\s?\\d',  'i') THEN 'mold'
    ELSE REGEXP_REPLACE(LOWER(TRIM(u.DISPLAY_RAW)),
                        '\\s*\\((substance|organism|finding)\\)\\s*', '')
  END AS DISPLAY_GROUP,

  CASE
    WHEN u.CATEGORY_RAW ILIKE '%medic%' THEN 'medication'
    WHEN u.CATEGORY_RAW ILIKE '%drug%'  THEN 'medication'
    WHEN u.CATEGORY_RAW ILIKE '%food%'  THEN 'food'
    WHEN u.CATEGORY_RAW ILIKE '%env%'   THEN 'environment'
    ELSE COALESCE(u.CATEGORY_RAW, 'unknown')
  END AS CATEGORY_NORM,

  CASE
    WHEN u.SEVERITY_RAW ILIKE 'sev%'  THEN 'SEVERE'
    WHEN u.SEVERITY_RAW ILIKE 'mod%'  THEN 'MODERATE'
    WHEN u.SEVERITY_RAW ILIKE 'mild%' THEN 'MILD'
    ELSE 'UNKNOWN'
  END AS SEVERITY_NORM,

  u.REACTION_TEXT
FROM (
  SELECT * FROM V_ALLERGY_EVENT_HL7_CANON
  UNION ALL
  SELECT * FROM V_ALLERGY_EVENT_CSV_CANON
) u;

/* ---------------------------------------------------------------------------
   G) Materialize HL7 + CSV normalized table
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER_ALLERGY_HL7_CSV
CLUSTER BY (MPI_ID, EVENT_DT) AS
SELECT
  MPI_ID,
  PATIENT_SOURCE_ID,
  EVENT_DT,
  CODE,
  DISPLAY_NORM       AS STANDARD_DISPLAY,
  DISPLAY_GROUP      AS DISPLAY_GROUP,
  CATEGORY_NORM      AS CATEGORY,
  SEVERITY_NORM      AS SEVERITY,
  REACTION_TEXT,
  SOURCE_SYSTEM
FROM V_ALLERGY_UNION_NORMALIZED;

/* ---------------------------------------------------------------------------
   H) Final union view (HL7 + CSV + CCDA)
--------------------------------------------------------------------------- */
CREATE OR REPLACE VIEW V_ALLERGY_EVENT_UNION_BOTH AS
SELECT
  MPI_ID,
  PATIENT_SOURCE_ID,
  EVENT_DT,
  CODE,
  STANDARD_DISPLAY,
  DISPLAY_GROUP,
  CATEGORY,
  SEVERITY,
  REACTION_TEXT,
  SOURCE_SYSTEM
FROM SILVER_ALLERGY_HL7_CSV
UNION ALL
SELECT
  MPI_ID,
  PATIENT_SOURCE_ID,
  EVENT_DT,
  CODE,
  STANDARD_DISPLAY,
  DISPLAY_GROUP,
  CATEGORY,
  SEVERITY,
  REACTION_TEXT,
  SOURCE_SYSTEM
FROM V_ALLERGY_EVENT_CCDA_CANON;

/* ---------------------------------------------------------------------------
   I) Final consolidated SILVER table (optional de-dup)
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER_ALLERGY
CLUSTER BY (MPI_ID, EVENT_DT) AS
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY MPI_ID, CODE, EVENT_DT, SOURCE_SYSTEM
           ORDER BY SOURCE_SYSTEM
         ) AS rn
  FROM V_ALLERGY_EVENT_UNION_BOTH
)
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   J) Validations
--------------------------------------------------------------------------- */
-- Nulls by source
SELECT SOURCE_SYSTEM, COUNT(*) AS null_event_dt
FROM SILVER_ALLERGY
WHERE EVENT_DT IS NULL
GROUP BY SOURCE_SYSTEM
ORDER BY SOURCE_SYSTEM;

-- Spot-check HL7 rows
SELECT MPI_ID, PATIENT_SOURCE_ID, EVENT_DT, STANDARD_DISPLAY, DISPLAY_GROUP, CATEGORY, SEVERITY, SOURCE_SYSTEM
FROM SILVER_ALLERGY
WHERE SOURCE_SYSTEM = 'HL7'
ORDER BY EVENT_DT DESC
LIMIT 20;

-- ADE (ADR proxy)
SELECT *
FROM SILVER_ALLERGY
WHERE EVENT_DT IS NOT NULL
  AND (
    UPPER(CATEGORY) IN ('DRUG ALLERGY','MEDICATION','MISCELLANEOUS CONTRAINDICATION')
    OR REGEXP_LIKE(UPPER(STANDARD_DISPLAY),
        '(AMOXICILLIN|PENICILLIN|IBUPROFEN|ASPIRIN|METFORMIN|LISINOPRIL|ATORVASTATIN)')
  );
select *
from silver_allergy;