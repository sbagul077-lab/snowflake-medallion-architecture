
/*==============================================================================
  GOLD LAYER — STATIC (NO DYNAMIC TABLES)
  Input: HL7_FINAL_ASSIGNMENT.SILVER.SILVER_ALLERGY
  Output: HL7_FINAL_ASSIGNMENT.GOLD.*
==============================================================================*/

USE DATABASE HL7_FINAL_ASSIGNMENT;
CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

/* =============================================================================
   1) GOLD_ADE_EVENTS  (Canonical ADE dataset, deduped per MPI_ID × Day × Agent)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_ADE_EVENTS AS
WITH src AS (
    SELECT
        sa.MPI_ID,
        sa.EVENT_DT,
        sa.CODE,
        sa.STANDARD_DISPLAY,
        sa.DISPLAY_GROUP,
        sa.CATEGORY,
        sa.SEVERITY,
        sa.SOURCE_SYSTEM,

        /* Severity bucket */
        CASE UPPER(COALESCE(sa.SEVERITY, 'UNKNOWN'))
            WHEN 'SEVERE'   THEN 'SEVERE'
            WHEN 'MODERATE' THEN 'MODERATE'
            WHEN 'MILD'     THEN 'MILD'
            ELSE 'UNKNOWN'
        END AS SEVERITY_BKT,

        /* Agent normalization */
        REGEXP_REPLACE(
            UPPER(COALESCE(sa.STANDARD_DISPLAY, sa.DISPLAY_GROUP, sa.CODE, 'UNKNOWN')),
            '\\s*\\((SUBSTANCE|ORGANISM|FINDING)\\)\\s*',''
        ) AS AGENT_NORM,

        /* Drug-event flag */
        CASE
            WHEN UPPER(sa.CATEGORY) IN ('DRUG ALLERGY','MEDICATION','MISCELLANEOUS CONTRAINDICATION') THEN 1
            WHEN REGEXP_LIKE(UPPER(COALESCE(sa.STANDARD_DISPLAY,'')),
                '(AMOXICILLIN|PENICILLIN|IBUPROFEN|ASPIRIN|METFORMIN|LISINOPRIL|ATORVASTATIN|WARFARIN|CODEINE)')
                 THEN 1
            ELSE 0
        END AS IS_DRUG_EVENT
    FROM HL7_FINAL_ASSIGNMENT.SILVER.SILVER_ALLERGY sa
    WHERE sa.EVENT_DT IS NOT NULL
),
dedup AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY MPI_ID, CAST(EVENT_DT AS DATE), AGENT_NORM
            ORDER BY EVENT_DT DESC
        ) AS rn
    FROM src
)
SELECT
    MPI_ID,
    EVENT_DT,
    CODE,
    STANDARD_DISPLAY,
    DISPLAY_GROUP,
    CATEGORY,
    SEVERITY_BKT,
    AGENT_NORM,
    IS_DRUG_EVENT,
    SOURCE_SYSTEM
FROM dedup
WHERE rn = 1;

-- OPTIONAL CHECK
SELECT COUNT(*) AS gold_ade_events_count FROM GOLD_ADE_EVENTS;


/* =============================================================================
   2) GOLD_ADE_EVENTS_DRUG  (Drug-only ADE events)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_ADE_EVENTS_DRUG AS
SELECT *
FROM GOLD_ADE_EVENTS
WHERE IS_DRUG_EVENT = 1;


/* =============================================================================
   3) GOLD_METRIC_ADE_MONTHLY  (Monthly aggregates)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_METRIC_ADE_MONTHLY AS
WITH events_m AS (
    SELECT
        DATE_TRUNC('MONTH', EVENT_DT)::DATE AS PERIOD_MONTH,
        AGENT_NORM,
        MPI_ID,
        IS_DRUG_EVENT
    FROM GOLD_ADE_EVENTS
),
event_counts AS (
    SELECT
        PERIOD_MONTH,
        COUNT(DISTINCT CASE WHEN IS_DRUG_EVENT = 1 THEN AGENT_NORM END) AS ADE_EVENTS_DRUG,
        COUNT(DISTINCT AGENT_NORM) AS ADE_EVENTS_ALL
    FROM events_m
    GROUP BY PERIOD_MONTH
),
member_counts AS (
    SELECT
        DATE_TRUNC('MONTH', EVENT_DT)::DATE AS PERIOD_MONTH,
        COUNT(DISTINCT MPI_ID) AS MEMBERS_OBS
    FROM HL7_FINAL_ASSIGNMENT.SILVER.SILVER_ALLERGY
    WHERE EVENT_DT IS NOT NULL
    GROUP BY PERIOD_MONTH
)
SELECT
    e.PERIOD_MONTH,
    e.ADE_EVENTS_DRUG,
    e.ADE_EVENTS_ALL,
    m.MEMBERS_OBS
FROM event_counts e
JOIN member_counts m
  ON m.PERIOD_MONTH = e.PERIOD_MONTH;


/* =============================================================================
   4) GOLD_ADE_RATE_MONTHLY  (Rates per 100 members)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_ADE_RATE_MONTHLY AS
SELECT
    PERIOD_MONTH,
    ADE_EVENTS_DRUG,
    ADE_EVENTS_ALL,
    MEMBERS_OBS,
    ROUND((ADE_EVENTS_DRUG / NULLIF(MEMBERS_OBS,0)) * 100, 2) AS ADE_PER_100_DRUG,
    ROUND((ADE_EVENTS_ALL  / NULLIF(MEMBERS_OBS,0)) * 100, 2) AS ADE_PER_100_ALL
FROM GOLD_METRIC_ADE_MONTHLY
ORDER BY PERIOD_MONTH;


/* =============================================================================
   5) GOLD_PATIENT_ADE_LONGITUDINAL (Patient-level ADE history)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_PATIENT_ADE_LONGITUDINAL AS
WITH base AS (
    SELECT
        MPI_ID,
        EVENT_DT::DATE AS EVENT_DATE,
        SEVERITY_BKT,
        AGENT_NORM,
        IS_DRUG_EVENT
    FROM GOLD_ADE_EVENTS
),
agg AS (
    SELECT
        MPI_ID,
        MIN(EVENT_DATE) AS FIRST_ADE_DATE,
        MAX(EVENT_DATE) AS LAST_ADE_DATE,
        COUNT(*) AS TOTAL_ADE_EVENTS,
        COUNT_IF(SEVERITY_BKT='SEVERE') AS TOTAL_SEVERE_ADE,
        COUNT_IF(SEVERITY_BKT='MODERATE') AS TOTAL_MODERATE_ADE,
        COUNT_IF(SEVERITY_BKT='MILD') AS TOTAL_MILD_ADE,
        COUNT(DISTINCT AGENT_NORM) AS DISTINCT_AGENTS
    FROM base
    WHERE IS_DRUG_EVENT = 1
    GROUP BY MPI_ID
)
SELECT
    *,
    DATEDIFF('DAY', LAST_ADE_DATE, CURRENT_DATE) AS DAYS_SINCE_LAST_ADE
FROM agg;


/* =============================================================================
   6) GOLD_PATIENT_MONTH_ADE  (Patient × Month ADE heatmap)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_PATIENT_MONTH_ADE AS
SELECT
    MPI_ID,
    DATE_TRUNC('MONTH', EVENT_DT)::DATE AS PERIOD_MONTH,
    COUNT_IF(IS_DRUG_EVENT=1) AS ADE_EVENTS_DRUG,
    COUNT(*) AS ADE_EVENTS_ALL,
    COUNT_IF(SEVERITY_BKT='SEVERE') AS SEVERE_EVENTS,
    COUNT_IF(SEVERITY_BKT='MODERATE') AS MODERATE_EVENTS,
    COUNT_IF(SEVERITY_BKT='MILD') AS MILD_EVENTS
FROM GOLD_ADE_EVENTS
GROUP BY MPI_ID, PERIOD_MONTH;


/* =============================================================================
   7) GOLD_TOP_AGENTS_GLOBAL (Top 100 ADE-causing agents)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_TOP_AGENTS_GLOBAL AS
SELECT
    AGENT_NORM AS DRUG_NAME,
    COUNT(*) AS NUMBER_OF_EVENTS
FROM GOLD_ADE_EVENTS_DRUG
GROUP BY AGENT_NORM
ORDER BY NUMBER_OF_EVENTS DESC
LIMIT 100;


/* =============================================================================
   8) GOLD_PATIENT_TOP_AGENTS (Top 5 agents per patient)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_PATIENT_TOP_AGENTS AS
WITH c AS (
    SELECT
        MPI_ID,
        AGENT_NORM,
        COUNT(*) AS EVENT_COUNT,
        ROW_NUMBER() OVER (PARTITION BY MPI_ID ORDER BY COUNT(*) DESC) AS RN
    FROM GOLD_ADE_EVENTS_DRUG
    GROUP BY MPI_ID, AGENT_NORM
)
SELECT *
FROM c
WHERE RN <= 5;


/* =============================================================================
   9) GOLD_SOURCE_MIX_MONTHLY  (Source system split)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_SOURCE_MIX_MONTHLY AS
SELECT
    DATE_TRUNC('MONTH', EVENT_DT)::DATE AS PERIOD_MONTH,
    SOURCE_SYSTEM,
    COUNT(*) AS EVENTS
FROM GOLD_ADE_EVENTS
GROUP BY PERIOD_MONTH, SOURCE_SYSTEM
ORDER BY PERIOD_MONTH DESC;


/* =============================================================================
   10) GOLD_SEVERITY_MIX_MONTHLY (Severity split)
============================================================================= */
CREATE OR REPLACE TABLE GOLD_SEVERITY_MIX_MONTHLY AS
SELECT
    DATE_TRUNC('MONTH', EVENT_DT)::DATE AS PERIOD_MONTH,
    SEVERITY_BKT,
    COUNT(*) AS EVENTS
FROM GOLD_ADE_EVENTS
GROUP BY PERIOD_MONTH, SEVERITY_BKT
ORDER BY PERIOD_MONTH DESC;

-----------------------------------------Query------------------------------------------


/* ============================================================
   FINAL GOLD SUMMARY QUERY
   – Patient-level + Monthly rates + Top agents
   – Safe to run after GOLD tables are created
============================================================ */
SELECT
    p.MPI_ID,
    p.FIRST_ADE_DATE,
    p.LAST_ADE_DATE,
    p.TOTAL_ADE_EVENTS,
    p.TOTAL_SEVERE_ADE,
    p.TOTAL_MODERATE_ADE,
    p.TOTAL_MILD_ADE,
    p.DISTINCT_AGENTS,
    p.DAYS_SINCE_LAST_ADE,

    r.PERIOD_MONTH,
    r.ADE_EVENTS_DRUG,
    r.ADE_EVENTS_ALL,
    r.MEMBERS_OBS,
    r.ADE_PER_100_DRUG,
    r.ADE_PER_100_ALL,

    t.AGENT_NORM AS TOP_AGENT,
    t.EVENT_COUNT AS TOP_AGENT_EVENT_COUNT

FROM HL7_FINAL_ASSIGNMENT.GOLD.GOLD_PATIENT_ADE_LONGITUDINAL p
LEFT JOIN HL7_FINAL_ASSIGNMENT.GOLD.GOLD_ADE_RATE_MONTHLY r
    ON r.PERIOD_MONTH = DATE_TRUNC('MONTH', p.LAST_ADE_DATE)
LEFT JOIN HL7_FINAL_ASSIGNMENT.GOLD.GOLD_PATIENT_TOP_AGENTS t
    ON t.MPI_ID = p.MPI_ID

ORDER BY
    p.MPI_ID,
    r.PERIOD_MONTH DESC,
    t.EVENT_COUNT DESC;











SELECT * FROM HL7_FINAL_ASSIGNMENT.GOLD.GOLD_ADE_EVENTS;
SELECT * FROM HL7_FINAL_ASSIGNMENT.GOLD.GOLD_ADE_EVENTS_DRUG;
SELECT * FROM HL7_FINAL_ASSIGNMENT.GOLD.GOLD_TOP_AGENTS_GLOBAL;
SELECT * FROM HL7_FINAL_ASSIGNMENT.GOLD.GOLD_ADE_RATE_MONTHLY;
SELECT * FROM HL7_FINAL_ASSIGNMENT.GOLD.GOLD_PATIENT_ADE_LONGITUDINAL;
SELECT * FROM HL7_FINAL_ASSIGNMENT.GOLD.GOLD_PATIENT_MONTH_ADE;
SELECT * FROM HL7_FINAL_ASSIGNMENT.GOLD.GOLD_PATIENT_TOP_AGENTS;
