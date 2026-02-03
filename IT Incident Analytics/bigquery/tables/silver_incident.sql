-- Silver Layer: Cleaned / deduplicated data
CREATE OR REPLACE TABLE incident_project.silver_incident AS
SELECT
    DISTINCT *
FROM
    incident_project.bronze_incident
-- Fill nulls for important columns
WHERE priority IS NOT NULL OR category IS NOT NULL;
