-- Gold Layer: Analytics-ready KPI table
CREATE OR REPLACE TABLE incident_project.gold_incident_kpi AS
SELECT
    category,
    COUNT(*) AS incident_count,
    'category_count' AS kpi_type
FROM
    incident_project.silver_incident
GROUP BY
    category;
