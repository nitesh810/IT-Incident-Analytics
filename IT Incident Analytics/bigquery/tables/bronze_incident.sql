-- Bronze Layer: Raw ingestion table (load CSV data)
CREATE OR REPLACE TABLE incident_project.bronze_incident AS
SELECT
    *
FROM
    `incident_project.raw_incident_csv`;
