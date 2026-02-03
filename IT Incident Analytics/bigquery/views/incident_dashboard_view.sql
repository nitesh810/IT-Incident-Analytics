-- View for reporting
CREATE OR REPLACE VIEW incident_project.views.incident_dashboard_view AS
SELECT
    category,
    incident_count
FROM
    incident_project.gold_incident_kpi
ORDER BY
    incident_count DESC;
