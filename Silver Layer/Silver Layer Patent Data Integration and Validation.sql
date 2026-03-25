-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE silver_patent_abstracts
AS
SELECT
  patent_id,
  title,
  abstract,
  assignee,
  jurisdiction,
  SPLIT(inventors, '; ') AS inventors_array,
  CAST(filing_date AS DATE) AS filing_date,
  CAST(publication_date AS DATE) AS publication_date
FROM LIVE.bronze_patent_abstracts_raw_dates
WHERE abstract IS NOT NULL AND patent_id IS NOT NULL
  AND filing_date IS NOT NULL AND filing_date <> '' AND LOWER(filing_date) <> 'unknown'
  AND publication_date IS NOT NULL AND publication_date <> '' AND LOWER(publication_date) <> 'unknown';

-- COMMAND ----------

-- DBTITLE 1,Patent ownership timeline with date logic
CREATE OR REFRESH LIVE TABLE silver_patent_ownership_and_grant_timeline
AS
SELECT
  ownership_id,
  patent_id,
  assignee_name,
  CAST(grant_date AS DATE) AS grant_date,
  CASE
    WHEN ownership_start_date IS NULL AND ownership_end_date IS NOT NULL AND LOWER(ownership_end_date) <> 'unknown'
      THEN CAST(DATE_SUB(CAST(ownership_end_date AS DATE), 365 * 20) AS DATE)
    WHEN ownership_start_date IS NOT NULL AND LOWER(ownership_start_date) <> 'unknown'
      THEN CAST(ownership_start_date AS DATE)
    ELSE NULL
  END AS ownership_start_date,
  CASE
    WHEN ownership_end_date IS NULL AND ownership_start_date IS NOT NULL AND LOWER(ownership_start_date) <> 'unknown'
      THEN CAST(DATE_ADD(CAST(ownership_start_date AS DATE), 365 * 20) AS DATE)
    WHEN ownership_end_date IS NOT NULL AND LOWER(ownership_end_date) <> 'unknown'
      THEN CAST(ownership_end_date AS DATE)
    ELSE NULL
  END AS ownership_end_date
FROM LIVE.bronze_patent_ownership_and_grant_timeline
WHERE NOT (
  (ownership_start_date IS NULL OR LOWER(ownership_start_date) = 'unknown')
    AND (ownership_end_date IS NULL OR LOWER(ownership_end_date) = 'unknown')
)
AND grant_date IS NOT NULL AND LOWER(grant_date) <> 'unknown';

-- COMMAND ----------

-- DBTITLE 1,Cell 3
CREATE OR REFRESH LIVE TABLE silver_patent_legal_risk_scores_dates
AS
SELECT
  risk_id,
  patent_id,
  CAST(risk_score AS FLOAT) AS risk_score,
  CAST(infringement_probability AS FLOAT) AS infringement_probability,
  CAST(analysis_date AS DATE) AS analysis_date,
  status
FROM LIVE.bronze_patent_legal_risk_scores_dates
WHERE risk_id IS NOT NULL AND patent_id IS NOT NULL
  AND analysis_date IS NOT NULL AND analysis_date <> '' AND LOWER(analysis_date) <> 'unknown';

-- COMMAND ----------

-- DBTITLE 1,Cell 4
CREATE OR REFRESH LIVE TABLE silver_oncology_drug_trial_fda_status
AS
SELECT
  patent_id,
  drug_id,
  drug_name,
  cancer_type,
  trial_phase,
  CAST(filing_date AS DATE) AS filing_date,
  fda_status
FROM LIVE.bronze_oncology_drug_trial_fda_status
WHERE patent_id IS NOT NULL AND drug_id IS NOT NULL AND drug_name IS NOT NULL
  AND filing_date IS NOT NULL AND filing_date <> '' AND LOWER(filing_date) <> 'unknown'
  AND fda_status IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,Cell 5
CREATE OR REFRESH LIVE TABLE silver_global_citation_network
AS
SELECT
  backward_citations,
  citation_id,
  country,
  forward_citations,
  patent_id,
  COALESCE(priority_country, 'Unknown') AS priority_country,
  CAST(priority_date AS DATE) AS priority_date
FROM LIVE.bronze_global_citation_network
WHERE patent_id IS NOT NULL AND citation_id IS NOT NULL
  AND priority_country IS NOT NULL AND priority_country <> '' AND LOWER(priority_country) <> 'unknown'
  AND priority_date IS NOT NULL AND LOWER(priority_date) <> 'unknown';

-- COMMAND ----------

-- DBTITLE 1,Cell 6
CREATE OR REFRESH LIVE TABLE silver_biomarker_tech_domains
AS
SELECT
  patent_id,
  biomarker_id,
  target_gene,
  tech_domain,
  cancer_type,
  CASE
    WHEN discovery_year RLIKE '^[0-9]{4}$' THEN CAST(discovery_year AS INT)
    ELSE NULL
  END AS discovery_year,
  CAST(confidence_score AS FLOAT) AS confidence_score,
  CAST(novelty_score AS FLOAT) AS novelty_score
FROM LIVE.bronze_biomarker_tech_domains
WHERE patent_id IS NOT NULL AND biomarker_id IS NOT NULL AND target_gene IS NOT NULL;