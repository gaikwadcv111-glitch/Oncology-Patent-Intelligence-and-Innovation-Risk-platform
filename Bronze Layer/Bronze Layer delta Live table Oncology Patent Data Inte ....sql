-- Databricks notebook source
CREATE  or refresh LIVE TABLE bronze_patent_abstracts_raw_dates
AS
SELECT
  patent_id,
  title,
  abstract,
  inventors,
  jurisdiction,
  assignee,
  filing_date,
  publication_date
FROM oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_patent_abstracts_raw_dates;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_patent_ownership_and_grant_timeline
AS
SELECT
  ownership_id,
  patent_id,
  assignee_name,
  grant_date,
  ownership_start_date,
  ownership_end_date
FROM oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_patent_ownership_and_grant_timeline;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_patent_legal_risk_scores_dates
AS
SELECT
  risk_id,
  patent_id,
  risk_score,
  infringement_probability,
  analysis_date,
  status
FROM oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_patent_legal_risk_scores_dates;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_oncology_drug_trial_fda_status
AS
SELECT
  patent_id,
  drug_id,
  drug_name,
  cancer_type,
  trial_phase,
  filing_date,
  fda_status
FROM oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_oncology_drug_trial_fda_status;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_global_citation_network
AS
SELECT
  backward_citations,
  citation_id,
  country,
  forward_citations,
  patent_id,
  priority_country,
  priority_date
FROM oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_global_citation_network;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_biomarker_tech_domains
AS
SELECT
  patent_id,
  biomarker_id,
  target_gene,
  tech_domain,
  cancer_type,
  discovery_year,
  confidence_score,
  novelty_score
FROM oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_biomarker_tech_domains;