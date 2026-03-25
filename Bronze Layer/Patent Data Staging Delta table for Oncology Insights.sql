-- Databricks notebook source
CREATE TABLE oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_patent_abstracts_raw_dates (
    patent_id STRING,
    title STRING,
    abstract STRING,
    inventors STRING,
    jurisdiction STRING,
    assignee STRING,
    filing_date String,
    publication_date STRING
)
USING delta;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_patent_ownership_and_grant_timeline (
  ownership_id STRING,
  patent_id STRING,
  assignee_name STRING,
  grant_date STRING,
  ownership_start_date STRING,
  ownership_end_date STRING
) USING DELTA;

-- COMMAND ----------

CREATE TABLE oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_patent_legal_risk_scores_dates (
    risk_id STRING,
    patent_id STRING,
    risk_score STRING,
    infringement_probability STRING,
    analysis_date STRING,
    status STRING
)
USING delta;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_oncology_drug_trial_fda_status (
  patent_id STRING,
  drug_id STRING,
  drug_name STRING,
  cancer_type STRING,
  trial_phase STRING,
  filing_date STRING,
  fda_status STRING
) USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_global_citation_network (
  backward_citations LONG,
  citation_id STRING,
  country STRING,
  forward_citations LONG,
  patent_id STRING,
  priority_country STRING,
  priority_date STRING
) USING DELTA;

-- COMMAND ----------

CREATE TABLE if not exists oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_biomarker_tech_domains
(
  patent_id STRING,
  biomarker_id STRING,
  target_gene STRING,
  tech_domain STRING,
  cancer_type STRING,
  discovery_year STRING,
  confidence_score STRING,
  novelty_score STRING
)
USING delta;