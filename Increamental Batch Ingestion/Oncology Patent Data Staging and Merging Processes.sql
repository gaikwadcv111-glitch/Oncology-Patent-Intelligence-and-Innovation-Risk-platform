-- Databricks notebook source
COPY INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_patent_abstracts
FROM '/Volumes/oncology_patent_intelligence/oncology_patent_intelligence_raw/oncology_patent_data/patent_abstracts/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'false','header'='true')
COPY_OPTIONS ('mergeSchema' = 'true');


-- COMMAND ----------

MERGE INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_patent_abstracts_raw_dates AS target
USING oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_patent_abstracts AS source
ON target.patent_id = source.patent_id
WHEN MATCHED THEN
  UPDATE SET
    target.title = source.title,
    target.abstract = source.abstract,
    target.inventors = source.inventors,
    target.jurisdiction = source.jurisdiction,
    target.assignee = source.assignee,
    target.filing_date = source.filing_date,
    target.publication_date = source.publication_date
WHEN NOT MATCHED THEN
  INSERT (patent_id, title, abstract, inventors, jurisdiction, assignee, filing_date, publication_date)
  VALUES (source.patent_id, source.title, source.abstract, source.inventors, source.jurisdiction, source.assignee, source.filing_date, source.publication_date);


-- COMMAND ----------

COPY INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_patent_legal_risk_scores_dates
FROM '/Volumes/oncology_patent_intelligence/oncology_patent_intelligence_raw/oncology_patent_data/legal_risk_scores/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

-- COMMAND ----------

MERGE INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_patent_legal_risk_scores_dates AS target
USING oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_patent_legal_risk_scores_dates AS source
ON target.risk_id = source.risk_id
WHEN MATCHED THEN
  UPDATE SET
    target.patent_id = source.patent_id,
    target.risk_score = source.risk_score,
    target.infringement_probability = source.infringement_probability,
    target.analysis_date = source.analysis_date,
    target.status = source.status
WHEN NOT MATCHED THEN
  INSERT (risk_id, patent_id, risk_score, infringement_probability, analysis_date, status)
  VALUES (source.risk_id, source.patent_id, source.risk_score, source.infringement_probability, source.analysis_date, source.status);

-- COMMAND ----------

COPY INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_global_citation_network
FROM '/Volumes/oncology_patent_intelligence/oncology_patent_intelligence_raw/oncology_patent_data/citation_network/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'false','header'='true');

-- COMMAND ----------

MERGE INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_global_citation_network AS target
USING oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_global_citation_network AS source
ON target.citation_id = source.citation_id
WHEN MATCHED THEN
  UPDATE SET
    target.backward_citations = source.backward_citations,
    target.country = source.country,
    target.forward_citations = source.forward_citations,
    target.patent_id = source.patent_id,
    target.priority_country = source.priority_country,
    target.priority_date = source.priority_date
WHEN NOT MATCHED THEN
  INSERT (backward_citations, citation_id, country, forward_citations, patent_id, priority_country, priority_date)
  VALUES (source.backward_citations, source.citation_id, source.country, source.forward_citations, source.patent_id, source.priority_country, source.priority_date);

-- COMMAND ----------

COPY INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_patent_ownership_and_grant_timeline
FROM '/Volumes/oncology_patent_intelligence/oncology_patent_intelligence_raw/oncology_patent_data/ownership_timeline/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

-- COMMAND ----------

MERGE INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_patent_ownership_and_grant_timeline AS target
USING oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_patent_ownership_and_grant_timeline AS source
ON target.ownership_id = source.ownership_id
WHEN MATCHED THEN
  UPDATE SET
    target.patent_id = source.patent_id,
    target.assignee_name = source.assignee_name,
    target.grant_date = source.grant_date,
    target.ownership_start_date = source.ownership_start_date,
    target.ownership_end_date = source.ownership_end_date
WHEN NOT MATCHED THEN
  INSERT (ownership_id, patent_id, assignee_name, grant_date, ownership_start_date, ownership_end_date)
  VALUES (source.ownership_id, source.patent_id, source.assignee_name, source.grant_date, source.ownership_start_date, source.ownership_end_date);


-- COMMAND ----------

COPY INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_oncology_drug_trial_fda_status
FROM '/Volumes/oncology_patent_intelligence/oncology_patent_intelligence_raw/oncology_patent_data/drug_trials_fda/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('multiLine' = 'false','header'='true');

-- COMMAND ----------

MERGE INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_oncology_drug_trial_fda_status AS target
USING oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_oncology_drug_trial_fda_status AS source
ON target.patent_id = source.patent_id AND target.drug_id = source.drug_id
WHEN MATCHED THEN
  UPDATE SET
    target.drug_name = source.drug_name,
    target.cancer_type = source.cancer_type,
    target.trial_phase = source.trial_phase,
    target.filing_date = source.filing_date,
    target.fda_status = source.fda_status
WHEN NOT MATCHED THEN
  INSERT (patent_id, drug_id, drug_name, cancer_type, trial_phase, filing_date, fda_status)
  VALUES (source.patent_id, source.drug_id, source.drug_name, source.cancer_type, source.trial_phase, source.filing_date, source.fda_status);

-- COMMAND ----------

COPY INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_biomarker_tech_domains
FROM '/Volumes/oncology_patent_intelligence/oncology_patent_intelligence_raw/oncology_patent_data/biomarker_tech_domains/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('multiLine' = 'false','header'='true');

-- COMMAND ----------

MERGE INTO oncology_patent_intelligence.oncology_patent_intelligence_raw.bronze_biomarker_tech_domains AS target
USING oncology_patent_intelligence.oncology_patent_intelligence_raw.staging_biomarker_tech_domains AS source
ON target.patent_id = source.patent_id AND target.biomarker_id = source.biomarker_id
WHEN MATCHED THEN
  UPDATE SET
    target.target_gene = source.target_gene,
    target.tech_domain = source.tech_domain,
    target.cancer_type = source.cancer_type,
    target.discovery_year = source.discovery_year,
    target.confidence_score = source.confidence_score,
    target.novelty_score = source.novelty_score
WHEN NOT MATCHED THEN
  INSERT (patent_id, biomarker_id, target_gene, tech_domain, cancer_type, discovery_year, confidence_score, novelty_score)
  VALUES (source.patent_id, source.biomarker_id, source.target_gene, source.tech_domain, source.cancer_type, source.discovery_year, source.confidence_score, source.novelty_score);