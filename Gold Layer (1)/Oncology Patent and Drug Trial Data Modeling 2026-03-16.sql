-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE dim_patent
AS
SELECT DISTINCT
    a.patent_id,
    a.title,
    a.abstract,
    a.jurisdiction,
    a.filing_date,
    a.publication_date
FROM LIVE.silver_patent_abstracts a;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_assignee
AS
SELECT DISTINCT
    patent_id,
    assignee AS assignee_name
FROM LIVE.silver_patent_abstracts;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_drug
AS
SELECT DISTINCT
    drug_id,
    drug_name,
    cancer_type
FROM LIVE.silver_oncology_drug_trial_fda_status;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_biomarker
AS
SELECT DISTINCT
    biomarker_id,
    target_gene,
    tech_domain,
    cancer_type,
    discovery_year
FROM LIVE.silver_biomarker_tech_domains;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_country
AS
SELECT DISTINCT
    country,
    priority_country
FROM LIVE.silver_global_citation_network;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_patent_innovation
AS
SELECT
    p.patent_id,
    a.assignee AS assignee_name,
    d.cancer_type,
    b.tech_domain,
    c.country,
    COUNT(DISTINCT g.citation_id) AS citation_count,
    AVG(b.novelty_score) AS avg_novelty_score
FROM LIVE.silver_patent_abstracts p
LEFT JOIN LIVE.silver_oncology_drug_trial_fda_status d
    ON p.patent_id = d.patent_id
LEFT JOIN LIVE.silver_biomarker_tech_domains b
    ON p.patent_id = b.patent_id
LEFT JOIN LIVE.silver_global_citation_network g
    ON p.patent_id = g.patent_id
LEFT JOIN LIVE.silver_patent_abstracts a
    ON p.patent_id = a.patent_id
LEFT JOIN LIVE.silver_global_citation_network c
    ON p.patent_id = c.patent_id
GROUP BY
    p.patent_id,
    a.assignee,
    d.cancer_type,
    b.tech_domain,
    c.country;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_patent_risk
AS
SELECT
    r.patent_id,
    a.assignee,
    d.cancer_type,
    r.risk_score,
    r.infringement_probability,
    r.analysis_date
FROM LIVE.silver_patent_legal_risk_scores_dates r
LEFT JOIN LIVE.silver_patent_abstracts a
    ON r.patent_id = a.patent_id
LEFT JOIN LIVE.silver_oncology_drug_trial_fda_status d
    ON r.patent_id = d.patent_id;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_drug_trials
AS
SELECT
    d.patent_id,
    d.drug_id,
    d.drug_name,
    d.cancer_type,
    d.trial_phase,
    d.fda_status,
    p.assignee
FROM LIVE.silver_oncology_drug_trial_fda_status d
LEFT JOIN LIVE.silver_patent_abstracts p
    ON d.patent_id = p.patent_id;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_patent_citations
AS
SELECT
    patent_id,
    country,
    priority_country,
    priority_date,
    forward_citations,
    backward_citations
FROM LIVE.silver_global_citation_network;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_patent_ai_features
AS
SELECT
    p.patent_id,
    p.assignee,
    p.jurisdiction,
    YEAR(p.filing_date) AS filing_year,

    d.cancer_type,
    d.trial_phase,

    b.tech_domain,
    b.target_gene,

    c.country,
    c.priority_country,

    COUNT(DISTINCT c.citation_id) AS citation_count,
    AVG(b.novelty_score) AS avg_novelty_score,
    AVG(b.confidence_score) AS avg_confidence_score,

    AVG(r.risk_score) AS avg_risk_score,
    AVG(r.infringement_probability) AS avg_infringement_probability,

    MAX(r.analysis_date) AS latest_risk_analysis

FROM LIVE.silver_patent_abstracts p

LEFT JOIN LIVE.silver_oncology_drug_trial_fda_status d
ON p.patent_id = d.patent_id

LEFT JOIN LIVE.silver_biomarker_tech_domains b
ON p.patent_id = b.patent_id

LEFT JOIN LIVE.silver_global_citation_network c
ON p.patent_id = c.patent_id

LEFT JOIN LIVE.silver_patent_legal_risk_scores_dates r
ON p.patent_id = r.patent_id

GROUP BY
    p.patent_id,
    p.assignee,
    p.jurisdiction,
    YEAR(p.filing_date),
    d.cancer_type,
    d.trial_phase,
    b.tech_domain,
    b.target_gene,
    c.country,
    c.priority_country;