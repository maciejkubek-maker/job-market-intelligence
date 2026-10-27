-- ============================================================================
-- Skrypt 02: Definicja widoku analitycznego dbo.vw_job_snapshots
-- Projekt: Polish Job Market Intelligence
-- Silnik: Synapse Serverless SQL (built-in pool)
-- Źródło: Azure Data Lake Gen2 - warstwa Gold (Parquet)
-- Reguła biznesowa Gold: Pierwszy skuteczny snapshot dla danego dnia
-- ============================================================================

USE job_market;
GO

CREATE OR ALTER VIEW dbo.vw_job_snapshots
AS
SELECT
    CAST(r.filepath(1) AS DATE) AS snapshot_date_partition,
    r.source,
    r.source_job_id,
    CAST(r.snapshot_date AS DATE) AS snapshot_date,
    r.run_id,
    r.title,
    r.company,
    r.location_raw,
    r.role_category,
    r.seniority,
    r.work_mode,
    r.contract_type,
    CAST(r.salary_from AS DECIMAL(12,2)) AS salary_from,
    CAST(r.salary_to AS DECIMAL(12,2)) AS salary_to,
    r.salary_currency,
    r.salary_period,
    CAST(CASE WHEN r.salary_from IS NOT NULL AND r.salary_to IS NOT NULL THEN 1 ELSE 0 END AS BIT) AS is_salary_given,
    r.skills,
    r.url,
    CAST(r.published_at AS DATETIME2) AS published_at,
    CAST(r.extracted_at AS DATETIME2) AS extracted_at
FROM OPENROWSET(
    BULK 'https://stjobmarketdev01.dfs.core.windows.net/gold/jobs/snapshot_date=*/*.parquet',
    FORMAT = 'PARQUET'
) AS [r];
GO
