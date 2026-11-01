-- ============================================================================
-- Skrypt 02: Definicja widoku analitycznego dbo.vw_job_snapshots
-- Projekt: Polish Job Market Intelligence
-- Silnik: Synapse Serverless SQL (built-in pool)
-- Status: LEGACY GOLD V1. Skrypt nie jest używany przez aktualny model Power BI.
-- Źródło: Azure Data Lake Gen2 - warstwa Gold v1 (Parquet)
-- Reguła biznesowa Gold: Pierwszy skuteczny snapshot dla danego dnia
-- Uwaga operacyjna: plik repozytoryjny; zmiana nie modyfikuje środowiska Azure,
-- dopóki skrypt nie zostanie wykonany ręcznie w Synapse Serverless SQL.
-- ============================================================================

USE job_market;
GO

CREATE OR ALTER VIEW dbo.vw_job_snapshots
AS
SELECT
    CAST(r.filepath(1) AS DATE) AS snapshot_date,
    r.*
FROM OPENROWSET(
    BULK 'https://stjobmarketdev01.dfs.core.windows.net/gold/jobs/fact_job_snapshots/snapshot_date=*/*.parquet',
    FORMAT = 'PARQUET'
) AS [r];
GO
