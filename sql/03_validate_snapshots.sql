-- ============================================================================
-- Skrypt 03: Zapytania walidacyjne jakości i kompletności snapshotów
-- Projekt: Polish Job Market Intelligence
-- Silnik: Synapse Serverless SQL
-- Cel: Weryfikacja unikalności, rozkładu kategorii, braków danych oraz ciągłości.
-- ============================================================================

USE job_market;
GO

-- 1. Zestawienie liczby ofert per data snapshotu i unikalne wykonania (run_id)
SELECT
    snapshot_date,
    run_id,
    COUNT(*) AS total_offers,
    COUNT(DISTINCT source_job_id) AS unique_offers,
    COUNT(*) - COUNT(DISTINCT source_job_id) AS duplicate_count
FROM dbo.vw_job_snapshots
GROUP BY snapshot_date, run_id
ORDER BY snapshot_date DESC;

-- 2. Weryfikacja unikalności klucza naturalnego w obrębie dnia (powinno zwrócić 0 wierszy)
SELECT
    source,
    source_job_id,
    snapshot_date,
    COUNT(*) AS occurrence_count
FROM dbo.vw_job_snapshots
GROUP BY source, source_job_id, snapshot_date
HAVING COUNT(*) > 1;

-- 3. Rozkład kategorii ról według dni obserwacji
SELECT
    snapshot_date,
    role_category,
    COUNT(*) AS offers_count,
    SUM(CAST(is_salary_given AS INT)) AS offers_with_salary,
    ROUND(CAST(SUM(CAST(is_salary_given AS INT)) AS FLOAT) / COUNT(*) * 100, 2) AS pct_with_salary
FROM dbo.vw_job_snapshots
GROUP BY snapshot_date, role_category
ORDER BY snapshot_date DESC, offers_count DESC;

-- 4. Statystyki widełek wynagrodzeń w PLN według formy umowy i roli
SELECT
    role_category,
    contract_type,
    salary_period,
    COUNT(*) AS sample_size,
    ROUND(AVG(salary_from), 0) AS avg_salary_min,
    ROUND(AVG(salary_to), 0) AS avg_salary_max
FROM dbo.vw_job_snapshots
WHERE is_salary_given = 1
  AND salary_currency = 'PLN'
GROUP BY role_category, contract_type, salary_period
ORDER BY role_category, sample_size DESC;

-- 5. Monitorowanie ciągłości snapshotów (oczekiwany dokładnie 1 zatwierdzony snapshot dziennie)
SELECT
    snapshot_date,
    COUNT(DISTINCT run_id) AS runs_recorded,
    MIN(extracted_at) AS first_extraction_time,
    MAX(extracted_at) AS last_extraction_time
FROM dbo.vw_job_snapshots
GROUP BY snapshot_date
ORDER BY snapshot_date DESC;
