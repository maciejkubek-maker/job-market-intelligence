-- ============================================================================
-- Skrypt 03: Zapytania walidacyjne jakości i kompletności snapshotów
-- Projekt: Polish Job Market Intelligence
-- Silnik: Synapse Serverless SQL
-- Status: LEGACY GOLD V1. Waliduje wyłącznie dbo.vw_job_snapshots.
-- Cel: Weryfikacja unikalności, rozkładu kategorii, braków danych oraz ciągłości.
-- Aktualne walidacje Gold v2 znajdują się w synapse/sqlscript/03_create_gold_v2_views.json.
-- Uwaga operacyjna: zapytania są tylko definicją repozytoryjną i nie zmieniają Azure.
-- ============================================================================

USE job_market;
GO

-- 1. Zestawienie liczby ofert per data snapshotu i unikalne wykonania (run_id)
SELECT
    snapshot_date,
    run_id,
    COUNT(*) AS total_offers,
    COUNT(DISTINCT job_id) AS unique_offers,
    COUNT(*) - COUNT(DISTINCT job_id) AS duplicate_count
FROM dbo.vw_job_snapshots
GROUP BY snapshot_date, run_id
ORDER BY snapshot_date DESC;

-- 2. Weryfikacja unikalności klucza naturalnego w obrębie dnia (powinno zwrócić 0 wierszy)
SELECT
    job_id,
    snapshot_date,
    COUNT(*) AS occurrence_count
FROM dbo.vw_job_snapshots
GROUP BY job_id, snapshot_date
HAVING COUNT(*) > 1;

-- 3. Rozkład kategorii ról według dni obserwacji
SELECT
    snapshot_date,
    category,
    COUNT(*) AS offers_count,
    SUM(CASE WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN 1 ELSE 0 END) AS offers_with_salary,
    ROUND(
        CAST(SUM(CASE WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT)
        / COUNT(*) * 100,
        2
    ) AS pct_with_salary
FROM dbo.vw_job_snapshots
GROUP BY snapshot_date, category
ORDER BY snapshot_date DESC, offers_count DESC;

-- 4. Statystyki widełek wynagrodzeń w PLN według formy umowy i roli
SELECT
    category,
    salary_period,
    COUNT(*) AS sample_size,
    ROUND(AVG(salary_from), 0) AS avg_salary_min,
    ROUND(AVG(salary_to), 0) AS avg_salary_max
FROM dbo.vw_job_snapshots
WHERE salary_from IS NOT NULL
  AND salary_to IS NOT NULL
  AND salary_currency = 'PLN'
GROUP BY category, salary_period
ORDER BY category, sample_size DESC;

-- 5. Monitorowanie ciągłości snapshotów (oczekiwany dokładnie 1 zatwierdzony snapshot dziennie)
SELECT
    snapshot_date,
    COUNT(DISTINCT run_id) AS runs_recorded,
    COUNT(*) AS offers_recorded
FROM dbo.vw_job_snapshots
GROUP BY snapshot_date
ORDER BY snapshot_date DESC;
