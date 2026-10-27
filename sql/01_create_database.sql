-- ============================================================================
-- Skrypt 01: Utworzenie bazy danych w Azure Synapse Serverless SQL Pool
-- Projekt: Polish Job Market Intelligence
-- Cel: Utworzenie dedykowanej bazy analitycznej job_market z kodowaniem UTF-8.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.databases WHERE name = 'job_market'
)
BEGIN
    CREATE DATABASE job_market
    COLLATE Latin1_General_100_BIN2_UTF8;
    PRINT 'Baza danych job_market została pomyślnie utworzona.';
END
ELSE
BEGIN
    PRINT 'Baza danych job_market już istnieje.';
END
GO
