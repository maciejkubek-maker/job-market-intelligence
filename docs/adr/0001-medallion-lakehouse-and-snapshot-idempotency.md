# ADR 0001: Architektura Medallion w ADLS Gen2 i Idempotentność Snapshotów

## Status
Zaakceptowany (Accepted)

## Kontekst
Projekt monitoruje oferty polskiego rynku pracy (począwszy od publicznego API SOLID.Jobs dla obszarów Data & Automation). Zbieranie danych oparte jest o dzienne migawki (snapshoty) całego rynku. Z uwagi na charakterystykę przetwarzania chmurowego (sieć, potencjalne ponowienia, awarie zadań Spark) konieczne było zaprojektowanie architektury zapewniającej:
1. Nienaruszalność i audytowalność danych surowych (reproducibility).
2. Odporność na wielokrotne uruchomienia tego samego dnia (idempotentność).
3. Wysoką wydajność analityczną dla zapytań SQL i narzędzi BI.

## Decyzja

1. **Warstwa Bronze (ADLS Gen2):**
   - Format: surowy JSON podzielony na strony (`offers_page_0000.json`, ...).
   - Metadane: plik `manifest.json` towarzyszący każdemu wykonaniu.
   - Ścieżka partycjonowania: `source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<timestamp_utc>/`.
   - Zasada: brak jakichkolwiek nieodwracalnych transformacji. Każda próba (udana, niepełna lub nieudana) otrzymuje unikalny `run_id`, co wyklucza nadpisywanie danych.
   - Tylko snapshoty o statusie `completed` w manifeście są kwalifikowane do dalszego przetwarzania.

2. **Warstwa Silver (ADLS Gen2):**
   - Format: kolumnowy Parquet z kompresją Snappy.
   - Ścieżka: `jobs/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<run_id>/`.
   - Przetwarzanie: Apache Spark w Azure Synapse (`nb_bronze_to_silver_jobs`). Rekordy są oczyszczane, typowane, deduplikowane w obrębie snapshotu i wzbogacane o metadane jakości.
   - Znacznik `_SUCCESS`: generowany przez Spark po pomyślnym zapisie partycji. Skrypt przesyłania sprawdza obecność pliku `_SUCCESS` — jeśli istnieje, ponowne wywołanie pipeline'u dla danego `run_id` jest pomijane.

3. **Warstwa Gold (ADLS Gen2) i Reguła Idempotentności:**
   - Format: zagregowany / analityczny Parquet zoptymalizowany pod zapytania analityczne.
   - **Reguła biznesowa:** Gold zachowuje **pierwszy skuteczny snapshot danego dnia**. Jeżeli w ciągu doby wykonano kilka pomyślnych ekstrakcji (np. z powodu testów lub ponowienia po awarii infrastruktury), do warstwy Gold włączany jest pierwszy kompletny zbiór. Zapewnia to determinizm analiz trendów dzień-do-dnia i chroni przed wielokrotnym zliczaniem ogłoszeń.

4. **Warstwa Prezentacji Analitycznej (Synapse Serverless SQL):**
   - Utworzenie dedykowanej logicznej bazy danych `job_market` z kodowaniem UTF-8 (`Latin1_General_100_BIN2_UTF8`).
   - Udostępnienie widoku `dbo.vw_job_snapshots` mapującego pliki Parquet warstwy Gold bez konieczności kopiowania danych i bez ponoszenia kosztów dedykowanej bazy SQL w stanie bezczynności.

## Konsekwencje
- **Pozytywne:**
  - Pełna audytowalność każdego żądania HTTP i stanu pobrania.
  - Ochrona przed nadpisywaniem historii i przypadkowym uszkodzeniem zbiorów analitycznych.
  - Niskie koszty przechowywania i zapytań (Serverless SQL płatny wyłącznie za przetworzone terabajty).
- **Negatywne / Kompromisy:**
  - Wymaga zarządzania retencją surowych plików JSON w Bronze (polityki ADLS Lifecycle Management).
  - Narzut czasowy związany z uruchamianiem klastra Spark dla małych wolumenów danych (uzasadniony przygotowaniem pod skalowalność i demonstrację technologii chmurowych).
