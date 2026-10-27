# Architektura Systemu — Polish Job Market Intelligence

Projekt realizuje zautomatyzowany, skalowalny proces gromadzenia i analityki danych polskiego rynku pracy IT w architekturze nowoczesnego jeziora danych (Modern Data Lakehouse) w chmurze Microsoft Azure.

---

## 1. Diagram Architektury Przepływu Danych

```
+-------------------------------------------------------------------------------+
|                                ŹRÓDŁO DANYCH                                 |
|                         SOLID.Jobs Public REST API                            |
|       (Ogłoszenia IT: Data Analyst, BI Analyst, Data Engineer, Automation)     |
+-------------------------------------------------------------------------------+
                                      |
                                      | HTTP GET (JSON, paginacja, timeout)
                                      v
+-------------------------------------------------------------------------------+
|                            ORKIESTRACJA I POBIERANIE                          |
|                  GitHub Actions (CRON: 06:00 Europe/Warsaw)                   |
|                  Ekstraktor Python (uv, requests, walidacja)                  |
|                  Autoryzacja Azure OIDC (bez haseł i sekretów)                |
+-------------------------------------------------------------------------------+
                                      |
                                      | Zapis stron JSON + manifest.json
                                      v
+-------------------------------------------------------------------------------+
|                        ADLS Gen2: WARSTWA BRONZE                              |
|           Kontener: bronze/source=solidjobs/snapshot_date=.../run_id=...      |
|           Nienaruszalne, audytowalne odpowiedzi źródłowe API                  |
|           Manifest stanu: status (completed / incomplete / failed)            |
+-------------------------------------------------------------------------------+
                                      |
                                      | Wyzwolenie pipeline'u ADF
                                      v
+-------------------------------------------------------------------------------+
|                        AZURE DATA FACTORY (Orkiestracja)                      |
|                       Pipeline: pl_job_market_daily_etl                       |
|           Krok 1: Synapse Notebook: nb_bronze_to_silver_jobs                  |
|           Krok 2: Synapse Notebook: nb_silver_to_gold_jobs (on success)       |
+-------------------------------------------------------------------------------+
                                      |
                         +------------+------------+
                         |                         |
                         v                         v
+----------------------------------+  +----------------------------------+
|    ADLS Gen2: WARSTWA SILVER     |  |     ADLS Gen2: WARSTWA GOLD      |
|    Format: Parquet (Snappy)      |  |    Format: Parquet (Snappy)      |
|    Ścieżka: jobs/source=...      |  |    Ścieżka: jobs/snapshot_date=..|
|    Deduplikacja i oczyszczanie   |  |    Zasada: Pierwszy skuteczny    |
|    Znacznik sukcesu: _SUCCESS    |  |    snapshot dla danego dnia      |
+----------------------------------+  +----------------------------------+
                                                           |
                                                           | Zapytania Serverless
                                                           v
                                      +-----------------------------------------+
                                      |        AZURE SYNAPSE ANALYTICS          |
                                      |       Serverless SQL Pool (built-in)    |
                                      |       Baza danych: job_market           |
                                      |       Widok: dbo.vw_job_snapshots       |
                                      +-----------------------------------------+
                                                           |
                                                           | Model semantyczny
                                                           v
                                      +-----------------------------------------+
                                      |         POWER BI (Faza Przyszła)        |
                                      |      Dashboardy analityczne rynku,      |
                                      |      trendy technologii i płac          |
                                      +-----------------------------------------+
```

---

## 2. Kontrakty Danych i Odpowiedzialność Warstw

### Warstwa Bronze (Surowy Zbiór Danych)
- **Format:** Nieskompresowane pliki JSON podzielone na strony API (`offers_page_0000.json`, `offers_page_0001.json`, ...) oraz plik kontrolny `manifest.json`.
- **Ścieżka przechowywania:** `source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<timestamp_utc>/`.
- **Reguła nienaruszalności:** Dane surowe nie są modyfikowane. W przypadku wielokrotnego uruchomienia w ciągu dnia, każde uruchomienie tworzy unikalny katalog `run_id`, co zapobiega nadpisywaniu prób niekompletnych lub błędnych.
- **Kontrakt manifestu:**
  ```json
  {
    "source": "solidjobs",
    "run_id": "20261027T100650200206Z",
    "snapshot_date": "2026-10-27",
    "started_at": "2026-10-27T10:06:50.200206+00:00",
    "status": "completed",
    "expected_offers": 1520,
    "expected_pages": 152,
    "downloaded_pages": 152,
    "downloaded_offers": 1520,
    "unique_offers": 1520,
    "finished_at": "2026-10-27T10:08:31.787027+00:00"
  }
  ```
- **Kryterium statusu:**
  - `completed`: Wyłącznie wtedy, gdy `downloaded_pages == expected_pages`, `downloaded_offers == expected_offers` oraz `unique_offers == expected_offers`.
  - `incomplete`: Gdy ekstrakcja dobiegła końca, lecz wystąpiła niezgodność liczby ofert lub duplikaty identyfikatorów.
  - `failed`: W razie wystąpienia błędu sieciowego (HTTP 4xx/5xx, timeout, zerwanie połączenia) lub błędu parsowania struktury.

### Warstwa Silver (Zbiór Oczyszczony i Ustrukturyzowany)
- **Format:** Kolumnowy format Parquet skompresowany Snappy.
- **Przetwarzanie:** Notatnik Azure Synapse Spark `nb_bronze_to_silver_jobs` uruchamiany na puli `sparkjobmarket`.
- **Transformacje:**
  - Rozpakowanie zagnieżdżonych struktur JSON.
  - Standaryzacja typów danych (daty publikacji, identyfikatory, wartości liczbowe).
  - Normalizacja widełek płacowych (rozbicie na `salary_from`, `salary_to`, `salary_currency`, `salary_period`).
  - Standaryzacja technologii i umiejętności do ujednoliconej tablicy.
  - Znakowanie flag jakości danych (`quality_flags`).
- **Idempotentność i znacznik `_SUCCESS`:**
  Po pomyślnym zapisie partycji Spark generuje plik `_SUCCESS`. Skrypt orkiestracji weryfikuje istnienie tego pliku przed zleceniem przetwarzania ADF, co chroni przed niepotrzebnym ponownym przetwarzaniem tego samego snapshotu.

### Warstwa Gold (Zbiór Analityczny)
- **Format:** Partycjonowany zbiór Parquet przygotowany do bezpośredniej konsumpcji analitycznej.
- **Ścieżka:** `gold/jobs/snapshot_date=YYYY-MM-DD/`.
- **Reguła biznesowa:** Warstwa Gold przechowuje **pierwszy zatwierdzony snapshot z danego dnia**. Jeżeli w danym dniu przeprowadzono więcej niż jedno pomyślne pobranie, kolejne snapshoty nie nadpisują stanu Gold. Pozwala to zachować stabilność analiz trendu dzień-do-dnia oraz chroni wskaźniki dynamiki ofert przed zniekształceniem.

---

## 3. Warstwa Analityczna i SQL Reproducibility

W celu zapewnienia natychmiastowego dostępu do danych bez konieczności utrzymywania kosztownej dedykowanej bazy relacyjnej 24/7, zastosowano **Synapse Serverless SQL Pool (built-in)**.

1. **Baza danych `job_market`:** Utworzona z kodowaniem UTF-8 (`Latin1_General_100_BIN2_UTF8`), co zapobiega problemom z polskimi znakami diakrytycznymi w nazwach stanowisk i opisach firm.
2. **Widok `dbo.vw_job_snapshots`:** Mapuje dane z ADLS Gen2 za pomocą funkcji `OPENROWSET` nad plikami Parquet warstwy Gold.
3. **Reprodukowalność skryptów:** W katalogu `sql/` wyodrębniono:
   - `01_create_database.sql`: idempotente tworzenie bazy danych.
   - `02_create_view_job_snapshots.sql`: definicja schematu i rzutowania typów.
   - `03_validate_snapshots.sql`: zestaw kwerend sprawdzających unikalność, spójność i dystrybucję danych.

---

## 4. Bezpieczeństwo i Federacja Tożsamości (OIDC)

- **Brak statycznych kluczy API i haseł:** Wdrożono federację tożsamości OpenID Connect pomiędzy GitHub Actions a Microsoft Entra ID.
- **Least Privilege (Zasada Najmniejszych Uprawnień):**
  - Workflow CI (`.github/workflows/ci.yml`) ma uprawnienia wyłącznie do odczytu kodu (`contents: read`) i nie ma dostępu do chmury.
  - Workflow ekstrakcji (`.github/workflows/daily_extract.yml`) korzysta z krótkoterminowego tokenu OIDC (`id-token: write`) i ma uprawnienia ściśle ograniczone do wymaganego kontenera storage i fabryki danych.
  - Żadne dane wrażliwe ani sekrety nie są umieszczane w repozytorium kodu.
