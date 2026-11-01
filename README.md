<a id="english"></a>

# Polish Job Market Intelligence — Azure Data Lakehouse ETL & Analytics

[![CI Quality Checks](https://github.com/maciejkubek-maker/job-market-intelligence/actions/workflows/ci.yml/badge.svg)](https://github.com/maciejkubek-maker/job-market-intelligence/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Code Style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v0.json)](https://github.com/astral-sh/ruff)

🇬🇧 English | [🇵🇱 Wersja polska ↓](#wersja-polska)

A comprehensive Data Engineering project that delivers a fully automated, idempotent ETL/ELT pipeline for monitoring the Polish job market in data and automation roles (**Data Analyst**, **BI Analyst**, **Data Engineer**, **Process Automation**).

The project implements a **Medallion Lakehouse** architecture on **Microsoft Azure** (Bronze, Silver, Gold v2), passwordless orchestration with **GitHub Actions OIDC** and **Azure Data Factory**, distributed processing in **Azure Synapse Spark**, and an analytical serving layer in **Azure Synapse Serverless SQL**.

---

## 1. Implemented Solution Architecture (Azure Lakehouse)

![Job Market Intelligence data flow architecture](readme%20pic/schema.png)

*Project architecture — automated data flow from SOLID.Jobs API to Azure Lakehouse, Synapse SQL, and Power BI.*

The architecture separates storage, distributed processing, and analytical presentation layers:

```
[ SOLID.Jobs Public API ]
          │  HTTP GET (pagination, schema validation, error handling)
          ▼
[ GitHub Actions Runner ]  ──(OIDC — passwordless and secretless)──► [ Azure Entra ID ]
          │  Workflow: Job Market - Daily ETL (Azure) [cron: 40 4 * * * UTC]
          ▼  Write raw JSON pages + manifest.json
[ ADLS Gen2: Bronze ] (source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=.../)
          │
          ▼  Asynchronous trigger (GHA success != ADF success)
[ Azure Data Factory ] (Pipeline: pl_job_market_daily_etl)
          │
          ├──► [ Synapse Spark: nb_bronze_to_silver_jobs ]
          │          │
          │          ▼ Write cleansed Parquet + _SUCCESS marker
          │    [ ADLS Gen2: Silver ] (jobs/source=solidjobs/snapshot_date=.../run_id=.../)
          │
          └──► [ Synapse Spark: nb_silver_to_gold_v2_jobs ] (active; runs after Silver succeeds)
                     │  [nb_silver_to_gold_jobs retained as legacy]
                     │
                     ▼ Write 3 physical tables + _manifest (idempotency, _SUCCESS checks)
               [ ADLS Gen2: Gold v2 ] (jobs/v2/)
                     ├── fact_job_snapshots/snapshot_date=YYYY-MM-DD/
                     ├── bridge_job_skills/snapshot_date=YYYY-MM-DD/
                     ├── bridge_job_locations/snapshot_date=YYYY-MM-DD/
                     └── _manifest/snapshot_date=YYYY-MM-DD/
                     │
                     ▼ OPENROWSET mapping (joins and manifest-based verification)
[ Synapse Serverless SQL ] (Database: job_market)
          ├── dbo.vw_job_snapshots_v2
          ├── dbo.vw_job_skills_v2
          ├── dbo.vw_job_locations_v2
          ├── dbo.vw_gold_v2_commits
          └── dbo.vw_job_snapshots (legacy Gold v1)
          │
          ▼ Analytical Presentation Layer
[ Power BI ]
          ├── Market Overview dashboard (Gold v2)
          └── Career Explorer dashboard (Gold v2)
```

### System Components

- **Data source:** Public [SOLID.Jobs](https://solid.jobs/api-ofert-pracy) API for IT job postings in Poland.
- **Extraction and validation:** Python 3.11 (`src/extract_solidjobs.py`), managed with `uv`.
- **Daily orchestration:** GitHub Actions (`.github/workflows/daily_extract.yml`), workflow named `Job Market - Daily ETL (Azure)`, scheduled with `cron: "40 4 * * *"` in UTC (05:40 Polish time in winter and 06:40 in summer). GitHub Actions cron syntax does not provide a `timezone` field.
- **Authentication:** Short-lived OpenID Connect (OIDC) tokens with no static secrets or passwords.
- **Data storage:** Azure Data Lake Storage Gen2 (`stjobmarketdev01`) with three Medallion architecture containers (`bronze`, `silver`, `gold`).
- **Cloud orchestration:** Azure Data Factory (`adf-job-market-dev`, pipeline `pl_job_market_daily_etl`), triggered asynchronously by the GitHub Actions runner.
- **Distributed processing:** Azure Synapse Spark (`sparkjobmarket`):
  - Step 1 (`Bronze_to_silver`): notebook `nb_bronze_to_silver_jobs`,
  - Step 2 (`SIlver_to_gold`): active notebook `nb_silver_to_gold_v2_jobs`. The previous `nb_silver_to_gold_jobs` notebook (Gold v1) remains available as legacy.
- **Analytical layer:** Synapse Serverless SQL Pool (database `job_market`):
  - Active Gold v2 views: `dbo.vw_job_snapshots_v2`, `dbo.vw_job_skills_v2`, `dbo.vw_job_locations_v2`, `dbo.vw_gold_v2_commits`,
  - Legacy view: `dbo.vw_job_snapshots` (Gold v1).
- **Reporting and BI:** The current Power BI project (`.pbip` with a TMDL semantic model) imports `dbo.vw_job_snapshots_v2`, `dbo.vw_job_skills_v2`, and `dbo.vw_job_locations_v2`. Its active dashboard pages are **Market Overview** and **Career Explorer**.

---

## 2. Data Contracts and Layer Responsibilities

| Layer | Technology / Format | Path Structure | Responsibilities and Business Rules |
|---|---|---|---|
| **Bronze** | Raw JSON + `manifest.json` | `bronze/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<timestamp_utc>/` | Stores source API responses 1:1, partitioned by page. Each attempt receives a unique timestamp-based `run_id`, eliminating the risk of overwriting historical data. The `completed` status is assigned only after the full page set and job-posting uniqueness have been verified. |
| **Silver** | Apache Parquet (Snappy) | `silver/jobs/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<run_id>/` | Applies data typing, deduplicates postings within a snapshot, normalizes salary ranges into `salary_from`, `salary_to`, and `currency`, flattens technology arrays, and adds data-quality flags. A successful write ends with an `_SUCCESS` marker. |
| **Gold v2 (active)** | Apache Parquet (Snappy) | `gold/jobs/v2/` (three physical tables + manifest) | Provides a normalized star-schema analytical model with bridge tables. All datasets are partitioned by `snapshot_date=YYYY-MM-DD`. Writes are idempotent and include `_SUCCESS` verification and an ETL manifest. |
| **Gold v1 (legacy)** | Apache Parquet (Snappy) | `gold/jobs/fact_job_snapshots/snapshot_date=YYYY-MM-DD/` | Contains the flat fact dataset from the initial implementation. Retained as an archive and rollback/reference layer; it is not the source of the current Power BI dashboards. |

### Gold v2 Layer Structure (`gold/jobs/v2/`)

All datasets are partitioned by `snapshot_date=YYYY-MM-DD`:

1. **`fact_job_snapshots`** (`gold/jobs/v2/fact_job_snapshots/snapshot_date=YYYY-MM-DD/`):
   - The primary fact table storing job postings as snapshots.
   - Key columns: `job_snapshot_key` (snapshot primary key), `source`, `job_id`, `run_id`, `title`, `division`, `category`, `sub_category`, `company`, `company_logo_url`, `experience_level`, `salary_from`, `salary_to`, `salary_currency`, `salary_period`, `employment_type`, `is_remote`, `is_hybrid`, `contract_time`, `valid_from`, `valid_to`, `updated_at`, `url`, `languages_json`, `benefits_json`.
2. **`bridge_job_skills`** (`gold/jobs/v2/bridge_job_skills/snapshot_date=YYYY-MM-DD/`):
   - A bridge table containing technical and soft skills associated with a job posting.
   - Key columns: `job_snapshot_key`, `job_id`, `run_id`, `skill_index` (the item's position in the posting's list), `skill_name`, `skill_level`.
3. **`bridge_job_locations`** (`gold/jobs/v2/bridge_job_locations/snapshot_date=YYYY-MM-DD/`):
   - A bridge table that normalizes locations assigned to a job posting.
   - Key columns: `job_snapshot_key`, `job_id`, `run_id`, `location_index` (the item's position in the location list), `location_name`.
4. **`_manifest`** (`gold/jobs/v2/_manifest/snapshot_date=YYYY-MM-DD/`):
   - The ETL manifest that persists metadata for a committed snapshot.
   - Columns: `snapshot_date`, `run_id`, `job_count`, `skill_count`, `location_count`.

### Table Relationships

- Relationships between the fact table and bridge tables use the unique **`job_snapshot_key`** (composite format: `solidjobs|{snapshot_date}|{jobOfferKey}`).
- Row uniqueness in the bridge tables is guaranteed by `(job_snapshot_key, skill_index)` and `(job_snapshot_key, location_index)` respectively.

### Bronze Manifest Schema

Each extraction attempt creates a `manifest.json` file:

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

If an error occurs (for example, HTTP 500, a network timeout, missing pages, or duplicates), the manifest is **always persisted to disk** with a `failed` or `incomplete` status. The process returns exit code `1`, stopping the workflow before incomplete data can be uploaded to the cloud.

---

## 3. Idempotency and Gold v2 Processing Guarantees

1. **Bronze run isolation:** A unique `run_id` with microsecond precision in UTC ensures that every run is written to a separate directory.
2. **Silver `_SUCCESS` guard:** Spark produces an `_SUCCESS` marker file. Silver processing is performed only for completed Bronze runs.
3. **Gold v2 idempotency and completeness controls (`nb_silver_to_gold_v2_jobs`):**
   - **Skipping completed dates:** The notebook checks for the `_SUCCESS` marker in `_manifest/snapshot_date=YYYY-MM-DD/` and in each of the three tables. If a complete snapshot exists, reprocessing is safely skipped.
   - **No overwrite after a partial write:** If any table directory exists but the manifest `_SUCCESS` marker is missing, the notebook immediately raises a `RuntimeError` and stops, requiring manual diagnostics instead of an unsafe overwrite.
   - **Strict pre-write validation:** Before writing data, the notebook validates that `job_id` values are not empty, `job_snapshot_key` is unique, `run_id` values are consistent, exploded-array cardinality is fully preserved (`skill_count == expected_skills`, `location_count == expected_locations`), relational tables contain no orphaned records (anti-join), and `skill_index` and `location_index` contain neither nulls nor duplicates.
   - **Write order and ETL manifest:** The three physical tables are written in `errorifexists` mode. After verifying the `_SUCCESS` marker for all three tables, the notebook writes the `_manifest` table last and confirms its `_SUCCESS` marker.

---

## 4. Security and Passwordless Architecture (OIDC)

The project eliminates the need to store passwords, tokens, or static client secrets in the GitHub repository:

- **Microsoft Entra ID workload identity federation:** GitHub Actions negotiates an OpenID Connect (OIDC) token with Azure.
- **Principle of least privilege:**
  - The continuous integration workflow (`.github/workflows/ci.yml`) has only `contents: read` permission and no access to cloud resources or secrets.
  - The production workflow (`.github/workflows/daily_extract.yml`) has `id-token: write` permission strictly limited to the `main` branch.
  - Azure RBAC roles restrict the identity to writing only to the `bronze` container in the `stjobmarketdev01` account and triggering the ADF pipeline in the `rg-job-market-dev` resource group.

A detailed description of this architectural decision is available in [docs/adr/0002-passwordless-github-oidc-security.md](docs/adr/0002-passwordless-github-oidc-security.md).

---

## 5. Installation, Local Execution, and Testing

The project uses the modern Python environment and package manager [`uv`](https://github.com/astral-sh/uv).

### Prerequisites

- Python 3.11+
- The `uv` tool installed (or standard `pip`)

### Step 1: Clone and Install Dependencies

```bash
git clone https://github.com/maciejkubek-maker/job-market-intelligence.git
cd job-market-intelligence

# Synchronize dependencies using the exact versions locked in uv.lock
uv sync
```

### Step 2: Run Unit Tests (pytest)

The unit test suite fully mocks external communication and requires neither an internet connection nor Azure credentials:

```bash
uv run pytest
```

The tests verify, among other things:

- Complete, successful multi-page extraction and manifest consistency.
- Handling of HTTP failures (4xx and 5xx) and network timeouts.
- Resilience to malformed or incomplete JSON structures.
- Detection of job-count inconsistencies against the API declaration.
- Detection of duplicate job identifiers (`jobOfferKey`).
- Correct exit codes (`0` for success and `1` for errors).

### Step 3: Static Analysis and Linting (Ruff)

```bash
uv run ruff check .
```

### Step 4: Local Test Extraction

```bash
uv run python src/extract_solidjobs.py
```

Downloaded data is saved in the local `data/bronze/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=.../` directory.

---

## 6. SQL Layer Reproducibility (Synapse Serverless SQL)

Database, view, and data-quality validation definitions are stored in the `sql/` and `synapse/sqlscript/` directories:

1. **`job_market` database:**
   Created with `Latin1_General_100_BIN2_UTF8` collation to prevent encoding errors for Polish diacritical characters.
2. **Active Gold v2 views (`synapse/sqlscript/03_create_gold_v2_views.json`):**
   - **`dbo.vw_job_snapshots_v2`:** Maps the `gold/jobs/v2/fact_job_snapshots/snapshot_date=*/*.parquet` fact table.
   - **`dbo.vw_job_skills_v2`:** Maps the `gold/jobs/v2/bridge_job_skills/snapshot_date=*/*.parquet` skills table.
   - **`dbo.vw_job_locations_v2`:** Maps the `gold/jobs/v2/bridge_job_locations/snapshot_date=*/*.parquet` locations table.
   - **`dbo.vw_gold_v2_commits`:** Maps the `gold/jobs/v2/_manifest/snapshot_date=*/*.parquet` ETL manifests.
3. **Legacy Gold v1 view (`sql/02_create_view_job_snapshots.sql`):**
   - **`dbo.vw_job_snapshots`:** Maps `gold/jobs/fact_job_snapshots/snapshot_date=*/*.parquet` for archival, rollback, and legacy validation purposes. The current Power BI model does not use this view.

> [!IMPORTANT]
> **SQL completeness verification:**
> Synapse Serverless SQL maps Parquet files with `OPENROWSET` and **does not independently inspect the physical `_SUCCESS` file**. Data consistency and commit status are controlled at query level by joining or filtering on `snapshot_date` and `run_id` from the `dbo.vw_gold_v2_commits` manifest view.

---

## 7. Operational Monitoring and Verification Procedures (Troubleshooting)

### Key Monitoring Principle: ADF Is Asynchronous

In the daily process, the ADF call in `src/trigger_adf.py` uses the `create_run` method asynchronously (fire-and-forget). It returns a `run_id` when ADF accepts the request.

- **A green GitHub Actions status means only that data was successfully extracted to Bronze, uploaded, and the ADF pipeline was successfully initiated.**
- **GitHub Actions success does not guarantee ADF success.** The actual processing in Spark notebooks takes from several to more than ten minutes.
- End-to-end monitoring requires checking the **Monitor** tab in Azure Data Factory or configuring Azure Monitor alerts (details in [docs/operational_monitoring.md](docs/operational_monitoring.md)).

### Scenario A: Extraction Failure in GitHub Actions

1. Open the workflow run summary in the **Actions** tab.
2. If the `Extract SOLID.Jobs offers` step failed, review the error reported in the manifest's `PODSUMOWANIE` section.
3. The `Upload to Azure Bronze` step will not run, preventing incomplete data from reaching the data lake.

### Scenario B: ADLS Gen2 File Integrity Verification

The `src/upload_to_adls.py` script performs a safe upload:

- It compares the local file size with the Azure object.
- It verifies SHA-256 checksums.
- If an interrupted transfer is detected, it automatically retries the upload.

### Scenario C: Diagnosing ADF / Synapse Spark Failures

1. Go to Azure Portal -> Azure Data Factory (`adf-job-market-dev`) -> **Monitor** -> **Pipeline runs**.
2. Find the run using the provided `run_id` or `snapshot` parameter.
3. Inspect the `Bronze_to_silver` and `SIlver_to_gold` activity runs. If Spark fails, review the Livy logs in Synapse Studio.

---

## 8. Deployment Validation Results (Rollout Audit)

> [!NOTE]
> The table below presents **historical Gold v2 deployment validation results** recorded during production testing and rollout from October 26–29, 2026. These values are retained for audit purposes and **are not updated automatically in this document**.

| Snapshot date (`snapshot_date`) | Job postings (`fact_job_snapshots`) | Skills (`bridge_job_skills`) | Locations (`bridge_job_locations`) | Consistency status |
|:---:|:---:|:---:|:---:|:---:|
| **2026-10-26** | 1,538 | 10,221 | 1,538 | Valid (OK) |
| **2026-10-27** | 1,520 | 10,042 | 1,520 | Valid (OK) |
| **2026-10-28** | 1,525 | 10,085 | 1,525 | Valid (OK) |
| **2026-10-29** | 1,537 | 10,152 | 1,537 | Valid (OK) |

All snapshots above successfully passed duplicate checks, array-cardinality preservation tests, and `job_snapshot_key` relational-integrity verification.

### Current Power BI Dashboards

The screenshots below show the current **Market Overview** and **Career Explorer** dashboards. The checked-in PBIP/TMDL semantic model imports the active Gold v2 Synapse SQL views.

![Power BI Market Overview dashboard](readme%20pic/Power_bi_1.png)

*Market Overview — hiring activity, in-demand skills, remote opportunities, and market movement.*

![Power BI Career Explorer dashboard](readme%20pic/Power_bi_2.png)

*Career Explorer — role filters, required skills, salary benchmarks, locations, and matching job offers.*

---

## 9. Deployment Status and Roadmap

### Implemented in the Current Version (Gold v2)

- [x] Stable API extractor with deterministic exit codes and Bronze manifest persistence.
- [x] Automated CI quality pipeline in GitHub Actions (`.github/workflows/ci.yml`).
- [x] Passwordless Azure OIDC authentication (`.github/workflows/daily_extract.yml`).
- [x] Parquet-based Silver layer with deduplication and quality indicators (`nb_bronze_to_silver_jobs`).
- [x] Active **Gold v2** layer (`nb_silver_to_gold_v2_jobs`):
  - Star schema separating facts (`fact_job_snapshots`) from relationships (`bridge_job_skills`, `bridge_job_locations`),
  - Support for technical and soft skills,
  - ETL manifest (`_manifest`) and write-completeness controls with `_SUCCESS` markers,
  - Full protection against overwriting partially processed data.
- [x] Safe retention of **Gold v1** as legacy, including the `dbo.vw_job_snapshots` view and `nb_silver_to_gold_jobs` notebook.
- [x] Synapse Serverless SQL views for Gold v2 (`dbo.vw_job_snapshots_v2`, `dbo.vw_job_skills_v2`, `dbo.vw_job_locations_v2`, `dbo.vw_gold_v2_commits`).
- [x] Read permissions for the `powerbi-reader` analytical account.
- [x] Power BI migration to Gold v2:
  - PBIP/TMDL semantic model importing the three active Gold v2 business views,
  - Relationships between facts, skills, and locations based on `job_snapshot_key`,
  - **Market Overview** and **Career Explorer** dashboard pages.

### Planned for Future Phases

- [ ] Implement automated Azure Monitor / Logic Apps alerting rules for ADF pipeline failures.
- [ ] Introduce a recurring historical-data retention policy for the Bronze container (ADLS Lifecycle Management).

---

<a id="wersja-polska"></a>

# Wersja polska

[↑ Back to English](#english)

Kompleksowy projekt inżynierii danych realizujący w pełni zautomatyzowany, idempotentny proces ETL/ELT monitorujący polski rynek pracy w obszarze danych i automatyzacji (**Data Analyst**, **BI Analyst**, **Data Engineer**, **Process Automation**).

Projekt implementuje architekturę **Medallion Lakehouse** w chmurze **Microsoft Azure** (Bronze, Silver, Gold v2), bezpieczną orkiestrację bez haseł przez **GitHub Actions OIDC** i **Azure Data Factory**, przetwarzanie rozproszone w **Azure Synapse Spark** oraz warstwę analityczną w **Azure Synapse Serverless SQL**.

---

## 1. Zrealizowana Architektura Rozwiązania (Azure Lakehouse)

![Job Market Intelligence data flow architecture](readme%20pic/schema.png)

*Project architecture — automated data flow from SOLID.Jobs API to Azure Lakehouse, Synapse SQL, and Power BI.*

Architektura opiera się na separacji warstw składowania, przetwarzania rozproszonego oraz prezentacji analitycznej:

```
[ SOLID.Jobs Public API ]
          │  HTTP GET (paginacja, walidacja schematu, obsługa błędów)
          ▼
[ GitHub Actions Runner ]  ──(OIDC - bez haseł i sekretów)──► [ Azure Entra ID ]
          │  Workflow: Job Market - Daily ETL (Azure) [cron: 40 4 * * * UTC]
          ▼  Zapis surowych stron JSON + manifest.json
[ ADLS Gen2: Bronze ] (source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=.../)
          │
          ▼  Wyzwalanie asynchroniczne (sukces GHA != sukces ADF)
[ Azure Data Factory ] (Pipeline: pl_job_market_daily_etl)
          │
          ├──► [ Synapse Spark: nb_bronze_to_silver_jobs ]
          │          │
          │          ▼ Zapis oczyszczonych Parquet + znacznik _SUCCESS
          │    [ ADLS Gen2: Silver ] (jobs/source=solidjobs/snapshot_date=.../run_id=.../)
          │
          └──► [ Synapse Spark: nb_silver_to_gold_v2_jobs ] (aktywny; na sukcesie Silver)
                     │  [nb_silver_to_gold_jobs zachowany jako legacy]
                     │
                     ▼ Zapis 3 tabel fizycznych + _manifest (idempotencja, sprawdzanie _SUCCESS)
               [ ADLS Gen2: Gold v2 ] (jobs/v2/)
                     ├── fact_job_snapshots/snapshot_date=YYYY-MM-DD/
                     ├── bridge_job_skills/snapshot_date=YYYY-MM-DD/
                     ├── bridge_job_locations/snapshot_date=YYYY-MM-DD/
                     └── _manifest/snapshot_date=YYYY-MM-DD/
                     │
                     ▼ Mapowanie przez OPENROWSET (złączenia i weryfikacja przez manifest)
[ Synapse Serverless SQL ] (Baza: job_market)
          ├── dbo.vw_job_snapshots_v2
          ├── dbo.vw_job_skills_v2
          ├── dbo.vw_job_locations_v2
          ├── dbo.vw_gold_v2_commits
          └── dbo.vw_job_snapshots (legacy Gold v1)
          │
          ▼ Warstwa Prezentacji Analitycznej
[ Power BI ]
          ├── Dashboard Market Overview (Gold v2)
          └── Dashboard Career Explorer (Gold v2)
```

### Komponenty systemu:
- **Źródło danych:** Publiczne API [SOLID.Jobs](https://solid.jobs/api-ofert-pracy) dla ofert IT w Polsce.
- **Ekstrakcja i walidacja:** Python 3.11 (`src/extract_solidjobs.py`), zarządzany przez narzędzie `uv`.
- **Orkiestracja dzienna:** GitHub Actions (`.github/workflows/daily_extract.yml`), workflow o nazwie `Job Market - Daily ETL (Azure)` z harmonogramem `cron: "40 4 * * *"` w UTC (co odpowiada 05:40 czasu polskiego zimą i 06:40 latem). W składni cron GitHub Actions nie występuje pole `timezone`.
- **Uwierzytelnianie:** Krótkoterminowe tokeny OpenID Connect (OIDC) bez statycznych sekretów i haseł.
- **Składowanie danych:** Azure Data Lake Storage Gen2 (`stjobmarketdev01`) z trzema kontenerami architektury Medallion (`bronze`, `silver`, `gold`).
- **Orkiestracja chmurowa:** Azure Data Factory (`adf-job-market-dev`, pipeline `pl_job_market_daily_etl`) wyzwalany asynchronicznie przez runnera GitHub Actions.
- **Przetwarzanie rozproszone:** Azure Synapse Spark (`sparkjobmarket`):
  - Krok 1 (`Bronze_to_silver`): notatnik `nb_bronze_to_silver_jobs`,
  - Krok 2 (`SIlver_to_gold`): aktywny notatnik `nb_silver_to_gold_v2_jobs`. Stary notatnik `nb_silver_to_gold_jobs` (Gold v1) pozostaje zachowany jako legacy.
- **Warstwa analityczna:** Synapse Serverless SQL Pool (baza `job_market`):
  - Aktywne widoki Gold v2: `dbo.vw_job_snapshots_v2`, `dbo.vw_job_skills_v2`, `dbo.vw_job_locations_v2`, `dbo.vw_gold_v2_commits`,
  - Widok legacy: `dbo.vw_job_snapshots` (Gold v1).
- **Raportowanie i BI:** Aktualny projekt Power BI (`.pbip` z modelem semantycznym TMDL) importuje `dbo.vw_job_snapshots_v2`, `dbo.vw_job_skills_v2` oraz `dbo.vw_job_locations_v2`. Aktywne strony dashboardu to **Market Overview** i **Career Explorer**.

---

## 2. Kontrakty Danych i Odpowiedzialność Warstw

| Warstwa | Technologia / Format | Struktura ścieżki | Odpowiedzialność i reguły biznesowe |
|---|---|---|---|
| **Bronze** | Surowy JSON + `manifest.json` | `bronze/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<timestamp_utc>/` | Zapis 1:1 odpowiedzi źródłowych API w podziale na strony. Każda próba otrzymuje unikalny znacznik czasu `run_id`, eliminując ryzyko nadpisania danych historycznych. Status `completed` nadawany jest wyłącznie po pełnej weryfikacji kompletu stron i unikalności ofert. |
| **Silver** | Apache Parquet (Snappy) | `silver/jobs/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<run_id>/` | Typowanie, deduplikacja ogłoszeń w obrębie danego snapshotu, normalizacja widełek wynagrodzeń (rozbicie na `salary_from`, `salary_to`, `currency`), spłaszczenie tablic technologii oraz flagi jakości danych. Pomyślny zapis wieńczy znacznik `_SUCCESS`. |
| **Gold v2 (aktywna)** | Apache Parquet (Snappy) | `gold/jobs/v2/` (trzy tabele fizyczne + manifest) | Znormalizowany model analityczny w architekturze gwiazdy z tabelami pomostowymi. Wszystkie zbiory partycjonowane są według `snapshot_date=YYYY-MM-DD`. Idempotentny zapis z weryfikacją `_SUCCESS` i manifestem ETL. |
| **Gold v1 (legacy)** | Apache Parquet (Snappy) | `gold/jobs/fact_job_snapshots/snapshot_date=YYYY-MM-DD/` | Płaski zbiór faktów z pierwszej wersji wdrożenia. Zachowany jako archiwum oraz warstwa rollback/referencyjna; nie zasila aktualnych dashboardów Power BI. |

### Struktura warstwy Gold v2 (`gold/jobs/v2/`):
Wszystkie zbiory są partycjonowane według `snapshot_date=YYYY-MM-DD`:

1. **`fact_job_snapshots`** (`gold/jobs/v2/fact_job_snapshots/snapshot_date=YYYY-MM-DD/`):
   - Główna tabela faktów przechowująca oferty pracy w ujęciu migawkowym.
   - Kluczowe kolumny: `job_snapshot_key` (klucz główny migawki), `source`, `job_id`, `run_id`, `title`, `division`, `category`, `sub_category`, `company`, `company_logo_url`, `experience_level`, `salary_from`, `salary_to`, `salary_currency`, `salary_period`, `employment_type`, `is_remote`, `is_hybrid`, `contract_time`, `valid_from`, `valid_to`, `updated_at`, `url`, `languages_json`, `benefits_json`.
2. **`bridge_job_skills`** (`gold/jobs/v2/bridge_job_skills/snapshot_date=YYYY-MM-DD/`):
   - Tabela pomostowa przechowująca umiejętności techniczne i kompetencje miękkie powiązane z ofertą.
   - Kluczowe kolumny: `job_snapshot_key`, `job_id`, `run_id`, `skill_index` (indeks pozycji na liście w ogłoszeniu), `skill_name`, `skill_level`.
3. **`bridge_job_locations`** (`gold/jobs/v2/bridge_job_locations/snapshot_date=YYYY-MM-DD/`):
   - Tabela pomostowa normalizująca lokalizacje przypisane do oferty.
   - Kluczowe kolumny: `job_snapshot_key`, `job_id`, `run_id`, `location_index` (indeks pozycji na liście lokalizacji), `location_name`.
4. **`_manifest`** (`gold/jobs/v2/_manifest/snapshot_date=YYYY-MM-DD/`):
   - Manifest ETL utrwalający metadane zatwierdzonego snapshotu.
   - Kolumny: `snapshot_date`, `run_id`, `job_count`, `skill_count`, `location_count`.

### Relacje między tabelami:
- Relacje między tabelą faktów a tabelami pomostowymi opierają się na unikalnym kluczu **`job_snapshot_key`** (format kompozytowy: `solidjobs|{snapshot_date}|{jobOfferKey}`).
- Unikalność wiersza w tabelach pomostowych gwarantuje para `(job_snapshot_key, skill_index)` oraz `(job_snapshot_key, location_index)`.

### Schemat pliku manifestu w warstwie Bronze:
Każda próba ekstrakcji generuje plik `manifest.json`:
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
W przypadku błędu (np. HTTP 500, timeout sieci, brakujące strony lub duplikaty) manifest jest **zawsze utrwalany na dysku** ze statusem `failed` lub `incomplete`, a proces zwraca kod błędu `1`, zatrzymując workflow przed wysłaniem niekompletnych danych do chmury.

---

## 3. Idempotentność i Gwarancje Przetwarzania w Gold v2

1. **Izolacja wykonań w Bronze:** Unikalny parametr `run_id` z dokładnością do mikrosekund w strefie UTC gwarantuje, że każde uruchomienie zapisywane jest w odrębnym katalogu.
2. **Strażnik `_SUCCESS` w Silver:** Silnik Spark generuje plik znacznika `_SUCCESS`. Przetwarzanie do Silver jest realizowane wyłącznie dla ukończonych prób z Bronze.
3. **Idempotencja i kontrola kompletności w Gold v2 (`nb_silver_to_gold_v2_jobs`):**
   - **Pomijanie ukończonych dni:** Notebook sprawdza istnienie znacznika `_SUCCESS` w katalogu `_manifest/snapshot_date=YYYY-MM-DD/` oraz w każdej z trzech tabel. Jeżeli kompletny snapshot istnieje, ponowne przetwarzanie jest bezpiecznie pomijane.
   - **Brak nadpisywania przy częściowym zapisie:** Jeżeli katalog którejkolwiek z tabel istnieje, lecz brak jest znacznika `_SUCCESS` manifestu, notatnik natychmiast zgłasza błąd (`RuntimeError`) i przerywa działanie, wymagając ręcznej diagnostyki zamiast niebezpiecznego nadpisywania danych.
   - **Rygorystyczna walidacja przed zapisem:** Zanim dane trafią na dysk, walidowane są: brak pustych `job_id`, unikalność `job_snapshot_key`, zgodność `run_id`, pełne zachowanie liczby elementów rozbitych tablic (`skill_count == expected_skills`, `location_count == expected_locations`), brak osieroconych rekordów w tabelach relacyjnych (anty-join) oraz brak pustych i powtórzonych indeksów `skill_index` i `location_index`.
   - **Kolejność zapisu i manifest ETL:** Trzy tabele fizyczne zapisywane są w trybie `errorifexists`. Po weryfikacji pliku `_SUCCESS` dla wszystkich trzech tabel, notebook zapisuje jako ostatni tabelę `_manifest` i potwierdza jej znacznik `_SUCCESS`.

---

## 4. Bezpieczeństwo i Architektura Bezhasłowa (OIDC)

W projekcie wyeliminowano konieczność przechowywania haseł, tokenów czy statycznych kluczy Client Secrets w repozytorium GitHub:
- **Federacja tożsamości Microsoft Entra ID:** GitHub Actions negocjuje z Azure token OpenID Connect (OIDC).
- **Zasada najmniejszych uprawnień (Least Privilege):**
  - Workflow ciągłej integracji (`.github/workflows/ci.yml`) posiada wyłącznie uprawnienie `contents: read` i nie ma dostępu do chmury ani sekretów.
  - Workflow produkcyjny (`.github/workflows/daily_extract.yml`) posiada uprawnienie `id-token: write` ściśle zawężone do gałęzi `main`.
  - Role RBAC w Azure ograniczają uprawnienia tożsamości wyłącznie do zapisu w kontenerze `bronze` konta `stjobmarketdev01` oraz wyzwalania pipeline'u ADF w grupie `rg-job-market-dev`.

Szczegółowy opis decyzji architektonicznej znajduje się w dokumencie [docs/adr/0002-passwordless-github-oidc-security.md](docs/adr/0002-passwordless-github-oidc-security.md).

---

## 5. Instalacja, Uruchomienie i Testy Lokalne

Projekt wykorzystuje nowoczesnego menedżera środowisk i pakietów Python — [`uv`](https://github.com/astral-sh/uv).

### Wymagania wstępne:
- Python 3.11+
- Zainstalowane narzędzie `uv` (lub standardowy `pip`)

### Krok 1: Klonowanie i instalacja zależności
```bash
git clone https://github.com/maciejkubek-maker/job-market-intelligence.git
cd job-market-intelligence

# Synchronizacja zależności ze ściśle zablokowanymi wersjami w uv.lock
uv sync
```

### Krok 2: Uruchomienie testów jednostkowych (pytest)
Zestaw testów jednostkowych w pełni mockuje komunikację zewnętrzną (nie wymaga połączenia z internetem ani danych uwierzytelniających Azure):
```bash
uv run pytest
```
Testy weryfikują m.in.:
- Pełną, pomyślną wielostronicową ekstrakcję i zgodność manifestu.
- Obsługę awarii HTTP (4xx, 5xx) oraz timeoutów sieciowych.
- Odporność na uszkodzone lub niepełne struktury JSON.
- Wykrywanie niespójności liczby ofert względem deklaracji API.
- Reakcję na duplikaty identyfikatorów ofert (`jobOfferKey`).
- Poprawność kodów wyjścia (kod `0` dla sukcesu, kod `1` dla błędów).

### Krok 3: Statyczna analiza kodu i linting (Ruff)
```bash
uv run ruff check .
```

### Krok 4: Lokalne próbne pobranie danych
```bash
uv run python src/extract_solidjobs.py
```
Pobrane dane zostaną zapisane w lokalnym katalogu `data/bronze/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=.../`.

---

## 6. Odtwarzalność Warstwy SQL (Synapse Serverless SQL)

Skrypty definicji bazy danych, widoków i walidacji jakości znajdują się w katalogach `sql/` oraz `synapse/sqlscript/`:

1. **Baza danych `job_market`:**
   Utworzona z kodowaniem `Latin1_General_100_BIN2_UTF8`, zapobiegającym błędom kodowania polskich znaków diakrytycznych.
2. **Aktywne widoki Gold v2 (`synapse/sqlscript/03_create_gold_v2_views.json`):**
   - **`dbo.vw_job_snapshots_v2`:** Mapuje tabelę faktów `gold/jobs/v2/fact_job_snapshots/snapshot_date=*/*.parquet`.
   - **`dbo.vw_job_skills_v2`:** Mapuje tabelę umiejętności `gold/jobs/v2/bridge_job_skills/snapshot_date=*/*.parquet`.
   - **`dbo.vw_job_locations_v2`:** Mapuje tabelę lokalizacji `gold/jobs/v2/bridge_job_locations/snapshot_date=*/*.parquet`.
   - **`dbo.vw_gold_v2_commits`:** Mapuje manifesty ETL `gold/jobs/v2/_manifest/snapshot_date=*/*.parquet`.
3. **Widok legacy Gold v1 (`sql/02_create_view_job_snapshots.sql`):**
   - **`dbo.vw_job_snapshots`:** Mapuje `gold/jobs/fact_job_snapshots/snapshot_date=*/*.parquet` na potrzeby archiwalne, rollbacku i walidacji legacy. Aktualny model Power BI nie korzysta z tego widoku.

> [!IMPORTANT]
> **Mechanizm weryfikacji kompletności w SQL:**
> Silnik Synapse Serverless SQL mapuje pliki Parquet za pomocą funkcji `OPENROWSET` i **nie sprawdza samodzielnie fizycznego pliku `_SUCCESS`**. Spójność i status zatwierdzenia danych są kontrolowane na poziomie zapytań poprzez łączenie lub filtrowanie po `snapshot_date` i `run_id` z widoku manifestu `dbo.vw_gold_v2_commits`.

---

## 7. Monitoring Operacyjny i Procedury Weryfikacji (Troubleshooting)

### Kluczowa zasada monitoringu: Asynchroniczność ADF
W procesie dziennym wywołanie ADF w skrypcie `src/trigger_adf.py` odbywa się metodą `create_run` w trybie asynchronicznym (fire-and-forget). Zwraca ona identyfikator `run_id` w momencie przyjęcia zlecenia przez ADF.
- **Zielony status w GitHub Actions oznacza jedynie pomyślne pobranie danych do Bronze, upload i skuteczne zainicjowanie potoku ADF.**
- **Sukces GitHub Actions nie gwarantuje sukcesu ADF.** Rzeczywiste przetwarzanie w notatnikach Spark trwa od kilku do kilkunastu minut.
- Pełny monitoring wymaga weryfikacji statusu w zakładce **Monitor** w usłudze Azure Data Factory lub konfiguracji alertów Azure Monitor (szczegóły w [docs/operational_monitoring.md](docs/operational_monitoring.md)).

### Scenariusz A: Niepowodzenie ekstrakcji w GitHub Actions
1. Otwórz podsumowanie wykonania workflow w zakładce **Actions**.
2. Jeśli krok `Extract SOLID.Jobs offers` zakończył się błędem, sprawdź komunikat błędu wypisany w sekcji `PODSUMOWANIE` manifestu.
3. Krok `Upload to Azure Bronze` nie zostanie wykonany, dzięki czemu do jeziora danych nie trafią dane niekompletne.

### Scenariusz B: Weryfikacja integralności plików w ADLS Gen2
Skrypt `src/upload_to_adls.py` realizuje bezpieczny upload:
- Porównuje rozmiar pliku lokalnego z obiektem w Azure.
- Weryfikuje sumy kontrolne SHA-256.
- W przypadku wykrycia przerwanego transferu automatycznie ponawia upload.

### Scenariusz C: Diagnostyka niepowodzenia ADF / Synapse Spark
1. Przejdź do Azure Portal -> Azure Data Factory (`adf-job-market-dev`) -> **Monitor** -> **Pipeline runs**.
2. Wyszukaj uruchomienie po przekazanym parametrze `run_id` lub `snapshot`.
3. Sprawdź szczegóły wykonania aktywności `Bronze_to_silver` oraz `SIlver_to_gold`. W razie błędu Spark przeanalizuj logi Livy w Synapse Studio.

---

## 8. Wyniki Walidacji Wdrożeniowej (Rollout Audit)

> [!NOTE]
> Poniższe zestawienie przedstawia **historyczne wyniki walidacji wdrożeniowej Gold v2** zarejestrowane podczas testów i wdrożenia produkcyjnego w dniach 26–29 października 2026 r. Dane te mają charakter audytowy i **nie są aktualizowane automatycznie w tym dokumencie**.

| Data snapshotu (`snapshot_date`) | Oferty pracy (`fact_job_snapshots`) | Umiejętności (`bridge_job_skills`) | Lokalizacje (`bridge_job_locations`) | Status spójności |
|:---:|:---:|:---:|:---:|:---:|
| **2026-10-26** | 1 538 | 10 221 | 1 538 | Zgodny (OK) |
| **2026-10-27** | 1 520 | 10 042 | 1 520 | Zgodny (OK) |
| **2026-10-28** | 1 525 | 10 085 | 1 525 | Zgodny (OK) |
| **2026-10-29** | 1 537 | 10 152 | 1 537 | Zgodny (OK) |

Wszystkie powyższe snapshoty pomyślnie przeszły testy braku duplikatów, zachowania kardynalności tablic oraz weryfikację spójności relacyjnej klucza `job_snapshot_key`.

### Aktualne dashboardy Power BI

Poniższe zrzuty przedstawiają aktualne dashboardy **Market Overview** i **Career Explorer**. Wersjonowany w repozytorium model semantyczny PBIP/TMDL importuje aktywne widoki Synapse SQL warstwy Gold v2.

![Power BI Market Overview dashboard](readme%20pic/Power_bi_1.png)

*Market Overview — hiring activity, in-demand skills, remote opportunities, and market movement.*

![Power BI Career Explorer dashboard](readme%20pic/Power_bi_2.png)

*Career Explorer — role filters, required skills, salary benchmarks, locations, and matching job offers.*

---

## 9. Status Wdrożenia a Planowany Rozwój (Roadmap)

### Zrealizowano w bieżącej wersji (Gold v2):
- [x] Stabilny ekstraktor API z deterministycznym kodem wyjścia i utrwalaniem manifestu Bronze.
- [x] Zautomatyzowany potok kontroli jakości CI w GitHub Actions (`.github/workflows/ci.yml`).
- [x] Bezhasłowe uwierzytelnianie Azure OIDC (`.github/workflows/daily_extract.yml`).
- [x] Warstwa Silver w Parquet z deduplikacją i wskaźnikami jakości (`nb_bronze_to_silver_jobs`).
- [x] Aktywna warstwa **Gold v2** (`nb_silver_to_gold_v2_jobs`):
  - Model gwiazdy z rozdzieleniem faktów (`fact_job_snapshots`) i relacji (`bridge_job_skills`, `bridge_job_locations`),
  - Obsługa umiejętności technicznych i miękkich,
  - Manifest ETL (`_manifest`) i kontrola kompletności zapisu ze znacznikami `_SUCCESS`,
  - Pełna ochrona przed nadpisywaniem częściowo przetworzonych danych.
- [x] Bezpieczne zachowanie warstwy **Gold v1** jako legacy (w tym widoku `dbo.vw_job_snapshots` i notatnika `nb_silver_to_gold_jobs`).
- [x] Widoki Synapse Serverless SQL dla Gold v2 (`dbo.vw_job_snapshots_v2`, `dbo.vw_job_skills_v2`, `dbo.vw_job_locations_v2`, `dbo.vw_gold_v2_commits`).
- [x] Uprawnienia odczytu dla konta analitycznego `powerbi-reader`.
- [x] Migracja Power BI do Gold v2:
  - Model semantyczny PBIP/TMDL importujący trzy aktywne widoki biznesowe Gold v2,
  - Relacje między faktami, umiejętnościami i lokalizacjami oparte na `job_snapshot_key`,
  - Strony dashboardu **Market Overview** i **Career Explorer**.

### Planowane w kolejnych etapach:
- [ ] Wdrożenie reguł automatycznych alertów w Azure Monitor / Logic Apps dla niepowodzeń pipeline'u ADF.
- [ ] Cykliczna polityka retencji danych historycznych w kontenerze Bronze (ADLS Lifecycle Management).
