# Polish Job Market Intelligence — Azure Data Lakehouse ETL & Analytics

[![CI Quality Checks](https://github.com/maciejkubek-maker/job-market-intelligence/actions/workflows/ci.yml/badge.svg)](https://github.com/maciejkubek-maker/job-market-intelligence/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Code Style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v0.json)](https://github.com/astral-sh/ruff)

Kompleksowy projekt inżynierii danych realizujący w pełni zautomatyzowany, idempotentny proces ETL/ELT monitorujący polski rynek pracy w obszarze danych i automatyzacji (**Data Analyst**, **BI Analyst**, **Data Engineer**, **Process Automation**).

Projekt implementuje architekturę **Medallion Lakehouse** w chmurze **Microsoft Azure** (Bronze, Silver, Gold v2), bezpieczną orkiestrację bez haseł przez **GitHub Actions OIDC** i **Azure Data Factory**, przetwarzanie rozproszone w **Azure Synapse Spark** oraz warstwę analityczną w **Azure Synapse Serverless SQL**.

---

## 1. Zrealizowana Architektura Rozwiązania (Azure Lakehouse)

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
          ├── Raport Power BI v1 (aktywny na widoku legacy dbo.vw_job_snapshots)
          └── Raport Power BI v2 (faza przyszła — migracja do modelu gwiazdy Gold v2)
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
- **Raportowanie i BI:** Istniejący raport Power BI v1 bazujący na Gold v1; migracja raportu do modelu gwiazdy opartego na Gold v2 zaplanowana jest jako przyszły etap.

---

## 2. Kontrakty Danych i Odpowiedzialność Warstw

| Warstwa | Technologia / Format | Struktura ścieżki | Odpowiedzialność i reguły biznesowe |
|---|---|---|---|
| **Bronze** | Surowy JSON + `manifest.json` | `bronze/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<timestamp_utc>/` | Zapis 1:1 odpowiedzi źródłowych API w podziale na strony. Każda próba otrzymuje unikalny znacznik czasu `run_id`, eliminując ryzyko nadpisania danych historycznych. Status `completed` nadawany jest wyłącznie po pełnej weryfikacji kompletu stron i unikalności ofert. |
| **Silver** | Apache Parquet (Snappy) | `silver/jobs/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<run_id>/` | Typowanie, deduplikacja ogłoszeń w obrębie danego snapshotu, normalizacja widełek wynagrodzeń (rozbicie na `salary_from`, `salary_to`, `currency`), spłaszczenie tablic technologii oraz flagi jakości danych. Pomyślny zapis wieńczy znacznik `_SUCCESS`. |
| **Gold v2 (aktywna)** | Apache Parquet (Snappy) | `gold/jobs/v2/` (trzy tabele fizyczne + manifest) | Znormalizowany model analityczny w architekturze gwiazdy z tabelami pomostowymi. Wszystkie zbiory partycjonowane są według `snapshot_date=YYYY-MM-DD`. Idempotentny zapis z weryfikacją `_SUCCESS` i manifestem ETL. |
| **Gold v1 (legacy)** | Apache Parquet (Snappy) | `gold/jobs/fact_job_snapshots/snapshot_date=YYYY-MM-DD/` | Płaski zbiór faktów z pierwszej wersji wdrożenia. Zachowany w celach archiwalnych oraz dla ciągłości działania raportu Power BI v1. |

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
   - **`dbo.vw_job_snapshots`:** Mapuje `gold/jobs/fact_job_snapshots/snapshot_date=*/*.parquet` i zasila raport Power BI v1.

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

### Planowane w kolejnych etapach:
- [ ] **Migracja Power BI do Gold v2:**
  - Budowa modelu gwiazdy w Power BI Desktop opartego o nowe widoki v2,
  - Zaawansowane analizy macierzy umiejętności twardych i miękkich skorelowane z widełkami płacowymi,
  - Analiza wielowymiarowa lokalizacji i elastyczności pracy (Remote vs Hybrid).
- [ ] Wdrożenie reguł automatycznych alertów w Azure Monitor / Logic Apps dla niepowodzeń pipeline'u ADF.
- [ ] Cykliczna polityka retencji danych historycznych w kontenerze Bronze (ADLS Lifecycle Management).
