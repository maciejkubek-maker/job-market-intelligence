# Polish Job Market Intelligence — Azure Data Lakehouse ETL & Analytics

[![CI Quality Checks](https://github.com/maciejkubek-maker/job-market-intelligence/actions/workflows/ci.yml/badge.svg)](https://github.com/maciejkubek-maker/job-market-intelligence/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Code Style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v0.json)](https://github.com/astral-sh/ruff)

Kompleksowy projekt inżynierii danych realizujący w pełni zautomatyzowany, idempotentny proces ETL/ELT monitorujący polski rynek pracy w obszarze danych i automatyzacji (**Data Analyst**, **BI Analyst**, **Data Engineer**, **Process Automation**).

Projekt implementuje architekturę **Medallion Lakehouse** w chmurze **Microsoft Azure** (Bronze, Silver, Gold), bezpieczną orkiestrację bez haseł przez **GitHub Actions OIDC** i **Azure Data Factory**, przetwarzanie rozproszone w **Azure Synapse Spark** oraz warstwę analityczną w **Azure Synapse Serverless SQL**.

---

## 1. Zrealizowana Architektura Rozwiązania (Azure Lakehouse)

Architektura opiera się na separacji warstw składowania, przetwarzania rozproszonego oraz prezentacji analitycznej:

```
[ SOLID.Jobs Public API ]
          │  HTTP GET (paginacja, walidacja schematu, obsługa błędów)
          ▼
[ GitHub Actions Runner ]  ──(OIDC - bez haseł i sekretów)──► [ Azure Entra ID ]
          │
          ▼  Zapis surowych stron JSON + manifest.json
[ ADLS Gen2: Bronze ] (source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=.../)
          │
          ▼  Wyzwalanie asynchroniczne
[ Azure Data Factory ] (Pipeline: pl_job_market_daily_etl)
          │
          ├──► [ Synapse Spark: nb_bronze_to_silver_jobs ]
          │          │
          │          ▼ Zapis oczyszczonych Parquet + znacznik _SUCCESS
          │    [ ADLS Gen2: Silver ] (jobs/source=solidjobs/...)
          │
          └──► [ Synapse Spark: nb_silver_to_gold_jobs ] (on Silver success)
                     │
                     ▼ Zapis zagregowanych Parquet (reguła pierwszego snapshotu)
               [ ADLS Gen2: Gold ] (jobs/snapshot_date=YYYY-MM-DD/...)
                     │
                     ▼ Mapowanie przez OPENROWSET
[ Synapse Serverless SQL ] (Baza: job_market | Widok: dbo.vw_job_snapshots)
                     │
                     ▼ (Faza Przyszła / Planowany rozwój)
           [ Power BI Dashboard ]
```

### Komponenty systemu:
- **Źródło danych:** Publiczne API [SOLID.Jobs](https://solid.jobs/api-ofert-pracy) dla ofert IT w Polsce.
- **Ekstrakcja i walidacja:** Python 3.11 (`src/extract_solidjobs.py`), zarządzany przez narzędzie `uv`.
- **Orkiestracja dzienna:** GitHub Actions (`.github/workflows/daily_extract.yml`) z harmonogramem o 6:00 rano czasu warszawskiego (`Europe/Warsaw`).
- **Składowanie danych:** Azure Data Lake Storage Gen2 (`stjobmarketdev01`) z trzema kontenerami architektury Medallion (`bronze`, `silver`, `gold`).
- **Przetwarzanie rozproszone:** Azure Data Factory (`adf-job-market-dev`) orkiestrujące notatniki Apache Spark na klastrze Synapse (`sparkjobmarket`).
- **Warstwa analityczna:** Synapse Serverless SQL Pool (baza `job_market`, widok `dbo.vw_job_snapshots`).
- **Faza przyszła:** Semantyczny model analityczny i raporty Power BI.

---

## 2. Kontrakty Danych i Odpowiedzialność Warstw

| Warstwa | Technologia / Format | Struktura ścieżki | Odpowiedzialność i reguły biznesowe |
|---|---|---|---|
| **Bronze** | Surowy JSON + `manifest.json` | `bronze/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<timestamp_utc>/` | Zapis 1:1 odpowiedzi źródłowych API w podziale na strony. Każda próba otrzymuje unikalny znacznik czasu `run_id`, eliminując ryzyko nadpisania danych historycznych. Status `completed` nadawany jest wyłącznie po pełnej weryfikacji kompletu stron i unikalności ofert. |
| **Silver** | Apache Parquet (Snappy) | `silver/jobs/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<run_id>/` | Typowanie, deduplikacja ogłoszeń w obrębie danego snapshotu, normalizacja widełek wynagrodzeń (rozbicie na `salary_from`, `salary_to`, `currency`), spłaszczenie tablic technologii oraz flagi jakości danych. |
| **Gold** | Apache Parquet (Snappy) | `gold/jobs/snapshot_date=YYYY-MM-DD/` | Zbiór analityczny zoptymalizowany pod zapytania SQL i dashboardy. **Zasada idempotentności Gold:** warstwa Gold zachowuje **pierwszy skuteczny snapshot z danego dnia**. Kolejne uruchomienia tego samego dnia nie zniekształcają wskaźników trendów ani nie dublują danych. |

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
W przypadku błędu (np. HTTP 500, timeout sieci, brakujące strony, zmiana liczby stron w trakcie pobierania lub duplikaty identyfikatorów ofert) manifest jest **zawsze utrwalany na dysku** ze statusem `failed` lub `incomplete` oraz dokładną treścią błędu w polu `error`, a proces zwraca niezerowy kod wyjścia (kod `1`), zatrzymując workflow przed wysłaniem niekompletnych danych do chmury.

---

## 3. Idempotentność i Gwarancje Przetwarzania

1. **Izolacja wykonań w Bronze:** Unikalny parametr `run_id` z dokładnością do mikrosekund w strefie UTC gwarantuje, że ponowne uruchomienia tego samego dnia zapisywane są w odrębnych katalogach.
2. **Strażnik `_SUCCESS` w Silver:** Po poprawnym przetworzeniu i zapisaniu danych w warstwie Silver, silnik Apache Spark tworzy plik znacznika `_SUCCESS`. Skrypt `src/upload_to_adls.py` weryfikuje istnienie `_SUCCESS` w Azure przed zleceniem zadania do ADF — jeśli snapshot został już przetworzony, wywołanie pipeline'u Spark jest bezpiecznie pomijane.
3. **Determinizm w Gold:** Warstwa Gold gwarantuje, że historia zachowuje dokładnie jeden reprezentatywny zbiór danych dla każdej doby obserwacji (pierwszy pełny snapshot).

---

## 4. Bezpieczeństwo i Architektura Bezhasłowa (OIDC)

W projekcie wyeliminowano konieczność przechowywania jakichkolwiek haseł, tokenów czy statycznych kluczy jednostek usługi (Client Secrets) w repozytorium GitHub:
- **Federacja tożsamości Microsoft Entra ID:** GitHub Actions negocjuje z Azure token OpenID Connect (OIDC).
- **Zasada najmniejszych uprawnień (Least Privilege):**
  - Workflow ciągłej integracji (`.github/workflows/ci.yml`) posiada wyłącznie uprawnienie `contents: read` i nie ma dostępu do chmury ani sekretów.
  - Workflow ekstrakcyjny (`.github/workflows/daily_extract.yml`) posiada uprawnienie `id-token: write` ściśle zawężone do gałęzi `main`.
  - Role RBAC w Azure ograniczają uprawnienia jednostki wyłącznie do zapisu w kontenerze `bronze` oraz uruchamiania wyznaczonego pipeline'u ADF.

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

Skrypty definicji bazy danych, widoków i walidacji jakości zostały odseparowane i umieszczone w katalogu `sql/`:

1. **`sql/01_create_database.sql`:**
   Tworzy logiczną bazę danych `job_market` z kodowaniem `Latin1_General_100_BIN2_UTF8`, zapobiegającym błędom kodowania polskich znaków.
2. **`sql/02_create_view_job_snapshots.sql`:**
   Tworzy widok `dbo.vw_job_snapshots` mapujący pliki Parquet warstwy Gold z kontenera ADLS Gen2 za pośrednictwem funkcji `OPENROWSET`.
3. **`sql/03_validate_snapshots.sql`:**
   Zestaw zapytań analitycznych weryfikujących:
   - unikalność identyfikatorów ogłoszeń w obrębie każdego dnia,
   - ciągłość snapshotów dzień-do-dnia,
   - rozkład ról zawodowych oraz udział ogłoszeń z widełkami płacowymi,
   - statystyki wynagrodzeń w podziale na formę umowy (B2B vs UoP).

---

## 7. Procedury Weryfikacji i Rozwiązywania Problemów (Troubleshooting)

### Scenariusz A: Niepowodzenie ekstrakcji w GitHub Actions
1. Otwórz podsumowanie wykonania workflow w zakładce **Actions**.
2. Jeśli krok `Extract SOLID.Jobs offers` zakończył się błędem, sprawdź komunikat błędu wypisany w sekcji `PODSUMOWANIE` manifestu.
3. Krok `Upload to Azure Bronze` nie zostanie wykonany, dzięki czemu do jeziora danych nie trafią dane niekompletne.

### Scenariusz B: Weryfikacja integralności plików w ADLS Gen2
Skrypt `src/upload_to_adls.py` realizuje bezpieczny upload:
- Porównuje rozmiar pliku lokalnego z obiektem w Azure.
- Weryfikuje sumy kontrolne SHA-256.
- W przypadku wykrycia przerwanego lub uszkodzonego transferu, automatycznie naprawia plik w chmurze (`upload_data(overwrite=True)`).

### Scenariusz C: Przegląd statusu wykonania w ADF
W przypadku pomyślnego wyzwolenia zadania przez `src/trigger_adf.py`, w konsoli wypisywany jest identyfikator `Pipeline Run ID`. Szczegółowy monitoring i zalecenia dotyczące eliminacji luki asynchronicznej orkiestracji opisano w dokumencie [docs/operational_monitoring.md](docs/operational_monitoring.md).

---

## 8. Status Wdrożenia a Planowany Rozwój (Roadmap)

### Zrealizowano w bieżącej wersji (v1.0):
- [x] Stabilny ekstraktor API z deterministycznym kodem wyjścia i utrwalaniem manifestu.
- [x] Kompletny zestaw testów jednostkowych w `pytest` z pełnym mockowaniem usług zewnętrznych.
- [x] Zautomatyzowany potok kontroli jakości CI w GitHub Actions (`.github/workflows/ci.yml`).
- [x] Architektura Medallion w ADLS Gen2 (Bronze JSON, Silver Parquet, Gold Parquet).
- [x] Idempotentna orkiestracja w ADF ze strażnikami `_SUCCESS` i regułą pierwszego dziennego snapshotu.
- [x] Odtwarzalna definicja warstwy analitycznej Synapse Serverless SQL (`sql/`).
- [x] Bezhasłowe uwierzytelnianie Azure OIDC.
- [x] Rejestr decyzji architektonicznych (ADR) oraz plan monitoringu operacyjnego.

### Planowane w kolejnych etapach:
- [ ] Zbudowanie modelu semantycznego (Star Schema) w Power BI Desktop.
- [ ] Raport analityczny w Power BI prezentujący:
  - Dynamikę pojawiania się i znikania ofert (wskaźniki churnu ogłoszeń).
  - Macierz technologii skorelowaną z widełkami wynagrodzeń.
  - Rozkład geograficzny i elastyczność pracy (Remote vs Hybrid).
- [ ] Konfiguracja automatycznych alertów Azure Monitor dla pipeline'ów ADF.
