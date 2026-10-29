# Architektura Systemu â€” Polish Job Market Intelligence

Projekt realizuje zautomatyzowany, skalowalny i idempotentny proces gromadzenia oraz analizy danych polskiego rynku pracy IT w architekturze nowoczesnego jeziora danych (Modern Data Lakehouse â€” Medallion Architecture) w chmurze Microsoft Azure.

Dokument opisuje architekturÄ™ systemu po wdroĹĽeniu produkcyjnym **Gold v2**, z zachowaniem warstwy **Gold v1** w trybie legacy.

---

## 1. Diagram Architektury PrzepĹ‚ywu Danych

PoniĹĽszy diagram ilustruje kompletny przepĹ‚yw danych: od pobrania z zewnÄ™trznego API do warstwy raportowania analitycznego.

```mermaid
flowchart TD
    API["SOLID.Jobs Public REST API<br/>(Oferty pracy: Data & Automation)"]

    subgraph GHA["Orkiestracja i Ekstrakcja (GitHub Actions)"]
        Cron["Harmonogram CRON: 40 4 * * * (UTC)<br/>(05:40 zima / 06:40 lato PL - brak pola timezone)"]
        OIDC["Azure Login (OIDC / Entra ID)<br/>BezhasĹ‚owa federacja toĹĽsamoĹ›ci"]
        Ext["Python Extractor (uv, requests)<br/>Walidacja stronicowania i unikalnoĹ›ci"]
        UpBronze["Upload do ADLS Gen2 Bronze<br/>upload_to_adls.py --bronze-only"]
        TrigADF["Trigger ADF Pipeline (Asynchroniczny)<br/>trigger_adf.py --execute"]
        Cron --> Ext
        OIDC --> UpBronze
        OIDC --> TrigADF
        Ext --> UpBronze
        UpBronze --> TrigADF
    end

    API -->|HTTP GET / JSON| Ext

    subgraph ADLS["Azure Data Lake Storage Gen2 (stjobmarketdev01)"]
        subgraph BronzeLayer["Warstwa Bronze"]
            BronzeData["bronze/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=.../<br/>â€˘ offers_page_XXXX.json<br/>â€˘ manifest.json (status: completed / failed)"]
        end

        subgraph SilverLayer["Warstwa Silver"]
            SilverData["silver/jobs/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=.../<br/>â€˘ Pliki Parquet (Snappy)<br/>â€˘ Znacznik _SUCCESS"]
        end

        subgraph GoldV2Layer["Warstwa Gold v2 (Aktywna)"]
            GoldFact["gold/jobs/v2/fact_job_snapshots/snapshot_date=.../"]
            GoldSkills["gold/jobs/v2/bridge_job_skills/snapshot_date=.../"]
            GoldLoc["gold/jobs/v2/bridge_job_locations/snapshot_date=.../"]
            GoldManifest["gold/jobs/v2/_manifest/snapshot_date=.../ (_SUCCESS)"]
        end

        subgraph GoldV1Layer["Warstwa Gold v1 (Legacy)"]
            GoldV1["gold/jobs/fact_job_snapshots/snapshot_date=.../"]
        end
    end

    UpBronze -->|Zapis JSON + manifest| BronzeData

    subgraph ADF["Azure Data Factory (adf-job-market-dev)"]
        ADFPipeline["Pipeline: pl_job_market_daily_etl<br/>(Uruchomienie asynchroniczne fire-and-forget)"]
        ActSilver["Activity 1: Bronze_to_silver<br/>(Synapse Notebook: nb_bronze_to_silver_jobs)"]
        ActGold["Activity 2: SIlver_to_gold (on success)<br/>(Synapse Notebook: nb_silver_to_gold_v2_jobs)"]
        ADFPipeline --> ActSilver
        ActSilver -->|Succeeded| ActGold
    end

    TrigADF -.->|Wywolanie asynchroniczne create_run| ADFPipeline
    BronzeData -->|Odczyt surowego JSON| ActSilver
    ActSilver -->|Zapis Parquet + _SUCCESS| SilverData
    SilverData -->|Odczyt zwalidowanego snapshotu| ActGold
    ActGold -->|Zapis 3 tabel + manifest ETL| GoldFact
    ActGold --> GoldSkills
    ActGold --> GoldLoc
    ActGold --> GoldManifest

    subgraph SQL["Azure Synapse Analytics (Serverless SQL Pool Built-in)"]
        DB["Baza danych: job_market (Latin1_General_100_BIN2_UTF8)"]
        VwFact["dbo.vw_job_snapshots_v2"]
        VwSkills["dbo.vw_job_skills_v2"]
        VwLoc["dbo.vw_job_locations_v2"]
        VwManifest["dbo.vw_gold_v2_commits (Manifest ETL)"]
        VwV1["dbo.vw_job_snapshots (Legacy Gold v1)"]
        DB --- VwFact
        DB --- VwSkills
        DB --- VwLoc
        DB --- VwManifest
        DB --- VwV1
    end

    GoldFact -->|OPENROWSET Parquet| VwFact
    GoldSkills -->|OPENROWSET Parquet| VwSkills
    GoldLoc -->|OPENROWSET Parquet| VwLoc
    GoldManifest -->|OPENROWSET Parquet| VwManifest
    GoldV1 -->|OPENROWSET Parquet| VwV1

    subgraph PowerBI["Warstwa Raportowania (Power BI)"]
        PBI_v1["Raport Power BI v1 (Aktywny na widoku legacy)"]
        PBI_v2["Raport Power BI v2 (Planowana migracja do modelu gwiazdy Gold v2)"]
    end

    VwV1 -->|ODBC / Entra ID| PBI_v1
    VwFact -.->|Planowane polaczenie v2| PBI_v2
    VwSkills -.->|Planowane polaczenie v2| PBI_v2
    VwLoc -.->|Planowane polaczenie v2| PBI_v2
```

---

## 2. Kontrakty Danych i OdpowiedzialnoĹ›Ä‡ Warstw

### 2.1. Warstwa Bronze (Surowy ZbiĂłr Danych)
- **Format:** Nieskompresowane pliki JSON podzielone na strony API (`offers_page_0000.json`, `offers_page_0001.json`, ...) oraz plik kontrolny `manifest.json`.
- **ĹšcieĹĽka ADLS Gen2:** `bronze/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<timestamp_utc>/`.
- **ReguĹ‚a nienaruszalnoĹ›ci (Audit Trail):** Dane surowe sÄ… niemodyfikowalne. KaĹĽda prĂłba ekstrakcji otrzymuje unikalny znacznik czasu `run_id` w standardzie UTC z dokĹ‚adnoĹ›ciÄ… do mikrosekund, co wyklucza ryzyko przypadkowego nadpisania danych historycznych.
- **Kontrakt manifestu Bronze:**
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
- **Kryteria walidacji:** Status `completed` nadawany jest wyĹ‚Ä…cznie wtedy, gdy liczba pobranych stron i ofert odpowiada nagĹ‚Ăłwkom API, a liczba unikalnych identyfikatorĂłw ofert jest rĂłwna liczbie pobranych rekordĂłw. W przeciwnym razie manifest zapisuje status `incomplete` lub `failed` wraz z opisem bĹ‚Ä™du, a proces koĹ„czy siÄ™ kodem bĹ‚Ä™du `1`.

---

### 2.2. Warstwa Silver (Oczyszczony ZbiĂłr Ustrukturyzowany)
- **Format:** Apache Parquet z kompresjÄ… Snappy.
- **ĹšcieĹĽka ADLS Gen2:** `silver/jobs/source=solidjobs/snapshot_date=YYYY-MM-DD/run_id=<run_id>/`.
- **Silnik przetwarzania:** Apache Spark w Azure Synapse Analytics (`nb_bronze_to_silver_jobs`) na puli `sparkjobmarket`.
- **Transformacje i reguĹ‚y jakoĹ›ciowe:**
  - Rozpakowanie zagnieĹĽdĹĽonych struktur JSON ze stron surowych.
  - SpĹ‚aszczenie i typowanie atrybutĂłw: identyfikatory, daty waĹĽnoĹ›ci i publikacji, wymiar czasu umowy.
  - Normalizacja wideĹ‚ek pĹ‚acowych (`salary.from`, `salary.to`, `salary.currency`, `salary.period`, `salary.employmentType`).
  - Znakowanie flag jakoĹ›ci danych (`quality_flags`) i deduplikacja ogĹ‚oszeĹ„ w obrÄ™bie danego snapshotu.
  - ZwieĹ„czenie pomyĹ›lnego zapisu partycji znacznikiem `_SUCCESS`.

---

### 2.3. Warstwa Gold v2 (Aktywny Model Analityczny Gwiazdy)
Warstwa Gold v2 zostaĹ‚a zaimplementowana w notatniku `nb_silver_to_gold_v2_jobs` jako model gwiazdy z rozbiciem tablic na tabele pomostowe (bridge tables).

- **ĹšcieĹĽka bazowa:** `gold/jobs/v2/`.
- **Partycjonowanie:** Wszystkie zbiory sÄ… partycjonowane Ĺ›ciĹ›le wedĹ‚ug **`snapshot_date=YYYY-MM-DD`**.
- **Fizyczne tabele biznesowe i manifest:**

```
gold/jobs/v2/
â”śâ”€â”€ fact_job_snapshots/
â”‚   â””â”€â”€ snapshot_date=YYYY-MM-DD/
â”‚       â”śâ”€â”€ part-*.parquet
â”‚       â””â”€â”€ _SUCCESS
â”śâ”€â”€ bridge_job_skills/
â”‚   â””â”€â”€ snapshot_date=YYYY-MM-DD/
â”‚       â”śâ”€â”€ part-*.parquet
â”‚       â””â”€â”€ _SUCCESS
â”śâ”€â”€ bridge_job_locations/
â”‚   â””â”€â”€ snapshot_date=YYYY-MM-DD/
â”‚       â”śâ”€â”€ part-*.parquet
â”‚       â””â”€â”€ _SUCCESS
â””â”€â”€ _manifest/
    â””â”€â”€ snapshot_date=YYYY-MM-DD/
        â”śâ”€â”€ part-*.parquet
        â””â”€â”€ _SUCCESS
```

#### Tabela 1: `fact_job_snapshots`
GĹ‚Ăłwna tabela faktĂłw przechowujÄ…ca pojedyncze ogĹ‚oszenia o pracÄ™ w ujÄ™ciu danego snapshotu dziennego.
- `job_snapshot_key` (STRING, klucz gĹ‚Ăłwny migawki)
- `source` (STRING, np. `solidjobs`)
- `job_id` (STRING, identyfikator ogĹ‚oszenia ze ĹşrĂłdĹ‚a `jobOfferKey`)
- `run_id` (STRING, identyfikator wykonania)
- `title` (STRING, tytuĹ‚ stanowiska)
- `division` (STRING, dziaĹ‚)
- `category` (STRING, kategoria gĹ‚Ăłwna)
- `sub_category` (STRING, podkategoria)
- `company` (STRING, nazwa firmy)
- `company_logo_url` (STRING, URL do logotypu)
- `experience_level` (STRING, poziom doĹ›wiadczenia)
- `salary_from` (DOUBLE / DECIMAL, dolna granica wynagrodzenia)
- `salary_to` (DOUBLE / DECIMAL, gĂłrna granica wynagrodzenia)
- `salary_currency` (STRING, waluta)
- `salary_period` (STRING, okres rozliczeniowy)
- `employment_type` (STRING, forma wspĂłĹ‚pracy, np. B2B, UoP)
- `is_remote` (BOOLEAN, praca w peĹ‚ni zdalna)
- `is_hybrid` (BOOLEAN, praca hybrydowa)
- `contract_time` (STRING, wymiar etatu)
- `valid_from` (STRING / TIMESTAMP)
- `valid_to` (STRING / TIMESTAMP)
- `updated_at` (STRING / TIMESTAMP)
- `url` (STRING, link do oferty)
- `languages_json` (STRING, serializowana tablica jÄ™zykĂłw)
- `benefits_json` (STRING, serializowana tablica benefitĂłw)

#### Tabela 2: `bridge_job_skills`
Tabela pomostowa mapujÄ…ca umiejÄ™tnoĹ›ci techniczne oraz kompetencje miÄ™kkie powiÄ…zane z ofertÄ….
- `job_snapshot_key` (STRING, klucz obcy do tabeli faktĂłw)
- `job_id` (STRING, identyfikator ogĹ‚oszenia)
- `run_id` (STRING, identyfikator wykonania)
- `skill_index` (INTEGER, indeks pozycji umiejÄ™tnoĹ›ci w ogĹ‚oszeniu uzyskany przez `posexplode_outer`)
- `skill_name` (STRING, nazwa technologii lub kompetencji miÄ™kkiej)
- `skill_level` (STRING, wymagany poziom biegĹ‚oĹ›ci)

#### Tabela 3: `bridge_job_locations`
Tabela pomostowa normalizujÄ…ca lokalizacje przypisane do oferty pracy.
- `job_snapshot_key` (STRING, klucz obcy do tabeli faktĂłw)
- `job_id` (STRING, identyfikator ogĹ‚oszenia)
- `run_id` (STRING, identyfikator wykonania)
- `location_index` (INTEGER, indeks pozycji lokalizacji w ogĹ‚oszeniu uzyskany przez `posexplode_outer`)
- `location_name` (STRING, znormalizowana nazwa miasta / lokalizacji)

#### Tabela 4 (Manifest ETL): `_manifest`
Struktura techniczna stanowiÄ…ca jedyny ostateczny znacznik poprawnego zatwierdzenia snapshotu w Gold v2.
- `snapshot_date` (STRING, data snapshotu)
- `run_id` (STRING, zatwierdzony identyfikator wykonania)
- `job_count` (LONG, liczba ofert w tabeli faktĂłw)
- `skill_count` (LONG, liczba powiÄ…zaĹ„ w tabeli umiejÄ™tnoĹ›ci)
- `location_count` (LONG, liczba powiÄ…zaĹ„ w tabeli lokalizacji)

---

### 2.4. Relacje MiÄ™dzy Tabelami Gold v2

Relacje logiczne i fizyczne opierajÄ… siÄ™ na kluczu syntetycznym `job_snapshot_key`:

```
               +--------------------------------------+
               |          fact_job_snapshots          |
               +--------------------------------------+
               | PK  job_snapshot_key                 |
               |     source, job_id, run_id           |
               |     title, company, salary_from ...  |
               +--------------------------------------+
                                  â”‚
                 â”Śâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”´â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
            1:N  â”‚                            1:N  â”‚
                 â–Ľ                                 â–Ľ
+---------------------------------+  +-----------------------------------+
|        bridge_job_skills        |  |       bridge_job_locations        |
+---------------------------------+  +-----------------------------------+
| FK  job_snapshot_key            |  | FK  job_snapshot_key              |
|     job_id, run_id              |  |     job_id, run_id                |
| PK  skill_index                 |  | PK  location_index                |
|     skill_name, skill_level     |  |     location_name                 |
+---------------------------------+  +-----------------------------------+
```

- **Format klucza gĹ‚Ăłwnego:** `job_snapshot_key` = `solidjobs|{snapshot_date}|{jobOfferKey}`.
- **KardynalnoĹ›Ä‡:** Jedna oferta w tabeli faktĂłw posiada wiele rekordĂłw w `bridge_job_skills` oraz wiele w `bridge_job_locations`.
- **Klucz zĹ‚oĹĽony tabel pomostowych:** UnikalnoĹ›Ä‡ kaĹĽdego wiersza w tabelach pomostowych wyznaczajÄ… pary: `(job_snapshot_key, skill_index)` oraz `(job_snapshot_key, location_index)`.

---

### 2.5. Warstwa Gold v1 (Legacy)
- **ĹšcieĹĽka ADLS Gen2:** `gold/jobs/fact_job_snapshots/snapshot_date=YYYY-MM-DD/`.
- **Status:** Warstwa historyczna (legacy). Notatnik `nb_silver_to_gold_jobs` pozostaje w repozytorium jako referencyjny.
- **Rola biznesowa:** Zachowuje ciÄ…gĹ‚oĹ›Ä‡ dziaĹ‚ania istniejÄ…cego raportu Power BI v1 do czasu wdroĹĽenia raportu Power BI v2.

---

## 3. IdempotentnoĹ›Ä‡, Walidacja i Mechanizm Zapisu w Gold v2

Proces przetwarzania w notatniku `nb_silver_to_gold_v2_jobs` zostaĹ‚ zaprojektowany z rygorystycznymi gwarancjami spĂłjnoĹ›ci:

### 1. Detekcja stanu i idempotencja:
- Notatnik sprawdza obecnoĹ›Ä‡ znacznika `_SUCCESS` w katalogu `_manifest/snapshot_date=YYYY-MM-DD/` oraz we wszystkich trzech tabelach.
- JeĹĽeli manifest istnieje i wszystkie tabele posiadajÄ… `_SUCCESS`, data jest bezpiecznie **pomijana** (`skipped_dates`).

### 2. Ochrona przed nadpisywaniem czÄ™Ĺ›ciowych danych:
- JeĹĽeli katalog ktĂłrejkolwiek z trzech tabel lub katalog `_manifest` istnieje fizycznie w ADLS, ale brak jest poprawnego manifestu ze znacznikiem `_SUCCESS`, notatnik **przerywa dziaĹ‚anie z bĹ‚Ä™dem (`RuntimeError: czÄ™Ĺ›ciowy Gold v2 ..., brak manifestu. Nie nadpisujÄ™!`)**.
- Zapobiega to cichemu uszkodzeniu lub nadpisaniu danych w przypadku wczeĹ›niejszej awarii zadania Spark. Wymagana jest manualna weryfikacja.

### 3. Weryfikacja jakoĹ›ci przed zapisem (Pre-write Quality Gates):
Przed fizycznym utrwaleniem na storage weryfikowane sÄ… warunki:
- `job_count > 0` oraz brak pustych/null wartoĹ›ci w `job_id`.
- CaĹ‚kowity brak duplikatĂłw `job_snapshot_key` w tabeli faktĂłw.
- ZgodnoĹ›Ä‡ parametru `run_id` we wszystkich wierszach z wybranym wykonaniem.
- IntegralnoĹ›Ä‡ eksplodowanych tablic: `expected_skills == skill_count` oraz `expected_locations == location_count`.
- Brak osieroconych rekordĂłw relacyjnych (`anti-join` miÄ™dzy dzieÄ‡mi a `fact_job_snapshots` musi zwrĂłciÄ‡ 0 wierszy).
- Brak brakujÄ…cych lub zduplikowanych indeksĂłw `skill_index` i `location_index`.

### 4. Kontrolowana sekwencja zapisu z manifestem zatwierdzenia:
1. Zapis tabeli `fact_job_snapshots` w trybie `.mode("errorifexists")` i sprawdzenie istnienia `_SUCCESS`.
2. Zapis tabeli `bridge_job_skills` w trybie `.mode("errorifexists")` i sprawdzenie istnienia `_SUCCESS`.
3. Zapis tabeli `bridge_job_locations` w trybie `.mode("errorifexists")` i sprawdzenie istnienia `_SUCCESS`.
4. Dopiero **PO** poprawnym zapisaniu i zweryfikowaniu wszystkich trzech tabel, generowany i zapisywany jest zbiĂłr `_manifest` w trybie `.mode("errorifexists")`.
5. Weryfikacja pliku `_manifest/_SUCCESS`.

---

## 4. Warstwa Prezentacji Analitycznej (Synapse Serverless SQL)

Do udostÄ™pniania danych analitykom i narzÄ™dziom BI bez ponoszenia kosztĂłw dedykowanych baz danych w stanie bezczynnoĹ›ci wykorzystywana jest pula **Synapse Serverless SQL (Built-in)**.

- **Baza danych:** `job_market`
- **Kodowanie:** `Latin1_General_100_BIN2_UTF8` (peĹ‚na obsĹ‚uga polskich znakĂłw diakrytycznych)
- **Skrypt wdroĹĽeniowy:** `synapse/sqlscript/03_create_gold_v2_views.json`

### Aktywne widoki Gold v2:
1. **`dbo.vw_job_snapshots_v2`:**
   ```sql
   CREATE OR ALTER VIEW dbo.vw_job_snapshots_v2 AS
   SELECT
       REPLACE(jobs.filepath(1), 'snapshot_date=', '') AS snapshot_date,
       jobs.*
   FROM OPENROWSET(
       BULK 'https://stjobmarketdev01.dfs.core.windows.net/gold/jobs/v2/fact_job_snapshots/snapshot_date=*/*.parquet',
       FORMAT = 'PARQUET'
   ) AS jobs;
   ```
2. **`dbo.vw_job_skills_v2`:**
   ```sql
   CREATE OR ALTER VIEW dbo.vw_job_skills_v2 AS
   SELECT
       REPLACE(skills.filepath(1), 'snapshot_date=', '') AS snapshot_date,
       skills.*
   FROM OPENROWSET(
       BULK 'https://stjobmarketdev01.dfs.core.windows.net/gold/jobs/v2/bridge_job_skills/snapshot_date=*/*.parquet',
       FORMAT = 'PARQUET'
   ) AS skills;
   ```
3. **`dbo.vw_job_locations_v2`:**
   ```sql
   CREATE OR ALTER VIEW dbo.vw_job_locations_v2 AS
   SELECT
       REPLACE(locations.filepath(1), 'snapshot_date=', '') AS snapshot_date,
       locations.*
   FROM OPENROWSET(
       BULK 'https://stjobmarketdev01.dfs.core.windows.net/gold/jobs/v2/bridge_job_locations/snapshot_date=*/*.parquet',
       FORMAT = 'PARQUET'
   ) AS locations;
   ```
4. **`dbo.vw_gold_v2_commits` (Manifest ETL):**
   ```sql
   CREATE OR ALTER VIEW dbo.vw_gold_v2_commits AS
   SELECT
       manifest.snapshot_date,
       manifest.run_id,
       manifest.job_count,
       manifest.skill_count,
       manifest.location_count
   FROM OPENROWSET(
       BULK 'https://stjobmarketdev01.dfs.core.windows.net/gold/jobs/v2/_manifest/snapshot_date=*/*.parquet',
       FORMAT = 'PARQUET'
   ) AS manifest;
   ```

### Widok legacy Gold v1:
- **`dbo.vw_job_snapshots`:** Zdefiniowany w `sql/02_create_view_job_snapshots.sql`, czyta Ĺ›cieĹĽkÄ™ `gold/jobs/fact_job_snapshots/` i zasila dotychczasowy raport Power BI v1.

> [!IMPORTANT]
> **Mechanizm weryfikacji kompletnoĹ›ci w Synapse Serverless SQL:**
> Silnik Serverless SQL czyta pliki Parquet za pomocÄ… `OPENROWSET` z wieloznacznikiem Ĺ›cieĹĽki (`snapshot_date=*/*.parquet`). Samodzielnie **nie sprawdza fizycznego pliku `_SUCCESS`**.
> Zapytania analityczne, procedury walidacyjne oraz zasilanie raportĂłw filtrujÄ… lub Ĺ‚Ä…czÄ… dane z widokiem manifestu `dbo.vw_gold_v2_commits`, co gwarantuje odczyt wyĹ‚Ä…cznie snapshotĂłw zatwierdzonych przez ETL.

---

## 5. Orkiestracja i Monitoring Operacyjny

### 5.1. RĂłĹĽnica miÄ™dzy uruchomieniem potoku a jego zakoĹ„czeniem (AsynchronicznoĹ›Ä‡ ADF)
Dzienny potok ETL jest wyzwalany automatycznie przez GitHub Actions (`.github/workflows/daily_extract.yml`):
- **Nazwa workflow:** `Job Market - Daily ETL (Azure)`.
- **Harmonogram:** `cron: "40 4 * * *"` w strefie UTC (brak parametru timezone w skĹ‚adni GitHub Actions; uruchomienie nastÄ™puje o 05:40 czasu warszawskiego zimÄ… oraz o 06:40 latem).
- **AsynchronicznoĹ›Ä‡ ADF:** Skrypt `src/trigger_adf.py` wywoĹ‚uje pipeline w Azure Data Factory za pomocÄ… metody `client.pipelines.create_run`. Jest to wywoĹ‚anie typu **fire-and-forget**. UsĹ‚uga ADF przyjmuje zlecenie, zwraca unikalny `Pipeline Run ID`, a skrypt w GitHub Actions koĹ„czy dziaĹ‚anie z kodem `0`.
- **Konsekwencja operacyjna:**
  - **Sukces workflow GitHub Actions (zielony status) NIE gwarantuje sukcesu pipeline'u ADF.** Potwierdza on wyĹ‚Ä…cznie poprawne pobranie ofert, upload surowego JSON do Bronze oraz skuteczne zlecenie wykonania w ADF.
  - Faktyczne zadania Apache Spark (`nb_bronze_to_silver_jobs` oraz `nb_silver_to_gold_v2_jobs`) wykonujÄ… siÄ™ na klastrze Synapse przez kolejne minuty.

### 5.2. Strategia monitoringu operacyjnego
Zgodnie z koncepcjÄ… opisanÄ… w [docs/operational_monitoring.md](docs/operational_monitoring.md):
1. **GitHub Actions Logs:** Weryfikacja krokĂłw ekstrakcji, liczby pobranych stron i ofert, sum kontrolnych uploadu do Bronze oraz identyfikatora uruchomienia ADF.
2. **Azure Data Factory Studio (zakĹ‚adka Monitor):** BieĹĽÄ…ce Ĺ›ledzenie statusĂłw aktywnoĹ›ci `Bronze_to_silver` oraz `SIlver_to_gold`.
3. **Azure Monitor Alerts:** Zalecana konfiguracja reguĹ‚ alertĂłw Azure Monitor opartych o tabelÄ™ `ADFPipelineRun` wysyĹ‚ajÄ…cych powiadomienia w razie statusu `Failed`, bez obciÄ…ĹĽania runnerĂłw CI/CD dĹ‚ugim czasem oczekiwania.

---

## 6. Wyniki Walidacji WdroĹĽeniowej (Rollout Audit)

> [!NOTE]
> PoniĹĽsza tabela zawiera **statyczne dane audytowe** z walidacji wdroĹĽeniowej architektury Gold v2, przeprowadzonej w dniach 26â€“29 paĹşdziernika 2026 r. Dane te majÄ… charakter referencyjnego audytu powdroĹĽeniowego i **nie sÄ… aktualizowane automatycznie**.

| Data snapshotu (`snapshot_date`) | Oferty pracy (`fact_job_snapshots`) | UmiejÄ™tnoĹ›ci (`bridge_job_skills`) | Lokalizacje (`bridge_job_locations`) | Status walidacji |
|:---:|:---:|:---:|:---:|:---:|
| **2026-10-26** | 1 538 | 10 221 | 1 538 | Zgodny (OK) |
| **2026-10-27** | 1 520 | 10 042 | 1 520 | Zgodny (OK) |
| **2026-10-28** | 1 525 | 10 085 | 1 525 | Zgodny (OK) |
| **2026-10-29** | 1 537 | 10 152 | 1 537 | Zgodny (OK) |

Wszystkie snapshoty zostaĹ‚y zweryfikowane pod kÄ…tem braku duplikatĂłw identyfikatorĂłw, zerowej liczby rekordĂłw osieroconych oraz spĂłjnoĹ›ci sum kontrolnych z manifestem ETL.

---

## 7. BezpieczeĹ„stwo i Architektura BezhasĹ‚owa (OIDC)

W Ĺ›rodowisku wdroĹĽono bezhasĹ‚owÄ… federacjÄ™ toĹĽsamoĹ›ci z Microsoft Entra ID:
- Workflow CI (`.github/workflows/ci.yml`) dziaĹ‚a w trybie tylko do odczytu kodu (`contents: read`) i nie posiada dostÄ™pu do ĹĽadnych zasobĂłw chmurowych.
- Workflow dziennego ETL (`.github/workflows/daily_extract.yml`) korzysta z tokenu OIDC (`id-token: write`) powiÄ…zanego z gaĹ‚Ä™ziÄ… `main`.
- Uprawnienia Azure RBAC toĹĽsamoĹ›ci ograniczone sÄ… Ĺ›ciĹ›le do kontenera `bronze` w storage account `stjobmarketdev01` oraz wyzwalania pipeline'u `pl_job_market_daily_etl`.
- DostÄ™p Power BI do bazy Synapse Serverless SQL realizowany jest poprzez dedykowanego uĹĽytkownika Entra ID `powerbi-reader@adamablergmail.onmicrosoft.com` z uprawnieniem `GRANT SELECT` wyĹ‚Ä…cznie do wymaganych widokĂłw.

SzczegĂłĹ‚owy opis decyzji architektonicznej znajduje siÄ™ w dokumencie [docs/adr/0002-passwordless-github-oidc-security.md](adr/0002-passwordless-github-oidc-security.md).
