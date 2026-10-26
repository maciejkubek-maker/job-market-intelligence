# Polish Job Market Intelligence — Azure ETL & Power BI

**Dokumentacja techniczna v1.0**  
**Data:** 2026-10-26  
**Status:** projektowanie / weryfikacja źródeł danych  
**Horyzont zbierania danych:** 30 kolejnych dni skutecznych snapshotów

## 1. Cel i zakres

Celem jest zaprojektowanie, zbudowanie i udokumentowanie procesu ETL/ELT monitorującego wybrane segmenty polskiego rynku pracy. System będzie codziennie pobierał rzeczywiste oferty, przechowywał surowe odpowiedzi źródłowe oraz przygotowywał dane do analizy w Power BI.

**Monitorowane obszary:** Data Analyst, BI Analyst, Data Engineer, Process Automation. Kategorie służą do wyszukiwania i klasyfikacji, a nie muszą być identyczne z tytułami ogłoszeń.

**Pytania biznesowe:**
- Jak zmienia się liczba aktywnych ofert dla obserwowanych specjalizacji?
- Które technologie i umiejętności najczęściej pojawiają się w ogłoszeniach?
- Jak kształtują się widełki wynagrodzeń według roli, poziomu, trybu pracy i formy zatrudnienia?
- Jak wiele ofert jest nowych, utrzymujących się lub znikających między kolejnymi pełnymi snapshotami?

**Istotne ograniczenie:** dane będą reprezentować ogłoszenia dostępne przez wybrane źródło lub źródła, a nie cały rynek pracy w Polsce.

## 2. Definicja sukcesu (Definition of Done)

1. Wybór źródła danych jest poparty testami pokrycia, pól, paginacji, stabilności i warunków użycia.
2. Proces gromadzi 30 dziennych, kompletnych snapshotów; nieudane lub częściowe próby są zachowane i oznaczone, ale nie są liczone jako kompletne dni.
3. Surowe odpowiedzi i manifesty są przechowywane bez nadpisywania historycznych uruchomień.
4. Transformacje tworzą powtarzalną warstwę Silver w Parquet i analityczne zbiory Gold.
5. Tabele w Azure SQL umożliwiają analizy trendu, technologii i wynagrodzeń.
6. Dashboard Power BI zawiera definicje miar oraz informację o pokryciu i jakości danych.
7. Repozytorium GitHub zawiera instrukcję odtworzenia, konfigurację bez sekretów, testy i rejestr decyzji architektonicznych (ADR).

## 3. Kandydaci na źródło danych

| Źródło | Potencjalne korzyści | Ryzyko / weryfikacja | Status |
|---|---|---|---|
| [SOLID.Jobs API](https://solid.jobs/api-ofert-pracy) | Polskie ogłoszenia, potencjalnie wynagrodzenia i technologie, JSON | Sprawdzić aktualny dostęp, regulamin, autoryzację, limity, paginację, kompletność i zakres pól | Pierwszy kandydat do testów |
| [Adzuna API](https://developer.adzuna.com/) | Wyszukiwanie ogłoszeń i potencjalne dane o wynagrodzeniach | Wymaga sprawdzenia warunków dostępu, klucza oraz realnego pokrycia Polski | Kandydat rezerwowy |
| [The Muse API](https://www.themuse.com/developers/api/v2) | Ustrukturyzowane odpowiedzi JSON | Sprawdzić obecny stan usługi, ograniczenia i adekwatność do rynku polskiego | Rezerwa |

**Zasada:** deklarowanych parametrów API (w tym limitów żądań) nie traktujemy jako zweryfikowanych do czasu testu i sprawdzenia aktualnej dokumentacji. Nie pobieramy danych z naruszeniem regulaminu ani ograniczeń dostępu.

### Protokół porównania źródeł

Dla każdej z czterech kategorii pracy wykonujemy próbę porównywalnymi filtrami i zapisujemy:

- datę i godzinę zapytania, parametry, odpowiedź HTTP i czas realizacji;
- liczbę ofert, liczbę unikalnych identyfikatorów, zakres paginacji i zachowanie przy końcu wyników;
- obecność oraz odsetek braków: `source_job_id`, `title`, `company`, `location`, `published_at`, `employment_type`, `salary_min`, `salary_max`, `salary_currency`, `salary_period`, `technologies`, `url`;
- odsetek ogłoszeń z widełkami wynagrodzeń i możliwość odróżnienia B2B od UoP;
- duplikaty, aktualność ogłoszeń, zakres geograficzny oraz stabilność identyfikatorów;
- ograniczenia prawne (licencja, przechowywanie, publikacja danych pochodnych, wymagane atrybucje).

**Bramka decyzyjna:** wybór źródła nastąpi dopiero po analizie przykładowego JSON i zestawieniu wyników. Próg minimalnej liczby ofert zostanie ustalony na podstawie obserwowanego pokrycia, a nie arbitralnego założenia.

## 4. Architektura docelowa

```text
Job offers API(s)
      |
      v
Python extractor (HTTP, paginacja, retry/backoff, walidacja)
      |
      v
Azure Data Lake Storage Gen2 — Bronze (raw JSON + manifest)
      |
      v
Azure Data Factory — orkiestracja i monitoring
      |
      v
Azure Synapse / PySpark — oczyszczanie, standaryzacja, deduplikacja
      |
      v
ADLS Gen2 — Silver (Parquet, historia ofert)
      |
      +--> ADLS Gen2 — Gold (zagregowane Parquet)
      |
      v
Azure SQL Database — model analityczny
      |
      v
Power BI — raporty i miary
```

**Uwaga projektowa:** dokładne miejsce wykonywania ekstraktora Python oraz sposób jego wywoływania przez ADF to decyzja otwarta. Należy porównać koszty, dostępne środowiska wykonawcze i niezawodność. Usługi uruchomimy dopiero po potwierdzeniu dostępu i budżetu Azure.

### Role warstw

- **Bronze:** surowy JSON, odpowiedź z każdej strony, metadane pobrania, manifest. Bez nieodwracalnych transformacji.
- **Silver:** typowane i oczyszczone rekordy ofert, ujednolicone pola, znaczniki jakości, deduplikacja w obrębie snapshotu.
- **Gold:** miary dzienne, przekroje ról, umiejętności i wynagrodzeń, przygotowane do SQL / raportowania.

## 5. Organizacja repozytorium i danych

```text
job-market-intelligence/
├── PROJECT_DOCUMENTATION_Job_Market_Intelligence.md
├── README.md
├── .gitignore
├── .env.example
├── configs/
├── docs/
│   └── adr/
├── src/
├── tests/
└── data/
    ├── bronze/
    │   └── source=solidjobs/
    │       └── snapshot_date=YYYY-MM-DD/
    │           └── run_id=<unikalny_id>/
    │               ├── offers_page_0001.json
    │               └── manifest.json
    ├── silver/
    └── gold/
```

**Kluczowy wariant względem prostego katalogu daty:** `run_id` pozwala zachować wiele prób tego samego dnia bez nadpisywania. Pełne snapshoty identyfikujemy przez manifest i status uruchomienia.

### Przykładowy manifest (projekt schematu, nie odpowiedź API)

```json
{
  "schema_version": "1.0",
  "source": "solidjobs",
  "snapshot_date": "2026-10-26",
  "run_id": "20261026T080000Z_a1b2c3",
  "started_at_utc": "2026-10-26T08:00:00Z",
  "finished_at_utc": null,
  "status": "running",
  "pages_requested": 0,
  "pages_succeeded": 0,
  "raw_records_count": 0,
  "unique_source_ids_count": null,
  "pagination_complete": false,
  "errors": []
}
```

Dozwolone statusy proponowane: `running`, `complete`, `partial`, `failed`. Proces ocenia kompletność na podstawie odpowiedzi API i kryterium końca paginacji, nie samego otrzymania kodu HTTP 200. Manifest finalny jest zapisywany po weryfikacji wszystkich stron; zapis powinien być odporny na awarię.

## 6. Proponowany kontrakt danych (do zatwierdzenia po próbie JSON)

| Pole kanoniczne | Znaczenie |
|---|---|
| `source` + `source_job_id` | Klucz naturalny w obrębie źródła |
| `snapshot_date`, `run_id` | Data obserwacji i konkretne pobranie |
| `title`, `company`, `location_raw` | Oryginalne informacje z oferty |
| `role_category` | Klasyfikacja własna (z wersją reguł) |
| `published_at`, `url` | Metadane publikacji i odnośnik |
| `work_mode`, `seniority`, `contract_type` | Kategorie po standaryzacji |
| `salary_min`, `salary_max`, `salary_currency`, `salary_period` | Surowe widełki, jednostki i waluta |
| `skills` | Lista technologii/umiejętności po ekstrakcji |
| `raw_payload_path`, `quality_flags` | Audytowalność i kontrola jakości |

Nie należy zakładać, że wszystkie powyższe pola są dostępne w każdym API. Braki reprezentujemy jako `null`, a transformacje dokumentujemy.

## 7. Wstępny model analityczny SQL

**Fakty:**
- `fact_job_snapshot`: jeden rekord na ofertę, źródło i dzień pełnego snapshotu (np. `snapshot_date`, `job_key`, `source_key`, `salary_*`, `is_active_observed`).
- `fact_daily_market`: agregaty według daty i odpowiednich przekrojów; unikalne ogłoszenia i liczba nowych / znikających ofert.
- `bridge_job_skill`: relacja wiele-do-wielu między ofertą a umiejętnością, z identyfikacją obserwacji lub zakresem ważności.

**Wymiary:** `dim_date`, `dim_source`, `dim_role`, `dim_location`, `dim_skill`, opcjonalnie `dim_company` i `dim_contract_type`.

**Decyzje otwarte:** ziarnistość historycznej tabeli ofert, obsługa aktualizacji tej samej oferty w różnych dniach, wykrywanie duplikatów między portalami oraz mapowanie wielu umiejętności. Nazwy tabel są propozycją do doprecyzowania po analizie danych.

## 8. Zasady analizy wynagrodzeń

- Zawsze przechowujemy wartość, walutę i okres wynagrodzenia **w oryginale**.
- UoP brutto i B2B netto + VAT lub wartości o niejasnej definicji **nie są bezpośrednio porównywalne**; prezentujemy je oddzielnie.
- Na początku porównujemy tylko oferty o zgodnej jednostce, walucie, okresie i typie umowy; nieznane zasady oznaczamy jako brak możliwości porównania.
- Jeśli przeliczamy godziny na miesiąc, używamy jawnego parametru (np. `hours_per_month`), zapisujemy regułę i zachowujemy oryginał. Nie stosujemy jej bez zidentyfikowania okresu wynagrodzenia.
- Konwersje walut wymagają kursu z określoną datą, źródłem i zasadą wyboru dnia; przed wdrożeniem decyzja pozostaje otwarta.
- W raportach pokazujemy liczebność próby wynagrodzeń, udział ofert z podanymi widełkami i ostrożną interpretację median.

## 9. Historyczność i metryki

Oferta jest **obserwowana** w dniu D tylko wtedy, gdy znalazła się w kompletnym snapshocie D. „Nowa” oznacza pierwszą obserwację w badanym zbiorze, a nie koniecznie pierwszą publikację na rynku. „Zniknięcie” oznacza brak w następnym porównywalnym kompletnym snapshocie — nie dowodzi, że rekrutacja się zakończyła.

Porównania trendu wykonujemy wyłącznie dla porównywalnych parametrów wyszukiwania, źródeł i pełnych snapshotów. Zmianę filtrów lub zakresu źródeł ujawniamy na dashboardzie.

**Proponowane KPI:** liczba zaobserwowanych unikalnych ofert, liczba nowych obserwacji, liczba ofert nieobecnych względem poprzedniego dnia, liczba ofert z wynagrodzeniem, udział ofert z wynagrodzeniem, mediana dolnej i górnej granicy dla porównywalnych grup, liczba ofert wymagających danej technologii.

## 10. Jakość danych i niezawodność

| Obszar | Kontrola |
|---|---|
| Dostęp do API | Timeout, obsługa 429/5xx, limitowanie żądań, exponential backoff z jitterem |
| Paginacja | Zatrzymanie wyłącznie przy potwierdzonym końcu, detekcja zapętleń i powtórzeń stron |
| Kompletność | Liczba stron, rekordów, błędów, status i `pagination_complete` w manifeście |
| Unikalność | Deduplikacja `(source, source_job_id, snapshot_date)` z polityką dla brakującego ID |
| Schemat | Walidacja typów i obecności kluczowych pól, zapis nieznanych atrybutów w Bronze |
| Audyt | `run_id`, timestamp UTC, hash lub kontrola integralności plików, wersja kodu i konfiguracji |
| Harmonogram | Jedna uzgodniona strefa czasowa dla snapshot_date (Europe/Warsaw) i timestampy UTC |
| Bezpieczeństwo | Sekrety poza Git, najmniejsze uprawnienia, minimalizacja danych osobowych, kontrola retencji |
| Koszty | Alerty budżetowe, monitoring zużycia usług, sprzątanie zasobów testowych |

## 11. Etapy realizacji — 30 dni

| Okres | Etap i wynik |
|---|---|
| Dni 1–3 | Testy SOLID.Jobs / alternatyw, próbki JSON, tabela pokrycia, decyzja o źródle |
| Dni 4–6 | Ekstraktor Python, retry, paginacja, surowy zapis, manifest i testy jednostkowe |
| Dni 7–9 | Projekt kontenera ADLS Gen2, pierwsze pełne snapshoty, walidacja kosztów i dostępu |
| Dni 10–14 | Orkiestracja ADF, monitoring i automatyczny harmonogram |
| Dni 15–19 | Transformacje PySpark, Bronze → Silver, deduplikacja i testy jakości |
| Dni 20–23 | Agregaty Gold, model Azure SQL i ładowanie inkrementalne |
| Dni 24–27 | Model Power BI, dashboardy, dokumentacja metryk |
| Dni 28–30 | Monitoring historii, testy odtwarzalności, ADR, README, publikacja GitHub |

**Ważne:** harmonogram wdrożenia i zbieranie 30 kompletnych dni to dwa różne zegary. Pełne 30 dni historii uzyskamy dopiero po 30 skutecznych dobach obserwacji; plan prac może przebiegać równolegle do gromadzenia snapshotów.

## 12. Ryzyka i świadome kompromisy

| Ryzyko | Ograniczenie |
|---|---|
| Zbyt mało danych | Ocena pokrycia przed wdrożeniem; możliwość rozszerzenia o drugie legalnie dostępne źródło |
| Limity lub zmiana API | Konfigurowalny ekstraktor, retry, alerty, zapis błędów i wersjonowanie parsera |
| Niejednolite płace | Oryginał + rozdzielne segmenty UoP/B2B, jawna polityka normalizacji |
| Fałszywe trendy wskutek niepełnych danych | Manifest kompletności i wykluczenie snapshotów częściowych |
| Koszty Azure | Budżety, minimalne rozmiary środowisk i plan wyłączania zasobów |
| Prywatność, licencje i regulaminy | Ocena przed pobieraniem, ograniczenie publikacji danych źródłowych |
| Brak dziennej zmienności | Prezentacja stabilnych wskaźników i ograniczeń krótkiego horyzontu |

## 13. Rejestr decyzji i argumentacja do rozmowy rekrutacyjnej

Dokumentuj każdą kluczową decyzję w `docs/adr/` w formacie: **kontekst → opcje → decyzja → konsekwencje → sposób weryfikacji**.

- **Dlaczego JSON w Bronze?** Zachowuje materiał źródłowy i umożliwia ponowne przetworzenie po zmianie parsera; koszt to większa objętość.
- **Dlaczego Parquet w Silver/Gold?** Kolumnowe przechowywanie wspiera odczyty analityczne i kompresję; ceną jest konieczność zarządzania schematem i partycjami.
- **Dlaczego snapshoty dzienne zamiast nadpisywania?** Umożliwiają obserwowanie zmian; wymagają kontroli kompletności i retencji.
- **Dlaczego SQL + Power BI?** Rozdzielają przygotowanie danych od konsumpcji i umożliwiają definiowanie spójnych miar; należy świadomie kontrolować podwójne przechowywanie.
- **Dlaczego ADF i Synapse?** Pozwalają zaprezentować orkiestrację i rozproszone przetwarzanie w Azure, ale przy małych wolumenach mogą być droższe i bardziej złożone niż prosty Python. Należy uzasadnić je celem edukacyjnym i skalowalnością.

## 14. Następna sesja — bramka źródła

Nie projektujemy ostatecznego schematu API „na wyczucie”. Kolejność pracy:

1. Zweryfikować dokumentację, regulamin oraz aktualną dostępność endpointów.
2. Wykonać ograniczoną liczbę legalnych zapytań testowych dla czterech kategorii.
3. Zachować przykładowe odpowiedzi JSON z usuniętymi danymi wrażliwymi, jeśli występują.
4. Policzyć kompletność pól, liczebność, duplikaty i zachowanie paginacji.
5. Wybrać źródło i potwierdzić kontrakt danych, strategię snapshotów oraz ostateczną strukturę katalogów.

---

*Wersja 1.0 jest dokumentem projektowym. Tabele i nazwy pól są propozycjami, nie potwierdzonymi schematami zwracanymi przez API.*
