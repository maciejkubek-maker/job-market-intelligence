# Polish Job Market Intelligence

Projekt portfolio: dzienne snapshoty polskich ogłoszeń o pracę → Azure ETL → SQL → Power BI.

**Stan:** dokumentacja techniczna v1.0; źródło danych i kontrakt JSON do weryfikacji.

Zacznij od [dokumentacji projektu](PROJECT_DOCUMENTATION_Job_Market_Intelligence.md).

## Otwarcie w PyCharm

1. Rozpakuj katalog `job-market-intelligence`.
2. W PyCharm wybierz **File → Open** i wskaż ten katalog.
3. Utwórz interpreter Pythona / środowisko wirtualne, gdy pojawi się kod aplikacji.
4. Przed rozpoczęciem ekstrakcji sprawdź dokumentację i warunki użycia źródłowego API.

Folder `data/` służy do lokalnych testów, a docelowa historia ma być przechowywana w ADLS Gen2. Nie commituj surowych danych ani sekretów.
