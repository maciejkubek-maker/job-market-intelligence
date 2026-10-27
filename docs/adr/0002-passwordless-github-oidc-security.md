# ADR 0002: Bezhasłowa Integracja CI/CD z Azure za Pomocą OpenID Connect (OIDC)

## Status
Zaakceptowany (Accepted)

## Kontekst
Automatyzacja codziennego procesu ekstrakcji danych wymaga bezpiecznego uwierzytelniania agentów GitHub Actions w środowisku Microsoft Azure (dostęp do Azure Data Lake Storage Gen2 oraz Azure Data Factory).
Tradycyjne podejście polega na tworzeniu tajnych kluczy jednostek usługi (Service Principal Client Secrets) i przechowywaniu ich jako GitHub Secrets. Wiąże się to z ryzykiem wycieku długoterminowych poświadczeń, koniecznością ich rotacji oraz brakiem ścisłej kontroli kontekstu wykonania.

## Decyzja

1. **Zastosowanie OpenID Connect (OIDC):**
   - Zastąpienie statycznych kluczy API/haseł federacją tożsamości Microsoft Entra ID z GitHub OIDC.
   - GitHub Actions uzyskuje krótkoterminowy token tożsamości (JWT) podpisany przez dostawcę tokenów GitHub.
   - Azure weryfikuje token i wymienia go na tymczasowy token dostępowy Azure Entra ID na czas trwania kroku workflow.

2. **Uprawnienia i Minimalizacja Dostępów (Least Privilege):**
   - Workflow testów jakości (`.github/workflows/ci.yml`) ma uprawnienia ściśle ograniczone do odczytu kodu repozytorium (`permissions: contents: read`) i **nie posiada dostępu do żadnych sekretów ani usług Azure**.
   - Workflow produkcyjny (`.github/workflows/daily_extract.yml`) posiada uprawnienie `permissions: id-token: write` wyłącznie w celu negocjacji tokenu OIDC.
   - W konfiguracji Microsoft Entra ID zdefiniowano Subject Identifier ograniczający federację do konkretnego repozytorium i gałęzi `main`.
   - Przypisane role Azure RBAC ograniczają działanie tożsamości wyłącznie do zapisu w dedykowanym storage account (`stjobmarketdev01`) i wyzwalania pipeline'u ADF (`rg-job-market-dev`).

## Konsekwencje
- **Pozytywne:**
  - Brak długoterminowych poświadczeń i haseł w repozytorium GitHub i środowisku wykonawczym.
  - Wyeliminowanie ryzyka wygaśnięcia haseł produkcyjnych (automatyczna rotacja per wykonanie).
  - Precyzyjny audyt operacji w Azure Entra ID Sign-in logs.
- **Negatywne / Kompromisy:**
  - Wymagana wstępna konfiguracja federacji tożsamości w Entra ID dla repozytorium GitHub.
