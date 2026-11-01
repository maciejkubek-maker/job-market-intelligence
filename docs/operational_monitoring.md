# Przegląd i Koncepcja Monitoringu Operacyjnego (ADF & Lakehouse)

## 1. Analiza stanu obecnego i identyfikacja luki (Monitoring Gap)

W obecnej architekturze orkiestracji proces dzienny wyzwalany jest przez GitHub Actions (`.github/workflows/daily_extract.yml`):
1. GitHub Actions uruchamia ekstraktor (`src/extract_solidjobs.py`), zapisując surowe dane do Bronze.
2. Skrypt `src/upload_to_adls.py` przesyła pliki JSON i manifest do ADLS Gen2.
3. Skrypt `src/trigger_adf.py` inicjuje wykonanie pipeline'u w Azure Data Factory:
   ```python
   result = client.pipelines.create_run(
       RESOURCE_GROUP,
       FACTORY_NAME,
       PIPELINE_NAME,
       parameters=parameters,
   )
   print(f"ADF uruchomiony. Pipeline Run ID: {result.run_id}")
   ```

### Zidentyfikowana luka:
Metoda `create_run` w Azure Data Factory SDK działa w trybie **asynchronicznym (fire-and-forget)**. Zwraca identyfikator `run_id` w momencie przyjęcia zlecenia przez usługę ADF, po czym proces `trigger_adf.py` kończy działanie z kodem wyjścia `0`.

GitHub Actions interpretuje to jako natychmiastowy sukces kroku i oznacza cały workflow zielonym znacznikiem (SUCCESS).

Rzeczywiste przetwarzanie w ADF składa się z dwóch zadań Synapse Spark (`nb_bronze_to_silver_jobs` oraz aktywnego `nb_silver_to_gold_v2_jobs`), które wymagają alokacji węzłów w puli `sparkjobmarket` i trwają od kilku do kilkunastu minut. Jeżeli na tym etapie wystąpi:
- brak zasobów lub błąd alokacji klastra Spark,
- nieobsługiwany błąd w kodzie PySpark (np. naruszenie typów, błąd deserializacji JSON),
- błąd uprawnień do kontenerów `silver` lub `gold` w ADLS Gen2,
- przekroczenie limitu czasu (timeout),

pipeline ADF zakończy się statusem **Failed**, podczas gdy w GitHub Actions status zadania pozostanie **Succeeded**. Powstaje ryzyko **cichej awarii (silent failure)** w warstwach Silver/Gold bez powiadomienia zespołu inżynierii danych.

---

## 2. Rekomendowane podejścia do monitoringu operacyjnego

W celu eliminacji luki bez naruszania zasady najmniejszych uprawnień (Least Privilege) w GitHub OIDC i bez generowania kosztów niepotrzebnego czasu pracy runnerów GitHub, proponuje się następujące rozwiązania:

### Rozwiązanie A: Natywny Azure Monitor + ADF Alerts (Rekomendowane)
Najbezpieczniejsza i najbardziej stabilna metoda klasy enterprise.

1. **Konfiguracja:**
   - Włączenie *Diagnostic Settings* w usłudze Azure Data Factory i kierowanie logów `PipelineRuns` do Log Analytics Workspace.
   - Skonfigurowanie reguły alertu (*Alert Rule*) w Azure Monitor na podstawie zapytania KQL:
     ```kql
     ADFPipelineRun
     | where PipelineName == "pl_job_market_daily_etl"
     | where Status == "Failed"
     ```
   - Przypisanie *Action Group* wysyłającej powiadomienie e-mail, SMS lub webhook (do Microsoft Teams / Slack).
2. **Zalety:**
   - **Zero dodatkowych uprawnień dla GitHub Actions:** tożsamość OIDC nie potrzebuje uprawnień do odczytu statusu wykonania ADF ani Synapse.
   - **Oszczędność kosztów runnerów CI/CD:** GitHub Actions zwalnia maszynę wirtualną natychmiast po wysłaniu zlecenia do ADF.
   - **Niezależność:** alert działa nawet w przypadku manualnego uruchomienia pipeline'u z portalu Azure.

### Rozwiązanie B: Event-Driven Alerts (Event Grid + Azure Logic App)
Rozwiązanie zorientowane na zdarzenia:

1. **Konfiguracja:**
   - Subskrypcja zdarzenia `Microsoft.DataFactory.PipelineRunFinished` w Azure Event Grid z filtrem na `data.status == "Failed"`.
   - Przekazanie zdarzenia do Azure Logic App, która parsuje parametry błędu (`run_id`, `errorMessage`, `parameters`) i publikuje sformatowany komunikat na kanale inżynierskim Slack/Teams.
2. **Zalety:**
   - Natychmiastowe powiadomienie (czas reakcji w sekundach po wystąpieniu błędu).
   - Bogaty kontekst błędu w powiadomieniu bez konieczności logowania się do Azure Portal.

### Rozwiązanie C: Opcjonalny polling w środowisku testowym / CI
W scenariuszach testów end-to-end, w których zachodzi konieczność synchronicznego potwierdzenia sukcesu przetwarzania:

1. **Implementacja:**
   - Rozbudowa `src/trigger_adf.py` o opcjonalną flagę `--wait` z pętlą sprawdzającą status:
     ```python
     while True:
         run = client.pipeline_runs.get(RESOURCE_GROUP, FACTORY_NAME, run_id)
         if run.status in ["Succeeded", "Failed", "Cancelled"]:
             break
         time.sleep(30)
     ```
2. **Ograniczenia i ryzyka:**
   - Wymaga rozszerzenia roli tożsamości OIDC o `Microsoft.DataFactory/factories/pipelineruns/read`.
   - Blokuje runner GitHub Actions przez cały czas trwania zadań Spark, zużywając płatne minuty GitHub Actions.
   - Ryzyko przekroczenia limitu czasu runnera przy opóźnieniach alokacji puli Spark.

---

## 3. Plan wdrożenia bez modyfikacji bieżących uprawnień
1. Utrzymanie obecnego modelu wyzwalania w `trigger_adf.py` (brak wpływu na bieżący koszt i uprawnienia).
2. Skonfigurowanie w portalu Azure reguły Azure Monitor Alert dla `ADFPipelineRun` ze statusem `Failed`.
3. Dodanie metryk w Synapse Serverless SQL (skrypt `sql/03_validate_snapshots.sql`) weryfikujących dzienną obecność nowych snapshotów w warstwie analitycznej.
