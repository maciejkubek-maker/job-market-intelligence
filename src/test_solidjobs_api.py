
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

# 1. Konfiguracja API
URL = "https://solid.jobs/public-api/offers/IT"

PARAMS = {
    "campaign": "job-market-intelligence",
    "pageIndex": 0,
    "pageSize": 10,
}

HEADERS = {
    "Accept": "application/json",
    "X-Api-Version": "1.0",
}

# 2. Pobranie danych
print("Pobieram oferty z SOLID.Jobs...")

response = requests.get(
    URL,
    params=PARAMS,
    headers=HEADERS,
    timeout=30,
)

response.raise_for_status()
data = response.json()

# 3. Sprawdzenie struktury odpowiedzi
print("\nSTATUS HTTP:", response.status_code)
print("KLUCZE JSON:", list(data.keys()))
print("WSZYSTKIE OFERTY:", data.get("totalCount"))
print("LICZBA STRON:", data.get("totalPages"))

jobs = data.get("jobs", [])

print("POBRANE OFERTY:", len(jobs))

# 4. Przykładowe ogłoszenia
print("\nPIERWSZE OFERTY:")

for job in jobs[:5]:
    print(
        job.get("title"),
        "|",
        job.get("company"),
    )

# 5. Zapis surowej odpowiedzi JSON
project_root = Path(__file__).resolve().parent.parent
snapshot_date = datetime.now(timezone.utc).date().isoformat()
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

output_dir = (
    project_root
    / "data"
    / "bronze"
    / "source=solidjobs"
    / f"snapshot_date={snapshot_date}"
)

output_dir.mkdir(parents=True, exist_ok=True)

output_file = output_dir / f"test_{run_id}_page_0000.json"

with output_file.open("w", encoding="utf-8") as file:
    json.dump(data, file, ensure_ascii=False, indent=2)

print("\nZAPISANO:", output_file)

# 6. Sprawdzenie dostępnych pól
if jobs:
    print("\nPOLA PIERWSZEJ OFERTY:")
    for key in jobs[0].keys():
        print("-", key)

print("\nTEST ZAKOŃCZONY")


# 7. Analiza zagnieżdżonych danych
if jobs:
    first_job = jobs[0]

    for field in [
        "jobOfferKey",
        "salary",
        "skills",
        "locations",
        "contractTime",
        "experienceLevel",
    ]:
        print(f"\n--- {field} ---")
        print(
            json.dumps(
                first_job.get(field),
                ensure_ascii=False,
                indent=2,
            )
        )

