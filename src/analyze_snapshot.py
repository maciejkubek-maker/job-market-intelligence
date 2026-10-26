
import json
import re
from collections import Counter
from pathlib import Path

# Katalog główny projektu
ROOT = Path(__file__).resolve().parent.parent
BRONZE = ROOT / "data" / "bronze" / "source=solidjobs"

# Znajdź ostatni ukończony snapshot
snapshots = []

for manifest_path in BRONZE.glob(
    "snapshot_date=*/run_id=*/manifest.json"
):
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    if manifest.get("status") == "completed":
        snapshots.append((manifest["run_id"], manifest_path.parent))

if not snapshots:
    raise RuntimeError("Nie znaleziono ukończonego snapshotu")

run_id, snapshot_dir = max(snapshots)

# Wczytaj zapisane oferty
jobs = []

for page_file in sorted(
    snapshot_dir.glob("offers_page_*.json")
):
    data = json.loads(
        page_file.read_text(encoding="utf-8")
    )
    jobs.extend(data["jobs"])

print("SNAPSHOT:", run_id)
print("WSZYSTKIE OFERTY:", len(jobs))

# Grupy zawodowe: wstępna klasyfikacja po tytule
categories = {
    "Data Analyst": r"data analyst|analityk danych|data analytics",
    "BI Analyst": r"bi analyst|business intelligence|power bi|bi developer",
    "Data Engineer": r"data engineer|inżynier danych|data engineering",
    "Process Automation": (
        r"process automation|rpa|automation developer|"
        r"power automate|automatyzacj[aeęi]"
    ),
}

print("\nOFERTY W INTERESUJĄCYCH NAS GRUPACH")

matched_ids = set()

for category, pattern in categories.items():
    matching = [
        job for job in jobs
        if re.search(
            pattern,
            job.get("title") or "",
            re.IGNORECASE,
        )
    ]

    matched_ids.update(
        job["jobOfferKey"] for job in matching
    )

    print(f"{category}: {len(matching)}")

print("Unikalne oferty z wybranych grup:", len(matched_ids))
print("(Kategorie mogą się nakładać)")

# Dostępność wynagrodzeń
with_salary = sum(
    1 for job in jobs
    if isinstance(job.get("salary"), dict)
    and job["salary"].get("from") is not None
    and job["salary"].get("to") is not None
)

print("\nWYNAGRODZENIA")
print("Oferty z pełnymi widełkami:", with_salary)
print(
    "Udział:",
    f"{with_salary / len(jobs) * 100:.1f}%"
    if jobs else "brak danych",
)

# Technologie
skills = Counter()

for job in jobs:
    for skill in job.get("skills") or []:
        if isinstance(skill, dict) and skill.get("name"):
            skills[skill["name"]] += 1

print("\nTOP 15 TECHNOLOGII")
for name, count in skills.most_common(15):
    print(f"{name}: {count}")

# Praca zdalna
remote_count = sum(
    1 for job in jobs if job.get("isRemote") is True
)

print("\nPRACA ZDALNA")
print("Oferty oznaczone jako zdalne:", remote_count)

print("\nANALIZA ZAKOŃCZONA")
