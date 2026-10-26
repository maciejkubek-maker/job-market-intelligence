
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

API_URL = "https://solid.jobs/public-api/offers/IT"
PROJECT_ROOT = Path(__file__).resolve().parent.parent

started_at = datetime.now(timezone.utc)
snapshot_date = started_at.date().isoformat()
run_id = started_at.strftime("%Y%m%dT%H%M%S%fZ")

output_dir = (
    PROJECT_ROOT / "data" / "bronze"
    / "source=solidjobs"
    / f"snapshot_date={snapshot_date}"
    / f"run_id={run_id}"
)
output_dir.mkdir(parents=True, exist_ok=False)

manifest = {
    "source": "solidjobs",
    "run_id": run_id,
    "snapshot_date": snapshot_date,
    "started_at": started_at.isoformat(),
    "status": "running",
    "expected_offers": None,
    "expected_pages": None,
    "downloaded_pages": 0,
    "downloaded_offers": 0,
    "unique_offers": 0,
}

seen_ids = set()


def save_manifest():
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


save_manifest()

try:
    with requests.Session() as session:
        session.headers.update({"Accept": "application/json"})

        page_index = 0

        while True:
            response = session.get(
                API_URL,
                params={
                    "campaign": "job-market-intelligence",
                    "pageIndex": page_index,
                    "pageSize": 10,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            jobs = data["jobs"]
            total_pages = data["totalPages"]

            if not isinstance(jobs, list):
                raise ValueError("Nieprawidłowe pole jobs")

            if manifest["expected_pages"] is None:
                manifest["expected_pages"] = total_pages
                manifest["expected_offers"] = data["totalCount"]
            elif total_pages != manifest["expected_pages"]:
                raise ValueError("Liczba stron zmieniła się podczas pobierania")

            if data["pageIndex"] != page_index:
                raise ValueError("Nieoczekiwany numer strony")

            page_file = output_dir / f"offers_page_{page_index:04d}.json"
            page_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            for job in jobs:
                if job.get("jobOfferKey"):
                    seen_ids.add(job["jobOfferKey"])

            manifest["downloaded_pages"] += 1
            manifest["downloaded_offers"] += len(jobs)
            manifest["unique_offers"] = len(seen_ids)
            save_manifest()

            print(
                f"Strona {page_index + 1}/{total_pages} | "
                f"Pobrano: {manifest['downloaded_offers']}"
            )

            page_index += 1

            if page_index >= total_pages:
                break

            time.sleep(0.5)

    complete = (
        manifest["downloaded_pages"] == manifest["expected_pages"]
        and manifest["downloaded_offers"] == manifest["expected_offers"]
        and manifest["unique_offers"] == manifest["expected_offers"]
    )

    manifest["status"] = "completed" if complete else "incomplete"

except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
    manifest["status"] = "failed"
    manifest["error"] = str(exc)
    print("Błąd:", exc)

finally:
    manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
    save_manifest()

    print("\nPODSUMOWANIE")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    print("Folder:", output_dir)
