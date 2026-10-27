import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

API_URL = "https://solid.jobs/public-api/offers/IT"
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def save_manifest(output_dir: Path, manifest: dict) -> None:
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def extract_solidjobs(
    base_dir: Path | None = None,
    api_url: str = API_URL,
    page_size: int = 10,
    delay_seconds: float = 0.5,
    session: requests.Session | None = None,
    started_at: datetime | None = None,
) -> tuple[dict, Path, int]:
    if started_at is None:
        started_at = datetime.now(UTC)

    snapshot_date = started_at.date().isoformat()
    run_id = started_at.strftime("%Y%m%dT%H%M%S%fZ")

    if base_dir is None:
        base_dir = (
            PROJECT_ROOT / "data" / "bronze"
            / "source=solidjobs"
        )

    output_dir = (
        base_dir
        / f"snapshot_date={snapshot_date}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    manifest: dict = {
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

    seen_ids: set[str] = set()
    save_manifest(output_dir, manifest)

    def _execute_extraction(sess: requests.Session) -> None:
        nonlocal manifest, seen_ids
        sess.headers.update({"Accept": "application/json"})
        page_index = 0

        while True:
            response = sess.get(
                api_url,
                params={
                    "campaign": "job-market-intelligence",
                    "pageIndex": page_index,
                    "pageSize": page_size,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict):
                raise TypeError("Odpowiedź API nie jest obiektem JSON")

            jobs = data.get("jobs")
            total_pages = data.get("totalPages")
            total_count = data.get("totalCount")

            if not isinstance(jobs, list):
                raise TypeError("Nieprawidłowe pole jobs: oczekiwano listy")

            if not isinstance(total_pages, int) or not isinstance(total_count, int):
                raise TypeError("Brak lub nieprawidłowe typy pól totalPages/totalCount")

            if manifest["expected_pages"] is None:
                manifest["expected_pages"] = total_pages
                manifest["expected_offers"] = total_count
            elif total_pages != manifest["expected_pages"]:
                raise ValueError("Liczba stron zmieniła się podczas pobierania")

            if data.get("pageIndex") != page_index:
                raise ValueError("Nieoczekiwany numer strony")

            page_file = output_dir / f"offers_page_{page_index:04d}.json"
            page_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            for job in jobs:
                if isinstance(job, dict) and job.get("jobOfferKey"):
                    seen_ids.add(str(job["jobOfferKey"]))

            manifest["downloaded_pages"] += 1
            manifest["downloaded_offers"] += len(jobs)
            manifest["unique_offers"] = len(seen_ids)
            save_manifest(output_dir, manifest)

            print(
                f"Strona {page_index + 1}/{total_pages} | "
                f"Pobrano: {manifest['downloaded_offers']}"
            )

            page_index += 1

            if total_pages == 0 or page_index >= total_pages:
                break

            if delay_seconds > 0:
                time.sleep(delay_seconds)

        complete = (
            manifest["expected_pages"] is not None
            and manifest["expected_pages"] > 0
            and manifest["downloaded_pages"] == manifest["expected_pages"]
            and manifest["downloaded_offers"] == manifest["expected_offers"]
            and manifest["unique_offers"] == manifest["expected_offers"]
        )

        if complete:
            manifest["status"] = "completed"
        else:
            manifest["status"] = "incomplete"
            if "error" not in manifest:
                manifest["error"] = (
                    f"Niezgodność danych: oczekiwano {manifest['expected_offers']} ofert "
                    f"na {manifest['expected_pages']} stronach, "
                    f"pobrano {manifest['downloaded_offers']} ofert "
                    f"({manifest['unique_offers']} unikalnych) "
                    f"na {manifest['downloaded_pages']} stronach."
                )

    try:
        if session is not None:
            _execute_extraction(session)
        else:
            with requests.Session() as default_session:
                _execute_extraction(default_session)
    except Exception as exc:  # noqa: BLE001
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        print("Błąd:", exc)
    finally:
        manifest["finished_at"] = datetime.now(UTC).isoformat()
        save_manifest(output_dir, manifest)

        print("\nPODSUMOWANIE")
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        print("Folder:", output_dir)

    exit_code = 0 if manifest["status"] == "completed" else 1
    return manifest, output_dir, exit_code


def main() -> int:
    _, _, exit_code = extract_solidjobs()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
