import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import requests

from src.extract_solidjobs import extract_solidjobs, main


def _create_mock_response(status_code: int = 200, json_data: dict | None = None, text: str = ""):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    if status_code >= 400:
        http_err = requests.exceptions.HTTPError(f"HTTP {status_code} Error", response=resp)
        resp.raise_for_status.side_effect = http_err
    else:
        resp.raise_for_status.return_value = None

    if json_data is not None:
        resp.json.return_value = json_data
    return resp


def test_extract_solidjobs_success(tmp_path):
    """Test successful extraction across multiple pages."""
    page_0_data = {
        "pageIndex": 0,
        "pageSize": 10,
        "totalPages": 2,
        "totalCount": 20,
        "jobs": [{"jobOfferKey": f"offer_{i}", "title": f"Dev {i}"} for i in range(10)],
    }
    page_1_data = {
        "pageIndex": 1,
        "pageSize": 10,
        "totalPages": 2,
        "totalCount": 20,
        "jobs": [{"jobOfferKey": f"offer_{i}", "title": f"Dev {i}"} for i in range(10, 20)],
    }

    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.side_effect = [
        _create_mock_response(json_data=page_0_data),
        _create_mock_response(json_data=page_1_data),
    ]

    fixed_time = datetime(2026, 10, 27, 8, 30, 0, tzinfo=UTC)
    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
        started_at=fixed_time,
    )

    assert exit_code == 0
    assert manifest["status"] == "completed"
    assert manifest["run_id"] == "20261027T083000000000Z"
    assert manifest["snapshot_date"] == "2026-10-27"
    assert manifest["expected_pages"] == 2
    assert manifest["expected_offers"] == 20
    assert manifest["downloaded_pages"] == 2
    assert manifest["downloaded_offers"] == 20
    assert manifest["unique_offers"] == 20
    assert manifest["finished_at"] is not None
    assert "error" not in manifest

    # Validate output files on disk
    manifest_on_disk = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest_on_disk == manifest
    assert (output_dir / "offers_page_0000.json").exists()
    assert (output_dir / "offers_page_0001.json").exists()


def test_extract_solidjobs_http_failure(tmp_path):
    """Test HTTP error handling, manifest failure state, and nonzero exit code."""
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.side_effect = requests.exceptions.HTTPError("500 Server Error")

    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "failed"
    assert "500 Server Error" in manifest["error"]
    assert manifest["finished_at"] is not None

    manifest_on_disk = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest_on_disk["status"] == "failed"
    assert "500 Server Error" in manifest_on_disk["error"]


def test_extract_solidjobs_timeout_failure(tmp_path):
    """Test network timeout handling."""
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.side_effect = requests.exceptions.Timeout("Connection timed out after 30s")

    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "failed"
    assert "timed out" in manifest["error"]

    manifest_on_disk = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest_on_disk["status"] == "failed"


def test_extract_solidjobs_malformed_json(tmp_path):
    """Test malformed JSON response handling."""
    bad_resp = _create_mock_response(status_code=200)
    bad_resp.json.side_effect = json.JSONDecodeError("Expecting value", "<html>", 0)

    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.return_value = bad_resp

    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "failed"
    assert manifest["finished_at"] is not None

    manifest_on_disk = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest_on_disk["status"] == "failed"


def test_extract_solidjobs_invalid_jobs_field(tmp_path):
    """Test handling when 'jobs' field is not a list."""
    payload = {
        "pageIndex": 0,
        "totalPages": 1,
        "totalCount": 10,
        "jobs": "invalid_string_not_list",
    }
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.return_value = _create_mock_response(json_data=payload)

    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "failed"
    assert "Nieprawidłowe pole jobs" in manifest["error"]

    manifest_on_disk = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest_on_disk["status"] == "failed"


def test_extract_solidjobs_invalid_metadata_types(tmp_path):
    """Test handling when totalPages or totalCount has invalid type."""
    payload = {
        "pageIndex": 0,
        "totalPages": "not_an_int",
        "totalCount": 10,
        "jobs": [],
    }
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.return_value = _create_mock_response(json_data=payload)

    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "failed"
    assert "totalPages/totalCount" in manifest["error"]


def test_extract_solidjobs_unexpected_page_index(tmp_path):
    """Test error when API returns unexpected page index."""
    payload = {
        "pageIndex": 99,
        "totalPages": 1,
        "totalCount": 1,
        "jobs": [{"jobOfferKey": "key_1"}],
    }
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.return_value = _create_mock_response(json_data=payload)

    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "failed"
    assert "Nieoczekiwany numer strony" in manifest["error"]


def test_extract_solidjobs_total_pages_changed_during_pagination(tmp_path):
    """Test error when totalPages changes mid-extraction."""
    page_0 = {
        "pageIndex": 0,
        "totalPages": 2,
        "totalCount": 20,
        "jobs": [{"jobOfferKey": f"k{i}"} for i in range(10)],
    }
    page_1 = {
        "pageIndex": 1,
        "totalPages": 3,  # Changed from 2 to 3
        "totalCount": 30,
        "jobs": [{"jobOfferKey": f"k{i}"} for i in range(10, 20)],
    }
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.side_effect = [
        _create_mock_response(json_data=page_0),
        _create_mock_response(json_data=page_1),
    ]

    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "failed"
    assert "Liczba stron zmieniła się" in manifest["error"]


def test_extract_solidjobs_incomplete_offers_count(tmp_path):
    """Test incomplete extraction when downloaded offers count doesn't match totalCount."""
    payload = {
        "pageIndex": 0,
        "totalPages": 1,
        "totalCount": 10,
        "jobs": [{"jobOfferKey": f"k{i}"} for i in range(7)],  # Only 7 instead of 10
    }
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.return_value = _create_mock_response(json_data=payload)

    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "incomplete"
    assert manifest["downloaded_offers"] == 7
    assert manifest["expected_offers"] == 10
    assert "Niezgodność danych" in manifest["error"]

    manifest_on_disk = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest_on_disk["status"] == "incomplete"


def test_extract_solidjobs_duplicate_offer_keys(tmp_path):
    """Test incomplete extraction when duplicate job keys reduce unique count."""
    payload = {
        "pageIndex": 0,
        "totalPages": 1,
        "totalCount": 3,
        "jobs": [
            {"jobOfferKey": "dup_1", "title": "Offer A"},
            {"jobOfferKey": "dup_1", "title": "Offer B"},  # Duplicate key
            {"jobOfferKey": "dup_2", "title": "Offer C"},
        ],
    }
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.return_value = _create_mock_response(json_data=payload)

    manifest, output_dir, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "incomplete"
    assert manifest["downloaded_offers"] == 3
    assert manifest["unique_offers"] == 2
    assert manifest["expected_offers"] == 3
    assert "Niezgodność danych" in manifest["error"]

    manifest_on_disk = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest_on_disk["status"] == "incomplete"


def test_extract_solidjobs_zero_pages_edge_case(tmp_path):
    """Test edge case where API returns 0 pages."""
    payload = {
        "pageIndex": 0,
        "totalPages": 0,
        "totalCount": 0,
        "jobs": [],
    }
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.return_value = _create_mock_response(json_data=payload)

    manifest, _, exit_code = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
    )

    assert exit_code == 1
    assert manifest["status"] == "incomplete"


def test_extract_solidjobs_main_cli():
    """Test CLI main() execution returning exit codes."""
    with patch("src.extract_solidjobs.extract_solidjobs") as mock_extract:
        mock_extract.return_value = ({}, MagicMock(), 0)
        assert main() == 0

        mock_extract.return_value = ({}, MagicMock(), 1)
        assert main() == 1


def test_bronze_path_convention(tmp_path):
    """Verify Bronze directory layout adheres to source=.../snapshot_date=.../run_id=..."""
    fixed_time = datetime(2026, 10, 27, 14, 0, 0, tzinfo=UTC)
    mock_session = MagicMock(spec=requests.Session)
    mock_session.headers = {}
    mock_session.get.return_value = _create_mock_response(
        json_data={
            "pageIndex": 0,
            "totalPages": 1,
            "totalCount": 1,
            "jobs": [{"jobOfferKey": "job_1"}],
        }
    )

    _, output_dir, _ = extract_solidjobs(
        base_dir=tmp_path,
        session=mock_session,
        delay_seconds=0,
        started_at=fixed_time,
    )

    assert output_dir.parent.name == "snapshot_date=2026-10-27"
    assert output_dir.name == "run_id=20261027T140000000000Z"
