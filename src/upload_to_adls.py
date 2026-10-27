import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

# ==========================================
# 1. KONFIGURACJA
# ==========================================

ROOT = Path(__file__).resolve().parent.parent

BRONZE = (
    ROOT / "data" / "bronze" / "source=solidjobs"
)

ACCOUNT = "stjobmarketdev01"
CONTAINER = "bronze"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--execute",
    action="store_true",
    help="Uruchom pipeline ADF po sprawdzeniu uploadu"
)
parser.add_argument(
    "--bronze-only",
    action="store_true",
    help="Tylko upload Bronze, bez Silver i ADF"
)


args = parser.parse_args()

# ==========================================
# 2. POŁĄCZENIE Z AZURE
# ==========================================

credential = AzureCliCredential()

service = DataLakeServiceClient(
    account_url=f"https://{ACCOUNT}.dfs.core.windows.net",
    credential=credential,
)

filesystem = service.get_file_system_client(CONTAINER)

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def silver_snapshot_exists(snapshot_date: str, run_id: str) -> bool:
    silver_fs = service.get_file_system_client("silver")

    success_path = (
        f"jobs/source=solidjobs/"
        f"snapshot_date={snapshot_date}/"
        f"run_id={run_id}/_SUCCESS"
    )

    return silver_fs.get_file_client(success_path).exists()

# ==========================================
# 3. ZNAJDŹ UKOŃCZONY SNAPSHOT
# ==========================================

snapshots = []

for manifest_path in BRONZE.glob(
    "snapshot_date=*/run_id=*/manifest.json"
):
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    if manifest.get("status") == "completed":
        snapshots.append(
            (manifest["run_id"], manifest_path.parent)
        )

if not snapshots:
    raise RuntimeError("Brak ukończonego snapshotu")

run_id, snapshot_dir = max(snapshots)

relative_dir = snapshot_dir.relative_to(BRONZE.parent)
files = sorted(snapshot_dir.glob("*.json"))

print("Snapshot:", run_id)
print("Pliki do wysłania:", len(files))

# ==========================================
# 4. UTWÓRZ FOLDERY W AZURE
# ==========================================

parts = relative_dir.parts

for i in range(1, len(parts) + 1):
    directory_path = "/".join(parts[:i])

    directory_client = filesystem.get_directory_client(
        directory_path
    )

    if not directory_client.exists():
        directory_client.create_directory()
        print("Utworzono katalog:", directory_path)

# ==========================================
# 5. PRZEŚLIJ PLIKI JSON
# ==========================================

uploaded = 0
skipped = 0
repaired = 0

for index, file_path in enumerate(files, start=1):

    azure_path = (
        relative_dir / file_path.name
    ).as_posix()

    file_client = filesystem.get_file_client(azure_path)

    local_size = file_path.stat().st_size

    if file_client.exists():
        properties = file_client.get_file_properties()

        if properties.size == local_size:
            local_hash = sha256(file_path.read_bytes())
            remote_hash = sha256(file_client.download_file().readall())

            if local_hash == remote_hash:
                print(f"[{index}/{len(files)}] POMINIĘTO: {file_path.name}")
                skipped += 1
                continue

            print(f"Niezgodna zawartość pliku: {file_path.name}")

        # Naprawa pliku pozostawionego po nieudanym uploadzie.
        print("Naprawiam plik w Azure:", file_path.name)

        with file_path.open("rb") as data:
            file_client.upload_data(
                data,
                overwrite=True,
            )

        repaired += 1

    else:
        with file_path.open("rb") as data:
            file_client.upload_data(
                data,
                overwrite=True,
            )

        uploaded += 1

    print(f"[{index}/{len(files)}] OK: {file_path.name}")

# ==========================================
# 6. PODSUMOWANIE
# ==========================================

print("\n========== PODSUMOWANIE ==========")
print("Storage Account:", ACCOUNT)
print("Kontener:", CONTAINER)
print("Snapshot:", run_id)
print("Nowe pliki:", uploaded)
print("Pominięte:", skipped)
print("Naprawione:", repaired)
print("Łącznie:", uploaded + skipped + repaired)
print("===================================")

print("\nUPLOAD ZAKOŃCZONY")

# ==========================================
# 7. PRZYGOTUJ URUCHOMIENIE ADF
# ==========================================

snapshot_date = snapshot_dir.parent.name.removeprefix("snapshot_date=")


if args.bronze_only:
    print("Tryb Bronze-only — pomijam Silver i ADF.")
    raise SystemExit(0)

print("\nPrzekazuję parametry do trigger_adf.py")

if silver_snapshot_exists(snapshot_date, run_id):
    print("Snapshot już istnieje w Silver — pomijam ADF.")
    raise SystemExit(0)

print("Snapshot nie ma znacznika _SUCCESS w Silver — można uruchomić ADF.")

command = [
    sys.executable,
    str(ROOT / "src" / "trigger_adf.py"),
    "--snapshot", snapshot_date,
    "--run-id", run_id,
]

if args.execute:
    command.append("--execute")

subprocess.run(command, check=True)
