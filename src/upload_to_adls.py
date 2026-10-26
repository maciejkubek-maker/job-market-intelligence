
import json
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

# ==========================================
# 2. POŁĄCZENIE Z AZURE
# ==========================================

credential = AzureCliCredential()

service = DataLakeServiceClient(
    account_url=f"https://{ACCOUNT}.dfs.core.windows.net",
    credential=credential,
)

filesystem = service.get_file_system_client(CONTAINER)

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
            print(f"[{index}/{len(files)}] POMINIĘTO: {file_path.name}")
            skipped += 1
            continue

        # Naprawa pliku pozostawionego po nieudanym uploadzie.
        print("Ponawiam niekompletny plik:", file_path.name)

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
