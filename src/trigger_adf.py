import argparse
import json
import subprocess

from azure.identity import AzureCliCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

RESOURCE_GROUP = "rg-job-market-dev"
FACTORY_NAME = "adf-job-market-dev"
PIPELINE_NAME = "pl_job_market_daily_etl"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    parameters = {
        "snapshot": args.snapshot,
        "run_id": args.run_id,
    }

    print("Parametry pipeline'u:")
    print(json.dumps(parameters, indent=2))

    if not args.execute:
        print("TRYB TESTOWY — ADF nie został uruchomiony.")
        return

    subscription_id = subprocess.check_output(
        ["az", "account", "show", "--query", "id", "-o", "tsv"],
        text=True,
    ).strip()

    client = DataFactoryManagementClient(
        AzureCliCredential(),
        subscription_id,
    )

    result = client.pipelines.create_run(
        RESOURCE_GROUP,
        FACTORY_NAME,
        PIPELINE_NAME,
        parameters=parameters,
    )

    print(f"ADF uruchomiony. Pipeline Run ID: {result.run_id}")


if __name__ == "__main__":
    main()