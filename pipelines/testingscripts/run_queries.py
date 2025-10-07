"""run_queries.py"""

import sys
import os
import json
from typing import List, Tuple

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

# Connect to Kusto Emulator
KUSTO_EMULATOR_URI = "http://localhost:8080"  # assuming default port 8080
DATABASE = "NetDefaultDB"  # default database


def get_queries() -> List[Tuple[str, str]]:
    """A function which retrieves all queries from detections.
    Returns a list of tuples, where the first element is the detection file name,
    and the second element is the query text."""
    queries = []
    sentinel_detection_folder = os.path.join(os.getcwd(), "detections", "sentinel")
    defender_detection_folder = os.path.join(os.getcwd(), "detections", "defender")

    # Loading Sentinel detections
    for detection_name in os.listdir(sentinel_detection_folder):
        sentinel_detection_path = os.path.join(
            sentinel_detection_folder, detection_name
        )
        # Load detection
        with open(sentinel_detection_path, encoding="utf8") as detection_file:
            detection = json.load(detection_file)
        queries.append((detection_name, detection["properties"]["query"]))

    # Loading Defender detections
    for detection_name in os.listdir(defender_detection_folder):
        defender_detection_path = os.path.join(
            defender_detection_folder, detection_name
        )
        # Load detection
        with open(defender_detection_path, encoding="utf8") as detection_file:
            detection = json.load(detection_file)
        queries.append((detection_name, detection["queryCondition"]["queryText"]))
    print(f"Collected {len(queries)} queries.\n")
    return queries


def run_queries(client: KustoClient):
    """A function which executes queries and produces human-readable results.
    Also exits with an errorcode in case of errors."""
    queries = get_queries()
    errors = []

    for query in queries:
        try:
            client.execute(DATABASE, query[1])
            print(f"Query {query[0]} ran successfully.")
        except KustoServiceError as e:
            print(f"ERROR: Query {query[0]}: {e}")
            errors.append((query[0], str(e)))

    if errors:
        print("\nErrors found in queries:")
        for query, error in errors:
            print(f"{query}: {error}")
        sys.exit(1)  # exit with an error code, since we have a validation failure


def main():
    """Function containing the main logic"""
    # Set up Kusto connection
    print("Setting up connection with the ADX cluster...")
    kcsb = KustoConnectionStringBuilder.with_aad_application_token_authentication(
        connection_string=KUSTO_EMULATOR_URI, application_token="123456"
    )

    with KustoClient(kcsb) as client:
        # Run and validate queries
        run_queries(client)


if __name__ == "__main__":
    sys.stdout.flush()
    main()
