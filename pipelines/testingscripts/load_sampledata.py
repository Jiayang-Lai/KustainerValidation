"""load_sampledata.py"""

import sys
import os
import json

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

# Connect to Kusto Emulator
KUSTO_EMULATOR_URI = "http://localhost:8080"  # assuming default port 8080
DATABASE = "NetDefaultDB"  # default database

def create_json_mapping(client: KustoClient, database_name: str, table_name: str, json_mapping_name: str, schema: list[dict[str, str]]):
    """A function responsible for creating a JSON mapping for the specified table based on the provided schema.
    Note: This will overwrite any existing mapping named <json_mapping_name> for the specified table.
    The function is very basic and assumes that the JSON structure directly maps to the table schema.
    """
    # https://sandervandevelde.wordpress.com/2023/05/17/test-kql-table-mappings-inline/
    print(f"Attempting to create JSON mapping for table {table_name}...")
    mapping_entries = ", ".join(
        [f'{{ "column": "{col['ColumnName']}", "path": "$.{col['ColumnName']}", "datatype": "{col['ColumnType']}" }}' for col in schema]
    )
    create_mapping_cmd = f'.create-or-alter table {table_name} ingestion json mapping "{json_mapping_name}" \'[ {mapping_entries} ]\''
    client.execute_mgmt(database_name, create_mapping_cmd)
    print(f"Created JSON mapping for table {table_name}!")

def setup_table(client: KustoClient, table_name: str, schema: list[dict[str, str]]):
    """A function responsible for formatting and running the create table command."""
    print(f"\nAttempting to create table {table_name}...")
    columns = ", ".join(
        [f"{col['ColumnName']} : {col['ColumnType']}" for col in schema]
    )
    create_table_cmd = f".create table {table_name} ({columns})"
    client.execute(DATABASE, create_table_cmd)
    print(f"Created table {table_name}!")


def ingest_data(client: KustoClient, table_name: str, json_mapping_name: str, data: list[dict]):
    """A function responsible for formatting and running the ingest inline commands."""
    print(f"Attempting to ingest data into {table_name}...")
    for row in data:
        insert_cmd = f".ingest inline into table {table_name} with (format = 'json', ingestionMappingReference = '{json_mapping_name}')  <| {json.dumps(row)}"
        client.execute(DATABASE, insert_cmd)
    print(f"Data ingested into {table_name}!")


def main():
    # Set up Kusto connection
    print("Setting up connection with the ADX cluster...")
    kcsb = KustoConnectionStringBuilder.with_aad_application_token_authentication(
        connection_string=KUSTO_EMULATOR_URI, application_token="123456"
    )

    with KustoClient(kcsb) as client:
        # Load schema and data files
        sample_data_dir = os.path.join(os.getcwd(), "sampledata")
        print(f"\nSearching for sample data in {sample_data_dir}")
        for table_folder in os.listdir(sample_data_dir):
            table_path = os.path.join(sample_data_dir, table_folder)

            # Load schema
            with open(
                os.path.join(table_path, "schema.json"), encoding="utf8"
            ) as schema_file:
                schema = json.load(schema_file)

            # Create table
            json_mapping_name = "JsonMapping"
            setup_table(client, table_folder, schema)
            create_json_mapping(client, DATABASE, table_folder, json_mapping_name, schema)

            # Load data
            with open(
                os.path.join(table_path, "data.json"), encoding="utf8"
            ) as data_file:
                data = json.load(data_file)
                ingest_data(client, table_folder, json_mapping_name, data)
    print("\nDone Loading sample data!")


if __name__ == "__main__":
    sys.stdout.flush()
    main()
