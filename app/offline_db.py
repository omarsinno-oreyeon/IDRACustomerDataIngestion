"""
Function that loads data from an SQLite database.
- Prepares the offline database for querying
- Exports the database schema to a JSON file
- Loads the images from the database

"""

# Standard imports
from typing import List, Tuple
import sqlite3
import logging
import glob
import json
import csv
import os

import argparse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger()
logger.setLevel("INFO")

def load_sqlite_db(db_path: str) -> sqlite3.Connection:
    """
    Load an SQLite Database Object.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        conn (sqlite3.Connection): SQLite database connection object.
    """
    try:
        conn = sqlite3.connect(db_path)
        logger.info("Successfully connected to the database.")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise e


def query_db(
    conn: sqlite3.Connection, query: str, params: Tuple[str] = ()
) -> List[object]:
    """
    Execute a query on the SQLite database.

    Args:
        conn (sqlite3.Connection): SQLite database connection object.
        query (str): SQL query to execute.

    Returns:
        results (List[object]): List of query results.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        logger.info("Query executed successfully.")

        return results
    except sqlite3.Error as e:
        logger.error(f"Error executing query: {e}")
        raise e


def export_sqlite_schema(
    conn: sqlite3.Connection, output_json_path: str = "schema.json"
):
    """
    Export the schema of all tables in an SQLite database to a JSON file.

    Args:
        conn (sqlite3.Connection): SQLite database connection object.
        output_json_path (str): Path for the output JSON file (default: 'schema.json')

    Returns:
        dict: Dictionary containing table names as keys and list of fields as values
    """
    try:
        # Dictionary to store schema
        schema = {}

        # Get all table names
        table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        tables = query_db(conn, table_query)

        # For each table, get its columns
        for table in tables:
            table_name = table[0]

            # Get column information using PRAGMA
            pragma_query = f"PRAGMA table_info({table_name});"
            columns = query_db(conn, pragma_query)

            # Extract column names and types
            # columns format: (cid, name, type, notnull, default_value, pk)
            fields = [
                {
                    "name": col[1],
                    "type": col[2],
                    "not_null": bool(col[3]),
                    "default": col[4],
                    "primary_key": bool(col[5]),
                }
                for col in columns
            ]

            schema[table_name] = fields

        # Write to JSON file
        with open(output_json_path, "w") as f:
            json.dump(schema, f, indent=2)

        logger.info(f"Schema exported successfully to {output_json_path}")
        logger.info(f"Found {len(schema)} tables")

        return schema

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise e

    except Exception as e:
        print(f"Error: {e}")
        raise e


def export_blobs(conn: sqlite3.Connection, run_id: int) -> None:
    """
    Function to export blobs (images) from the database to the filesystem.

    Args:
        conn (sqlite3.Connection): SQLite database connection object.

    Returns:
        None
    """
    # Create images directory if it doesn't exist
    images_path = "app/images/"
    run_path = os.path.join(images_path, f"run_{run_id}")
    os.makedirs(run_path, exist_ok=True)

    # Select blobs
    query = "SELECT fodImage, imageName FROM fodDataIdra WHERE runID = ?;"
    entries = query_db(conn, query, (run_id,))

    # Export each blob
    for e in entries:

        # Unpack entry
        blob = e[0]
        image_name = e[1]

        # Write blob to file
        image_path = os.path.join(run_path, image_name)

        if not os.path.isfile(image_path):

            with open(image_path, "wb") as f:
                f.write(blob)

            logger.info(f"Exported image: {image_name}")


if __name__ == "__main__":
    # Save information of the offline database locally
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True, help="Run number from offline db", type=int)

    args = parser.parse_args()

    # Unpack args
    run_id = args.run_id

    # Avoid hardcoding the database path
    db_path = list(glob.glob("**/*.sqlite", recursive=True))[0]

    # Load database
    db_conn = load_sqlite_db(db_path)

    # Query database
    schema_path = "app/offline-db/schemas/"
    os.makedirs(schema_path, exist_ok=True)

    schema_json_path = os.path.join(schema_path, "schema.json")

    if not os.path.isfile(schema_json_path):
        # Get tables in the database
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = query_db(db_conn, query)

        logger.info(f"Tables in the database: {tables}")

        # Export schema to JSON
        export_sqlite_schema(conn=db_conn, output_json_path=schema_json_path)
    else:
        logger.info(
            f"Schema file already exists at {schema_json_path}, skipping export."
        )

    # Find number of images per run
    query = "SELECT runID, COUNT(*) as count FROM fodDataIdra GROUP BY runID ORDER BY count DESC;"
    runs = query_db(db_conn, query)
    for run in runs:
        logger.info(f"Run ID: {run[0]}, Number of images: {run[1]}")

    # Export BLOB images from the database
    export_blobs(db_conn, run_id)

    # Export data as CSV
    def export_csv(table_name: str, run_id: int):
        """
        Helper function to export table query to CSV.

        Args:
            table_name(str): Name of table.
            run_id (int): Number of the run.

        Return:
            None
        """
        query = f"PRAGMA table_info({table_name})"
        columns = query_db(db_conn, query)
        non_blob_columns = [col[1] for col in columns if col[2].upper() != "BLOB"]
        columns_str = ", ".join(non_blob_columns)

        query = f"SELECT {columns_str} FROM {table_name} "
        if table_name == "runsIdra":
            query += "WHERE ID = ?;"
        elif table_name == "fodDataIdra":
            query += "WHERE runID = ?;"
        else:
            raise ValueError(f"{table_name} is not available in the offline database.")

        data = query_db(db_conn, query, (run_id,))

        csv_path = "app/offline-db/csv/"
        os.makedirs(csv_path, exist_ok=True)

        csv_file = os.path.join(csv_path, f"{table_name}_run_{run_id}.csv")

        with open(csv_file, "w") as f:
            writer = csv.writer(f)

            # Write header
            writer.writerow(non_blob_columns)

            # Write data rows
            writer.writerows(data)

    export_csv("fodDataIdra", run_id)
    export_csv("runsIdra", run_id)

    # Close db connection
    db_conn.close()
