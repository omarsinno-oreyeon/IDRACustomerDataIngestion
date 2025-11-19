"""
Lambda function to query IDRA COMMERCIAL DB.
"""

# Standard libraries
from datetime import date, datetime
from typing import List, Tuple
import logging
import json
import glob
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger()
logger.setLevel("INFO")

# Third party libraries
import dotenv
import mysql.connector

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))

def execute_query(conn, query, params, execution_type="once"):
    """
    Helper function to send single query.

    Args:
        conn
        query (str): Parameterized query string.
        params (List[str]): List of parameters


    """
    cursor = conn.cursor(dictionary=True)
    if conn.is_connected():
        # execute the query (whether read or push)

        if execution_type == "once":
            cursor.execute(query, params)
        elif execution_type == "many":
            cursor.executemany(query, params)

        # In case of push, the following variable records will be empty
        # In case of read, this records variable will have the returned results by the executed query
        records = cursor.fetchall()

        # Commit the changes if any to the database
        conn.commit()

        status_code = 200
        body = json.dumps(records, default=json_serial)

        cursor.close()
        return body

    else:
        cursor.close()
        raise ConnectionError("Error connecting to the database.")


def set_connection(env="prod"):
    """
    Setup db connection.

    Args:
    """
    # Find alias
    env_file = f".env.{env}.txt"

    # Lookup for env file
    env_file_path = os.path.join(
        os.getcwd(),
        "app",
        env_file
    )

    # Load environment variables
    if not dotenv.load_dotenv(env_file_path):
        raise Exception("Environment variables couldn't be loaded.")

    # Unpack environment variables
    db_configuration = {
        "host": os.environ["HOST"],
        "port": os.environ["PORT"],
        "username": os.environ["USERNAME"],
        "password": os.environ["PASSWORD"],
        "database": os.environ["DATABASE"]
    }

    conn = mysql.connector.connect(**db_configuration)

    return conn
