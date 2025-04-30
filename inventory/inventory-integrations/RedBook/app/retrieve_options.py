"""This module retrieves RedBook options for a given RedBook code."""

import json
import logging
from os import environ

import boto3
import requests
from botocore.exceptions import ClientError

from utils.db_config import get_connection

secrets_client = boto3.client("secretsmanager")

ENV = environ.get("ENVIRONMENT", "stage")
SCHEMA = "prod" if ENV == "prod" else "stage"
REDBOOK_API_ENDPOINT = "https://api.redbookdirect.com/v1/au/car/Vehicles/"
TOKEN_URL = "https://api.redbookdirect.com/token"

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """This function is the entry point for the Lambda function."""
    logger.info(f"Event: {event}")
    conn = None
    cursor = None
    redbook_code = str(event.get("redbookCode"))

    try:
        conn = get_connection()
        cursor = conn.cursor()

        results = check_db(cursor, redbook_code)
        if results:
            data = {"success": True, "results": results}
            logger.info(f"Returning data from DB: {data}")
            return json.dumps(data)

    except Exception as db_exception:
        logger.exception(f"Error accessing the DB: {db_exception}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    try:
        redbook_data = fetch_redbook_data(rbc=redbook_code)

        if not redbook_data.get("totalCount"):
            data = {"success": False, "message": "No valid data was retrieved from RedBook's API"}
            logger.warning(f"No valid data from RedBook API for code {redbook_code}")
            return json.dumps(data)

        processed_data = process(redbook_data)
        data = {"success": True, "results": processed_data}

        conn = get_connection()
        cursor = conn.cursor()
        save_data_to_db(redbook_code, processed_data, conn, cursor)

    except Exception as api_exception:
        logger.exception(f"Error processing data from RedBook API: {api_exception}")
        data = {"success": False, "message": str(api_exception)}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    logger.info(f"Returning retrieved data: {data}")
    return json.dumps(data)


def check_db(cursor=None, rbc=None):
    """Checks db for RedBook data."""
    if not cursor:
        raise ValueError("Database cursor is required.")
    if not rbc:
        raise ValueError("RedBook code (rbc) is required.")

    query = f"""
        SELECT options
        FROM {SCHEMA}.inv_redbook_code_options
        WHERE redbook_code = %s
    """
    cursor.execute(query, (rbc,))
    rows = cursor.fetchall()
    logger.info(f"Rows fetched from DB: {rows}")

    if not rows:
        logger.info(f"No data found in the database for RedBook code: {rbc}")
        return None

    return list(rows[0][0]) if rows[0] else None


def fetch_redbook_data(retries=3, rbc=None):
    """Fetches data from RedBook API using a valid token, with a retry mechanism."""
    if not rbc:
        raise ValueError("RedBook code (rbc) is required.")

    token = retrieve_token()
    if not token:
        token = refresh_token()
        save_token(token)

    endpoint = REDBOOK_API_ENDPOINT + rbc + "/standards"
    for _ in range(retries):
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(endpoint, headers=headers)
        logger.info(f"Response from RedBook API: {response.status_code} - {response.text}")

        if response.status_code == 200:
            return response.json()

        elif response.status_code == 401:
            token = refresh_token()
            save_token(token)

    response.raise_for_status()


def process(redbook_data):
    """Fetches data and concatenates equipment names."""

    equipment_names = [item["equipmentname"] for item in redbook_data["results"]]

    return equipment_names


def refresh_token():
    """Obtains a new token using the API key."""
    secret_id = f"{'prod' if ENV == 'prod' else 'test'}/redbook-api"
    secret_string = json.loads(
        secrets_client.get_secret_value(SecretId=secret_id)["SecretString"]
    )
    api_key = secret_string["api_key"]

    headers = {"x-api-key": api_key}

    try:
        response = requests.get(TOKEN_URL, headers=headers)
        response.raise_for_status()
        new_token = response.json().get("accessToken")

        if not new_token:
            raise ValueError("The response did not contain an 'accessToken'")

        return new_token

    except requests.exceptions.HTTPError as http_err:
        logger.error("HTTP error occurred while refreshing token: %s", http_err)
        raise

    except Exception as err:
        logger.error("Error occurred while refreshing token: %s", err)
        raise


def save_token(token):
    """Saves the new token in AWS Secrets Manager."""
    token_secret_id = f"{'prod' if ENV == 'prod' else 'test'}/redbook-token"
    token_dict = json.dumps({"token": token})

    try:
        secrets_client.put_secret_value(SecretId=token_secret_id, SecretString=token_dict)
        logger.info("New token saved successfully in Secrets Manager.")

    except Exception as e:
        logger.error("Failed to save the token in Secrets Manager: %s", e)
        raise


def retrieve_token():
    """Retrieves the token from AWS Secrets Manager."""
    token_secret_id = f"{'prod' if ENV == 'prod' else 'test'}/redbook-token"
    try:
        logger.info(f"secret id: {token_secret_id}")
        response = json.loads(
            secrets_client.get_secret_value(SecretId=token_secret_id)["SecretString"]
        )
        token = response["token"]
        logger.info(f"secret value: {response}")
        return token

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.info("token not found, retrieving a new one")
        return None


def save_data_to_db(rbc, options, conn, cursor):
    """Implements logic to save data to the database."""
    insert_query = f"""
        INSERT INTO {SCHEMA}.inv_redbook_code_options (redbook_code, options)
        VALUES (%s, %s);
    """
    try:
        cursor.execute(insert_query, (rbc, options))
        conn.commit()
    except Exception as e:
        logger.error("Failed to save data to the database: %s", e)
        raise
