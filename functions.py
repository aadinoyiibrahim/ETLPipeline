import requests
import pandas as pd
import sqlite3
import json
import re


def extract_api_data(api_url):
    """
    Extract data from a public API.
        param api_url: str: URL to the API
    """
    response = requests.get(api_url)
    data = response.json()  # Convert the response to JSON
    return data


def transform_api_data(data, nested_field=True):
    """
    Transform the data by selecting specific fields.

    param
        data: dict: Data from the API
        nested_field: bool: Extract nested fields
    """

    df = pd.DataFrame(data)  # convert data to a df
    if nested_field == True:
        # Extracting nested fields for city, lat., and long. only
        df["city"] = df["address"].apply(lambda x: x["city"])
        df["lat"] = df["address"].apply(lambda x: x["geo"]["lat"])
        df["lng"] = df["address"].apply(lambda x: x["geo"]["lng"])

        transformed_data = df[["id", "name", "username", "email", "city", "lat", "lng"]]
    else:
        # Select fewer columns
        transformed_data = df[["id", "name", "username", "email"]]
    return transformed_data


def load_data(data, db_name, table_name):
    """
    Load the transformed data into a SQLite database.
    """
    conn = sqlite3.connect(db_name)
    data.to_sql(
        table_name, conn, if_exists="replace", index=False
    )  # Replace the table if it exists
    conn.close()


def extract_faker_data(file_path):
    """
    Extract data from a CSV file.
    param file_path: str: Path to the CSV file
    """
    data = pd.read_csv(file_path)
    return data


def clean_address(address):
    """
    Clean the address string to make it valid JSON.
    param address: str: Address string
    """
    if pd.isnull(address):
        return None  # Handle NaN addresses

    # Replace single quotes with double quotes for valid JSON
    address = address.replace("'", '"')

    # Remove "Decimal" and convert to float
    address = re.sub(r"Decimal\((.*?)\)", r"\1", address)  # Remove Decimal wrapper
    address = address.replace("(", "").replace(")", "")  # Remove parentheses

    # Ensure there are no spaces after commas or colons
    address = re.sub(r"(?<=[{,])\s*", "", address)  # Remove whitespace after { or ,
    address = re.sub(r"\s*(?=[}:])", "", address)  # Remove whitespace before } or :

    try:
        # Try to parse as JSON
        return json.loads(address)
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError for address: {address} - Error: {e}")
        return None  # Return None for invalid JSON


def transform_faker_data(data):
    """
    Transform the data by selecting specific fields and flattening the address.
    param data: pd.DataFrame: Data from the CSV file
    """
    # Clean the address to ensure it is valid JSON
    data["address"] = data["address"].apply(clean_address)

    # Selecting relevant fields and flattening the address structure
    transformed_data = pd.DataFrame(
        {
            "id": data["id"],
            "name": data["name"],
            "username": data["username"],
            "email": data["email"],
            "city": data["address"].apply(
                lambda x: x["city"] if x is not None else None
            ),
            "lat": data["address"].apply(
                lambda x: x["geo"]["lat"] if x is not None else None
            ),
            "lng": data["address"].apply(
                lambda x: x["geo"]["lng"] if x is not None else None
            ),
        }
    )
    return transformed_data
