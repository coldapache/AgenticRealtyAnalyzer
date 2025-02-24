"""
pullrealestatedata.py

This module fetches real estate listings data from the HasData Zillow API,
processes and transforms the data into a pandas DataFrame, and stores the processed
results into a SQLite database. It is configured specifically to retrieve for-sale
listings for Austin, TX, with robust logging and error handling in place.

Core functionalities include:
    - Making HTTP GET requests to the API with proper headers and query parameters.
    - Parsing the returned JSON structure and extracting necessary fields.
    - Cleaning, renaming, and filtering the data to match the expected database schema.
    - Storing the final DataFrame into a SQLite database ("data_analysis_db.db") under
      the table "realty_listings".
      
Author: Your Name
Date: YYYY-MM-DD
"""

import requests
import pandas as pd
import sqlite3
import os
from datetime import datetime

# =============================================================================
# API CONFIGURATION
# =============================================================================
API_ENDPOINT = "https://api.hasdata.com/scrape/zillow/listing"  # Correct endpoint for listings
API_KEY = "58f7b948-8c5c-4ec9-b5d5-88d050f2c3a4"
HEADERS = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json"
}
# Query parameters for the search. These specify the target location and type of listing.
SEARCH_PARAMS = {
    "keyword": "Burbank CA",
    "type": "forSale"
}

def parse_property_data(response_json):
    """
    Parse the JSON response from the Zillow listings API.

    This function processes the JSON response, extracts the necessary fields from each
    property listing, and compiles them into a list of dictionaries. In addition, it extracts
    metadata, search information, and pagination details from the response.

    Arguments:
        response_json (dict): The JSON response from the API call.

    Returns:
        dict: A dictionary containing:
            - 'metadata': API metadata extracted from the 'requestMetadata' key.
            - 'searchInfo': Search information details from 'searchInformation' key.
            - 'properties': A list of dictionaries for each property with keys:
                'id', 'url', 'image', 'price', 'addressRaw', 'city', 'state', 
                'zipcode', 'beds', 'baths', 'area', 'brokerName', 'latitude', 'longitude'
            - 'pagination': Pagination details from the 'pagination' key.
    """
    properties = []
    
    # Iterate over the "properties" array within the response JSON.
    for prop in response_json.get('properties', []):
        address = prop.get('address', {})
        property_data = {
            'id': prop.get('id'),  # Capture the unique property id. Will be later renamed to zpid.
            'url': prop.get('url'),
            'image': prop.get('image'),
            'price': prop.get('price'),
            'addressRaw': prop.get('addressRaw'),
            'city': address.get('city'),
            'state': address.get('state'),
            'zipcode': address.get('zipcode'),
            'beds': prop.get('beds'),
            'baths': prop.get('baths'),
            'area': prop.get('area'),
            'brokerName': prop.get('brokerName'),
            'latitude': prop.get('latitude'),
            'longitude': prop.get('longitude')
        }
        properties.append(property_data)
    
    return {
        'metadata': response_json.get('requestMetadata', {}),
        'searchInfo': response_json.get('searchInformation', {}),
        'properties': properties,
        'pagination': response_json.get('pagination', {})
    }

try:
    # -------------------------------------------------------------------------
    # Make API request: Fetch the listings data from HasData Zillow API.
    # -------------------------------------------------------------------------
    print(f"🔄 Making API request to {API_ENDPOINT}...")
    response = requests.get(
        API_ENDPOINT,
        headers=HEADERS,
        params=SEARCH_PARAMS  # Query parameters: keyword and type
    )
    print(f"✅ API response status: {response.status_code}")
    response.raise_for_status()
    
    # -------------------------------------------------------------------------
    # Parse JSON response
    # -------------------------------------------------------------------------
    print("📦 Parsing response JSON...")
    data = response.json()
    
    # -------------------------------------------------------------------------
    # Validate API metadata to check for errors in the response.
    # -------------------------------------------------------------------------
    metadata = data.get('requestMetadata', {})
    api_status = metadata.get('status', 'unknown')
    print(f"🔍 API Metadata Status: {api_status}")
    
    if api_status != 'ok':
        error_msg = metadata.get('errorMessage') or \
                    metadata.get('error') or \
                    metadata.get('message') or \
                    'Unknown API error (no details provided)'
        print(f"🔧 Request ID: {metadata.get('id', 'N/A')}")
        print(f"🔧 API Version: {metadata.get('apiVersion', 'unknown')}")
        print(f"🔧 Request Parameters: {metadata.get('params', 'N/A')}")
        raise ValueError(
            f"API Error: {error_msg}\n"
            f"Debug URL: {metadata.get('url', 'No debug URL available')}"
        )
    
    # -------------------------------------------------------------------------
    # Process the parsed JSON, extracting listings data.
    # -------------------------------------------------------------------------
    parsed_data = parse_property_data(data)
    
    # -------------------------------------------------------------------------
    # Convert the listings properties to a pandas DataFrame.
    # -------------------------------------------------------------------------
    df = pd.DataFrame(parsed_data['properties'])
    print(f"📊 DataFrame created with {len(df)} rows and {len(df.columns)} columns")
    print("🔍 Initial DataFrame columns:", df.columns.tolist())
    
    # -------------------------------------------------------------------------
    # Clean and transform the DataFrame:
    #    - Rename columns to match desired naming convention.
    #    - Filter the DataFrame to only include necessary columns.
    # -------------------------------------------------------------------------
    print("🧹 Cleaning and transforming data...")
    df = df.rename(columns={
        'beds': 'bedrooms',
        'baths': 'bathrooms',
        'area': 'sqft',
        'addressRaw': 'address',
        'id': 'zpid'  # Rename property ID to zpid
    })
    
    print("🔍 Columns after renaming:", df.columns.tolist())
    
    available_columns = df.columns.tolist()
    desired_columns = [
        'price', 'bedrooms', 'bathrooms', 'sqft', 'address', 'zpid',
        'city', 'state', 'zipcode', 'url', 'image', 'brokerName',
        'latitude', 'longitude'
    ]
    # Only keep columns that exist in the DataFrame.
    filtered_columns = [col for col in desired_columns if col in available_columns]
    df = df[filtered_columns]
    
    # Warning if any expected column is missing.
    for col in desired_columns:
        if col not in df.columns:
            print(f"⚠️ Warning: '{col}' column missing from DataFrame")
    
    # Ensure that numeric fields are correctly typed.
    df['sqft'] = df['sqft'].fillna(0).astype(int)
    # Add a timestamp column to record when the data was scraped.
    df['last_scraped'] = datetime.now()
    
    print("🔍 Final DataFrame structure:")
    print(df.dtypes)
    print("💰 Price value:", df['price'].iloc[0] if len(df) > 0 else 'N/A')
    
    # -------------------------------------------------------------------------
    # Database setup:
    #    - Database name is 'data_analysis_db.db'
    #    - The listings will be stored in the table 'realty_listings'
    # -------------------------------------------------------------------------
    db_name = "data_analysis_db.db"
    print(f"💾 Creating database: {db_name}")
    conn = sqlite3.connect(db_name)
    
    # Create table structure with all columns.
    # Adding UNIQUE constraint on zpid prevents duplicate property entries.
    print("🛠️ Updating table structure...")
    conn.execute('''CREATE TABLE IF NOT EXISTS realty_listings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  price REAL, bedrooms INTEGER, bathrooms REAL,
                  sqft INTEGER, address TEXT, zpid TEXT UNIQUE,
                  city TEXT, state TEXT, zipcode TEXT, url TEXT, image TEXT, brokerName TEXT,
                  latitude REAL, longitude REAL, last_scraped TIMESTAMP)''')
    
    # -------------------------------------------------------------------------
    # Insert the DataFrame records into the database table using a temporary table.
    print("📤 Inserting data into database table 'realty_listings'...")
    df.to_sql('realty_listings_temp', conn, if_exists='replace', index=False)
    conn.execute("""
        INSERT OR IGNORE INTO realty_listings
        (price, bedrooms, bathrooms, sqft, address, zpid, city, state, zipcode, url, image, brokerName, latitude, longitude, last_scraped)
        SELECT price, bedrooms, bathrooms, sqft, address, zpid, city, state, zipcode, url, image, brokerName, latitude, longitude, last_scraped
        FROM realty_listings_temp
    """)
    conn.commit()
    # Report the number of rows inserted.
    changes = conn.total_changes
    print(f"🎉 Successfully inserted {changes} record(s) (new rows added)!")
    
except KeyError as ke:
    # Handle errors related to missing keys in the JSON structure.
    print(f"🔴 Structural Error: {ke}")
    print("Available top-level keys:", list(data.keys()))
    if 'requestMetadata' in data:
        print("API Error Details:", data['requestMetadata'].get('error', 'No error details'))
except ValueError as ve:
    # Handle API-related value errors.
    print(f"🔴 API Error: {ve}")
    print(f"Request ID: {metadata.get('id', 'N/A')}")
    print(f"Error URL: {metadata.get('url', 'No debug URL available')}")
except requests.exceptions.HTTPError as err:
    # Capture HTTP errors which occur during the API request.
    print(f"🔴 HTTP Error ({response.status_code}): {err}")
    print(f"Response snippet: {response.text[:500]}")
except Exception as e:
    # General exception handling.
    print(f"🔴 Unexpected error: {type(e).__name__}")
    print(f"Error details: {str(e)}")
    print(f"Response content: {response.text[:500] if 'response' in locals() else 'No response'}")
finally:
    # Ensure the database connection is properly closed.
    if 'conn' in locals():
        conn.close()
        print("🔒 Database connection closed")
