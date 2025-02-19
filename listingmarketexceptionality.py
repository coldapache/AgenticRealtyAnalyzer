#!/usr/bin/env python
"""
listingmarketexceptionality.py

This script performs analysis for each property in the 'realty_listings' table.
For each listing, it:
  - Checks for an existing analysis to avoid duplicates.
  - Retrieves all other listings in the same city that have the same number of bedrooms and bathrooms.
  - Computes summary statistics (min, max, average price) from those comparable listings.
  - Builds a prompt that includes the property's details and a market summary (including the count of comparable properties).
  - Calls an LLM (via the locally running Ollama API, model e.g. "mistral:7b") to classify the price as:
       "Good deal", "Average deal", or "Bad deal".
  - If a valid classification is returned and spatial data is valid, the analysis is inserted into the
    'listing_analysis' table with all extended attributes.
"""

import sqlite3
import pandas as pd
import requests
from datetime import datetime
import time

# --- CONFIGURATION ---
DB_PATH = 'data_analysis_db.db'
OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL = "mistral:7b"  # Parameter for the LLM model

# --- DATABASE FUNCTIONS ---

def ensure_analysis_table():
    """
    Creates the listing_analysis table if it does not exist.
    NOTE: If the table exists with an old schema (missing city/price), you must drop/recreate it.
    """
    print("🔄 Ensuring the 'listing_analysis' table exists with extended attributes...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS listing_analysis (
                address TEXT PRIMARY KEY,
                city TEXT,
                price REAL,
                bedrooms INTEGER,
                bathrooms REAL,
                latitude REAL,
                longitude REAL,
                market_exceptionality TEXT,
                analyzed_at TEXT
            )
        """)
        conn.commit()
        print("✅ 'listing_analysis' table verified/created with extended attributes.")
    except Exception as e:
        print(f"❌ Error ensuring 'listing_analysis' table: {e}")
    finally:
        conn.close()

def analysis_exists(address):
    """
    Checks if analysis already exists for the given address.
    
    Returns:
        bool: True if found, False otherwise.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM listing_analysis WHERE address = ?", (address,))
        exists = cur.fetchone() is not None
        return exists
    except Exception as e:
        print(f"❌ Error checking analysis existence for {address}: {e}")
        return False
    finally:
        conn.close()

def insert_analysis(address, city, price, bedrooms, bathrooms, latitude, longitude, assessment):
    """
    Inserts the LLM analysis into the listing_analysis table, including extended attributes.
    
    Parameters:
        address (str): Property address.
        city (str): City of the property.
        price (float): Listing price.
        bedrooms (int): Number of bedrooms.
        bathrooms (float): Number of bathrooms.
        latitude (float): Latitude value (validated).
        longitude (float): Longitude value (validated).
        assessment (str): LLM classification ("Good deal", "Average deal", or "Bad deal").
    """
    conn = None
    try:
        # Always convert lat/lon from the listing
        lat = pd.to_numeric(latitude, errors='coerce')
        lon = pd.to_numeric(longitude, errors='coerce')
        if pd.isna(lat) or pd.isna(lon):
            print(f"⚠️ Skipping insertion for {address} due to invalid coordinates: lat={latitude}, lon={longitude}")
            return

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO listing_analysis 
            (address, city, price, bedrooms, bathrooms, latitude, longitude, market_exceptionality, analyzed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            address,
            city,
            price,
            bedrooms,
            bathrooms,
            lat,
            lon,
            assessment,
            datetime.now().isoformat()
        ))
        conn.commit()
        print(f"✅ Inserted analysis for {address}")
    except Exception as e:
        print(f"❌ Error inserting analysis for {address}: {e}")
    finally:
        if conn is not None:
            conn.close()

def get_market_assessment(prompt):
    """
    Calls the Ollama LLM API with the given prompt and expects exactly one of:
        "Good deal", "Average deal", "Bad deal".
    
    Returns:
        str or None: The valid classification if successful, or None.
    """
    print("📝 Sending prompt to LLM:")
    print(prompt)
    
    def call_ollama(prompt, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    OLLAMA_API_URL,
                    json={"model": MODEL, "prompt": prompt, "stream": False},
                    timeout=60
                )
                response.raise_for_status()
                resp_text = response.json().get('response', '').strip()
                print(f"LLM response (attempt {attempt+1}): {resp_text}")
                return resp_text
            except Exception as e:
                print(f"⚠️ LLM call error (attempt {attempt+1}): {e}")
                time.sleep(2)
        return None

    response_text = call_ollama(prompt)
    if not response_text:
        print("🔴 LLM assessment failed to produce any response.")
        return None

    valid_choices = {"good deal", "average deal", "bad deal"}
    for choice in valid_choices:
        if choice in response_text.lower():
            if choice == "good deal":
                return "Good deal"
            elif choice == "average deal":
                return "Average deal"
            elif choice == "bad deal":
                return "Bad deal"
    print(f"🟡 LLM returned an invalid classification: {response_text}")
    return None

def get_all_listings():
    """
    Retrieves cleaned realty listings that have valid spatial data.
    
    Returns:
        pd.DataFrame: A DataFrame of valid listings.
    """
    print("🔄 Retrieving and cleaning listings...")
    try:
        conn = sqlite3.connect(DB_PATH)
        total_rows = pd.read_sql_query("SELECT COUNT(*) as total FROM realty_listings", conn).iloc[0]['total']
        print(f"🗄  Total rows in realty_listings: {total_rows}")
        query = """
            SELECT 
                address, 
                CAST(price AS REAL) as price,
                CAST(bedrooms AS INTEGER) as bedrooms,
                CAST(bathrooms AS REAL) as bathrooms,
                latitude,
                longitude,
                city,
                state,
                zipcode
            FROM realty_listings
            WHERE 
                city IS NOT NULL 
                AND state IS NOT NULL 
                AND zipcode IS NOT NULL
                AND price > 10000
                AND bedrooms > 0
                AND bathrooms > 0
        """
        df = pd.read_sql_query(query, conn)
        print(f"✅ Cleaned data: {len(df)} valid listings")
        if not df.empty:
            print("Sample listings:")
            print(df.head(2))
        return df
    except Exception as e:
        print(f"❌ Data retrieval error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_city_spec_listings(city, current_address, bedrooms, bathrooms):
    """
    Retrieves listings from the same city that match the exact number of bedrooms and bathrooms,
    excluding the current listing.
    
    Parameters:
        city (str): City name.
        current_address (str): The address to exclude.
        bedrooms (int): Number of bedrooms to match.
        bathrooms (float): Number of bathrooms to match.
        
    Returns:
        pd.DataFrame: A DataFrame of comparable listings.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT price, bedrooms, bathrooms
            FROM realty_listings
            WHERE 
                city = ? 
                AND address != ?
                AND bedrooms = ?
                AND bathrooms = ?
        """
        df = pd.read_sql_query(query, conn, params=(city, current_address, bedrooms, bathrooms))
        return df
    except Exception as e:
        print(f"❌ Error retrieving city listings for spec {bedrooms}bd/{bathrooms}ba in {city}: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def compute_city_stats(city_listings_df):
    """
    Computes summary statistics from the comparable listings.
    
    Returns:
        dict: Containing count, average price, min/max price.
    """
    if city_listings_df.empty or len(city_listings_df) < 1:
        return None
    stats = {
        "count": len(city_listings_df),
        "avg_price": city_listings_df['price'].mean(),
        "min_price": city_listings_df['price'].min(),
        "max_price": city_listings_df['price'].max()
    }
    return stats

def run_analysis():
    print("🚀 Starting market analysis...")
    ensure_analysis_table()
    listings = get_all_listings()
    if listings.empty:
        print("❌ No valid listings found. Exiting.")
        return

    # Process each property iteratively
    for idx, listing in listings.iterrows():
        address = listing['address']
        print("\n-------------------------")
        print(f"📍 Analyzing property: {address}")

        if analysis_exists(address):
            print(f"⏭️ Analysis already exists for {address}. Skipping.")
            continue

        # Ensure the listing has valid spatial data
        if pd.isna(listing['latitude']) or pd.isna(listing['longitude']):
            print(f"⚠️ Skipping {address} due to missing coordinates.")
            continue

        city = listing['city']
        beds = listing['bedrooms']
        baths = listing['bathrooms']
        print(f"🏙️  Looking for comparable listings in {city} with {beds}bd/{baths}ba (excluding current address).")
        spec_listings = get_city_spec_listings(city, address, beds, baths)
        if spec_listings.empty:
            print(f"⏭️ No comparable listings found in {city} for a {beds}bd/{baths}ba property. Skipping {address}.")
            continue
        
        stats = compute_city_stats(spec_listings)
        if stats is None or stats["count"] < 1:
            print(f"⏭️ Insufficient market data for {city} for a {beds}bd/{baths}ba property. Skipping {address}.")
            continue

        # Build the prompt including city, price, and comparable count details.
        prompt = f"""Analyze the pricing of the following property relative to its city's market.

Property Details:
  Address: {address}
  City: {city}
  Price: ${listing['price']:,.2f}
  Bedrooms: {beds}
  Bathrooms: {baths}
  Latitude: {listing['latitude']}
  Longitude: {listing['longitude']}

City Market Data for properties with {beds}bd and {baths}ba in {city} (based on {stats['count']} comparable listings):
  Average Price: ${stats['avg_price']:,.2f}
  Price Range: ${stats['min_price']:,.2f} - ${stats['max_price']:,.2f}

Based on the above, classify this property's price as one of:
  - Good deal
  - Average deal
  - Bad deal

Respond with exactly one of these options."""
        
        assessment = get_market_assessment(prompt)
        if not assessment:
            print(f"⏭️ Skipping property {address} due to invalid LLM assessment.")
            continue
        
        insert_analysis(
            address,
            city,
            listing['price'],
            beds,
            baths,
            listing['latitude'],
            listing['longitude'],
            assessment
        )
        time.sleep(1)

if __name__ == "__main__":
    run_analysis()
