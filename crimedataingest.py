#!/usr/bin/env python
r"""
crimedataingest.py

This script consolidates disparate crime CSV datasets from a folder and stores the incidents in a database based on:
  - Date of crime
  - Crime type
  - Latitude
  - Longitude

  This script only pulls the last year from TODAY of crime data from any csv it finds in the C:\Env\DataAnalyzer\CrimeData\DataRepo


For each crime, the script:
  - Stores the crime type, latitude, longitude, and date of the crime in the database.

"""

import os
import glob
import pandas as pd
import sqlite3
import requests
import json
from datetime import datetime
import re
import traceback

# --- CONFIGURATION ---
CSV_FOLDER = r"C:\Env\DataAnalyzer\CrimeData\DataRepo"  # Folder containing CSV files.
DB_PATH = 'data_analysis_db.db'
OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL = "mistral:7b"  # Using the base mistral model.
MAX_ALLOWED_TOKENS = 8000  # Not a primary concern here.

# Mapping for standard columns (each standard field may have multiple candidate names).
STANDARD_COLUMNS = {
    "crime_date": ["dateofcrime", "dateofoffense", "date", "CrimeDate", "RMSOccurrenceDate", "CrimeDateTime"],
    "crime_type": ["crime_type", "crimetype", "type", "offense", "incident_type", "crime_category", "offense_description", "description", "crime_description", "detail", "Description", "NIBRSDescription"],
    "latitude": ["latitude", "lat", "Latitude", "MapLatitude"],
    "longitude": ["longitude", "lon", "lng", "long", "Longitude", "MapLongitude"],
}

# --- DATABASE SETUP ---
CRIME_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS crime_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crime_date TEXT NOT NULL,
    crime_type TEXT,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    source_file TEXT,
    ingested_at TEXT,
    UNIQUE(crime_date, latitude, longitude)
)
"""

def ensure_table_exists():
    """Ensures crime_data table exists before operations"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(CRIME_TABLE_SCHEMA)
        print("✅ Verified/created crime_data table")

# --- HELPER FUNCTIONS ---

def clean_name(name):
    """Column name normalization with debug logging"""
    original = name
    cleaned = re.sub(r'[^a-z0-9]', '', str(name).lower())
    print(f"DEBUG: Cleaned column '{original}' → '{cleaned}'")
    return cleaned

def get_csv_files():
    """File discovery with path verification"""
    print(f"\n🔍 Checking data folder: {CSV_FOLDER}")
    if not os.path.exists(CSV_FOLDER):
        print(f"🔥 ERROR: Folder does not exist: {CSV_FOLDER}")
        return []
    
    csv_files = glob.glob(os.path.join(CSV_FOLDER, "*.csv"))
    print("Found files:")
    for f in csv_files:
        print(f" - {os.path.basename(f)}")
    
    return csv_files

def load_csv_file(file_path):
    """
    Load a CSV file containing crime data into a pandas DataFrame.
    
    This function is critical for the initial data ingestion step of the crime analysis pipeline.
    It handles loading potentially large CSV files with crime records while providing:
    - UTF-8 encoding support for special characters in location names and descriptions
    - Low memory usage optimization for large datasets
    - Robust error handling with informative debug messages
    - Graceful failure by returning empty DataFrame rather than crashing
    
    Args:
        file_path (str): Path to the CSV file to load
        
    Returns:
        pd.DataFrame: DataFrame containing the crime data, or empty DataFrame if load fails
    """
    try:
        # Attempt to load the CSV file with optimized settings
        # - encoding='utf-8' handles special characters 
        # - low_memory=False prevents mixed type inference issues
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
        
        # Log successful load with row count for debugging
        print(f"DEBUG: Loaded file: {file_path} with {len(df)} rows.")
        
        return df

    except Exception as e:
        # Log detailed error message to help diagnose data loading issues
        print(f"ERROR: Failed reading {file_path}: {e}")
        
        # Return empty DataFrame to allow pipeline to continue
        return pd.DataFrame()

def standardize_columns(df):
    """Column mapping with detailed diagnostics"""
    print("\n🔍 Starting column standardization")
    print(f"DEBUG: Original columns: {df.columns.tolist()}")
    
    # Clean all column names first
    df.columns = [clean_name(col) for col in df.columns]
    print(f"DEBUG: Cleaned columns: {df.columns.tolist()}")
    
    column_map = {}
    for standard_col, variants in STANDARD_COLUMNS.items():
        print(f"\n🔎 Mapping variants for '{standard_col}':")
        for variant in variants:
            clean_variant = clean_name(variant)
            print(f"  Checking variant '{variant}' → '{clean_variant}'")
            if clean_variant in df.columns:
                print(f"  ✅ Matched '{clean_variant}' to '{standard_col}'")
                column_map[clean_variant] = standard_col
                break
        else:
            print(f"  ❌ No variant found for '{standard_col}' in columns")
    
    # Validate required columns
    required = ['crime_date', 'latitude', 'longitude']
    missing = [col for col in required if col not in column_map.values()]
    
    if missing:
        print(f"\n🔥 CRITICAL: Missing mappings for {missing}")
        print("Possible column matches:")
        for col in df.columns:
            print(f" - '{col}'")
        print("\nSuggestions:")
        for col in required:
            variants = ", ".join(STANDARD_COLUMNS[col])
            print(f"{col}: Consider adding variants like {variants}")
        return None
    
    print(f"\n✅ Final column map: {column_map}")
    return df.rename(columns=column_map)[column_map.values()]

def validate_dates(df):
    """Ensure valid dates within last year with format specification"""
    # Explicit copy to prevent SettingWithCopyWarning
    df = df.copy()
    
    # Specify common date formats to try
    df['crime_date'] = pd.to_datetime(
        df['crime_date'],
        format='mixed',
        errors='coerce',
        dayfirst=False
    )
    
    one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
    filtered = df[df['crime_date'] >= one_year_ago].copy()
    print(f"DEBUG: Date filtering kept {len(filtered)}/{len(df)} records")
    return filtered

def validate_coordinates(df):
    """Clean and validate geographic coordinates with proper copy"""
    df = df.copy()
    for col in ['latitude', 'longitude']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    valid_coords = df.dropna(subset=['latitude', 'longitude']).copy()
    print(f"DEBUG: Coordinate validation kept {len(valid_coords)}/{len(df)} records")
    return valid_coords

def process_file(file_path):
    """Processing with proper column discovery"""
    try:
        # Load all columns first
        print(f"\n📂 Processing: {os.path.basename(file_path)}")
        raw_df = pd.read_csv(file_path, low_memory=False)
        print(f"DEBUG: Loaded {len(raw_df.columns)} columns: {raw_df.columns.tolist()}")
        
        # Standardize and validate
        std_df = standardize_columns(raw_df)
        if std_df is None: 
            print("⚠️ Skipping file due to column mapping failure")
            return
        
        # Continue with validation pipeline
        validated_df = (
            std_df
            .pipe(validate_dates)
            .pipe(validate_coordinates)
            .drop_duplicates(subset=['crime_date', 'latitude', 'longitude'])
        )
        
        # Add metadata and insert
        if not validated_df.empty:
            validated_df['source_file'] = os.path.basename(file_path)
            validated_df['ingested_at'] = datetime.now().isoformat()
            
            with sqlite3.connect(DB_PATH) as conn:
                for batch in chunk_dataframe(validated_df, chunk_size=250):
                    batch.to_sql(
                        'crime_data', 
                        conn, 
                        if_exists='append', 
                        index=False
                    )
                    print(f"💾 Inserted batch of {len(batch)} rows")
            
    except Exception as e:
        print(f"❌ ERROR processing {file_path}: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")

def chunk_dataframe(df, chunk_size=250):  # Reduced from 500
    """Smaller batches for SQLite safety"""
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i:i + chunk_size]

def main():
    """Main entry point with table verification"""
    print("\n🔍 Starting crime data ingestion")
    ensure_table_exists()
    
    files = get_csv_files()
    if not files:
        print("⏹️ No files to process")
        return
    
    for file_path in files:
        process_file(file_path)
    
    print("\n✅ Completed processing")

if __name__ == "__main__":
    main()
