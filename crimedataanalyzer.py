#!/usr/bin/env python
r"""
crimedataanalyzer.py

This script consolidates disparate crime CSV datasets from a folder and pre-clusters the incidents based on:
  - Month,
  - Crime type, and
  - Spatial proximity (based on latitude and longitude bins).

For each resulting spatial cluster (or subcluster if the cluster is too large) that has between 10 and 20 incidents,
the script:
  - Computes the geographical center,
  - Consolidates incident details into a summary text,
  - Prompts a locally running LLM (via the Ollama API) to provide a plain-language description of the cluster,
  - Immediately inserts the LLM's descriptive output (both raw and formatted) into the SQLite database,
  - Provides debug confirmation at every step.
"""

import os
import glob
import pandas as pd
import sqlite3
import requests
import json
from datetime import datetime

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

# Prioritized crime types.
PRIORITIZED_CRIME_TYPES = [
    "HOMICIDE",
    "AUTO THEFT",
    "BURGLARY",
    "LARCENY FROM AUTO",
    "COMMON ASSAULT",
    "ROBBERY",
    "FRAUD",
    "AGG. ASSAULT",
    "LARCENY",
    "VANDALISM",
    "LARCENY OF MOTOR VEHICLE PARTS OR ACCESSORIES",
    "STOLEN PROPERTY",
    "INTIMIDATION",
    "ROBBERY - COMMERCIAL",
    "ROBBERY - CARJACKING",
    "SEX OFFENSES",
    "ARSON",
    "KIDNAPPING",
    "RAPE",
    "DRUG/NARCOTIC VIOLATIONS",
    "ANIMAL CRUELTY",
    "EXTORTION",
    "HUMAN TRAFFICKING",
    "PORNOGRAPHY"
]

# --- HELPER FUNCTIONS ---

def approximate_token_count(text):
    """Estimate token count using a rough average of 1 token per 4 characters."""
    return len(text) // 4

def clean_name(name):
    """Normalize a column name by lowercasing and removing underscores and spaces."""
    return name.lower().replace("_", "").replace(" ", "")

def get_all_csv_files(folder_path):
    """Retrieve the most recently added CSV file path from the specified folder."""
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        print("No CSV files found in the folder.")
        return []
    # Sort the files by last modification time (most recent first)
    csv_files.sort(key=os.path.getmtime, reverse=True)
    most_recent_file = csv_files[0]
    print(f"DEBUG: Found {len(csv_files)} CSV file(s) in {folder_path}. Most recent file: {most_recent_file}")
    return [most_recent_file]

def load_csv_file(file_path):
    """Load a CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
        print(f"DEBUG: Loaded file: {file_path} with {len(df)} rows.")
        return df
    except Exception as e:
        print(f"ERROR: Failed reading {file_path}: {e}")
        return pd.DataFrame()

def standardize_columns(df, mapping):
    """
    Rename columns based on candidate names from the provided mapping.
    For each standard field, use the first matching candidate.
    """
    lower_cols = {clean_name(col): col for col in df.columns}
    rename_dict = {}
    for standard_col, candidates in mapping.items():
        for candidate in candidates:
            candidate_clean = clean_name(candidate)
            if candidate_clean in lower_cols:
                rename_dict[lower_cols[candidate_clean]] = standard_col
                break
    df = df.rename(columns=rename_dict)
    required = ['crime_date', 'latitude', 'longitude']
    missing_required = [col for col in required if col not in df.columns]
    if missing_required:
        print(f"ERROR: Missing required fields {missing_required}.")
        return None
    if 'crime_type' not in df.columns:
        print("DEBUG: Optional field 'crime_type' not found; filling with empty values.")
        df['crime_type'] = ""
    desired = [col for col in mapping.keys() if col in df.columns]
    print("DEBUG: Standardized columns available:", desired)
    return df[desired]

def consolidate_cluster_data(df, crime_type):
    """
    Consolidate details from the incidents in the cluster.
    Returns a string summarizing each incident's date, coordinates, and description.
    """
    text = f"Crime Type: {crime_type.upper()} (Total incidents: {len(df)})\n"
    for _, row in df.iterrows():
        text += f"Date: {row['crime_date']}, Coordinates: ({row['latitude']}, {row['longitude']}), Description: {row.get('description', '')}\n"
    return text

def simple_spatial_cluster(df, precision=2, min_cluster_size=10):
    """
    Performs simple spatial clustering based on binned coordinates.
    Rounds latitude and longitude to 'precision' decimal places and groups incidents.
    Returns a list of DataFrame clusters that have at least 'min_cluster_size' incidents.
    """
    df = df.copy()
    df['lat_bin'] = df['latitude'].astype(float).round(precision)
    df['lon_bin'] = df['longitude'].astype(float).round(precision)
    clusters = []
    grouped = df.groupby(['lat_bin', 'lon_bin'])
    for (lat_bin, lon_bin), group in grouped:
        if len(group) >= min_cluster_size:
            clusters.append(group)
    return clusters

def split_cluster(cluster_df, max_size=50):
    """
    If a cluster is too big (more than max_size incidents), split it into sequential chunks.
    Returns a list of DataFrame chunks.
    """
    chunks = []
    sorted_df = cluster_df.sort_values(by="crime_date")
    for i in range(0, len(sorted_df), max_size):
        chunk = sorted_df.iloc[i:i+max_size]
        chunks.append(chunk)
    return chunks

def get_crime_analysis(prompt):
    """
    Sends the prompt to the LLM via the Ollama API and returns the consolidated
    plain text description. This function handles streaming JSON responses.
    """
    try:
        payload = {"model": MODEL, "prompt": prompt, "max_tokens": MAX_ALLOWED_TOKENS}
        response = requests.post(OLLAMA_API_URL, json=payload)
        if response.status_code == 200:
            complete_text = ""
            # Process streaming response lines.
            for line in response.text.strip().splitlines():
                try:
                    token_data = json.loads(line)
                    complete_text += token_data.get("response", "")
                except Exception as e:
                    print("WARNING: Failed to parse line:", line, e)
            return complete_text.strip()
        else:
            print("ERROR: LLM API returned non-200 status:", response.status_code)
            return ""
    except Exception as e:
        print("ERROR: Exception while getting LLM analysis:", e)
        return ""

def ensure_llm_response_table(db_path):
    """
    Ensures the llm_responses table exists in the database.
    Drops any pre-existing table and re-creates it with the proper schema.
    """
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS llm_responses")
        cur.execute("""
            CREATE TABLE llm_responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT,
                crime_type TEXT,
                response TEXT,
                created_at TEXT
            )
        """)
        conn.commit()
        print("DEBUG: llm_responses table ensured.")
    except Exception as e:
        print("ERROR: Could not create llm_responses table:", e)
    finally:
        conn.close()

def insert_llm_response(month, crime, response_text, db_path):
    """
    Inserts an LLM response record into the llm_responses table.
    """
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO llm_responses (month, crime_type, response, created_at)
            VALUES (?, ?, ?, ?)
        """, (month, crime, response_text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        print(f"DEBUG: Inserted LLM response for {crime.upper()} in {month}.")
    except Exception as e:
        print("ERROR: Failed to insert llm response:", e)
    finally:
        conn.close()

def ensure_crime_analysis_table(db_path):
    """
    Ensures that the crime_analysis table exists in the database.
    """
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crime_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crime_type TEXT,
                latitude REAL,
                longitude REAL,
                incident_count INTEGER,
                cluster_description TEXT,
                analyzed_at TEXT
            )
        """)
        conn.commit()
        print("DEBUG: crime_analysis table ensured.")
    except Exception as e:
        print(f"ERROR: Failed to create crime_analysis table: {e}")
    finally:
        conn.close()

def insert_cluster(cluster_record, db_path):
    """
    Insert a single cluster record into the crime_analysis table.
    """
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO crime_analysis (crime_type, latitude, longitude, incident_count, cluster_description, analyzed_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            cluster_record.get("crime_type", ""),
            cluster_record["coordinates"][0],
            cluster_record["coordinates"][1],
            cluster_record["incident_count"],
            cluster_record.get("description", ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))
        conn.commit()
        cluster_id = cur.lastrowid
        print(f"DEBUG: Successfully inserted cluster record with ID {cluster_id} for {cluster_record.get('crime_type','')} ({cluster_record['incident_count']} incidents).")
    except Exception as e:
        print(f"ERROR: Failed to insert cluster: {e}")
    finally:
        conn.close()

def process_subcluster(sub_df, month_name, crime):
    """
    Process each subcluster immediately:
      - Compute the geographic center.
      - Consolidate incident details.
      - Build the LLM prompt.
      - Send the prompt and retrieve analysis.
      - Immediately insert the raw LLM response and the final cluster record into the database.
    """
    incident_count = len(sub_df)
    center_lat = sub_df['latitude'].astype(float).mean()
    center_lon = sub_df['longitude'].astype(float).mean()
    consolidated = consolidate_cluster_data(sub_df, crime)
    
    prompt = (
        f"The following crime incidents are part of a '{crime.upper()}' cluster that occurred in {month_name} "
        f"near coordinates ({center_lat:.5f}, {center_lon:.5f}). This cluster contains {incident_count} incidents.\n\n"
        "Details of the incidents:\n" +
        consolidated +
        "\nPlease provide a short (no more than two paragraphs) and concise description of this crime cluster, highlighting any common patterns, potential causes, and notable observations."
    )
    print("DEBUG: Sending prompt to LLM for cluster (first 200 characters):", prompt[:200])
    
    llm_response = get_crime_analysis(prompt)
    if not llm_response:
        print(f"DEBUG: LLM returned an empty response for {crime.upper()} cluster in {month_name}. Skipping this cluster.")
        return
    print("DEBUG: Received LLM analysis (first 200 characters):", llm_response[:200])
    
    # Immediately store the raw LLM response.
    ensure_llm_response_table(DB_PATH)
    insert_llm_response(month_name, crime, llm_response, DB_PATH)
    
    # Build and insert the final cluster record.
    cluster_record = {
        "crime_type": crime.upper(),
        "coordinates": [center_lat, center_lon],
        "incident_count": incident_count,
        "description": llm_response
    }
    ensure_crime_analysis_table(DB_PATH)
    insert_cluster(cluster_record, DB_PATH)
    
    print(f"DEBUG: Successfully processed and inserted analysis for {crime.upper()} cluster in {month_name}.\n")

# --- NEW STATISTICAL CLUSTER ANALYSIS FUNCTION ---
def process_statistical_crime_clusters(df, threshold=50, precision=2):
    """
    Processes large statistical clusters from the entire DataFrame, disregarding time.
    Groups incidents by crime_type and by spatial bins (using lat_bin and lon_bin rounded to 'precision').
    Only clusters with at least 'threshold' incidents are considered.
    Clusters are sorted in descending order by incident count.
    For each cluster, computes a representative coordinate, consolidates a sample of incident details,
    builds a prompt for the LLM to analyze, and then stores the analysis in the database.
    """
    df = df.copy()
    # Create spatial bins based on latitude and longitude.
    df['lat_bin'] = df['latitude'].astype(float).round(precision)
    df['lon_bin'] = df['longitude'].astype(float).round(precision)
    
    # Group by crime_type and spatial bins.
    group_cols = ['crime_type', 'lat_bin', 'lon_bin']
    grouped = df.groupby(group_cols)
    
    clusters_info = []
    for name, group in grouped:
        crime_type, lat_bin, lon_bin = name
        count = len(group)
        if count >= threshold:
            clusters_info.append((crime_type, lat_bin, lon_bin, count, group))
    
    if not clusters_info:
        print(f"DEBUG: No large clusters found meeting the threshold of {threshold} incidents.")
        return
    
    # Sort clusters by descending count.
    clusters_info.sort(key=lambda x: x[3], reverse=True)
    print(f"DEBUG: Found {len(clusters_info)} large clusters meeting the threshold of {threshold} incidents.")
    
    for crime_type, lat_bin, lon_bin, count, group in clusters_info:
        # Compute a representative coordinate.
        center_lat = group['latitude'].astype(float).mean()
        center_lon = group['longitude'].astype(float).mean()
        
        # Create a sample of incident details (up to 3 incidents).
        sample_df = group.head(3)
        sample_text = "Sample incidents:\n"
        for _, row in sample_df.iterrows():
            sample_text += f"Date: {row.get('crime_date')}, Coordinates: ({row['latitude']}, {row['longitude']}), Description: {row.get('description', 'N/A')}\n"
        
        prompt = (
            f"The following is an aggregated cluster of {crime_type.upper()} incidents. "
            f"This cluster contains {count} incidents, aggregated at approximately coordinates ({center_lat:.5f}, {center_lon:.5f}).\n\n"
            f"{sample_text}\n"
            "Please provide a concise statistical summary and plain language analysis of this large crime cluster, "
            "highlighting any common patterns, potential causes, and notable observations."
        )
        print("DEBUG: Sending prompt to LLM for large cluster (first 200 characters):", prompt[:200])
        
        llm_response = get_crime_analysis(prompt)
        if not llm_response:
            print(f"DEBUG: LLM returned an empty response for the {crime_type.upper()} cluster at ({lat_bin}, {lon_bin}). Skipping this cluster.")
            continue
        print("DEBUG: Received LLM analysis (first 200 characters):", llm_response[:200])
        
        # Store the LLM response.
        ensure_llm_response_table(DB_PATH)
        insert_llm_response("All Time", crime_type, llm_response, DB_PATH)
        
        # Build and insert the final cluster record.
        cluster_record = {
            "crime_type": crime_type.upper(),
            "coordinates": [center_lat, center_lon],
            "incident_count": count,
            "description": llm_response
        }
        ensure_crime_analysis_table(DB_PATH)
        insert_cluster(cluster_record, DB_PATH)
        
        print(f"DEBUG: Successfully processed and inserted analysis for {crime_type.upper()} cluster ({count} incidents) at ({center_lat:.5f}, {center_lon:.5f}).\n")

# --- MAIN PROCESSING FUNCTION ---

def process_csv_file(file_path):
    """
    Process a single CSV file:
      - Load CSV and standardize columns.
      - Convert crime_date to datetime and drop invalid dates.
      - Instead of grouping by month, process the entire dataset to find statistically significant clusters.
      - Only clusters with large incident counts (e.g. 50 or more) are analyzed with the LLM.
    """
    print(f"\n=== Processing file: {file_path} ===")
    df = load_csv_file(file_path)
    if df.empty:
        print("DEBUG: DataFrame is empty. Skipping this file.")
        return

    std_df = standardize_columns(df, STANDARD_COLUMNS)
    if std_df is None:
        print("ERROR: Required standard fields missing. Skipping this file.")
        return

    try:
        std_df['crime_date'] = pd.to_datetime(std_df['crime_date'], errors='coerce')
        std_df = std_df.dropna(subset=['crime_date'])
        print(f"DEBUG: After date conversion, {len(std_df)} rows remain.")
    except Exception as e:
        print(f"ERROR: Date conversion failed for {file_path}: {e}")
        return

    std_df = std_df.sort_values(by='crime_date', ascending=False)
    
    # Process large statistical clusters across the entire dataset.
    process_statistical_crime_clusters(std_df, threshold=50, precision=2)
    
    print("\nDEBUG: Completed processing of file:", file_path)

def run_crime_analysis():
    """Main routine: Retrieve the most recent CSV file from CSV_FOLDER and process it."""
    csv_files = get_all_csv_files(CSV_FOLDER)
    if not csv_files:
        return
    
    for file_path in csv_files:
        process_csv_file(file_path)
    
    print("\nDEBUG: Completed processing all CSV files.")

if __name__ == "__main__":
    run_crime_analysis()
