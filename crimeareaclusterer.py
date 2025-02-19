import os
import glob
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import unary_union
import sqlite3
from sklearn.cluster import DBSCAN
import numpy as np

# --- HELPER FUNCTION TO CLEAN AND STANDARDIZE COLUMN NAMES ---
def clean_name(name):
    return name.strip().lower().replace("_", "").replace(" ", "")

def standardize_columns(df, mapping):
    """
    Renames columns of the input DataFrame based on the provided mapping.
    mapping should be a dict where the key is the standardized name
    and the value is a list of candidate column names from the CSV.
    """
    lower_cols = {clean_name(col): col for col in df.columns}
    rename_dict = {}
    for std_col, candidates in mapping.items():
        for candidate in candidates:
            candidate_clean = clean_name(candidate)
            if candidate_clean in lower_cols:
                rename_dict[lower_cols[candidate_clean]] = std_col
                break
    df = df.rename(columns=rename_dict)
    required = list(mapping.keys())
    missing = [col for col in required if col not in df.columns]
    if missing:
        print(f"ERROR: Missing required standardized columns: {missing}")
        return None
    return df

# Mapping for required columns using common candidate names.
REQUIRED_COLUMNS_MAPPING = {
    "crime_type": ["crime_type", "crimetype", "type", "offense", "incident_type", "crime_category", "offense_description", "description", "crime_description", "detail", "Description", "NIBRSDescription"],
    "lat": ["latitude", "lat", "Latitude", "MapLatitude"],
    "lon": ["longitude", "lon", "lng", "long", "Longitude", "MapLongitude"]
}

# --- MAIN PROCESSING FUNCTIONS ---
def process_crime_data(crime_data_df):
    """
    Converts a DataFrame of crime data (with standardized columns 'crime_type', 'lat', and 'lon') 
    into a GeoDataFrame, buffers each point by 100 meters (after transforming to an appropriate UTM CRS for accuracy),
    and clusters & merges the buffered geometries by crime type using DBSCAN (with a 300-meter threshold).
    Returns a GeoDataFrame of merged polygon clusters in WGS84 along with centroid coordinates.
    """
    print(f"DEBUG: Starting process_crime_data() with {len(crime_data_df)} records.")
    # Create GeoDataFrame using standardized lat/lon (WGS84)
    gdf = gpd.GeoDataFrame(
        crime_data_df,
        geometry=gpd.points_from_xy(crime_data_df.lon, crime_data_df.lat),
        crs="EPSG:4326"
    )
    print(f"DEBUG: GeoDataFrame created with {len(gdf)} points.")
    
    # Transform to UTM for accurate buffering/distance measurement
    utm_crs = gdf.estimate_utm_crs()
    print(f"DEBUG: Estimated UTM CRS: {utm_crs}")
    gdf_utm = gdf.to_crs(utm_crs)
    
    # Buffer each point by 100 meters
    gdf_utm['geometry'] = gdf_utm.buffer(100)
    print("DEBUG: Applied 100-meter buffer to all points.")
    
    def cluster_and_merge(gdf_group):
        """
        For a group of buffered geometries (by crime type), extracts the centroids
        and applies DBSCAN clustering (300m eps, min_samples=2). Then merges geometries
        in each cluster using unary_union.
        """
        cur_crime = gdf_group['crime_type'].iloc[0]
        print(f"DEBUG: Clustering {len(gdf_group)} records for crime type: {cur_crime}")
        # Convert each buffered polygon to its centroid coordinates for clustering
        coords = np.array([[geom.centroid.x, geom.centroid.y] for geom in gdf_group.geometry])
        
        # DBSCAN clustering (300m threshold, minimum 2 points)
        db = DBSCAN(eps=300, min_samples=2).fit(coords)
        labels = db.labels_
        print(f"DEBUG: DBSCAN assigned labels for {cur_crime}: {set(labels)} (excluding noise: -1)")
        
        gdf_group = gdf_group.copy()
        gdf_group['cluster'] = labels
        
        # Filter out noise points (label -1)
        clustered = gdf_group[gdf_group['cluster'] != -1]
        if clustered.empty:
            print(f"DEBUG: No valid clusters found for {cur_crime} (all points considered noise).")
            return None
        
        merged_geometries = []
        for cluster_id in clustered['cluster'].unique():
            cluster_subset = clustered[clustered['cluster'] == cluster_id]
            print(f"DEBUG: Merging cluster_id {cluster_id} with {len(cluster_subset)} records for {cur_crime}.")
            merged_geom = unary_union(cluster_subset.geometry)
            merged_geometries.append({
                'crime_type': cur_crime,
                'geometry': merged_geom,
                'cluster_id': cluster_id
            })
        if merged_geometries:
            print(f"DEBUG: Generated {len(merged_geometries)} merged clusters for crime type: {cur_crime}")
            return gpd.GeoDataFrame(merged_geometries, crs=utm_crs)
        return None

    # Process each crime type group separately.
    geo_dfs = []
    grouped = gdf_utm.groupby('crime_type')
    for crime_type, group in grouped:
        print(f"DEBUG: Processing crime type: {crime_type} with {len(group)} records.")
        result = cluster_and_merge(group)
        if result is not None:
            geo_dfs.append(result)
        else:
            print(f"DEBUG: No clusters formed for crime type: {crime_type}")
    
    if not geo_dfs:
        print("DEBUG: No clusters formed from any crime data.")
        return gpd.GeoDataFrame()  # Return an empty GeoDataFrame if no clusters.
    
    clustered_gdf = gpd.GeoDataFrame(pd.concat(geo_dfs, ignore_index=True), crs=utm_crs)
    print(f"DEBUG: Total merged clusters before CRS conversion: {len(clustered_gdf)}")
    
    # Convert merged polygons back to WGS84 (EPSG:4326)
    clustered_gdf = clustered_gdf.to_crs("EPSG:4326")
    print("DEBUG: Converted merged clusters back to WGS84.")
    
    # Add centroid coordinates for mapping
    clustered_gdf['lat'] = clustered_gdf.geometry.centroid.y
    clustered_gdf['lon'] = clustered_gdf.geometry.centroid.x
    print("DEBUG: Calculated centroid coordinates for clusters.")
    
    return clustered_gdf

def save_to_sqlite(gdf, db_path='data_analysis_db.db'):
    """
    Saves the GeoDataFrame of crime areas to the existing SQLite database (data_analysis_db.db).
    Creates table 'crime_areas' (if it doesn't exist) with a UNIQUE constraint on (crime_type, geometry)
    to prevent duplicate insertions. Geometries are stored as WKT.
    """
    print(f"DEBUG: Connecting to existing database at {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        print("DEBUG: Successfully connected to the database.")
    except Exception as e:
        print(f"ERROR: Could not connect to database: {e}")
        return
    
    cur = conn.cursor()
    print("DEBUG: Ensuring crime_areas table exists.")
    cur.execute('''
        CREATE TABLE IF NOT EXISTS crime_areas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crime_type TEXT,
            lat REAL,
            lon REAL,
            geometry TEXT,
            UNIQUE(crime_type, geometry)
        )
    ''')
    conn.commit()

    # Insert each record individually, converting geometry to WKT
    print("DEBUG: Starting insertion of clusters into crime_areas table.")
    for idx, row in gdf.iterrows():
        geometry_wkt = row['geometry'].wkt
        try:
            cur.execute('''
                INSERT OR IGNORE INTO crime_areas (crime_type, lat, lon, geometry)
                VALUES (?, ?, ?, ?)
            ''', (row['crime_type'], row['lat'], row['lon'], geometry_wkt))
            conn.commit()
            if cur.rowcount > 0:
                print(f"DEBUG: Inserted cluster for {row['crime_type']} at ({row['lat']:.5f}, {row['lon']:.5f}).")
            else:
                print(f"DEBUG: Duplicate cluster for {row['crime_type']} at ({row['lat']:.5f}, {row['lon']:.5f}) skipped.")
        except Exception as e:
            print(f"ERROR: Failed to insert record for {row['crime_type']} at ({row['lat']:.5f}, {row['lon']:.5f}): {e}")
    
    conn.close()
    print("DEBUG: Database connection closed after insertion.")

def get_all_csv_files(folder_path):
    """
    Retrieves CSV file paths from the specified folder.
    """
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        print("DEBUG: No CSV files found in the folder.")
    else:
        print(f"DEBUG: Found {len(csv_files)} CSV file(s) in {folder_path}.")
    return csv_files

def process_csv_file(file_path, db_path='data_analysis_db.db'):
    """
    Processes a single CSV file:
      - Loads the crime data CSV.
      - Standardizes the column names based on common fields.
      - Processes the data into geographic crime cluster areas.
      - Inserts unique crime area polygons into the existing SQLite database.
    """
    print(f"DEBUG: Processing file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"DEBUG: CSV file {file_path} is empty; skipping.")
            return
        
        # Standardize columns using the common mapping.
        df_std = standardize_columns(df, REQUIRED_COLUMNS_MAPPING)
        if df_std is None:
            print(f"ERROR: Standardization failed for {file_path}; skipping.")
            return
        
        print("DEBUG: CSV file loaded and standardized successfully. Processing crime data...")
        clustered_gdf = process_crime_data(df_std)
        if clustered_gdf.empty:
            print(f"DEBUG: No clusters were generated for file: {file_path}")
            return
        
        print("DEBUG: Saving clusters to existing database...")
        save_to_sqlite(clustered_gdf, db_path)
    except Exception as e:
        print(f"ERROR: Exception processing file {file_path}: {e}")

def process_all_files(folder_path, db_path='data_analysis_db.db'):
    """
    Processes every CSV file found in the given folder:
      - For each file, computes crime cluster areas and saves them to the database.
      - Duplicate areas are skipped based on the UNIQUE constraint.
    """
    csv_files = get_all_csv_files(folder_path)
    if not csv_files:
        return
    for file in csv_files:
        process_csv_file(file, db_path)
    print("DEBUG: Completed processing all CSV files.")

# Example usage:
if __name__ == "__main__":
    CRIME_DATA_FOLDER = r"C:\Env\DataAnalyzer\CrimeData\DataRepo"  # Adjust path as needed.
    process_all_files(CRIME_DATA_FOLDER) 