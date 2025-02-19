"""
utils.py

This module contains utility functions for interacting with the 'data_analysis_db.db' SQLite 
database and generating interactive maps using Folium. It serves two primary purposes:

1. Retrieving real estate data from the SQLite database:
   - The database stores data in the 'realty_listings' table.
   - The function `get_property_data()` connects to the database, runs a SQL query to 
     fetch property records that contain valid geographic coordinates, and returns the 
     result as a pandas DataFrame.

2. Creating a Folium map from the retrieved property data:
   - The function `create_map()` takes the DataFrame of property details, computes a map center 
     based on the average latitude and longitude, and places markers on the map for each property.
   - Each marker includes a popup with key information such as address, price, number of bedrooms/bathrooms, and square footage.

The module includes robust print statements for terminal logging to facilitate debugging and to
provide visibility into the process flow, including database connections, query execution, and any
errors that might occur.

Author: Your Name
Date: YYYY-MM-DD
"""

import sqlite3
import pandas as pd
import folium
from folium.plugins import MarkerCluster

def get_property_data(db_path='data_analysis_db.db'):
    """
    Retrieve real estate property data from the SQLite database.
    
    Returns:
        pd.DataFrame: A DataFrame containing property data with the following columns:
            address, price, bedrooms, bathrooms, sqft, latitude, longitude
    """
    print("🔄 Starting get_property_data()")
    try:
        conn = sqlite3.connect(db_path)
        print("✅ Database connection established for property data.")
    except sqlite3.Error as e:
        print(f"❌ Error connecting to the database for property data: {e}")
        raise

    query = """
        SELECT 
            address,
            price,
            bedrooms,
            bathrooms,
            sqft,
            latitude,
            longitude
        FROM realty_listings
        WHERE latitude IS NOT NULL 
          AND longitude IS NOT NULL
    """
    print(f"🔍 Executing SQL query for property data:\n{query}")
    try:
        df = pd.read_sql_query(query, conn)
        print(f"✅ Query executed successfully. Retrieved {len(df)} property records.")
    except Exception as e:
        print(f"❌ Error executing query for property data: {e}")
        conn.close()
        raise

    conn.close()
    print("🔒 Database connection closed for property data.")
    return df

def get_listing_analysis_data(db_path='data_analysis_db.db'):
    """
    Retrieve listing analysis data from the SQLite database.
    
    Returns:
        pd.DataFrame: A DataFrame containing:
            - address, city, price, bedrooms, bathrooms, latitude, longitude, market_exceptionality, analyzed_at
    """
    print("🔄 Starting get_listing_analysis_data()")
    try:
        conn = sqlite3.connect(db_path)
        print("✅ Database connection established for listing analysis.")
    except sqlite3.Error as e:
        print(f"❌ Error connecting to the database for listing analysis: {e}")
        raise

    query = """
        SELECT 
            address,
            city,
            price,
            bedrooms,
            bathrooms,
            latitude,
            longitude,
            market_exceptionality,
            analyzed_at
        FROM listing_analysis
        WHERE latitude IS NOT NULL 
          AND longitude IS NOT NULL
    """
    print(f"🔍 Executing SQL query for listing analysis:\n{query}")
    try:
        df = pd.read_sql_query(query, conn)
        print(f"✅ Query executed successfully. Retrieved {len(df)} listing analysis records.")
    except Exception as e:
        print(f"❌ Error executing query for listing analysis: {e}")
        conn.close()
        raise
    
    conn.close()
    print("🔒 Database connection closed for listing analysis.")
    return df

def get_crime_clusters_data(db_path='data_analysis_db.db'):
    """
    Retrieve crime cluster data from the SQLite database.
    
    Returns:
        pd.DataFrame: A DataFrame containing crime cluster data with columns:
                      crime_type, incident_count, cluster_description, latitude, longitude, analyzed_at
    """
    print("🔄 Starting get_crime_clusters_data()")
    try:
        conn = sqlite3.connect(db_path)
        print("✅ Database connection established for crime clusters.")
    except sqlite3.Error as e:
        print(f"❌ Error connecting to the database for crime clusters: {e}")
        raise

    query = """
        SELECT 
            crime_type,
            incident_count,
            cluster_description,
            latitude,
            longitude,
            analyzed_at
        FROM crime_analysis
    """
    print(f"🔍 Executing SQL query for crime clusters:\n{query}")
    try:
        df = pd.read_sql_query(query, conn)
        print(f"✅ Query executed successfully. Retrieved {len(df)} crime cluster record(s).")
    except Exception as e:
        print(f"❌ Error executing query for crime clusters: {e}")
        conn.close()
        raise

    conn.close()
    print("🔒 Database connection closed for crime clusters.")
    return df

def get_color_for_crime(crime_type):
    """
    Dynamically generates a color for a crime type.
    Returns one of a predefined list of colors based on the hash of the crime type.
    """
    colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred', 'beige', 
              'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'pink', 'lightblue', 'lightgreen', 'gray', 'black']
    idx = abs(hash(crime_type)) % len(colors)
    return colors[idx]

def create_map_layers():
    """
    Create a Folium map with selectable layers:
      - 'Listing Analysis': Primary layer showing markers from the listing_analysis table using custom icons wrapped inside
        a MarkerCluster.
      - 'All Listings': Secondary layer (hidden by default) showing all property records inside a MarkerCluster.
      - 'Crime Clusters': New layer showing crime cluster markers with dynamically assigned colors by crime type inside a MarkerCluster.
    
    Returns:
        folium.Map: A Folium map object with the three feature groups plus a layer control.
    """
    print("🔄 Starting create_map_layers()")
    
    # Get listing analysis data
    df_analysis = get_listing_analysis_data()
    df_analysis['latitude'] = pd.to_numeric(df_analysis['latitude'], errors='coerce')
    df_analysis['longitude'] = pd.to_numeric(df_analysis['longitude'], errors='coerce')
    df_analysis = df_analysis.dropna(subset=['latitude', 'longitude'])
    
    # Get all listings data (for the secondary layer, which is hidden by default)
    df_all = get_property_data()
    df_all['latitude'] = pd.to_numeric(df_all['latitude'], errors='coerce')
    df_all['longitude'] = pd.to_numeric(df_all['longitude'], errors='coerce')
    df_all = df_all.dropna(subset=['latitude', 'longitude'])
    
    # Center the map preferentially on the analysis data (fallback to all listings, then USA center)
    if not df_analysis.empty:
        center_lat = df_analysis['latitude'].mean()
        center_lon = df_analysis['longitude'].mean()
    elif not df_all.empty:
        center_lat = df_all['latitude'].mean()
        center_lon = df_all['longitude'].mean()
    else:
        center_lat, center_lon = 37.0902, -95.7129  # Fallback to center of USA
    
    # Set zoom_start=4 and use the 'Cartodb Positron' tiles.
    m = folium.Map(location=[center_lat, center_lon], zoom_start=7, tiles='Cartodb Positron')
    print(f"📍 Map center set at latitude: {center_lat}, longitude: {center_lon}")
    
    ## Listing Analysis Layer using MarkerCluster ##
    fg_analysis = folium.FeatureGroup(name="Listing Analysis", show=True)
    marker_cluster_analysis = MarkerCluster().add_to(fg_analysis)
    for _, row in df_analysis.iterrows():
        market_val = str(row.get('market_exceptionality', '')).lower()
        if "good deal" in market_val:
            icon_params = {"prefix": "fa", "color": "green", "icon": "arrow-up"}
            angle = 0
        elif "average deal" in market_val:
            icon_params = {"prefix": "fa", "color": "orange", "icon": "arrow-right"}
            angle = 0
        elif "bad deal" in market_val:
            icon_params = {"prefix": "fa", "color": "red", "icon": "arrow-down"}
            angle = 0
        else:
            icon_params = {"prefix": "fa", "color": "gray", "icon": "question"}
            angle = 0
        icon = folium.Icon(angle=angle, **icon_params)
        
        popup_content = f"""
            <div style='font-family: "Univers Roman", Helvetica, sans-serif; font-size: 18px;'>
                <b>{row['market_exceptionality'].upper()}</b><br>
                <b>Price:</b> ${row['price']:,.2f}<br>
                <b>Address:</b> {row['address']}<br>
                <b>Beds/Baths:</b> {row['bedrooms']}/{row['bathrooms']}<br>
                <b>Analyzed At:</b> {row['analyzed_at']}
            </div>
        """
        tooltip_html = f"""
            <div style='font-family: "Univers Roman", Helvetica, sans-serif; font-size: 16px; font-weight: bold;'>
                {row['market_exceptionality'].upper()}
            </div>
        """
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_content, max_width=300),
            tooltip=folium.Tooltip(tooltip_html, parse_html=True),
            icon=icon
        ).add_to(marker_cluster_analysis)
    m.add_child(fg_analysis)
    
    ## All Listings Layer using MarkerCluster ##
    fg_all = folium.FeatureGroup(name="All Listings", show=False)
    marker_cluster_all = MarkerCluster().add_to(fg_all)
    for _, row in df_all.iterrows():
        popup_content = f"""
            <div style='font-family: Helvetica, sans-serif; font-size: 14px;'>
                <b>Address:</b> {row['address']}<br>
                <b>Price:</b> ${row['price']:,.2f}<br>
                <b>Beds/Baths:</b> {row['bedrooms']}/{row['bathrooms']}<br>
                <b>Sqft:</b> {row['sqft']:,}
            </div>
        """
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=6,
            popup=folium.Popup(popup_content, max_width=300),
            color="blue",
            fill=True,
            fill_color="blue",
            fill_opacity=0.5,
            weight=1
        ).add_to(marker_cluster_all)
    m.add_child(fg_all)
    
    ## Crime Clusters Layer with dynamic colors using MarkerCluster ##
    df_clusters = get_crime_clusters_data()
    fg_clusters = folium.FeatureGroup(name="Crime Clusters", show=True)
    marker_cluster_crime = MarkerCluster().add_to(fg_clusters)
    for _, row in df_clusters.iterrows():
        # Determine dynamic color based on crime type.
        dynamic_color = get_color_for_crime(row['crime_type'])
        popup_content = f"""
            <div style='font-family: Helvetica, sans-serif; font-size: 14px;'>
                <b>Crime Cluster:</b> {row['crime_type']}<br>
                <b>Incidents:</b> {row['incident_count']}<br>
                <b>Analyzed At:</b> {row['analyzed_at']}<br>
                <b>Description:</b> {row['cluster_description']}
            </div>
        """
        tooltip_text = f"Crime Cluster: {row['crime_type']}"
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=8,
            popup=folium.Popup(popup_content, max_width=300),
            tooltip=folium.Tooltip(tooltip_text),
            color=dynamic_color,
            fill=True,
            fill_color=dynamic_color,
            fill_opacity=0.7,
            weight=1
        ).add_to(marker_cluster_crime)
    m.add_child(fg_clusters)
    print(f"DEBUG: Added Crime Clusters layer with {len(df_clusters)} marker(s).")
    
    # Add layer control with a left-hand side position.
    folium.LayerControl(collapsed=False, position='topleft').add_to(m)
    
    print("✅ Map with all layers created successfully.")
    return m