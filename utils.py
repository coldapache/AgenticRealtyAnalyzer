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
# sqlite3: Database management library for interacting with SQLite databases
# Used for connecting to and querying our data_analysis_db.db file
import sqlite3

# pandas: Data manipulation and analysis library
# Used for working with data in DataFrame format and SQL query results
import pandas as pd

# folium: Library for creating interactive maps based on leaflet.js
# Used as the core mapping functionality to display our real estate data
import folium

# MarkerCluster: Folium plugin for clustering map markers
# Used to group nearby markers together for better map performance and visualization
from folium.plugins import MarkerCluster

# geopandas: Library for working with geospatial data
# Used for handling geographic data structures and operations
import geopandas as gpd

# loads from shapely.wkt: Function to parse Well-Known Text (WKT) format
# Used to convert WKT strings from database into shapely geometry objects for mapping
from shapely.wkt import loads as wkt_loads

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
    """Retrieve listing analysis data including top pick status"""
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
            crime_impact,
            top_pick,
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

def create_map_layers(zoom_start=4):
    """Create Folium map with layers including top pick visualization"""
    print("🔄 Initializing map layers")
    try:
        # Data loading with validation
        df_analysis = get_listing_analysis_data()
        df_all = get_property_data()
        
        # Validate and clean coordinate data for both dataframes
        # 1. Convert latitude and longitude to numeric values, replacing non-numeric with NaN
        # 2. Remove any rows with missing coordinates to ensure map functionality
        # This prevents mapping errors from invalid or missing coordinate data
        for df in [df_analysis, df_all]:
            # Convert coordinates to numeric, coercing errors to NaN instead of raising exceptions
            df[['latitude', 'longitude']] = df[['latitude', 'longitude']].apply(pd.to_numeric, errors='coerce')
            # Remove rows with NaN coordinates since they can't be mapped
            df.dropna(subset=['latitude', 'longitude'], inplace=True)

        # Map initialization - zooms to the US average latitude and longitude
        m = folium.Map(
            location=[df_analysis['latitude'].mean(), df_analysis['longitude'].mean()] if not df_analysis.empty else [39.2904, -76.6122],
            zoom_start=zoom_start,
            tiles='CartoDB Positron',
            control_scale=True
        )
        
        # Layer 1: Listing Analysis with adjusted clustering, does not cluster too much
        fg_analysis = folium.FeatureGroup(name="📊 Listing Analysis", show=True)
        mc_analysis = MarkerCluster(
            options={
                'disableClusteringAtZoom': 10,  # Starts breaking up at zoom level 10
                'maxClusterRadius': 40,
                'showCoverageOnHover': False
            }
        ).add_to(fg_analysis)
        for idx, row in df_analysis.iterrows():
            try:
                # Determine icon configuration based on market exceptionality and top pick status
                base_config = {
                    "good deal": {"color": "green", "icon": "arrow-up"},
                    "average deal": {"color": "orange", "icon": "arrow-right"},
                    "bad deal": {"color": "red", "icon": "arrow-down"}
                }.get(row['market_exceptionality'].strip().lower(), {"color": "gray", "icon": "question"})

                # Modify icon for top picks
                if row.get('top_pick') == 'Top Pick':
                    darker_gold = "#B8860B"  # Darker gold color (DarkGoldenRod)
                    base_config["color"] = darker_gold
                    base_config["prefix"] = 'fa'
                    # Add a more solid glowing effect using CSS
                    icon_html = f"""
                        <div class="top-pick-marker" 
                             style="animation: glow 2s ease-in-out infinite alternate;">
                            <i class="fa fa-{base_config['icon']}" 
                               style="color: {base_config['color']};
                                      text-shadow: 1px 1px 2px #000;">
                            </i>
                        </div>
                    """
                else:
                    icon_html = None

                # Create marker
                marker = folium.Marker(
                    [row['latitude'], row['longitude']],
                    icon=folium.DivIcon(html=icon_html) if icon_html else folium.Icon(**base_config, prefix='fa'),
                    popup=folium.Popup(f"""
                        {'<b>🌟 TOP PICK! 🌟</b><br>' if row.get('top_pick') == 'Top Pick' else ''}
                        <b>{row['market_exceptionality']}</b><br>
                        {'<b>' + row.get('crime_impact', 'Crime Impact: Not Analyzed') + '</b><br>' if row.get('crime_impact') else ''}
                        City: {row['city']}<br>
                        Price: ${row['price']:,.0f}<br>
                        {row['bedrooms']}BR/{row['bathrooms']}BA<br>
                        {row.get('sqft', 'N/A')} sqft<br>
                        Analyzed: {row['analyzed_at']}<br>
                        <i>{row['address']}</i>
                    """, max_width=300),
                    tooltip=f"{'🌟 TOP PICK! - ' if row.get('top_pick') == 'Top Pick' else ''}{row['market_exceptionality'].title()}"
                )

                # Add custom CSS for enhanced glowing effect
                if row.get('top_pick') == 'Top Pick':
                    m.get_root().header.add_child(folium.Element("""
                        <style>
                        @keyframes glow {
                            from {
                                filter: drop-shadow(0 0 3px #FFD700) 
                                       drop-shadow(0 0 6px #B8860B) 
                                       drop-shadow(0 0 9px #8B6914);
                            }
                            to {
                                filter: drop-shadow(0 0 6px #FFD700) 
                                       drop-shadow(0 0 12px #B8860B) 
                                       drop-shadow(0 0 18px #8B6914);
                            }
                        }
                        .top-pick-marker {
                            font-size: 22px;
                            filter: drop-shadow(0 0 4px #B8860B);
                            -webkit-text-stroke: 1px #000;
                        }
                        </style>
                    """))

                marker.add_to(mc_analysis)

            except Exception as e:
                print(f"⚠️ Error processing listing {idx}: {e}")
        
        # Layer 2: All Listings with adjusted clustering
        fg_all = folium.FeatureGroup(name="🏠 All Listings", show=False)
        mc_all = MarkerCluster(
            options={
                'disableClusteringAtZoom': 13,
                'maxClusterRadius': 30
            }
        ).add_to(fg_all)
        for idx, row in df_all.iterrows():
            folium.CircleMarker(
                [row['latitude'], row['longitude']],
                radius=6,
                color='#3186cc',
                fill=True,
                popup=f"${row['price']:,.0f}",
                tooltip=row['address']
            ).add_to(mc_all)



        # Add all layers and controls with optimized clustering
        for layer in [fg_analysis, fg_all]:
            m.add_child(layer)
        

        print("✅ Map layers successfully created")
        return m

    except Exception as e:
        print(f"🔥 Critical error in create_map_layers: {e}")
        raise
