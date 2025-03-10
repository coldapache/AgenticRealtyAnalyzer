"""
property_map.py

This is the main FastAPI application that serves an interactive real estate map.
It integrates with our utilities in 'utils.py' to retrieve property data from a SQLite database
and generate a Folium map with property markers.

Core functionalities:
    - Define a FastAPI endpoint ("/") that:
        * Fetches property data from the 'data_analysis_db.db' database.
        * Creates a Folium map with markers for each property.
        * Embeds the map into an HTML template.
    - Returns an HTMLResponse with the rendered map and additional page header information.
    - Includes robust print statements and error handling to provide terminal visibility 
      into any issues that arise during execution.

Author: Your Name
Date: YYYY-MM-DD
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn
from datetime import datetime
import sqlite3
import pandas as pd
import json
from utils import create_map_layers
import folium
from shapely.wkt import loads
import hashlib

app = FastAPI()

def get_city_locations(db_path='data_analysis_db.db'):
    """
    Retrieves distinct cities from the realty_listings table along with
    their average latitude and longitude, which will be used to zoom the map.
    
    Returns a list of dictionaries in the shape:
      [{'city': 'CityName', 'lat': 40.7128, 'lon': -74.0060}, ...]
    """
    try:
        conn = sqlite3.connect(db_path)
        query = """
            SELECT city, AVG(latitude) as lat, AVG(longitude) as lon
            FROM realty_listings
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY city
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"❌ Error retrieving city locations: {e}")
        return []

@app.get("/", response_class=HTMLResponse)
async def root():
    """
    FastAPI endpoint for the root URL ("/").
    Creates an HTML page embedding the interactive map built with listing analysis data.
    """
    print("🚀 Received request for root endpoint.")
    
    try:
        print("🔄 Creating map with listing analysis layers.")
        m = create_map_layers()
        print("✅ Map created successfully.")
    except Exception as e:
        print(f"❌ Error creating map: {e}")
        return HTMLResponse(content=f"<h1>Error creating map: {e}</h1>", status_code=500)
    
    # Retrieve the cities data (for the dropdown)
    cities = get_city_locations()
    city_data_json = json.dumps(cities)
    

    
    # Preserve existing layer control
    folium.LayerControl(position='topleft', collapsed=False).add_to(m)
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Real Estate Map</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{
                margin: 0;
                padding: 0;
                font-family: "Univers Roman", Helvetica, sans-serif;
                display: flex;
                flex-direction: column;
            }}
            #header {{
                background-color: #333;
                color: white;
                padding: 10px;
                text-align: center;
            }}
            #content {{
                display: flex;
                flex: 1;
                overflow: hidden;
            }}
            #sidebar {{
                width: 300px;
                background-color: #f8f9fa;
                padding: 10px;
                overflow-y: auto;
            }}
            #map-container {{
                flex: 1;
                height: calc(100vh - 50px);
            }}
            select {{
                width: 100%;
                padding: 8px;
                margin-bottom: 10px;
                font-size: 16px;
            }}
            @media (max-width: 600px) {{
                #content {{
                    flex-direction: column;
                }}
                #sidebar {{
                    width: 100%;
                    height: auto;
                }}
                #map-container {{
                    height: calc(100vh - 200px);
                }}
            }}
        </style>
    </head>
    <body>
        <div id="header">
            <h2>Real Estate Listing Analysis Map</h2>
            <p>Data updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        </div>
        <div id="content">
            <div id="sidebar">
                <h3>Select City</h3>
                <select id="cityDropdown" onchange="zoomToCity()">
                    <option value="">-- Choose a City --</option>
                </select>
            </div>
            <div id="map-container">
                {m.get_root().render()}
            </div>
        </div>
        <script>
            // City locations data injected from the backend.
            var cities = {city_data_json};

            // Populate the dropdown with cities.
            var dropdown = document.getElementById('cityDropdown');
            cities.forEach(function(city) {{
                var option = document.createElement('option');
                option.text = city.city;
                // Store the latitude and longitude as a JSON string.
                option.value = JSON.stringify({{lat: city.lat, lon: city.lon}});
                dropdown.add(option);
            }});

            // When a city is selected, zoom to its coordinates.
            function zoomToCity() {{
                var selected = dropdown.value;
                if (selected) {{
                    var coords = JSON.parse(selected);
                    // Locate the Leaflet map object; Folium creates global map variables like "map_xxx"
                    for (var prop in window) {{
                        if (prop.startsWith("map_") && window[prop] instanceof L.Map) {{
                            window[prop].setView([coords.lat, coords.lon], 12);
                            break;
                        }}
                    }}
                }}
            }}
        </script>
    </body>
    </html>
    """
    
    print("✅ Serving HTML content.")
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    print("🚀 Starting FastAPI app via uvicorn on host 127.0.0.1 and port 8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)