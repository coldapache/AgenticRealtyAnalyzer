#!/usr/bin/env python
"""
Crime Impact Analysis
Calculates relative crime impact scores for properties based on spatial density
compared to the general area's baseline.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import percentileofscore
from math import radians, sin, cos, sqrt, atan2
import traceback

# --- CONFIGURATION ---
DB_PATH = 'data_analysis_db.db'
IMMEDIATE_AREA_MILES = 0.25  # ~5 minute walk
GENERAL_AREA_MILES = 2.0    # Broader context area

print("🚀 Crime Impact Analysis v1.0")
print(f"📁 Using database: {DB_PATH}")
print(f"📏 Immediate area radius: {IMMEDIATE_AREA_MILES} miles")
print(f"📏 General area radius: {GENERAL_AREA_MILES} miles")

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two lat/lon points in miles"""
    R = 3959.87433  # Earth's radius in miles
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def calculate_crime_impact(property_lat, property_lon, crimes_df):
    """
    Calculate crime impact with careful handling of no-data scenarios
    Returns: (relative_score, analysis_dict)
    """
    print(f"\nDEBUG: Analyzing property at {property_lat}, {property_lon}")
    
    if crimes_df.empty:
        print("DEBUG: No crime data available in database")
        return -1.0, {"note": "Insufficient crime data for assessment"}

    # Calculate distances to all crimes
    distances = crimes_df.apply(
        lambda row: haversine_distance(
            property_lat, property_lon, 
            row['latitude'], row['longitude']
        ), 
        axis=1
    )
    
    # Count crimes in immediate and general areas
    immediate_mask = distances <= IMMEDIATE_AREA_MILES
    general_mask = distances <= GENERAL_AREA_MILES
    
    immediate_crimes = crimes_df[immediate_mask]
    general_crimes = crimes_df[general_mask]
    
    print(f"DEBUG: Found {len(immediate_crimes)} crimes in immediate area")
    print(f"DEBUG: Found {len(general_crimes)} crimes in general area")
    
    # If no crimes in general area, we can't make a reliable assessment
    if len(general_crimes) == 0:
        print("DEBUG: No crimes in assessment radius - insufficient data")
        return -1.0, {"note": "No crime data within assessment radius"}
    
    # Calculate areas (in square miles)
    immediate_area = np.pi * IMMEDIATE_AREA_MILES**2
    general_area = np.pi * GENERAL_AREA_MILES**2
    
    # Calculate densities with safety checks
    immediate_density = len(immediate_crimes) / immediate_area if not immediate_crimes.empty else 0
    general_density = len(general_crimes) / general_area if not general_crimes.empty else 0
    
    print(f"DEBUG: Immediate density: {immediate_density:.2f} crimes/sq.mile")
    print(f"DEBUG: General density: {general_density:.2f} crimes/sq.mile")
    
    # Analyze crime patterns
    analysis = {
        "immediate_crimes": len(immediate_crimes),
        "general_crimes": len(general_crimes),
        "immediate_density": immediate_density,
        "general_density": general_density
    }
    
    # Calculate relative density
    if general_density == 0:
        relative_density = 0
    else:
        relative_density = immediate_density / general_density
        
    analysis["relative_density"] = relative_density
    
    # Analyze crime type clusters if we have immediate area crimes
    if not immediate_crimes.empty:
        crime_types = {}
        for crime_type in immediate_crimes['crime_type'].unique():
            immediate_type_count = immediate_crimes[immediate_crimes['crime_type'] == crime_type].shape[0]
            general_type_count = general_crimes[general_crimes['crime_type'] == crime_type].shape[0]
            
            immediate_type_density = immediate_type_count / immediate_area
            general_type_density = general_type_count / general_area
            
            if general_type_density > 0:
                relative_type_density = immediate_type_density / general_type_density
                if relative_type_density > 1.5:  # Only record significant clusters
                    crime_types[crime_type] = relative_type_density
        
        if crime_types:
            analysis["crime_clusters"] = crime_types
            print(f"DEBUG: Found crime clusters: {crime_types}")
    
    # Determine relative score based on densities
    if relative_density < 0.75:  # Significantly lower than surrounding area
        return 0.0, analysis
    elif relative_density < 1.5:  # Similar to surrounding area
        return 0.5, analysis
    else:  # Higher than surrounding area
        return 1.0, analysis

def get_crime_category(score, all_scores):
    """Determine crime impact category based on relative scoring"""
    if score < 0:
        return "Insufficient Data"
    elif score == 0:
        return "Low Crime Impact"
    elif score == 0.5:
        return "Some Crime Impact"
    else:
        return "High Crime Impact"

def get_crime_coverage_areas():
    """Get bounding boxes of areas with crime data"""
    print("\n1️⃣ Identifying areas with crime data...")
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Get crime data boundaries
            crimes_df = pd.read_sql("""
                SELECT 
                    MIN(latitude) as min_lat,
                    MAX(latitude) as max_lat,
                    MIN(longitude) as min_lon,
                    MAX(longitude) as max_lon,
                    COUNT(*) as crime_count
                FROM crime_data
                WHERE crime_date >= date('now', '-1 year')
                AND latitude IS NOT NULL
                AND longitude IS NOT NULL
                GROUP BY 
                    CAST(latitude * 10 AS INT) / 10.0,
                    CAST(longitude * 10 AS INT) / 10.0
                HAVING crime_count > 10
            """, conn)
            
            if crimes_df.empty:
                print("❌ No crime data areas found")
                return None
                
            print(f"📍 Found {len(crimes_df)} areas with crime data")
            print("DEBUG: Crime coverage areas:")
            for _, area in crimes_df.iterrows():
                print(f"  Area with {area['crime_count']} crimes: "
                      f"({area['min_lat']:.3f}, {area['min_lon']:.3f}) to "
                      f"({area['max_lat']:.3f}, {area['max_lon']:.3f})")
            
            return crimes_df
            
    except Exception as e:
        print(f"❌ Error getting crime areas: {str(e)}")
        return None

def get_properties_in_crime_areas(crime_areas):
    """Get properties within or near areas with crime data"""
    print("\n2️⃣ Finding properties in crime coverage areas...")
    
    if crime_areas is None or crime_areas.empty:
        return None
        
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Buffer the crime areas slightly to include nearby properties
            buffer = GENERAL_AREA_MILES / 69  # Rough miles to degrees conversion
            
            # Build query for properties in any crime area
            conditions = []
            params = []
            for _, area in crime_areas.iterrows():
                conditions.append("""
                    (latitude BETWEEN ? AND ? 
                    AND longitude BETWEEN ? AND ?)
                """)
                params.extend([
                    area['min_lat'] - buffer,
                    area['max_lat'] + buffer,
                    area['min_lon'] - buffer,
                    area['max_lon'] + buffer
                ])
            
            query = f"""
                SELECT address, latitude, longitude 
                FROM listing_analysis 
                WHERE crime_impact IS NULL
                AND latitude IS NOT NULL 
                AND longitude IS NOT NULL
                AND ({' OR '.join(conditions)})
            """
            
            properties_df = pd.read_sql(query, conn, params=params)
            
            if properties_df.empty:
                print("ℹ️ No properties found in crime data areas")
                return None
                
            print(f"📍 Found {len(properties_df)} properties to analyze")
            print("DEBUG: Sample properties:")
            print(properties_df.head())
            
            return properties_df
            
    except Exception as e:
        print(f"❌ Error getting properties: {str(e)}")
        return None

def update_listing_analysis():
    """Update listing_analysis table with crime impact categories"""
    print("\n🔍 Starting crime impact analysis...")
    
    try:
        # First, identify areas with crime data
        crime_areas = get_crime_coverage_areas()
        if crime_areas is None:
            print("❌ Cannot proceed without crime data areas")
            return
            
        # Get properties in those areas
        properties_df = get_properties_in_crime_areas(crime_areas)
        if properties_df is None:
            print("❌ No properties to analyze")
            return
            
        # Load crime data for analysis
        with sqlite3.connect(DB_PATH) as conn:
            print("\n3️⃣ Loading crime data...")
            crimes_df = pd.read_sql("""
                SELECT crime_type, latitude, longitude
                FROM crime_data
                WHERE crime_date >= date('now', '-1 year')
                AND latitude IS NOT NULL
                AND longitude IS NOT NULL
            """, conn)
            
            print(f"🗃️ Loaded {len(crimes_df)} crimes from the past year")
            
            # Calculate impacts for properties in crime areas
            print("\n4️⃣ Calculating crime impacts...")
            scores = []
            analyses = []
            for idx, row in properties_df.iterrows():
                score, analysis = calculate_crime_impact(
                    row['latitude'], 
                    row['longitude'], 
                    crimes_df
                )
                scores.append(score)
                analyses.append(analysis)
                
                if idx % 100 == 0:
                    print(f"📊 Processed {idx}/{len(properties_df)} properties")
            
            # Determine categories and update database
            print("\n5️⃣ Updating database...")
            properties_df['crime_impact'] = [
                get_crime_category(score, scores) 
                for score in scores
            ]
            
            cursor = conn.cursor()
            update_count = 0
            for idx, row in properties_df.iterrows():
                if row['crime_impact'] != "Insufficient Data":
                    cursor.execute("""
                        UPDATE listing_analysis 
                        SET crime_impact = ?,
                            analyzed_at = datetime('now')
                        WHERE address = ?
                    """, (row['crime_impact'], row['address']))
                    update_count += cursor.rowcount
            
            print(f"\n✅ Successfully updated {update_count} properties")
            
            # Print distribution
            print("\n📊 Results distribution:")
            impact_counts = properties_df['crime_impact'].value_counts()
            for category, count in impact_counts.items():
                print(f"{category}: {count} properties")
            
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        print("DEBUG: Full error trace:")
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    update_listing_analysis() 