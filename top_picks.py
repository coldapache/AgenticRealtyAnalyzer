#!/usr/bin/env python
"""
Top Picks Analysis
Identifies exceptional properties by combining market and crime analysis.
"""

import sqlite3
import pandas as pd
import json
import requests
from datetime import datetime
import traceback

# --- CONFIGURATION ---
DB_PATH = 'data_analysis_db.db'
OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL = "mistral:7b"

def get_listing_context(conn, current_listing):
    """Get statistical context for similar listings nationwide"""
    
    price_range_buffer = 0.2  # 20% above and below current price
    min_price = current_listing['price'] * (1 - price_range_buffer)
    max_price = current_listing['price'] * (1 + price_range_buffer)
    
    context_query = """
        SELECT 
            COUNT(*) as total_similar,
            AVG(price) as avg_price,
            COUNT(CASE WHEN market_exceptionality LIKE '%Good%' THEN 1 END) as good_market_count,
            COUNT(CASE WHEN crime_impact = 'Low Crime Impact' THEN 1 END) as low_crime_count,
            COUNT(CASE WHEN top_pick = 'Top Pick' THEN 1 END) as existing_top_picks,
            GROUP_CONCAT(DISTINCT market_exceptionality) as market_patterns,
            GROUP_CONCAT(DISTINCT crime_impact) as crime_patterns
        FROM listing_analysis
        WHERE bedrooms = ?
        AND bathrooms = ?
        AND price BETWEEN ? AND ?
        AND market_exceptionality IS NOT NULL
        AND crime_impact IS NOT NULL
    """
    
    df_context = pd.read_sql(context_query, conn, params=[
        current_listing['bedrooms'],
        current_listing['bathrooms'],
        min_price,
        max_price
    ])
    
    # Get city-specific context
    city_query = """
        SELECT 
            COUNT(*) as city_total,
            AVG(price) as city_avg_price,
            COUNT(CASE WHEN market_exceptionality LIKE '%Good%' THEN 1 END) as city_good_market,
            COUNT(CASE WHEN crime_impact = 'Low Crime Impact' THEN 1 END) as city_low_crime
        FROM listing_analysis
        WHERE city = ?
        AND market_exceptionality IS NOT NULL
        AND crime_impact IS NOT NULL
    """
    
    df_city = pd.read_sql(city_query, conn, params=[current_listing['city']])
    
    print(f"\nDEBUG: Context for {current_listing['address']}")
    print(f"Similar properties found: {df_context.iloc[0]['total_similar']}")
    print(f"City properties found: {df_city.iloc[0]['city_total']}")
    
    return df_context.iloc[0], df_city.iloc[0]

def analyze_listing(row, conn):
    """Analyze a single listing for top pick status with full context"""
    
    if pd.isna(row['market_exceptionality']) or pd.isna(row['crime_impact']):
        print(f"DEBUG: Skipping {row['address']} - missing required analysis")
        return None
        
    cursor = conn.cursor()
    
    # First check if this is a candidate for top pick
    is_good_market = "good" in row['market_exceptionality'].lower()
    is_low_crime = "low crime impact" in row['crime_impact'].lower()
    
    if not (is_good_market and is_low_crime):
        print(f"DEBUG: Skipping {row['address']} - does not meet basic criteria")
        print(f"DEBUG: Market: {row['market_exceptionality']}, Crime: {row['crime_impact']}")
        
        # Clear any existing top pick status
        cursor.execute("""
            UPDATE listing_analysis 
            SET top_pick = NULL,
                analyzed_at = datetime('now')
            WHERE address = ?
        """, (row['address'],))
        conn.commit()
        return None
    
    print(f"\nDEBUG: ✨ Analyzing candidate: {row['address']}")
    print(f"DEBUG: Good market + Low crime combination detected")
    
    # Get contextual statistics for qualifying properties
    similar_stats, city_stats = get_listing_context(conn, row)
    
    prompt = f"""
    Analyze this property listing in the context of similar properties nationwide:

    CURRENT PROPERTY:
    Market Analysis: {row['market_exceptionality']}
    Crime Impact: {row['crime_impact']}
    Price: ${row['price']:,.2f}
    Location: {row['city']}
    Details: {row['bedrooms']}BR/{row['bathrooms']}BA

    CONTEXT OF SIMILAR PROPERTIES (Same beds/baths, ±20% price):
    Total Similar Properties: {similar_stats['total_similar']}
    Average Price: ${similar_stats['avg_price']:,.2f}
    Properties with Good Market: {similar_stats['good_market_count']}
    Properties with Low Crime: {similar_stats['low_crime_count']}
    Existing Top Picks: {similar_stats['existing_top_picks']}
    
    CITY CONTEXT ({row['city']}):
    Total Properties: {city_stats['city_total']}
    Average Price: ${city_stats['city_avg_price']:,.2f}
    Properties with Good Market: {city_stats['city_good_market']}
    Properties with Low Crime: {city_stats['city_low_crime']}

    Market Patterns Observed: {similar_stats['market_patterns']}
    Crime Patterns Observed: {similar_stats['crime_patterns']}

    IMPORTANT DECISION CRITERIA:
    This property has already qualified with both:
    1. "Good" market exceptionality (good deal)
    2. "Low Crime Impact"
    
    The only reason to NOT mark this as a top pick would be if there are
    extremely compelling reasons in the comparative analysis that suggest
    this property is significantly less attractive than other similar properties
    that also meet these criteria.

    Consider:
    - How does this property compare to other good deals in safe areas?
    - Is it particularly exceptional even among other qualified properties?
    - Are there any red flags in the comparative analysis?

    Respond with ONLY 'top pick' or 'not top pick'.
    """

    try:
        response = requests.post(
            OLLAMA_API_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False
            }
        )
        
        if response.status_code == 200:
            result = response.json()['response'].strip().lower()
            print(f"DEBUG: Final assessment for {row['address']}: {result}")
            
            if "not top pick" not in result:  # Default to top pick unless explicitly rejected
                print(f"🌟 Writing Top Pick to database for: {row['address']}")
                cursor.execute("""
                    UPDATE listing_analysis 
                    SET top_pick = 'Top Pick',
                        analyzed_at = datetime('now')
                    WHERE address = ?
                """, (row['address'],))
                conn.commit()
                return "Top Pick"
            else:
                cursor.execute("""
                    UPDATE listing_analysis 
                    SET top_pick = NULL,
                        analyzed_at = datetime('now')
                    WHERE address = ?
                """, (row['address'],))
                conn.commit()
                return None
            
    except Exception as e:
        print(f"❌ Error analyzing listing: {str(e)}")
        return None

def update_top_picks():
    """Update top picks in the database"""
    print("\n🔍 Starting top picks analysis...")
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # First, verify/create top_pick column
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(listing_analysis)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'top_pick' not in columns:
                print("Adding top_pick column...")
                cursor.execute("""
                    ALTER TABLE listing_analysis 
                    ADD COLUMN top_pick TEXT
                """)
                print("✅ Added top_pick column")
            
            # Get listings that need analysis (excluding existing top picks)
            print("\n1️⃣ Loading listings for analysis...")
            df = pd.read_sql("""
                SELECT 
                    address,
                    city,
                    price,
                    bedrooms,
                    bathrooms,
                    market_exceptionality,
                    crime_impact
                FROM listing_analysis
                WHERE market_exceptionality IS NOT NULL
                AND crime_impact IS NOT NULL
                AND (top_pick IS NULL OR 
                     (top_pick != 'Top Pick' AND analyzed_at < datetime('now', '-7 days')))
                ORDER BY price DESC
            """, conn)
            
            if df.empty:
                print("ℹ️ No new listings need analysis")
                return
                
            print(f"📊 Analyzing {len(df)} listings")
            
            # Process listings
            update_count = 0
            top_pick_count = 0
            for idx, row in df.iterrows():
                result = analyze_listing(row, conn)
                update_count += 1
                
                if result == "Top Pick":
                    top_pick_count += 1
                
                if idx % 10 == 0:
                    print(f"✍️ Processed {idx + 1}/{len(df)} listings")
                    print(f"📈 Current stats: {top_pick_count} top picks out of {update_count} analyzed")
            
            # Print final summary
            print(f"\n✅ Analysis complete!")
            print(f"📊 Processed {update_count} listings")
            print(f"🌟 Found {top_pick_count} top picks")
            
            # Get overall distribution
            cursor.execute("""
                SELECT 
                    COALESCE(top_pick, 'Not Top Pick') as status,
                    COUNT(*) as count
                FROM listing_analysis
                WHERE market_exceptionality IS NOT NULL
                AND crime_impact IS NOT NULL
                GROUP BY COALESCE(top_pick, 'Not Top Pick')
            """)
            
            print("\n📊 Overall Top Picks Distribution:")
            for status, count in cursor.fetchall():
                print(f"{status}: {count} listings")
            
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        print("DEBUG: Full error trace:")
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    print("🏠 Top Picks Analysis v1.0")
    print(f"📁 Using database: {DB_PATH}")
    update_top_picks() 