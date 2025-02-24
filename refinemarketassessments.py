import sqlite3
import json
import time
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# --- CONFIGURATION ---
DB_PATH = 'data_analysis_db.db'
OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL = "mistral:7b"
VALID_CHOICES = {"good deal", "average deal", "bad deal"}

def get_market_context(listing_data):
    """Get enhanced market statistics for context"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Get overall market stats
            df = pd.read_sql("""
                SELECT 
                    price,
                    bedrooms,
                    bathrooms,
                    market_exceptionality,
                    analyzed_at
                FROM listing_analysis
                WHERE analyzed_at >= date('now', '-30 days')
            """, conn)
            
            # Get specific stats for properties with same specs
            similar_properties = df[
                (df['bedrooms'] == listing_data['bedrooms']) & 
                (df['bathrooms'] == listing_data['bathrooms'])
            ]
            
            # Calculate percentiles for similar properties
            price_percentiles = np.percentile(similar_properties['price'], [25, 50, 75]) if len(similar_properties) > 0 else [0, 0, 0]
            
            stats = {
                'overall': {
                    'median_price': df['price'].median(),
                    'mean_price': df['price'].mean(),
                    'min_price': df['price'].min(),
                    'max_price': df['price'].max(),
                    'total_listings': len(df),
                    'good_deals': len(df[df['market_exceptionality'] == 'good deal']),
                    'bad_deals': len(df[df['market_exceptionality'] == 'bad deal']),
                    'average_deals': len(df[df['market_exceptionality'] == 'average deal'])
                },
                'similar': {
                    'count': len(similar_properties),
                    'median_price': similar_properties['price'].median(),
                    'mean_price': similar_properties['price'].mean(),
                    'min_price': similar_properties['price'].min(),
                    'max_price': similar_properties['price'].max(),
                    'price_25th': price_percentiles[0],
                    'price_75th': price_percentiles[2],
                    'good_deals': len(similar_properties[similar_properties['market_exceptionality'] == 'good deal']),
                    'bad_deals': len(similar_properties[similar_properties['market_exceptionality'] == 'bad deal']),
                    'average_deals': len(similar_properties[similar_properties['market_exceptionality'] == 'average deal'])
                }
            }
            
            print("\n📊 Market context loaded:")
            print(f"Overall market (last 30 days):")
            print(f"- Median: ${stats['overall']['median_price']:,.2f}")
            print(f"- Range: ${stats['overall']['min_price']:,.2f} - ${stats['overall']['max_price']:,.2f}")
            print(f"\nSimilar properties ({stats['similar']['count']} listings):")
            print(f"- Median: ${stats['similar']['median_price']:,.2f}")
            print(f"- Range: ${stats['similar']['min_price']:,.2f} - ${stats['similar']['max_price']:,.2f}")
            
            return stats
    except Exception as e:
        print(f"❌ Error getting market context: {e}")
        return None

def get_reassessment(listing_data, market_context):
    """Get LLM reassessment with detailed market analysis"""
    prompt = f"""You are a real estate market analyst re-evaluating a property listing.

PROPERTY DETAILS:
- Price: ${listing_data['price']:,.2f}
- Bedrooms: {listing_data['bedrooms']}
- Bathrooms: {listing_data['bathrooms']}
- Previous assessment: {listing_data['market_exceptionality']}
- Initial assessment date: {listing_data['analyzed_at']}

CURRENT MARKET CONTEXT (Last 30 Days):
Overall Market Statistics:
- Median Price: ${market_context['overall']['median_price']:,.2f}
- Mean Price: ${market_context['overall']['mean_price']:,.2f}
- Price Range: ${market_context['overall']['min_price']:,.2f} - ${market_context['overall']['max_price']:,.2f}
- Total Listings: {market_context['overall']['total_listings']}
- Deal Distribution:
  * Good Deals: {market_context['overall']['good_deals']}
  * Average Deals: {market_context['overall']['average_deals']}
  * Bad Deals: {market_context['overall']['bad_deals']}

Similar Properties ({market_context['similar']['count']} listings with same bed/bath):
- Median Price: ${market_context['similar']['median_price']:,.2f}
- Mean Price: ${market_context['similar']['mean_price']:,.2f}
- Price Range: ${market_context['similar']['min_price']:,.2f} - ${market_context['similar']['max_price']:,.2f}
- Price Quartiles: ${market_context['similar']['price_25th']:,.2f} (25th) - ${market_context['similar']['price_75th']:,.2f} (75th)
- Deal Distribution:
  * Good Deals: {market_context['similar']['good_deals']}
  * Average Deals: {market_context['similar']['average_deals']}
  * Bad Deals: {market_context['similar']['bad_deals']}

First, explain your analysis of this property considering:
1. How does it compare to similar properties?
2. Has the market context changed since initial assessment?
3. What specific factors influence your decision?

Then, provide your final assessment as EXACTLY one of these phrases on a new line:
"good deal", "average deal", or "bad deal"
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
        response.raise_for_status()
        
        response_data = response.json()
        raw_response = response_data.get('response', '').strip()
        print(f"\nDEBUG: LLM Analysis:\n{raw_response}\n")
        
        # Extract the final assessment
        lines = raw_response.lower().split('\n')
        for line in lines:
            for choice in VALID_CHOICES:
                if choice in line:
                    assessment = choice
                    print(f"🤔 Final Assessment: {assessment}")
                    return assessment
        
        print("❌ No valid assessment found in response")
        return None
            
    except Exception as e:
        print(f"❌ Error getting LLM assessment: {e}")
        return None

def update_assessment(address, new_assessment):
    """Update the market assessment in the database"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                UPDATE listing_analysis 
                SET market_exceptionality = ?,
                    analyzed_at = datetime('now')
                WHERE address = ?
            """, (new_assessment, address))
            print(f"✅ Updated assessment for {address}: {new_assessment}")
            return True
    except Exception as e:
        print(f"❌ Error updating assessment: {e}")
        return False

def process_listings():
    """Process all listings for reassessment with improved error handling"""
    print("\n🔄 Starting market reassessment process...")
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            listings = pd.read_sql("""
                SELECT 
                    address,
                    price,
                    bedrooms,
                    bathrooms,
                    market_exceptionality,
                    analyzed_at
                FROM listing_analysis
                WHERE market_exceptionality IS NOT NULL
                ORDER BY analyzed_at ASC
            """, conn)
            
        print(f"\n📝 Found {len(listings)} listings to reassess")
        
        for _, listing in listings.iterrows():
            try:
                print(f"\n🏠 Reassessing: {listing['address']}")
                
                # Get market context for this specific listing
                market_context = get_market_context(listing)
                if not market_context:
                    print("⚠️ Skipping due to missing market context")
                    continue
                
                new_assessment = get_reassessment(listing, market_context)
                if new_assessment:
                    if new_assessment != listing['market_exceptionality']:
                        print(f"📊 Assessment changed: {listing['market_exceptionality']} -> {new_assessment}")
                        update_assessment(listing['address'], new_assessment)
                    else:
                        print("✓ Assessment remains unchanged")
                else:
                    print("⚠️ Skipping due to invalid assessment")
                    
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                print(f"❌ Error processing listing {listing['address']}: {e}")
                continue  # Continue with next listing
            
    except Exception as e:
        print(f"❌ Error processing listings: {e}")

if __name__ == "__main__":
    process_listings()
