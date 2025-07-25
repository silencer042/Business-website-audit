import asyncio
import csv
import re
import ssl
import time
import random
import sys
import os
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from tqdm import tqdm

# Configuration - Modified for GitHub Actions
INPUT_FILE = sys.argv[1] if len(sys.argv) > 1 else "input/businesses.csv"
OUTPUT_CSV = "output/qualified_businesses.csv"
ACTIVE_ONLINE_CSV = "output/active_online_businesses.csv"
INACTIVE_BUSINESSES_CSV = "output/inactive_businesses.csv"
CLOSED_BUSINESSES_CSV = "output/closed_businesses.csv"
FAILED_BUSINESSES_CSV = "output/failed_businesses.csv"

# Performance settings - Optimized for GitHub Actions
CONCURRENT_BROWSERS = 1  # Reduced for stability
CONCURRENT_PAGES_PER_BROWSER = 2
TIMEOUT = 10000  # 10 seconds
BATCH_SIZE = 50  # Larger batches for efficiency

# Mobile viewport for testing
MOBILE_VIEWPORT = {"width": 390, "height": 844}

# Social media and non-website URL patterns
SOCIAL_MEDIA_PATTERNS = [
    re.compile(r".*linkedin\.com.*", re.IGNORECASE),
    re.compile(r".*facebook\.com.*", re.IGNORECASE),
    re.compile(r".*twitter\.com.*", re.IGNORECASE),
    re.compile(r".*instagram\.com.*", re.IGNORECASE),
    re.compile(r".*youtube\.com.*", re.IGNORECASE),
    re.compile(r".*tiktok\.com.*", re.IGNORECASE),
    re.compile(r".*pinterest\.com.*", re.IGNORECASE),
    re.compile(r".*snapchat\.com.*", re.IGNORECASE),
    re.compile(r".*telegram\.me.*", re.IGNORECASE),
    re.compile(r".*whatsapp\.com.*", re.IGNORECASE),
    re.compile(r".*yelp\.com.*", re.IGNORECASE),
    re.compile(r".*foursquare\.com.*", re.IGNORECASE),
    re.compile(r".*maps\.google\.com.*", re.IGNORECASE),
    re.compile(r".*goo\.gl/maps.*", re.IGNORECASE),
]

def is_social_media_url(url):
    """Check if URL is a social media profile"""
    if not url:
        return False
    
    for pattern in SOCIAL_MEDIA_PATTERNS:
        if pattern.match(url):
            return True
    return False

def load_data_file(file_path):
    """Load data from CSV or Excel file, preserving all columns"""
    try:
        file_path = Path(file_path)
        
        if not file_path.exists():
            print(f"❌ File not found: {file_path}")
            return None
            
        print(f"📁 Loading file: {file_path}")
        
        if file_path.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
            print(f"📊 Loaded Excel file with {len(df)} rows")
        elif file_path.suffix.lower() == '.csv':
            # Try different encodings for CSV
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    print(f"📊 Loaded CSV file with {len(df)} rows (encoding: {encoding})")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                print(f"❌ Could not read CSV file with any encoding")
                return None
        else:
            print(f"❌ Unsupported file format: {file_path.suffix}")
            return None
        
        # Print column information
        print(f"📋 Available columns ({len(df.columns)}):")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None

def detect_columns(df):
    """Auto-detect column mappings from DataFrame"""
    columns = [col.lower().strip() for col in df.columns]
    column_mapping = {}
    
    # Business name patterns
    business_patterns = ['business name', 'company name', 'name', 'business', 'company']
    for pattern in business_patterns:
        for i, col in enumerate(columns):
            if pattern in col:
                column_mapping['business_name'] = df.columns[i]
                break
        if 'business_name' in column_mapping:
            break
    
    # Website patterns
    website_patterns = ['website', 'url', 'site', 'web', 'link']
    for pattern in website_patterns:
        for i, col in enumerate(columns):
            if pattern in col:
                column_mapping['website'] = df.columns[i]
                break
        if 'website' in column_mapping:
            break
    
    # City patterns
    city_patterns = ['city', 'location', 'place', 'town']
    for pattern in city_patterns:
        for i, col in enumerate(columns):
            if pattern in col:
                column_mapping['city'] = df.columns[i]
                break
        if 'city' in column_mapping:
            break
    
    print(f"🔍 Detected columns:")
    for key, value in column_mapping.items():
        print(f"  {key}: {value}")
    
    return column_mapping

def clean_url(url):
    """Clean and validate URL"""
    if not url or pd.isna(url):
        return None
    
    url = str(url).strip()
    
    # Remove common prefixes that aren't URLs
    prefixes_to_remove = ['www.', 'http://', 'https://']
    for prefix in prefixes_to_remove:
        if url.lower().startswith(prefix):
            url = url[len(prefix):]
    
    # Add https:// if no protocol
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Basic URL validation
    parsed = urlparse(url)
    if not parsed.netloc:
        return None
    
    return url

def save_progress(results, closed, failed, batch_num):
    """Save progress periodically"""
    if results:
        results_df = pd.DataFrame(results)
        results_df.to_csv(f"output/progress_qualified_batch_{batch_num}.csv", index=False)
    
    if closed:
        closed_df = pd.DataFrame(closed)
        closed_df.to_csv(f"output/progress_closed_batch_{batch_num}.csv", index=False)
    
    print(f"✅ Progress saved for batch {batch_num}")

def estimate_completion_time(total_businesses, businesses_per_minute=8):
    """Estimate completion time"""
    minutes = total_businesses / businesses_per_minute
    hours = minutes / 60
    
    if hours > 5.5:  # Close to 6-hour limit
        print(f"⚠️  WARNING: Estimated time {hours:.1f} hours may exceed GitHub Actions 6-hour limit")
        print(f"📊 Consider splitting your CSV into smaller files of ~2000-2500 businesses each")
    else:
        print(f"⏱️  Estimated completion time: {hours:.1f} hours ({minutes:.0f} minutes)")

async def check_website_quality(page, url):
    """Analyze website quality and technology"""
    try:
        # Navigate to website
        response = await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
        
        if not response or response.status >= 400:
            return {
                'accessible': False,
                'mobile_responsive': False,
                'modern_design': False,
                'has_ssl': False,
                'technology_stack': 'Unknown',
                'error': f'HTTP {response.status if response else "No response"}'
            }
        
        # Check SSL
        has_ssl = url.startswith('https://')
        
        # Check mobile responsiveness
        await page.set_viewport_size(MOBILE_VIEWPORT)
        await page.wait_for_timeout(1000)
        
        # Check for mobile-responsive indicators
        mobile_indicators = await page.evaluate("""
            () => {
                const viewport = document.querySelector('meta[name="viewport"]');
                const mediaQueries = Array.from(document.styleSheets).some(sheet => {
                    try {
                        return Array.from(sheet.cssRules || []).some(rule => 
                            rule.media && rule.media.mediaText.includes('max-width')
                        );
                    } catch(e) { return false; }
                });
                
                return {
                    hasViewportMeta: !!viewport,
                    hasMediaQueries: mediaQueries,
                    bodyWidth: document.body.scrollWidth,
                    windowWidth: window.innerWidth
                };
            }
        """)
        
        mobile_responsive = (
            mobile_indicators['hasViewportMeta'] or 
            mobile_indicators['hasMediaQueries'] or
            mobile_indicators['bodyWidth'] <= mobile_indicators['windowWidth'] + 50
        )
        
        # Check technology stack
        technology = await page.evaluate("""
            () => {
                const technologies = [];
                
                // Check for common frameworks/CMS
                if (window.jQuery) technologies.push('jQuery');
                if (window.React) technologies.push('React');
                if (window.Vue) technologies.push('Vue');
                if (window.Angular) technologies.push('Angular');
                if (document.querySelector('[data-reactroot]')) technologies.push('React');
                if (document.querySelector('meta[name="generator"]')) {
                    const generator = document.querySelector('meta[name="generator"]').content;
                    technologies.push(generator);
                }
                
                // Check for WordPress
                if (document.querySelector('link[href*="wp-content"]') || 
                    document.querySelector('script[src*="wp-content"]') ||
                    document.querySelector('meta[name="generator"][content*="WordPress"]')) {
                    technologies.push('WordPress');
                }
                
                # Check for Shopify
                if (window.Shopify || document.querySelector('[data-shopify]')) {
                    technologies.push('Shopify');
                }
                
                return technologies.length > 0 ? technologies.join(', ') : 'Custom/Unknown';
            }
        """)
        
        # Analyze design modernity
        design_analysis = await page.evaluate("""
            () => {
                const body = document.body;
                const styles = window.getComputedStyle(body);
                
                // Check for modern CSS features
                const hasModernCSS = [
                    'flexbox', 'grid', 'transform', 'transition'
                ].some(prop => styles.display?.includes('flex') || 
                              styles.display?.includes('grid') ||
                              styles.transform !== 'none' ||
                              styles.transition !== 'all 0s ease 0s');
                
                // Check for modern design patterns
                const hasHamburgerMenu = !!document.querySelector('.hamburger, .menu-toggle, [class*="mobile-menu"]');
                const hasHeroSection = !!document.querySelector('.hero, [class*="banner"], [class*="header-image"]');
                
                // Check last modified date
                const lastModified = document.lastModified;
                const isRecentlyUpdated = new Date(lastModified) > new Date(Date.now() - 365 * 24 * 60 * 60 * 1000);
                
                return {
                    hasModernCSS,
                    hasHamburgerMenu,
                    hasHeroSection,
                    isRecentlyUpdated,
                    lastModified
                };
            }
        """)
        
        modern_design = (
            design_analysis['hasModernCSS'] or 
            design_analysis['hasHamburgerMenu'] or 
            design_analysis['hasHeroSection'] or
            design_analysis['isRecentlyUpdated']
        )
        
        return {
            'accessible': True,
            'mobile_responsive': mobile_responsive,
            'modern_design': modern_design,
            'has_ssl': has_ssl,
            'technology_stack': technology,
            'error': None
        }
        
    except PlaywrightTimeoutError:
        return {
            'accessible': False,
            'mobile_responsive': False,
            'modern_design': False,
            'has_ssl': False,
            'technology_stack': 'Unknown',
            'error': 'Timeout'
        }
    except Exception as e:
        return {
            'accessible': False,
            'mobile_responsive': False,
            'modern_design': False,
            'has_ssl': False,
            'technology_stack': 'Unknown',
            'error': str(e)
        }

async def search_google_maps(page, business_name, city=None):
    """Search for business on Google Maps to verify it's active"""
    try:
        search_query = business_name
        if city:
            search_query += f" {city}"
        
        # Go to Google Maps
        await page.goto("https://www.google.com/maps", timeout=TIMEOUT)
        await page.wait_for_timeout(1000)
        
        # Search for business
        search_box = await page.wait_for_selector('input[id="searchboxinput"]', timeout=5000)
        await search_box.fill(search_query)
        await page.keyboard.press('Enter')
        
        # Wait for results
        await page.wait_for_timeout(3000)
        
        # Check if business is found and active
        try:
            # Look for business listing
            business_result = await page.wait_for_selector('[data-value="Directions"]', timeout=5000)
            
            # Check if it's marked as closed
            closed_indicators = await page.query_selector_all('text=/permanently closed/i, text=/closed/i')
            
            return {
                'found': True,
                'appears_active': len(closed_indicators) == 0,
                'google_maps_url': page.url
            }
            
        except PlaywrightTimeoutError:
            return {
                'found': False,
                'appears_active': False,
                'google_maps_url': None
            }
        
    except Exception as e:
        return {
            'found': False,
            'appears_active': False,
            'google_maps_url': None,
            'error': str(e)
        }

async def process_business(context, business_data, column_mapping):
    """Process a single business"""
    try:
        page = await context.new_page()
        
        # Extract business information
        business_name = business_data.get(column_mapping['business_name'], '')
        website = business_data.get(column_mapping.get('website', ''), '')
        city = business_data.get(column_mapping.get('city', ''), '')
        
        # Skip if no business name
        if not business_name:
            await page.close()
            return None, 'no_name'
        
        # Clean website URL
        cleaned_website = clean_url(website)
        
        # Skip social media URLs
        if cleaned_website and is_social_media_url(cleaned_website):
            await page.close()
            return None, 'social_media'
        
        # Search Google Maps first
        maps_result = await search_google_maps(page, business_name, city)
        
        # If business appears closed on Google Maps, mark as closed
        if maps_result['found'] and not maps_result['appears_active']:
            result = {**business_data, 'status': 'closed', 'google_maps_check': 'closed'}
            await page.close()
            return result, 'closed'
        
        # If no website and not found on maps, skip
        if not cleaned_website and not maps_result['found']:
            await page.close()
            return None, 'no_website_or_maps'
        
        # If no website but found on maps, consider for manual review
        if not cleaned_website:
            result = {
                **business_data,
                'status': 'needs_website',
                'google_maps_check': 'found',
                'qualification_reason': 'No website but active on Google Maps'
            }
            await page.close()
            return result, 'qualified'
        
        # Check website quality
        website_analysis = await check_website_quality(page, cleaned_website)
        
        # Determine qualification
        qualification_score = 0
        qualification_reasons = []
        
        if not website_analysis['accessible']:
            qualification_score += 3
            qualification_reasons.append("Website not accessible")
        
        if not website_analysis['mobile_responsive']:
            qualification_score += 2
            qualification_reasons.append("Not mobile responsive")
        
        if not website_analysis['modern_design']:
            qualification_score += 2
            qualification_reasons.append("Outdated design")
        
        if not website_analysis['has_ssl']:
            qualification_score += 1
            qualification_reasons.append("No SSL certificate")
        
        # Qualify if score >= 2 and business appears active
        is_qualified = qualification_score >= 2 and (maps_result['appears_active'] or not maps_result['found'])
        
        # Prepare result
        result = {
            **business_data,
            'website_cleaned': cleaned_website,
            'website_accessible': website_analysis['accessible'],
            'mobile_responsive': website_analysis['mobile_responsive'],
            'modern_design': website_analysis['modern_design'],
            'has_ssl': website_analysis['has_ssl'],
            'technology_stack': website_analysis['technology_stack'],
            'google_maps_found': maps_result['found'],
            'appears_active': maps_result['appears_active'],
            'qualification_score': qualification_score,
            'qualification_reasons': '; '.join(qualification_reasons),
            'status': 'qualified' if is_qualified else 'not_qualified',
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        await page.close()
        
        if is_qualified:
            return result, 'qualified'
        else:
            return result, 'not_qualified'
        
    except Exception as e:
        try:
            await page.close()
        except:
            pass
        return None, f'error: {str(e)}'

async def process_businesses_batch(businesses, column_mapping, pbar):
    """Process a batch of businesses with concurrent execution"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu'
            ]
        )
        
        context = await browser.new_context(
            viewport=MOBILE_VIEWPORT,
            user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15'
        )
        
        results = []
        closed = []
        failed = []
        
        # Process businesses with limited concurrency
        semaphore = asyncio.Semaphore(CONCURRENT_PAGES_PER_BROWSER)
        
        async def process_with_semaphore(business_data):
            async with semaphore:
                result, status = await process_business(context, business_data, column_mapping)
                
                # Add delay between requests
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                return result, status
        
        # Create tasks for all businesses
        tasks = [process_with_semaphore(business) for business in businesses]
        
        # Process tasks and update progress
        for task in asyncio.as_completed(tasks):
            result, status = await task
            
            if status == 'qualified':
                results.append(result)
            elif status == 'closed':
                closed.append(result)
            elif result is None:
                failed.append(status)
            
            pbar.update(1)
        
        await browser.close()
        
        return results, closed, failed

def separate_active_inactive_businesses(qualified_businesses):
    """Separate qualified businesses into active online and inactive based on criteria"""
    active_online = []
    inactive = []
    
    for business in qualified_businesses:
        # Check if business is truly active online
        is_website_accessible = business.get('website_accessible', False)
        is_appears_active = business.get('appears_active', False)
        
        # Convert string values to boolean if needed
        if isinstance(is_website_accessible, str):
            is_website_accessible = is_website_accessible.lower() == 'true'
        if isinstance(is_appears_active, str):
            is_appears_active = is_appears_active.lower() == 'true'
        
        # Business is active online if both conditions are met
        if is_website_accessible and is_appears_active:
            active_online.append(business)
        else:
            inactive.append(business)
    
    return active_online, inactive

async def main():
    """Main function to orchestrate the entire process"""
    try:
        print(f"🚀 Starting Business Website Audit")
        print(f"📁 Input file: {INPUT_FILE}")
        
        # Create output directory
        os.makedirs("output", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        # Load data
        df = load_data_file(INPUT_FILE)
        if df is None:
            return
        
        # Estimate completion time
        estimate_completion_time(len(df))
        
        # Detect columns
        column_mapping = detect_columns(df)
        
        # Validate required columns
        if 'business_name' not in column_mapping:
            print("❌ Could not find business name column")
            return
        
        print(f"\n🚀 Starting analysis of {len(df)} businesses...")
        
        # Convert to list of dictionaries
        businesses = df.to_dict('records')
        
        # Initialize progress bar
        pbar = tqdm(
            total=len(businesses),
            desc="Processing businesses",
            unit="business",
            ncols=100,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        )
        
        # Process in batches
        all_results = []
        all_closed = []
        all_failed = []
        
        total_batches = (len(businesses) + BATCH_SIZE - 1) // BATCH_SIZE
        
        for i in range(0, len(businesses), BATCH_SIZE):
            batch = businesses[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            
            print(f"\n📊 Processing batch {batch_num}/{total_batches} ({len(batch)} businesses)")
            
            try:
                results, closed, failed = await process_businesses_batch(batch, column_mapping, pbar)
                
                all_results.extend(results)
                all_closed.extend(closed)
                all_failed.extend(failed)
                
                print(f"✅ Batch {batch_num} complete:")
                print(f"  - Qualified businesses: {len(results)}")
                print(f"  - Closed businesses: {len(closed)}")
                print(f"  - Failed: {len(failed)}")
                
                # Save progress every 5 batches
                if batch_num % 5 == 0:
                    save_progress(all_results, all_closed, all_failed, batch_num)
                
            except Exception as e:
                print(f"❌ Error in batch {batch_num}: {e}")
                continue
        
        # Close progress bar
        pbar.close()
        
        # Separate active online businesses from inactive ones
        if all_results:
            active_online, inactive = separate_active_inactive_businesses(all_results)
            
            print(f"\n📈 BUSINESS SEPARATION RESULTS:")
            print(f"🟢 Active online businesses: {len(active_online)}")
            print(f"🟡 Inactive businesses: {len(inactive)}")
            
            # Save active online businesses
            if active_online:
                active_df = pd.DataFrame(active_online)
                active_df.to_csv(ACTIVE_ONLINE_CSV, index=False)
                print(f"💾 Active online businesses saved to: {ACTIVE_ONLINE_CSV}")
            
            # Save inactive businesses
            if inactive:
                inactive_df = pd.DataFrame(inactive)
                inactive_df.to_csv(INACTIVE_BUSINESSES_CSV, index=False)
                print(f"💾 Inactive businesses saved to: {INACTIVE_BUSINESSES_CSV}")
        
        # Save all qualified businesses (for reference)
        if all_results:
            results_df = pd.DataFrame(all_results)
            results_df.to_csv(OUTPUT_CSV, index=False)
            print(f"💾 All qualified businesses saved to: {OUTPUT_CSV}")
        
        # Save closed businesses
        if all_closed:
            closed_df = pd.DataFrame(all_closed)
            closed_df.to_csv(CLOSED_BUSINESSES_CSV, index=False)
            print(f"💾 Closed businesses saved to: {CLOSED_BUSINESSES_CSV}")
        
        # Save failed businesses
        if all_failed:
            failed_df = pd.DataFrame({'reason': all_failed})
            failed_df.to_csv(FAILED_BUSINESSES_CSV, index=False)
            print(f"💾 Failed businesses saved to: {FAILED_BUSINESSES_CSV}")
        
        print(f"\n🎉 Analysis complete!")
        print(f"📊 FINAL SUMMARY:")
        print(f"✅ Total qualified businesses: {len(all_results) if all_results else 0}")
        print(f"🟢 Active online businesses: {len(active_online) if all_results else 0}")
        print(f"🟡 Inactive businesses: {len(inactive) if all_results else 0}")
        print(f"🚫 Closed businesses: {len(all_closed)}")
        print(f"❌ Failed to process: {len(all_failed)}")
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"❌ Main process error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())