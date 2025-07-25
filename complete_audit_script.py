import asyncio
import csv
import re
import ssl
import time
import random
import sys
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from tqdm import tqdm

# Configuration - Enhanced for better accuracy
INPUT_FILE = sys.argv[1] if len(sys.argv) > 1 else "input/businesses.csv"
OUTPUT_CSV = "output/qualified_businesses.csv"
CLOSED_BUSINESSES_CSV = "output/closed_businesses.csv"
FAILED_BUSINESSES_CSV = "output/failed_businesses.csv"
LOW_PRIORITY_CSV = "output/low_priority_businesses.csv"

# Enhanced performance settings
CONCURRENT_BROWSERS = 1
CONCURRENT_PAGES_PER_BROWSER = 2
TIMEOUT = 15000  # Increased to 15 seconds for better accuracy
BATCH_SIZE = 30  # Reduced for more thorough analysis

# Enhanced viewport configurations
DESKTOP_VIEWPORT = {"width": 1920, "height": 1080}
MOBILE_VIEWPORT = {"width": 390, "height": 844}
TABLET_VIEWPORT = {"width": 768, "height": 1024}

# Enhanced social media patterns
SOCIAL_MEDIA_PATTERNS = [
    re.compile(r".*linkedin\.com.*", re.IGNORECASE),
    re.compile(r".*facebook\.com.*", re.IGNORECASE),
    re.compile(r".*twitter\.com.*", re.IGNORECASE),
    re.compile(r".*x\.com.*", re.IGNORECASE),  # New X.com
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
    re.compile(r".*nextdoor\.com.*", re.IGNORECASE),
    re.compile(r".*discord\.com.*", re.IGNORECASE),
    re.compile(r".*reddit\.com.*", re.IGNORECASE),
]

# Enhanced URL cleaning with better validation
def clean_url(url):
    """Enhanced URL cleaning and validation"""
    if not url or pd.isna(url):
        return None
    
    url = str(url).strip()
    
    # Remove common non-URL text
    if url.lower() in ['n/a', 'none', 'null', 'undefined', '']:
        return None
    
    # Handle special cases
    if 'mailto:' in url.lower():
        return None
    
    # Clean common prefixes
    url = re.sub(r'^(www\.)', '', url, flags=re.IGNORECASE)
    url = re.sub(r'^(http://|https://)', '', url, flags=re.IGNORECASE)
    
    # Add protocol
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Enhanced URL validation
    try:
        parsed = urlparse(url)
        if not parsed.netloc or '.' not in parsed.netloc:
            return None
        
        # Check for valid TLD
        tld = parsed.netloc.split('.')[-1]
        if len(tld) < 2 or not tld.isalpha():
            return None
            
        return url
    except:
        return None

# Enhanced website quality analysis
async def check_website_quality(page, url):
    """Enhanced website quality analysis with better accuracy"""
    try:
        # Multiple viewport testing
        quality_scores = {}
        
        # Test desktop first
        await page.set_viewport_size(DESKTOP_VIEWPORT)
        response = await page.goto(url, wait_until="networkidle", timeout=TIMEOUT)
        
        if not response or response.status >= 400:
            return create_failed_analysis(response)
        
        # Enhanced SSL and security check
        security_analysis = await analyze_security(page, url)
        
        # Enhanced mobile responsiveness test
        mobile_analysis = await test_mobile_responsiveness(page)
        
        # Enhanced design modernity analysis
        design_analysis = await analyze_design_modernity(page)
        
        # Performance analysis
        performance_analysis = await analyze_performance(page)
        
        # Content analysis
        content_analysis = await analyze_content_quality(page)
        
        # SEO basics check
        seo_analysis = await analyze_seo_basics(page)
        
        # Technology stack detection (enhanced)
        tech_analysis = await analyze_technology_stack(page)
        
        return {
            'accessible': True,
            'mobile_responsive': mobile_analysis['is_responsive'],
            'mobile_score': mobile_analysis['score'],
            'modern_design': design_analysis['is_modern'],
            'design_score': design_analysis['score'],
            'has_ssl': security_analysis['has_ssl'],
            'security_score': security_analysis['score'],
            'performance_score': performance_analysis['score'],
            'content_score': content_analysis['score'],
            'seo_score': seo_analysis['score'],
            'technology_stack': tech_analysis['stack'],
            'last_updated': design_analysis['last_updated'],
            'load_time': performance_analysis['load_time'],
            'page_size': performance_analysis['page_size'],
            'overall_score': calculate_overall_score({
                'mobile': mobile_analysis['score'],
                'design': design_analysis['score'],
                'security': security_analysis['score'],
                'performance': performance_analysis['score'],
                'content': content_analysis['score'],
                'seo': seo_analysis['score']
            }),
            'error': None,
            'detailed_issues': compile_detailed_issues({
                'mobile': mobile_analysis,
                'design': design_analysis,
                'security': security_analysis,
                'performance': performance_analysis,
                'content': content_analysis,
                'seo': seo_analysis
            })
        }
        
    except PlaywrightTimeoutError:
        return create_timeout_analysis()
    except Exception as e:
        return create_error_analysis(str(e))

async def analyze_security(page, url):
    """Enhanced security analysis"""
    has_ssl = url.startswith('https://')
    score = 10 if has_ssl else 0
    
    # Check for security headers
    try:
        security_headers = await page.evaluate("""
            () => {
                const headers = {};
                // Note: Can't access response headers from client-side
                // This would need server-side implementation
                return {
                    contentSecurityPolicy: false,
                    strictTransportSecurity: false,
                    xFrameOptions: false
                };
            }
        """)
        
        # Check for mixed content
        has_mixed_content = await page.evaluate("""
            () => {
                const images = Array.from(document.images);
                const scripts = Array.from(document.scripts);
                const links = Array.from(document.querySelectorAll('link'));
                
                const httpResources = [
                    ...images.filter(img => img.src.startsWith('http:')),
                    ...scripts.filter(script => script.src.startsWith('http:')),
                    ...links.filter(link => link.href.startsWith('http:'))
                ];
                
                return httpResources.length > 0;
            }
        """)
        
        if has_mixed_content:
            score -= 3
            
    except:
        pass
    
    return {
        'has_ssl': has_ssl,
        'score': max(0, score),
        'issues': [] if has_ssl else ['No SSL certificate']
    }

async def test_mobile_responsiveness(page):
    """Enhanced mobile responsiveness testing"""
    issues = []
    score = 10
    
    # Test different viewport sizes
    viewports = [
        {"width": 390, "height": 844, "name": "iPhone"},
        {"width": 768, "height": 1024, "name": "Tablet"},
        {"width": 1920, "height": 1080, "name": "Desktop"}
    ]
    
    responsive_scores = []
    
    for viewport in viewports:
        await page.set_viewport_size({"width": viewport["width"], "height": viewport["height"]})
        await page.wait_for_timeout(1000)
        
        viewport_analysis = await page.evaluate(f"""
            () => {{
                const viewport = document.querySelector('meta[name="viewport"]');
                const bodyWidth = document.body.scrollWidth;
                const windowWidth = window.innerWidth;
                const hasHorizontalScroll = bodyWidth > windowWidth + 10;
                
                // Check for responsive design patterns
                const hasFlexbox = Array.from(document.querySelectorAll('*')).some(el => 
                    getComputedStyle(el).display.includes('flex')
                );
                
                const hasGrid = Array.from(document.querySelectorAll('*')).some(el => 
                    getComputedStyle(el).display.includes('grid')
                );
                
                const hasMediaQueries = Array.from(document.styleSheets).some(sheet => {{
                    try {{
                        return Array.from(sheet.cssRules || []).some(rule => 
                            rule.media && rule.media.mediaText.includes('max-width')
                        );
                    }} catch(e) {{ return false; }}
                }});
                
                return {{
                    hasViewportMeta: !!viewport,
                    hasHorizontalScroll,
                    hasFlexbox,
                    hasGrid,
                    hasMediaQueries,
                    bodyWidth,
                    windowWidth,
                    viewportName: '{viewport["name"]}'
                }};
            }}
        """)
        
        viewport_score = 10
        if viewport_analysis['hasHorizontalScroll']:
            viewport_score -= 5
            issues.append(f"Horizontal scroll on {viewport['name']}")
        
        if not viewport_analysis['hasViewportMeta']:
            viewport_score -= 2
            issues.append("Missing viewport meta tag")
        
        if not viewport_analysis['hasMediaQueries'] and viewport['name'] != 'Desktop':
            viewport_score -= 3
            issues.append("No responsive CSS media queries")
        
        responsive_scores.append(viewport_score)
    
    avg_score = sum(responsive_scores) / len(responsive_scores)
    
    return {
        'is_responsive': avg_score >= 7,
        'score': max(0, int(avg_score)),
        'issues': issues
    }

async def analyze_design_modernity(page):
    """Enhanced design modernity analysis"""
    design_analysis = await page.evaluate("""
        () => {
            const body = document.body;
            const allElements = Array.from(document.querySelectorAll('*'));
            
            // Check for modern CSS features
            const modernFeatures = {
                flexbox: allElements.some(el => getComputedStyle(el).display.includes('flex')),
                grid: allElements.some(el => getComputedStyle(el).display.includes('grid')),
                transforms: allElements.some(el => getComputedStyle(el).transform !== 'none'),
                transitions: allElements.some(el => getComputedStyle(el).transition !== 'all 0s ease 0s'),
                borderRadius: allElements.some(el => getComputedStyle(el).borderRadius !== '0px'),
                boxShadow: allElements.some(el => getComputedStyle(el).boxShadow !== 'none'),
                gradients: allElements.some(el => {
                    const bg = getComputedStyle(el).backgroundImage;
                    return bg && bg.includes('gradient');
                })
            };
            
            // Check for modern design patterns
            const designPatterns = {
                hasHamburgerMenu: !!document.querySelector('.hamburger, .menu-toggle, [class*="mobile-menu"], [class*="nav-toggle"]'),
                hasHeroSection: !!document.querySelector('.hero, [class*="banner"], [class*="header-image"], [class*="jumbotron"]'),
                hasCards: !!document.querySelector('.card, [class*="card"], .tile, [class*="tile"]'),
                hasModals: !!document.querySelector('.modal, [class*="modal"], .popup, [class*="popup"]'),
                hasSlider: !!document.querySelector('.slider, [class*="slider"], .carousel, [class*="carousel"]')
            };
            
            // Check color scheme and typography
            const typography = {
                hasCustomFonts: Array.from(document.fonts).length > 0,
                hasWebFonts: Array.from(document.styleSheets).some(sheet => {
                    try {
                        return Array.from(sheet.cssRules || []).some(rule => 
                            rule.cssText && rule.cssText.includes('@font-face')
                        );
                    } catch(e) { return false; }
                })
            };
            
            // Check last modified date
            const lastModified = document.lastModified;
            const lastModifiedDate = new Date(lastModified);
            const oneYearAgo = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000);
            const twoYearsAgo = new Date(Date.now() - 2 * 365 * 24 * 60 * 60 * 1000);
            
            return {
                modernFeatures,
                designPatterns,
                typography,
                lastModified,
                isRecentlyUpdated: lastModifiedDate > oneYearAgo,
                isVeryOld: lastModifiedDate < twoYearsAgo
            };
        }
    """)
    
    score = 0
    issues = []
    
    # Score modern CSS features
    modern_css_count = sum(design_analysis['modernFeatures'].values())
    score += min(4, modern_css_count)
    
    if modern_css_count < 2:
        issues.append("Limited modern CSS features")
    
    # Score design patterns
    pattern_count = sum(design_analysis['designPatterns'].values())
    score += min(3, pattern_count)
    
    if pattern_count < 2:
        issues.append("Limited modern design patterns")
    
    # Score typography
    if design_analysis['typography']['hasCustomFonts'] or design_analysis['typography']['hasWebFonts']:
        score += 2
    else:
        issues.append("Using default system fonts")
    
    # Score freshness
    if design_analysis['isRecentlyUpdated']:
        score += 1
    elif design_analysis['isVeryOld']:
        score -= 2
        issues.append("Website appears very outdated")
    
    return {
        'is_modern': score >= 6,
        'score': max(0, min(10, score)),
        'issues': issues,
        'last_updated': design_analysis['lastModified']
    }

async def analyze_performance(page):
    """Basic performance analysis"""
    try:
        # Measure load time
        start_time = time.time()
        await page.reload(wait_until="networkidle")
        load_time = time.time() - start_time
        
        # Get page size estimate
        page_analysis = await page.evaluate("""
            () => {
                const images = Array.from(document.images);
                const scripts = Array.from(document.scripts);
                const links = Array.from(document.querySelectorAll('link'));
                
                return {
                    imageCount: images.length,
                    scriptCount: scripts.length,
                    linkCount: links.length,
                    domElements: document.querySelectorAll('*').length
                };
            }
        """)
        
        score = 10
        issues = []
        
        if load_time > 5:
            score -= 4
            issues.append("Slow loading time (>5s)")
        elif load_time > 3:
            score -= 2
            issues.append("Moderate loading time (>3s)")
        
        if page_analysis['imageCount'] > 50:
            score -= 2
            issues.append("Too many images")
        
        if page_analysis['scriptCount'] > 20:
            score -= 1
            issues.append("Many script files")
        
        return {
            'score': max(0, score),
            'load_time': load_time,
            'page_size': 'Unknown',
            'issues': issues
        }
        
    except:
        return {
            'score': 5,
            'load_time': 'Unknown',
            'page_size': 'Unknown',
            'issues': ['Could not measure performance']
        }

async def analyze_content_quality(page):
    """Analyze content quality and completeness"""
    try:
        content_analysis = await page.evaluate("""
            () => {
                const text = document.body.innerText || '';
                const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
                const images = document.querySelectorAll('img');
                const links = document.querySelectorAll('a');
                const forms = document.querySelectorAll('form');
                
                // Check for key business information
                const hasContactInfo = /contact|phone|email|address/i.test(text);
                const hasAboutInfo = /about|service|product|team/i.test(text);
                const hasBusinessHours = /hour|open|close|monday|tuesday|wednesday|thursday|friday|saturday|sunday/i.test(text);
                
                // Check for images with alt text
                const imagesWithAlt = Array.from(images).filter(img => img.alt && img.alt.trim().length > 0);
                
                return {
                    wordCount: text.split(/\s+/).length,
                    headingCount: headings.length,
                    imageCount: images.length,
                    imagesWithAlt: imagesWithAlt.length,
                    linkCount: links.length,
                    formCount: forms.length,
                    hasContactInfo,
                    hasAboutInfo,
                    hasBusinessHours,
                    hasMainHeading: !!document.querySelector('h1')
                };
            }
        """)
        
        score = 0
        issues = []
        
        # Content volume
        if content_analysis['wordCount'] > 200:
            score += 2
        else:
            issues.append("Limited content")
        
        # Structure
        if content_analysis['hasMainHeading']:
            score += 1
        else:
            issues.append("Missing main heading (H1)")
        
        if content_analysis['headingCount'] >= 3:
            score += 1
        else:
            issues.append("Poor content structure")
        
        # Business information
        if content_analysis['hasContactInfo']:
            score += 2
        else:
            issues.append("Missing contact information")
        
        if content_analysis['hasAboutInfo']:
            score += 1
        else:
            issues.append("Missing about/services information")
        
        # Image optimization
        if content_analysis['imageCount'] > 0:
            alt_ratio = content_analysis['imagesWithAlt'] / content_analysis['imageCount']
            if alt_ratio > 0.8:
                score += 1
            else:
                issues.append("Images missing alt text")
        
        # Interactive elements
        if content_analysis['formCount'] > 0:
            score += 1
        
        return {
            'score': min(10, score),
            'issues': issues
        }
        
    except:
        return {
            'score': 5,
            'issues': ['Could not analyze content']
        }

async def analyze_seo_basics(page):
    """Analyze basic SEO elements"""
    try:
        seo_analysis = await page.evaluate("""
            () => {
                const title = document.title;
                const metaDescription = document.querySelector('meta[name="description"]');
                const metaKeywords = document.querySelector('meta[name="keywords"]');
                const h1Tags = document.querySelectorAll('h1');
                const canonicalLink = document.querySelector('link[rel="canonical"]');
                const ogTags = document.querySelectorAll('meta[property^="og:"]');
                
                return {
                    hasTitle: !!title && title.length > 0,
                    titleLength: title ? title.length : 0,
                    hasMetaDescription: !!metaDescription,
                    metaDescriptionLength: metaDescription ? metaDescription.content.length : 0,
                    hasMetaKeywords: !!metaKeywords,
                    h1Count: h1Tags.length,
                    hasCanonical: !!canonicalLink,
                    ogTagCount: ogTags.length
                };
            }
        """)
        
        score = 0
        issues = []
        
        # Title tag
        if seo_analysis['hasTitle']:
            if 30 <= seo_analysis['titleLength'] <= 60:
                score += 2
            else:
                score += 1
                issues.append("Title length not optimal")
        else:
            issues.append("Missing title tag")
        
        # Meta description
        if seo_analysis['hasMetaDescription']:
            if 120 <= seo_analysis['metaDescriptionLength'] <= 160:
                score += 2
            else:
                score += 1
                issues.append("Meta description length not optimal")
        else:
            issues.append("Missing meta description")
        
        # H1 tag
        if seo_analysis['h1Count'] == 1:
            score += 1
        elif seo_analysis['h1Count'] > 1:
            issues.append("Multiple H1 tags")
        else:
            issues.append("Missing H1 tag")
        
        # Social media tags
        if seo_analysis['ogTagCount'] >= 3:
            score += 1
        else:
            issues.append("Missing Open Graph tags")
        
        return {
            'score': min(10, score),
            'issues': issues
        }
        
    except:
        return {
            'score': 3,
            'issues': ['Could not analyze SEO elements']
        }

async def analyze_technology_stack(page):
    """Enhanced technology stack detection"""
    try:
        tech_analysis = await page.evaluate("""
            () => {
                const technologies = [];
                
                // JavaScript frameworks/libraries
                if (window.jQuery) technologies.push('jQuery ' + (window.jQuery.fn.jquery || ''));
                if (window.React) technologies.push('React');
                if (window.Vue) technologies.push('Vue.js');
                if (window.Angular) technologies.push('Angular');
                if (window.ember) technologies.push('Ember.js');
                if (document.querySelector('[data-reactroot]')) technologies.push('React');
                if (document.querySelector('[data-vue]') || document.querySelector('[v-cloak]')) technologies.push('Vue.js');
                
                // CSS frameworks
                if (document.querySelector('link[href*="bootstrap"]') || document.querySelector('[class*="bootstrap"]')) {
                    technologies.push('Bootstrap');
                }
                if (document.querySelector('link[href*="foundation"]') || document.querySelector('[class*="foundation"]')) {
                    technologies.push('Foundation');
                }
                if (document.querySelector('[class*="tailwind"]') || document.querySelector('link[href*="tailwind"]')) {
                    technologies.push('Tailwind CSS');
                }
                
                // CMS detection
                const generator = document.querySelector('meta[name="generator"]');
                if (generator) {
                    technologies.push(generator.content);
                }
                
                // WordPress
                if (document.querySelector('link[href*="wp-content"]') || 
                    document.querySelector('script[src*="wp-content"]') ||
                    document.querySelector('meta[name="generator"][content*="WordPress"]')) {
                    technologies.push('WordPress');
                }
                
                // Shopify
                if (window.Shopify || document.querySelector('[data-shopify]') || 
                    document.querySelector('script[src*="shopify"]')) {
                    technologies.push('Shopify');
                }
                
                // Squarespace
                if (document.querySelector('script[src*="squarespace"]') || 
                    document.querySelector('[class*="squarespace"]')) {
                    technologies.push('Squarespace');
                }
                
                // Wix
                if (document.querySelector('script[src*="wix.com"]') || 
                    document.querySelector('[class*="wix"]')) {
                    technologies.push('Wix');
                }
                
                // Webflow
                if (document.querySelector('script[src*="webflow"]') || 
                    document.querySelector('[class*="webflow"]')) {
                    technologies.push('Webflow');
                }
                
                return technologies.length > 0 ? technologies : ['Custom/Unknown'];
            }
        """)
        
        return {
            'stack': tech_analysis.join(', '),
            'technologies': tech_analysis
        }
        
    except:
        return {
            'stack': 'Unknown',
            'technologies': ['Unknown']
        }

def calculate_overall_score(scores):
    """Calculate weighted overall score"""
    weights = {
        'mobile': 0.25,      # Mobile is critical
        'design': 0.20,      # Design modernity important
        'security': 0.15,    # Security baseline
        'performance': 0.15, # Performance matters
        'content': 0.15,     # Content quality
        'seo': 0.10         # SEO basics
    }
    
    weighted_score = sum(scores[key] * weights[key] for key in scores)
    return round(weighted_score, 1)

def compile_detailed_issues(analyses):
    """Compile detailed issues from all analyses"""
    all_issues = []
    
    for category, analysis in analyses.items():
        if 'issues' in analysis and analysis['issues']:
            category_issues = [f"{category.title()}: {issue}" for issue in analysis['issues']]
            all_issues.extend(category_issues)
    
    return '; '.join(all_issues)

def create_failed_analysis(response):
    """Create analysis result for failed websites"""
    return {
        'accessible': False,
        'mobile_responsive': False,
        'mobile_score': 0,
        'modern_design': False,
        'design_score': 0,
        'has_ssl': False,
        'security_score': 0,
        'performance_score': 0,
        'content_score': 0,
        'seo_score': 0,
        'technology_stack': 'Unknown',
        'overall_score': 0,
        'error': f'HTTP {response.status if response else "No response"}',
        'detailed_issues': 'Website not accessible'
    }

def create_timeout_analysis():
    """Create analysis result for timeout"""
    return {
        'accessible': False,
        'mobile_responsive': False,
        'mobile_score': 0,
        'modern_design': False,
        'design_score': 0,
        'has_ssl': False,
        'security_score': 0,
        'performance_score': 0,
        'content_score': 0,
        'seo_score': 0,
        'technology_stack': 'Unknown',
        'overall_score': 0,
        'error': 'Timeout',
        'detailed_issues': 'Website loading timeout'
    }

def create_error_analysis(error):
    """Create analysis result for errors"""
    return {
        'accessible': False,
        'mobile_responsive': False,
        'mobile_score': 0,
        'modern_design': False,
        'design_score': 0,
        'has_ssl': False,
        'security_score': 0,
        'performance_score': 0,
        'content_score': 0,
        'seo_score': 0,
        'technology_stack': 'Unknown',
        'overall_score': 0,
        'error': error,
        'detailed_issues': f'Analysis error: {error}'
    }

# Enhanced qualification logic
def determine_qualification(website_analysis, maps_result, business_data):
    """Enhanced qualification determination with better prioritization"""
    
    # Skip if not accessible at all
    if not website_analysis['accessible'] and not maps_result['found']:
        return None, 'not_accessible'
    
    # Business appears closed
    if maps_result['found'] and not maps_result['appears_active']:
        return {**business_data, 'status': 'closed', 'reason': 'Appears closed on Google Maps'}, 'closed'
    
    # No website but active business
    if not website_analysis['accessible'] and maps_result['found'] and maps_result['appears_active']:
        result = {
            **business_data,
            'overall_score': 0,
            'priority': 'HIGH',
            'status': 'needs_website',
            'qualification_reason': 'Active business without functional website',
            'detailed_issues': 'No functional website found'
        }
        return result, 'high_priority'
    
    # Calculate priority based on overall score and specific issues
    overall_score = website_analysis['overall_score']
    
    # High priority: Major issues that need immediate attention
    if overall_score <= 4:
        priority = 'HIGH'
        reason = 'Multiple critical website issues'
    elif overall_score <= 6:
        priority = 'MEDIUM'
        reason = 'Several website improvements needed'
    elif overall_score <= 7.5:
        priority = 'LOW'
        reason = 'Minor website improvements recommended'
    else:
        priority = 'VERY_LOW'
        reason = 'Website in good condition, minimal improvements needed'
    
    # Adjust priority based on specific critical issues
    critical_issues = []
    
    if not website_analysis['has_ssl']:
        priority = 'HIGH'
        critical_issues.append('No SSL certificate')
    
    if not website_analysis['mobile_responsive']:
        if priority not in ['HIGH']:
            priority = 'MEDIUM'
        critical_issues.append('Not mobile responsive')
    
    if website_analysis['mobile_score'] <= 3:
        priority = 'HIGH'
        critical_issues.append('Poor mobile experience')
    
    # Only qualify businesses that need significant improvements
    if priority in ['HIGH', 'MEDIUM'] or overall_score <= 7:
        result = {
            **business_data,
            'website_cleaned': clean_url(business_data.get('website', '')),
            'overall_score': overall_score,
            'mobile_score': website_analysis['mobile_score'],
            'design_score': website_analysis['design_score'],
            'security_score': website_analysis['security_score'],
            'performance_score': website_analysis['performance_score'],
            'content_score': website_analysis['content_score'],
            'seo_score': website_analysis['seo_score'],
            'website_accessible': website_analysis['accessible'],
            'mobile_responsive': website_analysis['mobile_responsive'],
            'modern_design': website_analysis['modern_design'],
            'has_ssl': website_analysis['has_ssl'],
            'technology_stack': website_analysis['technology_stack'],
            'last_updated': website_analysis.get('last_updated', 'Unknown'),
            'load_time': website_analysis.get('load_time', 'Unknown'),
            'google_maps_found': maps_result['found'],
            'appears_active': maps_result['appears_active'],
            'priority': priority,
            'qualification_reason': reason,
            'detailed_issues': website_analysis['detailed_issues'],
            'critical_issues': '; '.join(critical_issues) if critical_issues else 'None',
            'status': 'qualified',
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return result, 'qualified'
    else:
        # Low priority businesses - save separately for reference
        result = {
            **business_data,
            'overall_score': overall_score,
            'priority': priority,
            'status': 'low_priority',
            'qualification_reason': reason
        }
        return result, 'low_priority'

# Enhanced Google Maps search with better business verification
async def search_google_maps(page, business_name, city=None):
    """Enhanced Google Maps search with better accuracy"""
    try:
        search_query = business_name.strip()
        if city and city.strip():
            search_query += f" {city.strip()}"
        
        # Go to Google Maps
        await page.goto("https://www.google.com/maps", timeout=TIMEOUT)
        await page.wait_for_timeout(2000)
        
        # Accept cookies if present
        try:
            accept_button = await page.wait_for_selector('button:has-text("Accept all"), button:has-text("I agree")', timeout=3000)
            if accept_button:
                await accept_button.click()
                await page.wait_for_timeout(1000)
        except:
            pass
        
        # Search for business
        search_box = await page.wait_for_selector('input[id="searchboxinput"]', timeout=10000)
        await search_box.fill(search_query)
        await page.keyboard.press('Enter')
        
        # Wait for results to load
        await page.wait_for_timeout(5000)
        
        # Enhanced business verification
        business_info = await page.evaluate("""
            () => {
                // Look for business results
                const businessResults = document.querySelectorAll('[data-value="Directions"], [role="article"], .hfpxzc');
                
                if (businessResults.length === 0) {
                    return { found: false, appears_active: false, confidence: 'none' };
                }
                
                // Check for closed indicators
                const pageText = document.body.innerText.toLowerCase();
                const closedIndicators = [
                    'permanently closed',
                    'temporarily closed', 
                    'closed until',
                    'no longer in business',
                    'out of business'
                ];
                
                const isClosed = closedIndicators.some(indicator => 
                    pageText.includes(indicator)
                );
                
                // Check for active indicators
                const activeIndicators = [
                    'open now',
                    'closes at',
                    'opens at',
                    'hours',
                    'phone',
                    'website',
                    'directions'
                ];
                
                const hasActiveIndicators = activeIndicators.some(indicator => 
                    pageText.includes(indicator)
                );
                
                // Determine confidence level
                let confidence = 'low';
                if (businessResults.length === 1 && hasActiveIndicators) {
                    confidence = 'high';
                } else if (businessResults.length <= 3 && hasActiveIndicators) {
                    confidence = 'medium';
                }
                
                return {
                    found: true,
                    appears_active: !isClosed && hasActiveIndicators,
                    confidence: confidence,
                    resultCount: businessResults.length,
                    isClosed: isClosed
                };
            }
        """)
        
        # Get the current URL for reference
        maps_url = page.url if business_info['found'] else None
        
        return {
            'found': business_info['found'],
            'appears_active': business_info['appears_active'],
            'confidence': business_info.get('confidence', 'low'),
            'google_maps_url': maps_url,
            'result_count': business_info.get('resultCount', 0),
            'is_closed': business_info.get('isClosed', False)
        }
        
    except Exception as e:
        return {
            'found': False,
            'appears_active': False,
            'confidence': 'error',
            'google_maps_url': None,
            'error': str(e)
        }

# Enhanced business processing with better error handling
async def process_business(context, business_data, column_mapping):
    """Enhanced business processing with better accuracy and error handling"""
    page = None
    try:
        page = await context.new_page()
        
        # Extract business information with better validation
        business_name = str(business_data.get(column_mapping['business_name'], '')).strip()
        website = str(business_data.get(column_mapping.get('website', ''), '')).strip()
        city = str(business_data.get(column_mapping.get('city', ''), '')).strip()
        
        # Skip if no business name
        if not business_name or business_name.lower() in ['nan', 'none', 'n/a']:
            return None, 'no_name'
        
        # Clean and validate website URL
        cleaned_website = clean_url(website)
        
        # Skip social media URLs
        if cleaned_website and is_social_media_url(cleaned_website):
            return None, 'social_media'
        
        # Search Google Maps with enhanced verification
        maps_result = await search_google_maps(page, business_name, city)
        
        # Initialize website analysis
        if cleaned_website:
            website_analysis = await check_website_quality(page, cleaned_website)
        else:
            website_analysis = create_failed_analysis(None)
        
        # Enhanced qualification determination
        result, status = determine_qualification(website_analysis, maps_result, business_data)
        
        return result, status
        
    except Exception as e:
        return None, f'error: {str(e)}'
    finally:
        if page:
            try:
                await page.close()
            except:
                pass

# Enhanced data loading with better column detection
def load_data_file(file_path):
    """Enhanced data loading with better error handling"""
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
            # Enhanced CSV reading with better encoding detection
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    print(f"📊 Loaded CSV file with {len(df)} rows (encoding: {encoding})")
                    break
                except (UnicodeDecodeError, pd.errors.EmptyDataError):
                    continue
            
            if df is None:
                print(f"❌ Could not read CSV file with any encoding")
                return None
        else:
            print(f"❌ Unsupported file format: {file_path.suffix}")
            return None
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Print column information
        print(f"📋 Available columns ({len(df.columns)}):")
        for i, col in enumerate(df.columns, 1):
            non_null_count = df[col].notna().sum()
            print(f"  {i:2d}. {col} ({non_null_count} non-null values)")
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None

# Enhanced column detection with more patterns
def detect_columns(df):
    """Enhanced column detection with more comprehensive patterns"""
    columns = [col.lower().strip() for col in df.columns]
    column_mapping = {}
    
    # Business name patterns (expanded)
    business_patterns = [
        'business name', 'company name', 'business', 'company', 'name', 
        'business_name', 'company_name', 'firm name', 'organization',
        'client name', 'account name', 'entity name'
    ]
    for pattern in business_patterns:
        for i, col in enumerate(columns):
            if pattern in col or col == pattern:
                column_mapping['business_name'] = df.columns[i]
                break
        if 'business_name' in column_mapping:
            break
    
    # Website patterns (expanded)
    website_patterns = [
        'website', 'url', 'site', 'web', 'link', 'homepage', 'web site',
        'website url', 'web address', 'domain', 'web_site', 'website_url'
    ]
    for pattern in website_patterns:
        for i, col in enumerate(columns):
            if pattern in col or col == pattern:
                column_mapping['website'] = df.columns[i]
                break
        if 'website' in column_mapping:
            break
    
    # City patterns (expanded)
    city_patterns = [
        'city', 'location', 'place', 'town', 'municipality', 'locality',
        'address', 'region', 'area', 'district'
    ]
    for pattern in city_patterns:
        for i, col in enumerate(columns):
            if pattern in col or col == pattern:
                column_mapping['city'] = df.columns[i]
                break
        if 'city' in column_mapping:
            break
    
    print(f"🔍 Detected columns:")
    for key, value in column_mapping.items():
        if value:
            print(f"  {key}: {value}")
        else:
            print(f"  {key}: NOT FOUND")
    
    return column_mapping

# Enhanced progress tracking
def save_progress(results, closed, failed, low_priority, batch_num):
    """Enhanced progress saving with all categories"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if results:
        results_df = pd.DataFrame(results)
        results_df.to_csv(f"output/progress_qualified_batch_{batch_num}_{timestamp}.csv", index=False)
    
    if closed:
        closed_df = pd.DataFrame(closed)
        closed_df.to_csv(f"output/progress_closed_batch_{batch_num}_{timestamp}.csv", index=False)
    
    if low_priority:
        low_priority_df = pd.DataFrame(low_priority)
        low_priority_df.to_csv(f"output/progress_lowpriority_batch_{batch_num}_{timestamp}.csv", index=False)
    
    print(f"✅ Progress saved for batch {batch_num}")

# Enhanced batch processing with better error recovery
async def process_businesses_batch(businesses, column_mapping, pbar):
    """Enhanced batch processing with better error handling and recovery"""
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
                '--disable-gpu',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding'
            ]
        )
        
        context = await browser.new_context(
            viewport=DESKTOP_VIEWPORT,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )
        
        results = []
        closed = []
        failed = []
        low_priority = []
        
        # Process with controlled concurrency
        semaphore = asyncio.Semaphore(CONCURRENT_PAGES_PER_BROWSER)
        
        async def process_with_semaphore(business_data):
            async with semaphore:
                try:
                    result, status = await process_business(context, business_data, column_mapping)
                    
                    # Add randomized delay to avoid rate limiting
                    await asyncio.sleep(random.uniform(1.0, 2.5))
                    
                    return result, status
                except Exception as e:
                    return None, f'batch_error: {str(e)}'
        
        # Create and process tasks
        tasks = [process_with_semaphore(business) for business in businesses]
        
        for task in asyncio.as_completed(tasks):
            try:
                result, status = await task
                
                if status == 'qualified':
                    results.append(result)
                elif status == 'high_priority':
                    results.append(result)  # High priority goes to qualified
                elif status == 'closed':
                    closed.append(result)
                elif status == 'low_priority':
                    low_priority.append(result)
                elif result is None:
                    failed.append({'reason': status, 'timestamp': datetime.now().isoformat()})
                
                pbar.update(1)
                
            except Exception as e:
                failed.append({'reason': f'task_error: {str(e)}', 'timestamp': datetime.now().isoformat()})
                pbar.update(1)
                continue
        
        await browser.close()
        
        return results, closed, failed, low_priority

# Enhanced main function with better reporting
async def main():
    """Enhanced main function with comprehensive reporting"""
    try:
        print(f"🚀 Starting Enhanced Business Website Audit")
        print(f"📁 Input file: {INPUT_FILE}")
        
        # Create output directories
        for directory in ["output", "logs", "reports"]:
            os.makedirs(directory, exist_ok=True)
        
        # Load and validate data
        df = load_data_file(INPUT_FILE)
        if df is None:
            return
        
        # Estimate completion time (updated rate)
        businesses_per_minute = 6  # More realistic with enhanced analysis
        minutes = len(df) / businesses_per_minute
        hours = minutes / 60
        print(f"⏱️  Estimated completion time: {hours:.1f} hours ({minutes:.0f} minutes)")
        
        if hours > 5.5:
            print(f"⚠️  WARNING: Estimated time may exceed GitHub Actions 6-hour limit")
            print(f"📊 Consider splitting into files of ~1500-2000 businesses each")
        
        # Detect and validate columns
        column_mapping = detect_columns(df)
        
        if 'business_name' not in column_mapping:
            print("❌ Could not find business name column")
            print("Available columns:", list(df.columns))
            return
        
        print(f"\n🚀 Starting enhanced analysis of {len(df)} businesses...")
        
        # Convert to records
        businesses = df.to_dict('records')
        
        # Initialize progress tracking
        pbar = tqdm(
            total=len(businesses),
            desc="Analyzing businesses",
            unit="biz",
            ncols=120,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
        )
        
        # Process in batches with enhanced tracking
        all_results = []
        all_closed = []
        all_failed = []
        all_low_priority = []
        
        total_batches = (len(businesses) + BATCH_SIZE - 1) // BATCH_SIZE
        start_time = time.time()
        
        for i in range(0, len(businesses), BATCH_SIZE):
            batch = businesses[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            
            print(f"\n📊 Processing batch {batch_num}/{total_batches} ({len(batch)} businesses)")
            
            try:
                results, closed, failed, low_priority = await process_businesses_batch(
                    batch, column_mapping, pbar
                )
                
                all_results.extend(results)
                all_closed.extend(closed)
                all_failed.extend(failed)
                all_low_priority.extend(low_priority)
                
                # Update progress bar postfix
                pbar.set_postfix({
                    'Qualified': len(all_results),
                    'Closed': len(all_closed),
                    'Low Priority': len(all_low_priority),
                    'Failed': len(all_failed)
                })
                
                print(f"✅ Batch {batch_num} complete:")
                print(f"  - Qualified: {len(results)} (Total: {len(all_results)})")
                print(f"  - Closed: {len(closed)} (Total: {len(all_closed)})")
                print(f"  - Low Priority: {len(low_priority)} (Total: {len(all_low_priority)})")
                print(f"  - Failed: {len(failed)} (Total: {len(all_failed)})")
                
                # Save progress every 3 batches
                if batch_num % 3 == 0:
                    save_progress(all_results, all_closed, all_failed, all_low_priority, batch_num)
                
            except Exception as e:
                print(f"❌ Error in batch {batch_num}: {e}")
                # Add failed businesses from this batch
                batch_failed = [{'reason': f'batch_failure: {str(e)}', 'batch': batch_num} for _ in batch]
                all_failed.extend(batch_failed)
                continue
        
        pbar.close()
        
        # Calculate final statistics
        total_processed = len(all_results) + len(all_closed) + len(all_low_priority) + len(all_failed)
        processing_time = time.time() - start_time
        
        print(f"\n📈 FINAL ENHANCED RESULTS:")
        print(f"={'='*60}")
        print(f"✅ HIGH/MEDIUM Priority (Qualified): {len(all_results)}")
        print(f"🔍 Low Priority (Minor issues): {len(all_low_priority)}")
        print(f"🚫 Closed/Inactive Businesses: {len(all_closed)}")
        print(f"❌ Failed to Process: {len(all_failed)}")
        print(f"📊 Total Processed: {total_processed}/{len(df)}")
        print(f"⏱️  Processing Time: {processing_time/60:.1f} minutes")
        print(f"📈 Rate: {total_processed/(processing_time/60):.1f} businesses/minute")
        
        # Save all results with enhanced data
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if all_results:
            results_df = pd.DataFrame(all_results)
            # Sort by priority and overall score
            priority_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2, 'VERY_LOW': 3}
            results_df['priority_rank'] = results_df['priority'].map(priority_order)
            results_df = results_df.sort_values(['priority_rank', 'overall_score'], ascending=[True, True])
            results_df = results_df.drop('priority_rank', axis=1)
            
            results_df.to_csv(OUTPUT_CSV, index=False)
            results_df.to_csv(f"output/qualified_businesses_{timestamp}.csv", index=False)
            print(f"💾 Qualified businesses saved to: {OUTPUT_CSV}")
            
            # Generate priority breakdown
            priority_counts = results_df['priority'].value_counts()
            print(f"📋 Priority Breakdown:")
            for priority, count in priority_counts.items():
                print(f"  - {priority}: {count} businesses")
        
        if all_low_priority:
            low_priority_df = pd.DataFrame(all_low_priority)
            low_priority_df.to_csv(LOW_PRIORITY_CSV, index=False)
            print(f"💾 Low priority businesses saved to: {LOW_PRIORITY_CSV}")
        
        if all_closed:
            closed_df = pd.DataFrame(all_closed)
            closed_df.to_csv(CLOSED_BUSINESSES_CSV, index=False)
            print(f"💾 Closed businesses saved to: {CLOSED_BUSINESSES_CSV}")
        
        if all_failed:
            failed_df = pd.DataFrame(all_failed)
            failed_df.to_csv(FAILED_BUSINESSES_CSV, index=False)
            print(f"💾 Failed businesses saved to: {FAILED_BUSINESSES_CSV}")
        
        # Generate summary report
        generate_summary_report(all_results, all_closed, all_low_priority, all_failed, processing_time)
        
        print(f"\n🎉 Enhanced analysis complete!")
        print(f"📊 Check the output folder for detailed results and reports")
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"❌ Main process error: {e}")
        import traceback
        traceback.print_exc()

def generate_summary_report(qualified, closed, low_priority, failed, processing_time):
    """Generate a comprehensive summary report"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    report = f"""
# Business Website Audit Summary Report
Generated: {timestamp}

## Overview
- **Processing Time**: {processing_time/60:.1f} minutes
- **Total Businesses Analyzed**: {len(qualified) + len(closed) + len(low_priority) + len(failed)}

## Results Breakdown

### 🎯 Qualified Businesses (Need Website Improvements): {len(qualified)}
These businesses have websites that need significant improvements and represent good prospects for web development services.

"""
    
    if qualified:
        qualified_df = pd.DataFrame(qualified)
        priority_counts = qualified_df['priority'].value_counts()
        
        report += "**Priority Distribution:**\n"
        for priority, count in priority_counts.items():
            percentage = (count / len(qualified)) * 100
            report += f"- {priority}: {count} ({percentage:.1f}%)\n"
        
        if 'overall_score' in qualified_df.columns:
            avg_score = qualified_df['overall_score'].mean()
            report += f"\n**Average Overall Score**: {avg_score:.1f}/10\n"
        
        # Top issues
        if 'detailed_issues' in qualified_df.columns:
            all_issues = ' '.join(qualified_df['detailed_issues'].fillna('').astype(str))
            common_issues = [
                ('mobile', 'Mobile responsiveness'),
                ('ssl', 'SSL certificate'),
                ('design', 'Modern design'),
                ('performance', 'Performance'),
                ('content', 'Content quality')
            ]
            
            report += "\n**Most Common Issues:**\n"
            for keyword, issue_name in common_issues:
                if keyword.lower() in all_issues.lower():
                    count = all_issues.lower().count(keyword.lower())
                    report += f"- {issue_name}: ~{count} businesses\n"
    
    report += f"""

### 🔍 Low Priority Businesses: {len(low_priority)}
These businesses have minor website issues but are not high-priority prospects.

### 🚫 Closed/Inactive Businesses: {len(closed)}
These businesses appear to be closed or inactive based on Google Maps data.

### ❌ Failed to Process: {len(failed)}
These businesses could not be analyzed due to technical issues.

## Recommendations

1. **Focus on HIGH priority businesses first** - they have the most critical issues
2. **Mobile responsiveness is crucial** - many businesses lack mobile-friendly websites
3. **SSL certificates are basic security** - businesses without HTTPS need immediate attention
4. **Consider package deals** - many businesses need multiple improvements

## Next Steps

1. Review the qualified_businesses.csv file
2. Sort by priority and overall_score for best prospects
3. Use the detailed_issues column to customize your outreach
4. Check Google Maps status before contacting businesses

---
Report generated by Enhanced Business Website Audit Tool
"""
    
    # Save report
    with open(f"reports/audit_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md", 'w') as f:
        f.write(report)
    
    print(f"📄 Summary report generated in reports/ folder")

if __name__ == "__main__":
    asyncio.run(main())