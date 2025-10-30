import pandas as pd
import requests
from bs4 import BeautifulSoup ,Tag
import json
import os
import time
from urllib.parse import urljoin, urlparse, unquote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import re
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
import hashlib
from collections import defaultdict, deque
import random

# ============================================================================
# ENHANCED CONFIGURATION AND SETUP
# ============================================================================

@dataclass
class EnhancedAnalyzerConfig:
    """Enhanced configuration settings for comprehensive website analysis"""
    MAX_WORKERS: int = 20                 # Maximum parallel processes
    REQUEST_TIMEOUT: int = 50          # HTTP request timeout
    SELENIUM_TIMEOUT: int = 55            # Selenium page load timeout
    MAX_PAGES_PER_SITE: int = 1000         # Maximum pages to analyze per site
    MAX_CRAWL_DEPTH: int = 45               # Maximum crawl depth
    MIN_HTML_LENGTH: int = 1000            # Minimum HTML length to consider valid
    MAX_RETRIES: int = 3                   # Maximum retry attempts
    DELAY_BETWEEN_REQUESTS: float = 20    # Delay between requests
    COMPREHENSIVE_CRAWL: bool = True       # Enable comprehensive crawling
    EXTRACT_ALL_LINKS: bool = True         # Extract every possible link
    DISCOVER_GOOGLE_MAPS: bool = True      # Enhanced Google Maps discovery
    USER_AGENT: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

config = EnhancedAnalyzerConfig()

# Error patterns to detect
ERROR_PATTERNS = [
    r"520: Web server is returning an unknown error",
    r"Just a moment...",
    r"Not Found",
    r"Access Denied",
    r"Service Unavailable",
    r"Cloudflare",
    r"captcha",
    r"security check",
    r"Page Not Found",
    r"404 Error",
    r"503 Service Unavailable"
]

def detect_page_errors(html: str) -> bool:
    """
    Detect common error patterns in HTML content.
    Returns True if any error pattern is found, False otherwise.
    """
    html_lower = html.lower()
    return any(re.search(pattern, html_lower) for pattern in ERROR_PATTERNS)

def should_retry_fetch(status_code: int, html: str) -> bool:
    """
    Determine if a fetch should be retried based on status code or HTML content.
    Returns True for retryable errors, False otherwise.
    """
    if status_code in [429, 500, 502, 503, 504]:  # Retryable status codes
        return True
        
    if detect_page_errors(html):
        return True
        
    return False

def handle_fetch_error(url: str, status_code: int, html: str, attempt: int, max_retries: int) -> None:
    """
    Handle fetch errors with appropriate logging and behavior.
    Implements exponential backoff for retryable errors.
    """
    error_type = "UNKNOWN"
    
    if "520: Web server is returning an unknown error" in html:
        error_type = "520 Unknown Server Error"
    elif "Just a moment..." in html:
        error_type = "Cloudflare Challenge"
    elif "Access Denied" in html:
        error_type = "Access Denied"
    elif "Service Unavailable" in html:
        error_type = "Service Unavailable"
    elif "Cloudflare" in html:
        error_type = "Cloudflare Protection"
    elif "captcha" in html.lower() or "security check" in html.lower():
        error_type = "CAPTCHA Challenge"
    elif "Not Found" in html or "Page Not Found" in html or "404 Error" in html:
        error_type = "404 Not Found"
    elif "503 Service Unavailable" in html:
        error_type = "503 Service Unavailable"
    
    if should_retry_fetch(status_code, html) and attempt < max_retries:
        wait_time = 2 ** attempt + random.uniform(0, 1)
        logger.warning(f"Retryable error ({error_type}) on {url} - attempt {attempt+1}/{max_retries}. Retrying in {wait_time:.1f}s")
        time.sleep(wait_time)
    else:
        logger.error(f"Permanent error ({error_type}) on {url} - skipping after {attempt} attempts")

def fetch_with_error_handling(url: str, max_retries: int = 3) -> Tuple[Optional[str], Optional[int]]:
    """
    Fetch URL with comprehensive error handling and retry logic
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'},
                timeout=30,
                allow_redirects=True
            )
            html = response.text
            
            # Check for HTML errors
            if detect_page_errors(html):
                handle_fetch_error(url, response.status_code, html, attempt, max_retries)
                continue
                
            return html, response.status_code
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.uniform(0, 1)
                time.sleep(wait_time)
                
    return None, None

@dataclass
class LinkInfo:
    """Enhanced structure for storing link information"""
    url: str
    anchor_text: str = ""
    link_type: str = "internal"       # internal, external, social, contact, maps
    source_page: str = ""             # Page where link was found
    depth: int = 0                    # Crawl depth level
    is_navigation: bool = False       # Navigation menu link
    is_footer: bool = False           # Footer link
    is_contact: bool = False          # Contact-related link
    is_about: bool = False            # About page link
    is_google_maps: bool = False      # Google Maps link
    context: str = ""                 # Surrounding context of the link
    menu_type: str = "body"           # body, header, footer, dropdown, subdropdown
    css_classes: str = ""             # CSS classes of the link element

def safe_extract_links(base_url: str, html: str) -> List[LinkInfo]:
    """
    Extract links with error handling and fallback
    """
    if not html or detect_page_errors(html):
        return []
    
    try:
        return extract_clickable_links(base_url, html)
    except Exception as e:
        logger.error(f"Link extraction failed: {e}")
        return []

# Setup enhanced logging
def setup_enhanced_logging():
    """Setup comprehensive logging system with enhanced detail"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / f"enhanced_analyzer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_enhanced_logging()

# Thread safety
THREAD_LOCK = threading.Lock()
processed_urls = set()
discovered_links = defaultdict(set)

# ============================================================================
# ENHANCED DATA STRUCTURES
# ============================================================================

@dataclass
class LinkInfo:
    """Enhanced structure for storing link information"""
    url: str
    anchor_text: str = ""
    link_type: str = "internal"       # internal, external, social, contact, maps
    source_page: str = ""             # Page where link was found
    depth: int = 0                    # Crawl depth level
    is_navigation: bool = False       # Navigation menu link
    is_footer: bool = False           # Footer link
    is_contact: bool = False          # Contact-related link
    is_about: bool = False            # About page link
    is_google_maps: bool = False      # Google Maps link
    context: str = ""                 # Surrounding context of the link
    menu_type: str = "body"           # body, header, footer, dropdown, subdropdown
    css_classes: str = ""             # CSS classes of the link element

def extract_clickable_links(base_url, html):
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.replace('www.', '')
    
    # Define classification parameters
    SOCIAL_DOMAINS = {
        'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
        'youtube.com', 'pinterest.com', 'tumblr.com', 'reddit.com',
        'snapchat.com', 'whatsapp.com', 't.me', 'weibo.com', 'vk.com'
    }
    
    def is_maps_link(url):
        url_lower = url.lower()
        return ('google.com/maps' in url_lower or
                'goo.gl/maps' in url_lower or
                'maps.google.com' in url_lower or
                'maps.app.goo.gl' in url_lower)
    
    contact_keywords = ['contact', 'call us', 'call', 'reach us', 'reach', 'email', 'phone', 'tel', 'mail', 'contact us']
    about_keywords = ['about', 'about us', 'about the company', 'about our', 'who we are', 'our story', 'our team', 'about the team']
    
    # Menu classification patterns
    menu_class_patterns = {
        'header': r'header|top[-_]?menu|main[-_]?nav|primary[-_]?menu|menu[-_]?primary[-_]?navigation|genesis[-_]?nav[-_]?menu|slideout[-_]?menu[-_]?toggle',
        'footer': r'footer|bottom[-_]?menu',
        'dropdown': r'drop[-_]?down|has[-_]?children|sub[-_]?menu|child[-_]?menu',
        'subdropdown': r'sub[-_]?dropdown|grandchild|sub-sub',
        'mobile': r'mobile[-_]?menu',
        'quicklinks': r'quick[-_]?links'
    }
    
    for tag in soup.find_all('a', href=True):
        href = tag['href'].strip()
        if not href or href.startswith(('javascript:', 'mailto:', 'tel:')):
            continue
            
        full_url = urljoin(base_url, href)
        parsed_url = urlparse(full_url)
        
        if parsed_url.scheme not in ['http', 'https']:
            continue
            
        full_url = full_url.split('#')[0]
        anchor_text = tag.get_text(" ", strip=True)
        css_classes = " ".join(tag.get('class', []))
        
        # Determine link position and menu type
        menu_type = "body"
        is_navigation = False
        is_footer = False
        
        # Check structural elements
        if tag.find_parent('nav') or tag.find_parent(id=re.compile(r'nav|menu', re.I)):
            is_navigation = True
            menu_type = "header"
        elif tag.find_parent('footer'):
            is_footer = True
            menu_type = "footer"
        
        # Check class patterns for menu classification
        for mtype, pattern in menu_class_patterns.items():
            if re.search(pattern, css_classes, re.I) or re.search(pattern, tag.get('class', ''), re.I):
                menu_type = mtype
                if mtype in ['header', 'dropdown', 'subdropdown']:
                    is_navigation = True
                elif mtype == 'footer':
                    is_footer = True
        
        # Homepage detection
        if full_url == base_url or full_url == base_url.rstrip('/') + '/':
            menu_type = "homepage" if menu_type == "body" else menu_type
        
        # Determine link type
        link_domain = parsed_url.netloc.replace('www.', '')
        same_domain = base_domain == link_domain
        
        if same_domain:
            link_type = 'internal'
        else:
            if link_domain in SOCIAL_DOMAINS:
                link_type = 'social'
            elif is_maps_link(full_url):
                link_type = 'maps'
            else:
                url_lower = full_url.lower()
                anchor_lower = anchor_text.lower()
                if any(kw in url_lower or kw in anchor_lower for kw in contact_keywords):
                    link_type = 'contact'
                else:
                    link_type = 'external'
        
        # Set special flags
        url_lower = full_url.lower()
        anchor_lower = anchor_text.lower()
        is_contact = any(kw in url_lower or kw in anchor_lower for kw in contact_keywords)
        is_about = any(kw in url_lower or kw in anchor_lower for kw in about_keywords)
        is_google_maps = is_maps_link(full_url)
        
        # Extract context from parent elements
        context = ""
        context_elements = tag.find_parents(['p', 'div', 'li', 'section', 'article', 'main', 'header', 'footer'])
        if context_elements:
            # Get the immediate parent context
            parent = context_elements[0]
            context = parent.get_text(" ", strip=True)
            context = re.sub(r'\s+', ' ', context)[:500] + '...' if len(context) > 500 else context
        
        # Create link info object
        link_info = LinkInfo(
            url=full_url,
            anchor_text=anchor_text,
            link_type=link_type,
            source_page=base_url,
            depth=0,
            is_navigation=is_navigation,
            is_footer=is_footer,
            is_contact=is_contact,
            is_about=is_about,
            is_google_maps=is_google_maps,
            context=context,
            menu_type=menu_type,
            css_classes=css_classes
        )
        
        links.append(link_info)
    
    return links

@dataclass
class GoogleMapsInfo:
    """Structure for storing Google Maps information"""
    maps_links: List[str] = field(default_factory=list)
    iframe_embeds: List[str] = field(default_factory=list)
    javascript_maps: List[str] = field(default_factory=list)
    structured_data_maps: List[str] = field(default_factory=list)
    contact_page_maps: List[str] = field(default_factory=list)
    all_maps_links: List[str] = field(default_factory=list)
    primary_maps_link: str = ""
    maps_integration_status: str = "Not Found"
    total_maps_found: int = 0

@dataclass
class SiteMap:
    """Enhanced structure for complete website mapping"""
    domain: str
    main_url: str
    total_pages: int = 0
    total_links: int = 0
    internal_links: List[LinkInfo] = field(default_factory=list)
    external_links: List[LinkInfo] = field(default_factory=list)
    social_links: List[LinkInfo] = field(default_factory=list)
    contact_links: List[LinkInfo] = field(default_factory=list)
    google_maps_info: GoogleMapsInfo = field(default_factory=GoogleMapsInfo)
    crawl_depth_reached: int = 0
    page_types: Dict[str, List[str]] = field(default_factory=dict)
    navigation_structure: Dict[str, List[str]] = field(default_factory=dict)
    website_structure_complexity: str = "Unknown"

@dataclass
class ContactInfo:
    """Enhanced structure for storing contact information"""
    brand_name: Optional[str] = None
    email: Optional[str] = None
    mobile_phone: Optional[str] = None
    corporate_phone: Optional[str] = None
    support_phone: Optional[str] = None
    company_phone: Optional[str] = None
    address: Optional[str] = None
    company_city: Optional[str] = None
    company_state: Optional[str] = None
    google_map: Optional[str] = None
    all_google_maps_links: List[str] = field(default_factory=list)
    google_maps_integration: str = "Not Found"
    social_media: Dict[str, Optional[str]] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.social_media:
            self.social_media = {
                'facebook': None, 'instagram': None, 'tiktok': None,
                'linkedin': None, 'twitter': None, 'pinterest': None, 'youtube': None
            }

@dataclass
class EnhancedMetadata:
    """Enhanced structure for storing website metadata"""
    site_title: Optional[str] = None
    meta_description: Optional[str] = None
    meta_keywords: Optional[str] = None
    about_us_text: Optional[str] = None
    about_us_url: Optional[str] = None
    logo_url: Optional[str] = None
    favicon_url: Optional[str] = None
    all_page_titles: List[str] = field(default_factory=list)
    keywords_compilation: List[str] = field(default_factory=list)

@dataclass
class BusinessMetrics:
    """Enhanced structure for storing business metrics"""
    industry: str = "Unknown"
    employees: Optional[str] = None
    annual_revenue: Optional[str] = None
    founded_year: Optional[str] = None
    firmographic_score: int = 0
    engagement_score: int = 0
    segmentation: str = "Unknown"
    website_structure_complexity: str = "Unknown"
    digital_presence_strength: str = "Unknown"
    contact_accessibility: str = "Unknown"

@dataclass
class MarketingIntelligence:
    """Enhanced structure for storing marketing intelligence"""
    instagram_handle: Optional[str] = None
    integrated_video_links: List[str] = field(default_factory=list)
    ig_score: int = 0
    worked_with_creators: bool = False
    ad_library_proof: List[str] = field(default_factory=list)
    social_media_engagement: Dict[str, int] = field(default_factory=dict)

@dataclass
class WebsiteFeatures:
    """Enhanced structure for storing website features"""
    d2c_presence: bool = False
    ecommerce_presence: bool = False
    social_media_presence: bool = False
    video_presence: bool = False
    saas_platform: bool = False
    blog_presence: bool = False
    cta_presence: bool = False
    product_listings: bool = False
    ssl_secure: bool = False
    mobile_responsive: bool = False
    contact_forms: bool = False
    newsletter_signup: bool = False

@dataclass
class PageMetadata:
    """Enhanced structure for storing individual page metadata"""
    url: str
    title: Optional[str] = None
    description: Optional[str] = None
    html: Optional[str] = None
    text_content: Optional[str] = None
    status_code: Optional[int] = None
    load_time: float = 0.0
    word_count: int = 0
    json_ld: List[Dict] = field(default_factory=list)
    meta_tags: List[Dict] = field(default_factory=list)
    is_about_page: bool = False
    is_contact_page: bool = False
    page_type: str = "general"
    depth: int = 0
    links_found: List[LinkInfo] = field(default_factory=list)
    google_maps_found: List[str] = field(default_factory=list)

# ============================================================================
# ENHANCED GOOGLE MAPS DETECTOR
# ============================================================================

class EnhancedGoogleMapsDetector:
    """Comprehensive Google Maps detection with multiple methods"""
    
    def __init__(self):
        self.maps_patterns = [
            r'https?://(?:www\.)?google\.com/maps[^\s"\'<>]*',
            r'https?://(?:www\.)?maps\.google\.com[^\s"\'<>]*',
            r'https?://goo\.gl/maps/[^\s"\'<>]*',
            r'https?://maps\.app\.goo\.gl/[^\s"\'<>]*'
        ]
        
        self.maps_keywords = [
            'google maps', 'google map', 'maps.google.com', 'google.com/maps',
            'directions', 'location', 'address', 'find us', 'visit us'
        ]
    
    def detect_all_google_maps(self, pages_data: List[PageMetadata], sitemap: SiteMap) -> GoogleMapsInfo:
        """Detect Google Maps using all available methods"""
        logger.info("Starting comprehensive Google Maps detection")
        
        maps_info = GoogleMapsInfo()
        all_maps_found = set()
        
        for page in pages_data:
            if not page.html:
                continue
            
            soup = BeautifulSoup(page.html, 'html.parser')
            
            # Method 1: Direct link detection
            direct_links = self._detect_direct_links(soup, page.url)
            maps_info.maps_links.extend(direct_links)
            all_maps_found.update(direct_links)
            
            # Method 2: Iframe embed detection
            iframe_embeds = self._detect_iframe_embeds(soup, page.url)
            maps_info.iframe_embeds.extend(iframe_embeds)
            all_maps_found.update(iframe_embeds)
            
            # Method 3: JavaScript detection
            js_maps = self._detect_javascript_maps(soup, page.url)
            maps_info.javascript_maps.extend(js_maps)
            all_maps_found.update(js_maps)
            
            # Method 4: Structured data detection
            structured_maps = self._detect_structured_data_maps(page.json_ld, page.url)
            maps_info.structured_data_maps.extend(structured_maps)
            all_maps_found.update(structured_maps)
            
            # Store maps found on this page
            page.google_maps_found = list(all_maps_found)
            
            # Special handling for contact pages
            if page.is_contact_page or 'contact' in page.url.lower():
                contact_maps = list(all_maps_found)
                maps_info.contact_page_maps.extend(contact_maps)
        
        # Compile all unique maps
        maps_info.all_maps_links = list(all_maps_found)
        maps_info.total_maps_found = len(all_maps_found)
        
        # Determine primary maps link and integration status
        if all_maps_found:
            maps_info.primary_maps_link = self._select_primary_maps_link(list(all_maps_found))
            maps_info.maps_integration_status = "Integrated"
        else:
            maps_info.maps_integration_status = "Not Found"
        
        logger.info(f"Google Maps detection completed. Found {maps_info.total_maps_found} maps links")
        return maps_info
    
    def _detect_direct_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Detect direct Google Maps links"""
        maps_links = []
        
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(base_url, href)
            
            # Check if it's a Google Maps link
            if any(pattern in full_url.lower() for pattern in [
                'maps.google.com', 'google.com/maps', 'goo.gl/maps', 'maps.app.goo.gl'
            ]):
                maps_links.append(full_url)
                logger.debug(f"Found direct Google Maps link: {full_url}")
        
        return maps_links
    
    def _detect_iframe_embeds(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Detect Google Maps iframe embeds"""
        iframe_maps = []
        
        for iframe in soup.find_all('iframe', src=True):
            src = iframe['src']
            full_url = urljoin(base_url, src)
            
            if any(pattern in full_url.lower() for pattern in [
                'maps.google.com', 'google.com/maps'
            ]):
                iframe_maps.append(full_url)
                logger.debug(f"Found Google Maps iframe: {full_url}")
        
        return iframe_maps
    
    def _detect_javascript_maps(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Detect Google Maps in JavaScript code"""
        js_maps = []
        
        for script in soup.find_all('script'):
            if script.string:
                script_content = script.string
                
                # Look for Google Maps URLs in JavaScript
                for pattern in self.maps_patterns:
                    matches = re.findall(pattern, script_content, re.IGNORECASE)
                    for match in matches:
                        # Clean up the URL
                        clean_url = match.strip('\'"')
                        if clean_url not in js_maps:
                            js_maps.append(clean_url)
                            logger.debug(f"Found Google Maps in JavaScript: {clean_url}")
        
        return js_maps
    
    def _detect_structured_data_maps(self, json_ld_data: List[Dict], base_url: str) -> List[str]:
        """Detect Google Maps in structured data (JSON-LD)"""
        structured_maps = []
        
        for data in json_ld_data:
            if isinstance(data, dict):
                # Look for location or address data
                maps_url = self._extract_maps_from_structured_data(data)
                if maps_url:
                    structured_maps.append(maps_url)
                    logger.debug(f"Found Google Maps in structured data: {maps_url}")
        
        return structured_maps
    
    def _extract_maps_from_structured_data(self, data: Dict) -> Optional[str]:
        """Extract Google Maps URL from structured data"""
        if not isinstance(data, dict):
            return None
        
        # Look for hasMap property
        if 'hasMap' in data:
            return data['hasMap']
        
        # Look for location with coordinates
        if 'location' in data:
            location = data['location']
            if isinstance(location, dict):
                if 'geo' in location:
                    geo = location['geo']
                    if isinstance(geo, dict) and 'latitude' in geo and 'longitude' in geo:
                        lat = geo['latitude']
                        lng = geo['longitude']
                        return f"https://www.google.com/maps?q={lat},{lng}"
        
        # Look for address
        if 'address' in data:
            address = data['address']
            if isinstance(address, dict):
                address_parts = []
                for key in ['streetAddress', 'addressLocality', 'addressRegion', 'postalCode']:
                    if key in address:
                        address_parts.append(str(address[key]))
                
                if address_parts:
                    address_string = ', '.join(address_parts)
                    encoded_address = address_string.replace(' ', '+')
                    return f"https://www.google.com/maps/search/{encoded_address}"
        
        return None
    
    def _select_primary_maps_link(self, maps_links: List[str]) -> str:
        """Select the primary Google Maps link from available options"""
        if not maps_links:
            return ""
        
        # Priority order: direct links > iframe embeds > JavaScript > generated
        priority_patterns = [
            'maps.google.com/place/',  # Specific place
            'google.com/maps/place/',  # Specific place
            'maps.google.com/dir/',    # Directions
            'google.com/maps/dir/',    # Directions
            'maps.google.com',         # General maps
            'google.com/maps',         # General maps
        ]
        
        for pattern in priority_patterns:
            for link in maps_links:
                if pattern in link.lower():
                    return link
        
        # Return first available link if no priority match
        return maps_links[0]

# ============================================================================
# COMPREHENSIVE LINK DISCOVERER
# ============================================================================

class ComprehensiveLinkDiscoverer:
    """Discovers ALL clickable links across entire website"""
    
    def __init__(self, http_client):
        self.http_client = http_client
        self.discovered_urls = set()
        self.url_queue = deque()
        self.link_relationships = defaultdict(list)
        
        # Link classification patterns
        self.social_domains = [
            'facebook.com', 'instagram.com', 'twitter.com', 'x.com',
            'linkedin.com', 'youtube.com', 'tiktok.com', 'pinterest.com'
        ]
        
        self.contact_keywords = [
            'contact', 'about', 'team', 'staff', 'location', 'office',
            'reach', 'touch', 'support', 'help', 'customer-service'
        ]
        
        self.excluded_extensions = [
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.tar', '.gz', '.jpg', '.jpeg', '.png',
            '.gif', '.webp', '.svg', '.mp4', '.avi', '.mov', '.mp3'
        ]
    
    def discover_all_links(self, main_url: str) -> SiteMap:
        """Discover ALL links across entire website with comprehensive mapping"""
        logger.info(f"Starting comprehensive link discovery for: {main_url}")
        
        domain = urlparse(main_url).netloc.replace("www.", "")
        sitemap = SiteMap(domain=domain, main_url=main_url)
        
        # Initialize with main URL
        self.url_queue.append((main_url, 0))  # (url, depth)
        self.discovered_urls.add(main_url)
        
        # Comprehensive crawling with depth control
        while self.url_queue and len(self.discovered_urls) < config.MAX_PAGES_PER_SITE:
            current_url, depth = self.url_queue.popleft()
            
            if depth > config.MAX_CRAWL_DEPTH:
                continue
            
            logger.info(f"Discovering links from: {current_url} (depth: {depth})")
            
            # Get page content
            html, status_code = self.http_client.get_with_retry(current_url)
            if not html:
                continue
            
            # Extract all links from current page
            page_links = self._extract_all_links_from_page(html, current_url, depth)
            
            # Process and classify links
            for link_info in page_links:
                self._classify_and_store_link(link_info, sitemap)
                
                # Add internal links to queue for further crawling
                if (link_info.link_type == "internal" and 
                    link_info.url not in self.discovered_urls and
                    depth < config.MAX_CRAWL_DEPTH):
                    
                    self.url_queue.append((link_info.url, depth + 1))
                    self.discovered_urls.add(link_info.url)
            
            # Update sitemap statistics
            sitemap.total_links += len(page_links)
            sitemap.crawl_depth_reached = max(sitemap.crawl_depth_reached, depth)
        
        # Finalize sitemap
        sitemap.total_pages = len(self.discovered_urls)
        sitemap.website_structure_complexity = self._assess_website_complexity(sitemap)
        
        logger.info(f"Link discovery completed. Found {sitemap.total_pages} pages and {sitemap.total_links} links")
        return sitemap
    
    def _extract_all_links_from_page(self, html: str, base_url: str, depth: int) -> List[LinkInfo]:
        """Extract ALL links from a single page with comprehensive analysis"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        # Extract all anchor tags
        for a in soup.find_all('a', href=True):
            href = a['href']
            anchor_text = a.get_text(strip=True)
            
            # Skip empty or invalid links
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue
            
            # Convert to absolute URL
            full_url = urljoin(base_url, href)
            
            # Skip excluded file types
            if any(full_url.lower().endswith(ext) for ext in self.excluded_extensions):
                continue
            
            # Create link info
            link_info = LinkInfo(
                url=full_url,
                anchor_text=anchor_text,
                source_page=base_url,
                depth=depth + 1,
                context=self._get_link_context(a)
            )
            
            # Determine link location context
            link_info.is_navigation = self._is_navigation_link(a)
            link_info.is_footer = self._is_footer_link(a)
            
            links.append(link_info)
        
        return links
    
    def _classify_and_store_link(self, link_info: LinkInfo, sitemap: SiteMap):
        """Classify link and store in appropriate sitemap category"""
        parsed_url = urlparse(link_info.url)
        base_domain = urlparse(sitemap.main_url).netloc.replace("www.", "")
        link_domain = parsed_url.netloc.replace("www.", "")
        
        # Classify link type
        if link_domain == base_domain:
            link_info.link_type = "internal"
            sitemap.internal_links.append(link_info)
            
            # Check for special page types
            url_lower = link_info.url.lower()
            if any(keyword in url_lower for keyword in self.contact_keywords):
                link_info.is_contact = True
                sitemap.contact_links.append(link_info)
            
            if 'about' in url_lower:
                link_info.is_about = True
        
        elif any(social in link_domain for social in self.social_domains):
            link_info.link_type = "social"
            sitemap.social_links.append(link_info)
        
        elif 'maps.google.com' in link_info.url or 'google.com/maps' in link_info.url:
            link_info.link_type = "maps"
            link_info.is_google_maps = True
            sitemap.google_maps_info.maps_links.append(link_info.url)
        
        else:
            link_info.link_type = "external"
            sitemap.external_links.append(link_info)
    
    def _get_link_context(self, a_tag) -> str:
        """Get surrounding context of the link"""
        parent = a_tag.parent
        if parent:
            context = parent.get_text(strip=True)
            return context[:100]  # Limit context length
        return ""
    
    def _is_navigation_link(self, a_tag) -> bool:
        """Determine if link is in navigation"""
        nav_indicators = ['nav', 'menu', 'header', 'navbar', 'navigation']
        
        # Check parent elements
        current = a_tag.parent
        for _ in range(5):  # Check up to 5 levels up
            if current is None:
                break
            
            # Check class and id attributes
            classes = current.get('class', [])
            id_attr = current.get('id', '')
            
            if any(indicator in ' '.join(classes).lower() for indicator in nav_indicators):
                return True
            if any(indicator in id_attr.lower() for indicator in nav_indicators):
                return True
            
            current = current.parent
        
        return False
    
    def _is_footer_link(self, a_tag) -> bool:
        """Determine if link is in footer"""
        footer_indicators = ['footer', 'bottom', 'copyright']
        
        # Check parent elements
        current = a_tag.parent
        for _ in range(5):  # Check up to 5 levels up
            if current is None:
                break
            
            if current.name == 'footer':
                return True
            
            # Check class and id attributes
            classes = current.get('class', [])
            id_attr = current.get('id', '')
            
            if any(indicator in ' '.join(classes).lower() for indicator in footer_indicators):
                return True
            if any(indicator in id_attr.lower() for indicator in footer_indicators):
                return True
            
            current = current.parent
        
        return False
    
    def _assess_website_complexity(self, sitemap: SiteMap) -> str:
        """Assess website structure complexity"""
        total_pages = sitemap.total_pages
        
        if total_pages >= 50:
            return "Complex"
        elif total_pages >= 20:
            return "Moderate"
        elif total_pages >= 6:
            return "Simple"
        else:
            return "Basic"

# ============================================================================
# WEB DRIVER MANAGEMENT (Enhanced)
# ============================================================================

class EnhancedWebDriverManager:
    """Enhanced Chrome WebDriver management with better optimization"""
    
    def __init__(self):
        self.driver = None
        
    def get_driver(self):
        """Get optimized Chrome driver instance with enhanced settings"""
        if self.driver is None:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-images')
            options.add_argument('--disable-javascript')  # Disable JS for faster loading
            options.add_argument(f'--user-agent={config.USER_AGENT}')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            
            # Enhanced performance optimizations
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.notifications": 2,
                "profile.managed_default_content_settings.media_stream": 2,
                "profile.managed_default_content_settings.stylesheets": 2,
            }
            options.add_experimental_option("prefs", prefs)
            
            try:
                self.driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()), 
                    options=options
                )
                self.driver.set_page_load_timeout(config.SELENIUM_TIMEOUT)
                logger.info("Enhanced Chrome driver initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Chrome driver: {e}")
                raise
                
        return self.driver
    
    def quit(self):
        """Safely quit driver"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Chrome driver closed successfully")
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")
            finally:
                self.driver = None

# ============================================================================
# HTTP CLIENT (Enhanced)
# ============================================================================

class EnhancedHTTPClient:
    """Enhanced HTTP client with better retry logic and session management"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
    def get_with_retry(self, url: str, retries: int = config.MAX_RETRIES) -> Tuple[Optional[str], Optional[int]]:
        """Enhanced GET with retry logic and better error handling"""
        for attempt in range(retries):
            try:
                logger.debug(f"HTTP attempt {attempt + 1} for {url}")
                response = self.session.get(
                    url, 
                    timeout=config.REQUEST_TIMEOUT,
                    allow_redirects=True
                )
                
                if response.status_code == 200 and len(response.text) > config.MIN_HTML_LENGTH:
                    logger.debug(f"HTTP success for {url}")
                    return response.text, response.status_code
                elif response.status_code in [403, 404, 500, 503]:
                    logger.warning(f"HTTP error {response.status_code} for {url}")
                    return None, response.status_code
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"HTTP attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(config.DELAY_BETWEEN_REQUESTS * (attempt + 1))
                    
        logger.error(f"All HTTP attempts failed for {url}")
        return None, None

# ============================================================================
# ENHANCED CONTACT EXTRACTOR
# ============================================================================

class EnhancedContactExtractor:
    """Enhanced contact information extraction with Google Maps integration"""
    
    def __init__(self):
        self.setup_patterns()
        
    def setup_patterns(self):
        """Initialize enhanced extraction patterns"""
        
        # Phone patterns by type
        self.phone_patterns = {
            'mobile': [
                r'mobile[:\s]*(\+?1?[-.\s]?$$?[2-9]\d{2}$$?[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
                r'cell[:\s]*(\+?1?[-.\s]?$$?[2-9]\d{2}$$?[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
            ],
            'corporate': [
                r'corporate[:\s]*(\+?1?[-.\s]?$$?[2-9]\d{2}$$?[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
                r'headquarters[:\s]*(\+?1?[-.\s]?$$?[2-9]\d{2}$$?[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
                r'main[:\s]*(\+?1?[-.\s]?$$?[2-9]\d{2}$$?[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
            ],
            'support': [
                r'support[:\s]*(\+?1?[-.\s]?$$?[2-9]\d{2}$$?[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
                r'customer\s*service[:\s]*(\+?1?[-.\s]?$$?[2-9]\d{2}$$?[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
                r'help[:\s]*(\+?1?[-.\s]?$$?[2-9]\d{2}$$?[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
            ],
            'general': [
                r'\b(\+?1?[-.\s]?$$?[2-9]\d{2}$$?[-.\s]?[2-9]\d{2}[-.\s]?\d{4})\b',
            ]
        }
        
        # Email pattern
        self.email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        
        # Address patterns
        self.address_patterns = [
            r'\b(\d{1,5}\s[\w\s]{3,30},\s*[\w\s]{3,20},\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?)\b',
            r'\b(\d{1,5}\s[\w\s]{3,30},?\s*[\w\s]{3,20},?\s*[A-Za-z]{2,15}\s*\d{5,10})\b'
        ]
        
        # Social media platforms
        self.social_platforms = {
            'facebook.com': 'facebook',
            'instagram.com': 'instagram',
            'tiktok.com': 'tiktok',
            'linkedin.com': 'linkedin',
            'twitter.com': 'twitter',
            'x.com': 'twitter',
            'pinterest.com': 'pinterest',
            'youtube.com': 'youtube'
        }
    
    def extract_contact_info(self, pages_data: List[PageMetadata], sitemap: SiteMap) -> ContactInfo:
        """Extract comprehensive contact information with Google Maps integration"""
        logger.info("Extracting enhanced contact information with Google Maps")
        
        contact_info = ContactInfo()
        
        # Combine all text content
        all_text = " ".join([page.text_content or "" for page in pages_data])
        all_html = " ".join([page.html or "" for page in pages_data])
        
        # Extract brand name
        contact_info.brand_name = self._extract_brand_name(pages_data)
        
        # Extract phone numbers by type
        contact_info.mobile_phone = self._extract_phone_by_type(all_text, 'mobile')
        contact_info.corporate_phone = self._extract_phone_by_type(all_text, 'corporate')
        contact_info.support_phone = self._extract_phone_by_type(all_text, 'support')
        contact_info.company_phone = self._extract_phone_by_type(all_text, 'general')
        
        # Extract email
        contact_info.email = self._extract_email(all_text, pages_data)
        
        # Extract address
        address = self._extract_address(all_text)
        contact_info.address = address
        if address:
            city, state = self._parse_city_state(address)
            contact_info.company_city = city
            contact_info.company_state = state
        
        # Enhanced Google Maps extraction from sitemap
        if sitemap.google_maps_info.all_maps_links:
            contact_info.google_map = sitemap.google_maps_info.primary_maps_link
            contact_info.all_google_maps_links = sitemap.google_maps_info.all_maps_links
            contact_info.google_maps_integration = sitemap.google_maps_info.maps_integration_status
        else:
            contact_info.google_maps_integration = "Not Found"
        
        # Extract social media
        contact_info.social_media = self._extract_social_media(pages_data)
        
        logger.info("Enhanced contact information extraction completed")
        return contact_info
    
    def _extract_brand_name(self, pages_data: List[PageMetadata]) -> Optional[str]:
        """Extract brand name from various sources"""
        # Try main page title first
        if pages_data:
            main_page = pages_data[0]
            if main_page.title:
                title = main_page.title
                # Clean title
                for separator in [' | ', ' - ', ' :: ', ' — ']:
                    if separator in title:
                        return title.split(separator)[0].strip()
                return title.strip()
        
        # Try JSON-LD data
        for page in pages_data:
            for ld in page.json_ld:
                if isinstance(ld, dict) and ld.get('name'):
                    return ld['name']
        
        return None
    
    def _extract_phone_by_type(self, text: str, phone_type: str) -> Optional[str]:
        """Extract specific type of phone number"""
        if phone_type not in self.phone_patterns:
            return None
        
        found_phones = set()
        for pattern in self.phone_patterns[phone_type]:
            try:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    # Clean and validate phone
                    clean_phone = re.sub(r'[^\d]', '', match)
                    if len(clean_phone) == 10 and self._is_valid_phone(clean_phone):
                        formatted = f"({clean_phone[:3]}) {clean_phone[3:6]}-{clean_phone[6:]}"
                        found_phones.add(formatted)
            except re.error as e:
                logger.warning(f"Regex error in phone pattern: {e}")
                continue
        
        return ', '.join(sorted(found_phones)[:3]) if found_phones else None
    
    def _is_valid_phone(self, phone: str) -> bool:
        """Validate US phone number"""
        if len(phone) != 10:
            return False
        
        area_code = phone[:3]
        exchange = phone[3:6]
        
        return (area_code[0] in '23456789' and 
                exchange[0] in '23456789' and
                area_code != '911' and 
                exchange != '911')
    
    def _extract_email(self, text: str, pages_data: List[PageMetadata]) -> Optional[str]:
        """Extract and validate email addresses"""
        # Try mailto links first
        for page in pages_data:
            if page.html:
                soup = BeautifulSoup(page.html, 'html.parser')
                mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
                if mailto_links:
                    email = mailto_links[0]['href'].replace('mailto:', '').split('?')[0]
                    if self._is_valid_email(email):
                        return email
        
        # Extract from text
        try:
            emails = set(re.findall(self.email_pattern, text, re.IGNORECASE))
            valid_emails = [email for email in emails if self._is_valid_email(email)]
            return valid_emails[0] if valid_emails else None
        except re.error as e:
            logger.warning(f"Regex error in email pattern: {e}")
            return None
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format and filter false positives"""
        if not email or len(email) > 254:
            return False
        
        # Check for asset files
        asset_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.css', '.js']
        if any(ext in email.lower() for ext in asset_extensions):
            return False
        
        # Validate format
        pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        return bool(re.match(pattern, email))
    
    def _extract_address(self, text: str) -> Optional[str]:
        """Extract physical address"""
        for pattern in self.address_patterns:
            try:
                matches = re.findall(pattern, text)
                if matches:
                    for match in matches:
                        if len(match.split(',')) >= 2:
                            return match.strip()
            except re.error as e:
                logger.warning(f"Regex error in address pattern: {e}")
                continue
        return None
    
    def _parse_city_state(self, address: str) -> Tuple[str, str]:
        """Parse city and state from address"""
        pattern = r'([^,]+),\s*([A-Z]{2})\s*\d{5}'
        match = re.search(pattern, address)
        if match:
            return match.group(1).strip(), match.group(2).strip()
        return "", ""
    
    def _extract_social_media(self, pages_data: List[PageMetadata]) -> Dict[str, Optional[str]]:
        """Extract social media links"""
        social_media = {platform: None for platform in self.social_platforms.values()}
        
        for page in pages_data:
            if page.html:
                soup = BeautifulSoup(page.html, 'html.parser')
                
                for a in soup.find_all('a', href=True):
                    href = a['href'].lower()
                    for domain, platform in self.social_platforms.items():
                        if domain in href and not social_media[platform]:
                            clean_url = a['href'].split('?')[0].rstrip('/')
                            social_media[platform] = clean_url
        
        return social_media

# ============================================================================
# ENHANCED BUSINESS INTELLIGENCE EXTRACTOR
# ============================================================================

class EnhancedBusinessIntelligenceExtractor:
    """Enhanced business metrics and intelligence extraction"""
    
    def __init__(self):
        self.setup_patterns()
        self.setup_classifications()
    
    def setup_patterns(self):
        """Initialize business intelligence patterns"""
        
        # Employee patterns
        self.employee_patterns = [
            r'(\d+)[-\s]*(\d+)?\s*employees?',
            r'team\s*of\s*(\d+)',
            r'(\d+)\s*people',
            r'staff\s*of\s*(\d+)',
            r'(\d+)\s*members?'
        ]
        
        # Revenue patterns
        self.revenue_patterns = [
            r'\$(\d+(?:,\d{3})*(?:\.\d{2})?)\s*(?:million|M|billion|B)',
            r'revenue[:\s]*\$(\d+(?:,\d{3})*(?:\.\d{2})?)\s*(?:million|M|billion|B)?',
            r'annual\s*revenue[:\s]*\$(\d+(?:,\d{3})*(?:\.\d{2})?)',
        ]
        
        # Founded year patterns
        self.founded_patterns = [
            r'founded\s*(?:in\s*)?(\d{4})',
            r'established\s*(?:in\s*)?(\d{4})',
            r'since\s*(\d{4})',
            r'started\s*(?:in\s*)?(\d{4})'
        ]
    
    def setup_classifications(self):
        """Setup industry and budget classifications"""
        
        self.industry_keywords = {
            'Technology / SaaS': [
                'software', 'saas', 'platform', 'api', 'cloud', 'tech', 'digital',
                'app', 'application', 'system', 'solution', 'automation'
            ],
            'E-commerce / Retail': [
                'ecommerce', 'retail', 'shop', 'store', 'marketplace', 'selling',
                'products', 'merchandise', 'commerce', 'shopping'
            ],
            'Marketing / Advertising': [
                'marketing', 'advertising', 'campaign', 'brand', 'promotion',
                'social media', 'influencer', 'content', 'ugc', 'creator'
            ],
            'Healthcare / Medical': [
                'healthcare', 'medical', 'health', 'clinic', 'hospital',
                'doctor', 'patient', 'treatment', 'therapy'
            ],
            'Financial Services': [
                'finance', 'banking', 'investment', 'insurance', 'loan',
                'credit', 'payment', 'fintech', 'trading'
            ],
            'Education / Training': [
                'education', 'training', 'course', 'learning', 'school',
                'university', 'academy', 'tutorial', 'certification'
            ]
        }
        
        self.budget_indicators = {
            'High-End': [
                'enterprise', 'premium', 'luxury', 'high-end', 'exclusive',
                'custom', 'bespoke', 'white-glove', 'concierge'
            ],
            'Mid-Budget': [
                'professional', 'business', 'standard', 'pro', 'plus',
                'advanced', 'growth', 'scale', 'team'
            ],
            'Low-End': [
                'basic', 'starter', 'free', 'budget', 'affordable',
                'small business', 'individual', 'personal', 'lite'
            ]
        }
    
    def extract_business_metrics(self, pages_data: List[PageMetadata], contact_info: ContactInfo, sitemap: SiteMap) -> BusinessMetrics:
        """Extract comprehensive business metrics with enhanced analysis"""
        logger.info("Extracting enhanced business metrics")
        
        metrics = BusinessMetrics()
        
        # Combine all text
        all_text = " ".join([page.text_content or "" for page in pages_data])
        
        # Extract industry
        metrics.industry = self._classify_industry(all_text)
        
        # Extract employee count
        metrics.employees = self._extract_employees(all_text)
        
        # Extract revenue
        metrics.annual_revenue = self._extract_revenue(all_text)
        
        # Extract founded year
        metrics.founded_year = self._extract_founded_year(all_text)
        
        # Enhanced metrics from sitemap
        metrics.website_structure_complexity = sitemap.website_structure_complexity
        metrics.digital_presence_strength = self._assess_digital_presence(sitemap, contact_info)
        metrics.contact_accessibility = self._assess_contact_accessibility(contact_info, sitemap)
        
        # Calculate scores
        metrics.firmographic_score = self._calculate_firmographic_score(metrics, contact_info)
        metrics.engagement_score = self._calculate_engagement_score(pages_data, contact_info, sitemap)
        
        # Classify budget segment
        metrics.segmentation = self._classify_budget_segment(all_text, metrics)
        
        logger.info("Enhanced business metrics extraction completed")
        return metrics
    
    def _classify_industry(self, text: str) -> str:
        """Classify industry based on content"""
        text_lower = text.lower()
        
        industry_scores = {}
        for industry, keywords in self.industry_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            if score > 0:
                industry_scores[industry] = score
        
        return max(industry_scores, key=industry_scores.get) if industry_scores else "Unknown"
    
    def _extract_employees(self, text: str) -> Optional[str]:
        """Extract employee count"""
        for pattern in self.employee_patterns:
            try:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    match = matches[0]
                    if isinstance(match, tuple):
                        if match[1]:
                            return f"{match[0]}-{match[1]}"
                        else:
                            return match[0]
                    else:
                        return match
            except re.error as e:
                logger.warning(f"Regex error in employee pattern: {e}")
                continue
        return None
    
    def _extract_revenue(self, text: str) -> Optional[str]:
        """Extract annual revenue"""
        for pattern in self.revenue_patterns:
            try:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    revenue = matches[0]
                    if isinstance(revenue, tuple):
                        revenue = revenue[0]
                    return f"${revenue}"
            except re.error as e:
                logger.warning(f"Regex error in revenue pattern: {e}")
                continue
        return None
    
    def _extract_founded_year(self, text: str) -> Optional[str]:
        """Extract founded year"""
        for pattern in self.founded_patterns:
            try:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    year = matches[0]
                    if 1800 <= int(year) <= datetime.now().year:
                        return year
            except (re.error, ValueError) as e:
                logger.warning(f"Error in founded year pattern: {e}")
                continue
        return None
    
    def _assess_digital_presence(self, sitemap: SiteMap, contact_info: ContactInfo) -> str:
        """Assess digital presence strength"""
        social_count = sum(1 for url in contact_info.social_media.values() if url)
        
        if social_count >= 5:
            return "Strong"
        elif social_count >= 3:
            return "Moderate"
        elif social_count >= 1:
            return "Basic"
        else:
            return "Limited"
    
    def _assess_contact_accessibility(self, contact_info: ContactInfo, sitemap: SiteMap) -> str:
        """Assess contact information accessibility"""
        contact_methods = 0
        
        if contact_info.email:
            contact_methods += 1
        if contact_info.company_phone or contact_info.mobile_phone:
            contact_methods += 1
        if contact_info.address:
            contact_methods += 1
        if contact_info.google_map:
            contact_methods += 1
        if len(sitemap.contact_links) > 0:
            contact_methods += 1
        
        if contact_methods >= 4:
            return "Excellent"
        elif contact_methods >= 2:
            return "Good"
        elif contact_methods >= 1:
            return "Basic"
        else:
            return "Limited"
    
    def _calculate_firmographic_score(self, metrics: BusinessMetrics, contact_info: ContactInfo) -> int:
        """Calculate firmographic completeness score (0-100)"""
        score = 0
        
        # Basic info (40 points)
        if contact_info.brand_name:
            score += 10
        if metrics.industry != 'Unknown':
            score += 10
        if metrics.founded_year:
            score += 10
        if contact_info.address:
            score += 10
        
        # Contact info (30 points)
        if contact_info.email:
            score += 10
        if contact_info.company_phone:
            score += 10
        if contact_info.social_media and any(contact_info.social_media.values()):
            score += 10
        
        # Business metrics (30 points)
        if metrics.employees:
            score += 15
        if metrics.annual_revenue:
            score += 15
        
        return min(100, score)
    
    def _calculate_engagement_score(self, pages_data: List[PageMetadata], contact_info: ContactInfo, sitemap: SiteMap) -> int:
        """Calculate digital engagement score (0-100) with enhanced metrics"""
        score = 0
        
        # Social media presence (30 points)
        if contact_info.social_media:
            active_platforms = sum(1 for url in contact_info.social_media.values() if url)
            score += min(30, active_platforms * 5)
        
        # Content richness (25 points)
        total_words = sum(page.word_count for page in pages_data)
        if total_words > 10000:
            score += 25
        elif total_words > 5000:
            score += 20
        elif total_words > 1000:
            score += 15
        
        # Site structure complexity (25 points)
        if sitemap.total_pages > 20:
            score += 25
        elif sitemap.total_pages > 10:
            score += 20
        elif sitemap.total_pages > 5:
            score += 15
        
        # Google Maps integration (10 points)
        if contact_info.google_maps_integration == "Integrated":
            score += 10
        
        # Technical quality (10 points)
        if pages_data and pages_data[0].url.startswith('https://'):
            score += 10
        
        return min(100, score)
    
    def _classify_budget_segment(self, text: str, metrics: BusinessMetrics) -> str:
        """Classify budget segment"""
        text_lower = text.lower()
        
        # Score each segment
        segment_scores = {}
        for segment, keywords in self.budget_indicators.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            if score > 0:
                segment_scores[segment] = score
        
        if segment_scores:
            return max(segment_scores, key=segment_scores.get)
        
        # Fallback based on employee count
        if metrics.employees:
            try:
                emp_num = int(metrics.employees.split('-')[0])
                if emp_num > 100:
                    return 'High-End'
                elif emp_num > 20:
                    return 'Mid-Budget'
                else:
                    return 'Low-End'
            except (ValueError, AttributeError):
                pass
        
        return 'Unknown'

# ============================================================================
# ENHANCED MARKETING INTELLIGENCE EXTRACTOR
# ============================================================================

class EnhancedMarketingIntelligenceExtractor:
    """Enhanced marketing and advertising intelligence extraction"""
    
    def extract_marketing_intelligence(self, pages_data: List[PageMetadata], contact_info: ContactInfo, sitemap: SiteMap) -> MarketingIntelligence:
        """Extract comprehensive marketing intelligence with enhanced analysis"""
        logger.info("Extracting enhanced marketing intelligence")
        
        intel = MarketingIntelligence()
        
        # Combine all text
        all_text = " ".join([page.text_content or "" for page in pages_data])
        
        # Extract Instagram handle
        instagram_url = contact_info.social_media.get('instagram', '')
        if instagram_url:
            match = re.search(r'instagram\.com/([^/?]+)', instagram_url)
            if match:
                intel.instagram_handle = f"@{match.group(1)}"
        
        # Check for creator collaborations
        creator_keywords = [
            'influencer', 'creator', 'collaboration', 'partnership',
            'sponsored', 'brand ambassador', 'ugc', 'user generated'
        ]
        intel.worked_with_creators = any(keyword in all_text.lower() for keyword in creator_keywords)
        
        # Extract video links
        intel.integrated_video_links = self._extract_video_links(pages_data)
        
        # Calculate enhanced IG score
        intel.ig_score = self._calculate_enhanced_ig_score(intel, contact_info, sitemap)
        
        # Enhanced social media engagement analysis
        intel.social_media_engagement = self._analyze_social_engagement(contact_info, sitemap)
        
        # Look for ad library proof (placeholder for future implementation)
        intel.ad_library_proof = []
        
        logger.info("Enhanced marketing intelligence extraction completed")
        return intel
    
    def _extract_video_links(self, pages_data: List[PageMetadata]) -> List[str]:
        """Extract integrated video links with enhanced detection"""
        video_links = []
        
        for page in pages_data:
            if page.html:
                soup = BeautifulSoup(page.html, 'html.parser')
                
                # Find video elements and embeds
                videos = soup.find_all(['video', 'iframe'])
                for video in videos:
                    src = video.get('src') or video.get('data-src')
                    if src and any(platform in src for platform in ['youtube', 'vimeo', 'tiktok', 'instagram']):
                        video_links.append(src)
                
                # Look for video links in anchor tags
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if any(platform in href for platform in ['youtube.com/watch', 'vimeo.com/', 'tiktok.com/']):
                        video_links.append(href)
        
        return video_links[:10]  # Limit to 10 videos
    
    def _calculate_enhanced_ig_score(self, intel: MarketingIntelligence, contact_info: ContactInfo, sitemap: SiteMap) -> int:
        """Calculate enhanced Instagram engagement score"""
        score = 0
        
        if intel.instagram_handle:
            score += 25
        
        if intel.worked_with_creators:
            score += 20
        
        if intel.integrated_video_links:
            score += min(20, len(intel.integrated_video_links) * 2)
        
        # Social media presence bonus
        active_platforms = sum(1 for url in contact_info.social_media.values() if url)
        score += min(20, active_platforms * 3)
        
        # Website complexity bonus
        if sitemap.website_structure_complexity in ['Complex', 'Moderate']:
            score += 15
        
        return min(100, score)
    
    def _analyze_social_engagement(self, contact_info: ContactInfo, sitemap: SiteMap) -> Dict[str, int]:
        """Analyze social media engagement across platforms"""
        engagement = {}
        
        for platform, url in contact_info.social_media.items():
            if url:
                # Basic engagement score based on presence
                engagement[platform] = 50
                
                # Bonus for multiple social links found
                if len(sitemap.social_links) > 3:
                    engagement[platform] += 25
        
        return engagement

# ============================================================================
# ENHANCED WEBSITE FEATURES DETECTOR
# ============================================================================

class EnhancedWebsiteFeaturesDetector:
    """Enhanced website features and capabilities detection"""
    
    def __init__(self):
        self.feature_keywords = {
            'd2c_presence': ['d2c', 'direct to consumer', 'shop', 'buy now', 'cart', 'checkout'],
            'ecommerce_presence': ['shopify', 'woocommerce', 'shop', 'ecommerce', 'online store'],
            'social_media_presence': ['instagram', 'facebook', 'tiktok', 'twitter', 'linkedin'],
            'saas_platform': ['dashboard', 'platform', 'analytics', 'api', 'subscription'],
            'blog_presence': ['blog', 'post', 'article', 'read more', 'news'],
            'cta_presence': ['signup', 'sign up', 'register', 'get started', 'subscribe'],
            'product_listings': ['our products', 'catalog', 'featured items', 'shop now'],
            'contact_forms': ['contact form', 'get in touch', 'send message', 'form'],
            'newsletter_signup': ['newsletter', 'subscribe', 'email updates', 'mailing list']
        }
    
    def detect_features(self, pages_data: List[PageMetadata], sitemap: SiteMap) -> WebsiteFeatures:
        """Detect enhanced website features with comprehensive analysis"""
        logger.info("Detecting enhanced website features")
        
        features = WebsiteFeatures()
        
        # Combine all text and HTML
        all_text = " ".join([page.text_content or "" for page in pages_data]).lower()
        all_html = " ".join([page.html or "" for page in pages_data]).lower()
        
        # Check text-based features
        for feature, keywords in self.feature_keywords.items():
            if hasattr(features, feature):
                setattr(features, feature, any(kw in all_text for kw in keywords))
        
        # Enhanced social media presence detection
        features.social_media_presence = len(sitemap.social_links) > 0
        
        # Special checks
        features.video_presence = any(tag in all_html for tag in [
            '<video', 'youtube.com/embed', 'vimeo.com', 'video-js', 'tiktok.com'
        ])
        
        # Technical features
        if pages_data:
            main_page = pages_data[0]
            features.ssl_secure = main_page.url.startswith('https://')
            features.mobile_responsive = 'viewport' in (main_page.html or '').lower()
        
        logger.info("Enhanced website features detection completed")
        return features

# ============================================================================
# ENHANCED METADATA EXTRACTOR
# ============================================================================

class EnhancedMetadataExtractor:
    """Enhanced metadata extraction with comprehensive analysis"""
    
    def __init__(self):
        self.about_keywords = [
            'about', 'about-us', 'about_us', 'aboutus', 'story', 'our-story', 
            'company', 'history', 'mission', 'vision', 'team', 'who-we-are',
            'our-company', 'background', 'overview', 'profile'
        ]
        
        self.logo_selectors = [
            'img[class*="logo" i]',
            'img[id*="logo" i]',
            'img[alt*="logo" i]',
            '.logo img',
            '#logo img',
            'header img:first-child',
            '.navbar-brand img',
            '.header-logo img',
            '.site-logo img',
            '[class*="brand"] img'
        ]
    
    def extract_enhanced_metadata(self, pages_data: List[PageMetadata], main_url: str, sitemap: SiteMap) -> EnhancedMetadata:
        """Extract comprehensive enhanced metadata from all pages with sitemap integration"""
        logger.info("Extracting enhanced metadata with comprehensive analysis")
        
        metadata = EnhancedMetadata()
        
        if not pages_data:
            return metadata
        
        # Extract from main page first
        main_page = pages_data[0]
        if main_page.html:
            soup = BeautifulSoup(main_page.html, 'html.parser')
            
            # Extract site title
            metadata.site_title = self._extract_site_title(soup)
            
            # Extract meta description
            metadata.meta_description = self._extract_meta_description(soup)
            
            # Extract meta keywords
            metadata.meta_keywords = self._extract_meta_keywords(soup)
            
            # Extract logo URL
            metadata.logo_url = self._extract_logo_url(soup, main_url)
            
            # Extract favicon
            metadata.favicon_url = self._extract_favicon_url(soup, main_url)
        
        # Collect all page titles for comprehensive analysis
        metadata.all_page_titles = [page.title for page in pages_data if page.title]
        
        # Find and extract about us content using sitemap
        about_data = self._find_and_extract_about_content_enhanced(pages_data, sitemap)
        metadata.about_us_text = about_data['text']
        metadata.about_us_url = about_data['url']
        
        # Compile keywords from all pages
        metadata.keywords_compilation = self._compile_keywords_from_all_pages(pages_data)
        
        logger.info("Enhanced metadata extraction completed")
        return metadata
    
    def _extract_site_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract site title from <title> tag"""
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            title = title_tag.string.strip()
            # Clean up common artifacts
            title = re.sub(r'\s+', ' ', title)
            return title[:200]  # Limit length
        return None
    
    def _extract_meta_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract meta description from <meta name="description">"""
        meta_desc = soup.find('meta', attrs={'name': re.compile(r'^description$', re.I)})
        if meta_desc and meta_desc.get('content'):
            description = meta_desc['content'].strip()
            # Clean up the description
            description = re.sub(r'\s+', ' ', description)
            return description[:500]  # Limit length
        return None
    
    def _extract_meta_keywords(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract meta keywords from <meta name="keywords">"""
        meta_keywords = soup.find('meta', attrs={'name': re.compile(r'^keywords$', re.I)})
        if meta_keywords and meta_keywords.get('content'):
            keywords = meta_keywords['content'].strip()
            # Clean up keywords
            keywords = re.sub(r'\s+', ' ', keywords)
            return keywords[:300]  # Limit length
        return None
    
    def _extract_logo_url(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract company logo URL with comprehensive detection"""
        for selector in self.logo_selectors:
            try:
                logo_imgs = soup.select(selector)
                for img in logo_imgs:
                    src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                    if src:
                        # Convert relative URLs to absolute
                        if src.startswith('//'):
                            logo_url = 'https:' + src
                        elif src.startswith('/'):
                            logo_url = urljoin(base_url, src)
                        elif src.startswith('http'):
                            logo_url = src
                        else:
                            logo_url = urljoin(base_url, src)
                        
                        # Validate logo URL
                        if self._is_valid_logo_url(logo_url):
                            return logo_url
            except Exception as e:
                logger.debug(f"Error with logo selector {selector}: {e}")
                continue
        
        return None
    
    def _extract_favicon_url(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract favicon URL"""
        # Look for favicon link tags
        favicon_selectors = [
            'link[rel="icon"]',
            'link[rel="shortcut icon"]',
            'link[rel="apple-touch-icon"]'
        ]
        
        for selector in favicon_selectors:
            favicon = soup.select_one(selector)
            if favicon and favicon.get('href'):
                href = favicon['href']
                if href.startswith('//'):
                    return 'https:' + href
                elif href.startswith('/'):
                    return urljoin(base_url, href)
                elif href.startswith('http'):
                    return href
                else:
                    return urljoin(base_url, href)
        
        # Default favicon location
        return urljoin(base_url, '/favicon.ico')
    
    def _is_valid_logo_url(self, url: str) -> bool:
        """Validate if URL is likely a logo"""
        if not url:
            return False
        
        # Check file extension
        valid_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp']
        url_lower = url.lower()
        
        # Must have valid image extension
        if not any(ext in url_lower for ext in valid_extensions):
            return False
        
        # Should not be too generic
        generic_terms = ['placeholder', 'default', 'sample', 'test']
        if any(term in url_lower for term in generic_terms):
            return False
        
        return True
    
    def _find_and_extract_about_content_enhanced(self, pages_data: List[PageMetadata], sitemap: SiteMap) -> Dict[str, Optional[str]]:
        """Find about page and extract comprehensive about content using sitemap"""
        about_content = {'text': None, 'url': None}
        
        # First, look for dedicated about pages using sitemap
        about_pages = []
        for link in sitemap.internal_links:
            if link.is_about:
                # Find corresponding page data
                for page in pages_data:
                    if page.url == link.url:
                        about_pages.append(page)
                        page.is_about_page = True
                        break
        
        # Also check pages that weren't caught by sitemap
        for page in pages_data:
            if self._is_about_page(page.url, page.text_content or ''):
                if page not in about_pages:
                    about_pages.append(page)
                    page.is_about_page = True
        
        # If we found dedicated about pages, extract from the most comprehensive one
        if about_pages:
            # Sort by content length to get the most comprehensive
            about_pages.sort(key=lambda p: len(p.text_content or ''), reverse=True)
            best_about_page = about_pages[0]
            
            about_text = self._extract_about_text_from_page(best_about_page)
            if about_text:
                about_content['text'] = about_text
                about_content['url'] = best_about_page.url
                return about_content
        
        # If no dedicated about page, look for about sections in all pages
        all_about_text = []
        for page in pages_data:
            if page.html:
                soup = BeautifulSoup(page.html, 'html.parser')
                about_sections = self._extract_about_sections_from_soup(soup)
                all_about_text.extend(about_sections)
        
        if all_about_text:
            # Combine all about text, removing duplicates
            unique_about_text = []
            seen_text = set()
            
            for text in all_about_text:
                # Create a hash of the text to check for duplicates
                text_hash = hashlib.md5(text.lower().encode()).hexdigest()
                if text_hash not in seen_text and len(text) > 50:
                    unique_about_text.append(text)
                    seen_text.add(text_hash)
            
            # Combine the unique about text
            combined_text = '\n\n'.join(unique_about_text)
            about_content['text'] = combined_text[:3000]  # Limit to 3000 characters
        
        return about_content
    
    def _is_about_page(self, url: str, text_content: str) -> bool:
        """Determine if a page is an about page"""
        url_lower = url.lower()
        
        # Check URL for about keywords
        url_indicators = any(keyword in url_lower for keyword in self.about_keywords)
        
        # Check content for about indicators
        text_lower = text_content.lower() if text_content else ''
        content_indicators = (
            'about us' in text_lower or 
            'our story' in text_lower or 
            'our company' in text_lower or
            'who we are' in text_lower or
            'our mission' in text_lower
        )
        
        return url_indicators or content_indicators
    
    def _extract_about_text_from_page(self, page: PageMetadata) -> Optional[str]:
        """Extract about text from a dedicated about page"""
        if not page.html:
            return page.text_content[:2000] if page.text_content else None
        
        soup = BeautifulSoup(page.html, 'html.parser')
        
        # Remove navigation, footer, and other non-content elements
        for element in soup(['nav', 'footer', 'header', 'aside', 'script', 'style']):
            element.decompose()
        
        # Look for main content areas
        content_selectors = [
            'main', 'article', '.content', '#content', '.main-content',
            '.about-content', '.page-content', '.entry-content'
        ]
        
        for selector in content_selectors:
            content_area = soup.select_one(selector)
            if content_area:
                text = content_area.get_text(separator=' ', strip=True)
                if len(text) > 100:
                    return self._clean_about_text(text)
        
        # Fallback: get all text from body
        body = soup.find('body')
        if body:
            text = body.get_text(separator=' ', strip=True)
            return self._clean_about_text(text)[:2000]
        
        return None
    
    def _extract_about_sections_from_soup(self, soup: BeautifulSoup) -> List[str]:
        """Extract about sections from any page"""
        about_sections = []
        
        # Look for sections with about-related IDs or classes
        about_selectors = [
            'section[id*="about" i]', 'div[class*="about" i]',
            'section[class*="about" i]', 'div[id*="about" i]',
            'section[id*="story" i]', 'div[class*="story" i]',
            'section[id*="mission" i]', 'div[class*="mission" i]',
            'section[id*="company" i]', 'div[class*="company" i]'
        ]
        
        for selector in about_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(separator=' ', strip=True)
                    if len(text) > 100:  # Only meaningful content
                        cleaned_text = self._clean_about_text(text)
                        if cleaned_text:
                            about_sections.append(cleaned_text)
            except Exception as e:
                logger.debug(f"Error with about selector {selector}: {e}")
                continue
        
        return about_sections
    
    def _clean_about_text(self, text: str) -> str:
        """Clean and format about text"""
        if not text:
            return ''
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common navigation and footer text
        unwanted_phrases = [
            'home', 'contact us', 'privacy policy', 'terms of service',
            'copyright', '©', 'all rights reserved', 'follow us',
            'subscribe', 'newsletter', 'social media'
        ]
        
        # Split into sentences and filter
        sentences = re.split(r'[.!?]+', text)
        filtered_sentences = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if (len(sentence) > 20 and 
                not any(phrase in sentence.lower() for phrase in unwanted_phrases)):
                filtered_sentences.append(sentence)
        
        cleaned_text = '. '.join(filtered_sentences)
        
        # Limit length
        if len(cleaned_text) > 2000:
            cleaned_text = cleaned_text[:2000] + '...'
        
        return cleaned_text
    
    def _compile_keywords_from_all_pages(self, pages_data: List[PageMetadata]) -> List[str]:
        """Compile keywords from all pages"""
        all_keywords = set()
        
        for page in pages_data:
            if page.html:
                soup = BeautifulSoup(page.html, 'html.parser')
                
                # Extract meta keywords
                meta_keywords = soup.find('meta', attrs={'name': re.compile(r'^keywords$', re.I)})
                if meta_keywords and meta_keywords.get('content'):
                    keywords = meta_keywords['content'].split(',')
                    for keyword in keywords:
                        clean_keyword = keyword.strip()
                        if clean_keyword and len(clean_keyword) > 2:
                            all_keywords.add(clean_keyword)
        
        return list(all_keywords)[:20]  # Limit to 20 keywords

# ============================================================================
# MAIN ENHANCED WEBSITE ANALYZER
# ============================================================================

class EnhancedCompleteWebsiteAnalyzer:
    """Enhanced complete website analysis system with comprehensive link discovery"""
    
    def __init__(self):
        self.http_client = EnhancedHTTPClient()
        self.link_discoverer = ComprehensiveLinkDiscoverer(self.http_client)
        self.maps_detector = EnhancedGoogleMapsDetector()
        self.contact_extractor = EnhancedContactExtractor()
        self.business_extractor = EnhancedBusinessIntelligenceExtractor()
        self.marketing_extractor = EnhancedMarketingIntelligenceExtractor()
        self.features_detector = EnhancedWebsiteFeaturesDetector()
        self.metadata_extractor = EnhancedMetadataExtractor()
    
    def analyze_website_comprehensive(self, url: str) -> Dict[str, Any]:
        """Perform comprehensive website analysis with complete link discovery"""
        logger.info(f"Starting comprehensive enhanced analysis for: {url}")
        
        domain = urlparse(url).netloc.replace("www.", "")
        base_folder = Path("analyzed") / domain
        base_folder.mkdir(parents=True, exist_ok=True)
        
        driver_manager = EnhancedWebDriverManager()
        
        try:
            # Step 1: Comprehensive link discovery
            logger.info("Phase 1: Comprehensive link discovery")
            sitemap = self.link_discoverer.discover_all_links(url)
            
            # Step 2: Analyze ALL discovered pages
            logger.info("Phase 2: Analyzing all discovered pages")
            pages_data = self._analyze_all_discovered_pages(sitemap, driver_manager)
            
            if not pages_data:
                logger.error(f"No data extracted for {url}")
                return {}
            
            # Step 3: Enhanced Google Maps detection
            logger.info("Phase 3: Enhanced Google Maps detection")
            sitemap.google_maps_info = self.maps_detector.detect_all_google_maps(pages_data, sitemap)
            
            # Step 4: Save comprehensive raw data
            logger.info("Phase 4: Saving comprehensive raw data")
            self._save_comprehensive_raw_data(pages_data, sitemap, base_folder, domain)
            
            # Step 5: Extract enhanced business intelligence
            logger.info("Phase 5: Extracting enhanced business intelligence")
            contact_info = self.contact_extractor.extract_contact_info(pages_data, sitemap)
            business_metrics = self.business_extractor.extract_business_metrics(pages_data, contact_info, sitemap)
            marketing_intel = self.marketing_extractor.extract_marketing_intelligence(pages_data, contact_info, sitemap)
            website_features = self.features_detector.detect_features(pages_data, sitemap)
            enhanced_metadata = self.metadata_extractor.extract_enhanced_metadata(pages_data, url, sitemap)
            
            # Step 6: Compile comprehensive summary data
            summary_data = {
                'domain': domain,
                'main_url': url,
                'analysis_date': datetime.now().isoformat(),
                'sitemap': sitemap,
                'pages_analyzed': len(pages_data),
                'total_links_discovered': sitemap.total_links,
                'contact_info': contact_info,
                'business_metrics': business_metrics,
                'marketing_intelligence': marketing_intel,
                'website_features': website_features,
                'enhanced_metadata': enhanced_metadata,
                'pages_data': pages_data
            }
            
            # Step 7: Generate enhanced summary report with Google Maps
            logger.info("Phase 7: Generating enhanced summary report")
            summary_report = self._generate_comprehensive_summary_report(summary_data)
            self._save_enhanced_summary_report(summary_report, base_folder, domain)
            
            # Step 8: Save master JSON
            self._save_enhanced_master_json(summary_data, base_folder, domain)
            
            logger.info(f"Comprehensive enhanced analysis completed for {url}")
            return summary_data
            
        except Exception as e:
            logger.error(f"Critical error analyzing {url}: {e}")
            return {}
        finally:
            driver_manager.quit()
    
    def _analyze_all_discovered_pages(self, sitemap: SiteMap, driver_manager: EnhancedWebDriverManager) -> List[PageMetadata]:
        """Analyze all discovered pages from sitemap"""
        pages_data = []
        
        # Get all unique URLs from sitemap
        all_urls = set()
        all_urls.add(sitemap.main_url)
        
        for link in sitemap.internal_links:
            all_urls.add(link.url)
        
        # Limit to MAX_PAGES_PER_SITE
        urls_to_analyze = list(all_urls)[:config.MAX_PAGES_PER_SITE]
        
        logger.info(f"Analyzing {len(urls_to_analyze)} discovered pages")
        
        # Analyze pages with controlled parallelism
        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._analyze_single_page_enhanced, url, EnhancedWebDriverManager()): url 
                for url in urls_to_analyze
            }
            
            for future in as_completed(futures):
                try:
                    page_data = future.result()
                    if page_data:
                        pages_data.append(page_data)
                except Exception as e:
                    logger.error(f"Error analyzing page: {e}")
        
        logger.info(f"Successfully analyzed {len(pages_data)} pages")
        return pages_data
    
    def _analyze_single_page_enhanced(self, url: str, driver_manager: EnhancedWebDriverManager) -> Optional[PageMetadata]:
        """Analyze individual page with enhanced extraction"""
        start_time = time.time()
        
        # Skip if already processed
        with THREAD_LOCK:
            if url in processed_urls:
                return None
            processed_urls.add(url)
        
        logger.debug(f"Analyzing enhanced page: {url}")
        
        try:
            # Try HTTP first
            html, status_code = self.http_client.get_with_retry(url)
            
            # Fallback to Selenium if needed
            if not html or len(html) < config.MIN_HTML_LENGTH:
                try:
                    driver = driver_manager.get_driver()
                    driver.get(url)
                    time.sleep(2)
                    html = driver.page_source
                    status_code = 200
                except Exception as e:
                    logger.warning(f"Selenium failed for {url}: {e}")
                    return None
            
            if not html:
                return None
            
            
            # Parse HTML
            soup = BeautifulSoup(html, 'html.parser')
            text_content = soup.get_text(separator=' ', strip=True)
            
            # Create enhanced page metadata
            page_data = PageMetadata(url=url)
            page_data.html = html
            page_data.text_content = text_content
            page_data.status_code = status_code
            page_data.load_time = time.time() - start_time
            page_data.word_count = len(text_content.split())
            
            # Extract metadata
            page_data.title = soup.title.string.strip() if soup.title else None
            
            # Meta description
            desc_tag = soup.find('meta', attrs={'name': 'description'})
            if desc_tag:
                page_data.description = desc_tag.get('content', '')
            
            # Extract meta tags
            for meta in soup.find_all('meta'):
                tag_data = {k: meta.get(k) for k in meta.attrs}
                page_data.meta_tags.append(tag_data)
            
            # Extract JSON-LD
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string.strip())
                    page_data.json_ld.append(data)
                except:
                    continue
            
            # Determine page type
            page_data.page_type = self._determine_page_type(url, text_content)
            page_data.is_contact_page = 'contact' in url.lower() or 'contact' in text_content.lower()
            
            logger.debug(f"Successfully analyzed enhanced page: {url}")
            return page_data
            
        except Exception as e:
            logger.error(f"Error analyzing enhanced page {url}: {e}")
            return None
        finally:
            driver_manager.quit()
    
    def _determine_page_type(self, url: str, text_content: str) -> str:
        """Determine the type of page based on URL and content"""
        url_lower = url.lower()
        text_lower = text_content.lower()
        
        if any(keyword in url_lower for keyword in ['about', 'story', 'company']):
            return 'about'
        elif any(keyword in url_lower for keyword in ['contact', 'reach', 'touch']):
            return 'contact'
        elif any(keyword in url_lower for keyword in ['product', 'service', 'solution']):
            return 'product'
        elif any(keyword in url_lower for keyword in ['blog', 'news', 'article']):
            return 'blog'
        elif any(keyword in url_lower for keyword in ['team', 'staff', 'people']):
            return 'team'
        else:
            return 'general'
    
    def _save_comprehensive_raw_data(self, pages_data: List[PageMetadata], sitemap: SiteMap, base_folder: Path, domain: str):
        """Save comprehensive raw data including sitemap"""
        logger.info("Saving comprehensive raw data with sitemap")
        
        # Create directories
        html_folder = base_folder / "html"
        json_folder = base_folder / "json"
        images_folder = base_folder / "images"
        sitemap_folder = base_folder / "sitemap"
        
        for folder in [html_folder, json_folder, images_folder, sitemap_folder]:
            folder.mkdir(exist_ok=True)
        
        # Save comprehensive sitemap
        sitemap_data = {
            'domain': sitemap.domain,
            'main_url': sitemap.main_url,
            'total_pages': sitemap.total_pages,
            'total_links': sitemap.total_links,
            'crawl_depth_reached': sitemap.crawl_depth_reached,
            'website_structure_complexity': sitemap.website_structure_complexity,
            'internal_links': [
                {
                    'url': link.url,
                    'anchor_text': link.anchor_text,
                    'source_page': link.source_page,
                    'depth': link.depth,
                    'is_navigation': link.is_navigation,
                    'is_footer': link.is_footer,
                    'is_contact': link.is_contact,
                    'is_about': link.is_about,
                    'context': link.context
                }
                for link in sitemap.internal_links
            ],
            'external_links': [
                {
                    'url': link.url,
                    'anchor_text': link.anchor_text,
                    'source_page': link.source_page
                }
                for link in sitemap.external_links
            ],
            'social_links': [
                {
                    'url': link.url,
                    'anchor_text': link.anchor_text,
                    'source_page': link.source_page
                }
                for link in sitemap.social_links
            ],
            'contact_links': [
                {
                    'url': link.url,
                    'anchor_text': link.anchor_text,
                    'source_page': link.source_page
                }
                for link in sitemap.contact_links
            ],
            'google_maps_info': {
                'maps_links': sitemap.google_maps_info.maps_links,
                'iframe_embeds': sitemap.google_maps_info.iframe_embeds,
                'javascript_maps': sitemap.google_maps_info.javascript_maps,
                'structured_data_maps': sitemap.google_maps_info.structured_data_maps,
                'contact_page_maps': sitemap.google_maps_info.contact_page_maps,
                'all_maps_links': sitemap.google_maps_info.all_maps_links,
                'primary_maps_link': sitemap.google_maps_info.primary_maps_link,
                'maps_integration_status': sitemap.google_maps_info.maps_integration_status,
                'total_maps_found': sitemap.google_maps_info.total_maps_found
            }
        }

       # ============================================================================
       # DUPLICATE PREVENTION IN DATA SAVING
       # ============================================================================
       

    def _save_comprehensive_raw_data(self, pages_data: List[PageMetadata], sitemap: SiteMap, base_folder: Path, domain: str):
        """Save comprehensive raw data with duplicate prevention"""
        logger.info("Saving comprehensive raw data with duplicate prevention")
        
        # Create directories
        html_folder = base_folder / "html"
        json_folder = base_folder / "json"
        images_folder = base_folder / "images"
        sitemap_folder = base_folder / "sitemap"
        
        for folder in [html_folder, json_folder, images_folder, sitemap_folder]:
            folder.mkdir(exist_ok=True)
        
        # Save comprehensive sitemap with unique links
        unique_urls = set()
        sitemap_data = {
            'domain': sitemap.domain,
            'main_url': sitemap.main_url,
            'total_pages': sitemap.total_pages,
            'total_links': sitemap.total_links,
            'crawl_depth_reached': sitemap.crawl_depth_reached,
            'website_structure_complexity': sitemap.website_structure_complexity,
            'internal_links': [],
            'external_links': [],
            'social_links': [],
            'contact_links': [],
            'google_maps_info': {
                'maps_links': sitemap.google_maps_info.maps_links,
                'iframe_embeds': sitemap.google_maps_info.iframe_embeds,
                'javascript_maps': sitemap.google_maps_info.javascript_maps,
                'structured_data_maps': sitemap.google_maps_info.structured_data_maps,
                'contact_page_maps': sitemap.google_maps_info.contact_page_maps,
                'all_maps_links': list(set(sitemap.google_maps_info.all_maps_links)),  # Deduplicate
                'primary_maps_link': sitemap.google_maps_info.primary_maps_link,
                'maps_integration_status': sitemap.google_maps_info.maps_integration_status,
                'total_maps_found': sitemap.google_maps_info.total_maps_found
            }
        }
        
        # Process links with duplicate prevention
        seen_urls = set()
        for link in sitemap.internal_links:
            if link.url not in seen_urls:
                seen_urls.add(link.url)
                sitemap_data['internal_links'].append({
                    'url': link.url,
                    'anchor_text': link.anchor_text,
                    'source_page': link.source_page,
                    'depth': link.depth,
                    'is_navigation': link.is_navigation,
                    'is_footer': link.is_footer,
                    'is_contact': link.is_contact,
                    'is_about': link.is_about,
                    'context': link.context
                })
                
        seen_urls = set()
        for link in sitemap.external_links:
            if link.url not in seen_urls:
                seen_urls.add(link.url)
                sitemap_data['external_links'].append({
                    'url': link.url,
                    'anchor_text': link.anchor_text,
                    'source_page': link.source_page
                })
                
        seen_urls = set()
        for link in sitemap.social_links:
            if link.url not in seen_urls:
                seen_urls.add(link.url)
                sitemap_data['social_links'].append({
                    'url': link.url,
                    'anchor_text': link.anchor_text,
                    'source_page': link.source_page
                })
                
        seen_urls = set()
        for link in sitemap.contact_links:
            if link.url not in seen_urls:
                seen_urls.add(link.url)
                sitemap_data['contact_links'].append({
                    'url': link.url,
                    'anchor_text': link.anchor_text,
                    'source_page': link.source_page
                })
        
        # Save sitemap JSON
        with open(sitemap_folder / f"{domain}_complete_sitemap.json", 'w', encoding='utf-8') as f:
            json.dump(sitemap_data, f, indent=2, ensure_ascii=False)
        
        # Save human-readable sitemap summary
        sitemap_summary = self._generate_sitemap_summary(sitemap)
        with open(sitemap_folder / f"{domain}_sitemap_summary.txt", 'w', encoding='utf-8') as f:
            f.write(sitemap_summary)
        
        # Save HTML and JSON for each page
        for i, page in enumerate(pages_data):
            filename = self._clean_filename(page.url, domain, i)
            
            # Save HTML
            if page.html:
                with open(html_folder / f"{filename}.html", 'w', encoding='utf-8') as f:
                    f.write(page.html)
            
            # Save JSON metadata
            page_json = {
                'url': page.url,
                'title': page.title,
                'description': page.description,
                'status_code': page.status_code,
                'load_time': page.load_time,
                'word_count': page.word_count,
                'page_type': page.page_type,
                'is_about_page': page.is_about_page,
                'is_contact_page': page.is_contact_page,
                'depth': page.depth,
                'json_ld': page.json_ld,
                'meta_tags': page.meta_tags,
                'google_maps_found': page.google_maps_found
            }
            
            with open(json_folder / f"{filename}.json", 'w', encoding='utf-8') as f:
                json.dump(page_json, f, indent=2, ensure_ascii=False)
            
            # Download images
            self._download_images(page.html, page.url, images_folder)
        
        logger.info("Comprehensive raw data saved successfully")
    
    def _generate_sitemap_summary(self, sitemap: SiteMap) -> str:
        """Generate human-readable sitemap summary"""
        summary = f"""
🗺️ COMPREHENSIVE WEBSITE SITEMAP SUMMARY
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Domain: {sitemap.domain}

{'='*80}
📊 DISCOVERY STATISTICS
{'='*80}

🔍 Crawling Results:
  • Total Pages Discovered: {sitemap.total_pages}
  • Total Links Found: {sitemap.total_links}
  • Maximum Crawl Depth Reached: {sitemap.crawl_depth_reached}
  • Website Structure Complexity: {sitemap.website_structure_complexity}

📋 Link Classification:
  • Internal Links: {len(sitemap.internal_links)}
  • External Links: {len(sitemap.external_links)}
  • Social Media Links: {len(sitemap.social_links)}
  • Contact-Related Links: {len(sitemap.contact_links)}

{'='*80}
🗺️ GOOGLE MAPS INTEGRATION ANALYSIS
{'='*80}

📍 Maps Discovery Results:
  • Integration Status: {sitemap.google_maps_info.maps_integration_status}
  • Total Maps Links Found: {sitemap.google_maps_info.total_maps_found}
  • Primary Maps Link: {sitemap.google_maps_info.primary_maps_link or 'Not found'}

🔍 Detection Methods Used:
  • Direct Links Found: {len(sitemap.google_maps_info.maps_links)}
  • Iframe Embeds Found: {len(sitemap.google_maps_info.iframe_embeds)}
  • JavaScript Maps Found: {len(sitemap.google_maps_info.javascript_maps)}
  • Structured Data Maps Found: {len(sitemap.google_maps_info.structured_data_maps)}

📍 All Google Maps Links Discovered:"""
        
        if sitemap.google_maps_info.all_maps_links:
            for i, link in enumerate(sitemap.google_maps_info.all_maps_links, 1):
                summary += f"\n  {i}. {link}"
        else:
            summary += "\n  No Google Maps links found"
        
        summary += f"""

{'='*80}
🔗 INTERNAL LINK STRUCTURE
{'='*80}

📄 Page Types Discovered:"""
        
        page_types = {}
        for link in sitemap.internal_links:
            if link.is_about:
                page_types.setdefault('About Pages', []).append(link.url)
            elif link.is_contact:
                page_types.setdefault('Contact Pages', []).append(link.url)
            elif link.is_navigation:
                page_types.setdefault('Navigation Links', []).append(link.url)
            elif link.is_footer:
                page_types.setdefault('Footer Links', []).append(link.url)
            else:
                page_types.setdefault('General Pages', []).append(link.url)
        
        for page_type, urls in page_types.items():
            summary += f"\n\n{page_type} ({len(urls)}):"
            for url in urls[:5]:  # Show first 5 URLs
                summary += f"\n  • {url}"
            if len(urls) > 5:
                summary += f"\n  • ... and {len(urls) - 5} more"
        
        summary += f"""

{'='*80}
🌐 EXTERNAL CONNECTIONS
{'='*80}

📱 Social Media Presence:"""
        
        if sitemap.social_links:
            for link in sitemap.social_links:
                summary += f"\n  • {link.url} (from: {link.source_page})"
        else:
            summary += "\n  No social media links found"
        
        summary += f"""

🔗 External References ({len(sitemap.external_links)} total):"""
        
        if sitemap.external_links:
            # Group external links by domain
            external_domains = {}
            for link in sitemap.external_links:
                domain = urlparse(link.url).netloc
                external_domains.setdefault(domain, []).append(link.url)
            
            for domain, urls in list(external_domains.items())[:10]:  # Show top 10 domains
                summary += f"\n  • {domain}: {len(urls)} links"
        else:
            summary += "\n  No external links found"
        
        summary += f"""

{'='*80}
📊 ANALYSIS SUMMARY
{'='*80}

This comprehensive sitemap analysis discovered {sitemap.total_pages} pages across {sitemap.crawl_depth_reached} 
levels of depth, providing complete visibility into the website structure. The analysis found 
{sitemap.google_maps_info.total_maps_found} Google Maps integrations and classified the website 
structure as {sitemap.website_structure_complexity.lower()}.

Generated by Enhanced Website Analyzer v3.0
Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        return summary
    
    def _download_images(self, html: str, base_url: str, images_folder: Path):
        """Download images from HTML"""
        if not html:
            return
        
        soup = BeautifulSoup(html, 'html.parser')
        
        for img in soup.find_all('img', src=True):
            try:
                img_url = urljoin(base_url, img['src'])
                parsed = urlparse(img_url)
                filename = os.path.basename(parsed.path.split("?")[0])
                
                if not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
                    continue
                
                # Download image
                response = requests.get(img_url, timeout=5)
                if response.status_code == 200:
                    with open(images_folder / filename, 'wb') as f:
                        f.write(response.content)
                        
            except Exception as e:
                logger.debug(f"Failed to download image {img.get('src')}: {e}")
                continue
    
    def _clean_filename(self, url: str, domain: str, index: int) -> str:
        """Generate clean filename for URL"""
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_").replace("?", "_")
        
        if not path or path == domain:
            return f"{domain}_page_{index:03d}"
        
        return f"{domain}_{path}_{index:03d}"[:100]  # Limit length
    
    def _generate_comprehensive_summary_report(self, summary_data: Dict[str, Any]) -> str:
        """Generate comprehensive summary report with enhanced Google Maps integration"""
        logger.info("Generating comprehensive summary report with Google Maps")
        
        domain = summary_data['domain']
        sitemap = summary_data['sitemap']
        contact_info = summary_data['contact_info']
        business_metrics = summary_data['business_metrics']
        marketing_intel = summary_data['marketing_intelligence']
        website_features = summary_data['website_features']
        enhanced_metadata = summary_data['enhanced_metadata']
        pages_data = summary_data['pages_data']
        
        # Calculate additional metrics
        total_words = sum(page.word_count for page in pages_data)
        avg_load_time = sum(page.load_time for page in pages_data) / len(pages_data) if pages_data else 0
        
        # Website status
        main_status = pages_data[0].status_code if pages_data else 200
        if main_status == 200:
            website_status = "✅ Good (200 OK)"
        elif main_status in [301, 302]:
            website_status = f"↗️ Redirect ({main_status})"
        else:
            website_status = f"⚠️ Warning ({main_status})"
        
        # Generate comprehensive report with enhanced Google Maps integration
        report = f"""🔍 COMPREHENSIVE BUSINESS INTELLIGENCE REPORT - ENHANCED v3.0
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Domain: {domain}

{'='*80}
📊 COMPANY OVERVIEW
{'='*80}

🏷️ Basic Information:
  • Company Name: {contact_info.company_name or 'Unknown'}
  • Company Title: {contact_info.brand_name or 'Unknown'}
  • Industry: {business_metrics.industry}
  • Website: {summary_data['main_url']}
  • Website Status: {website_status}
  • Founded Year: {business_metrics.founded_year or 'Unknown'}

🔍 Discovery Statistics:
  • Total Pages Discovered: {sitemap.total_pages}
  • Total Links Found: {sitemap.total_links}
  • Maximum Crawl Depth: {sitemap.crawl_depth_reached}
  • Website Structure Complexity: {sitemap.website_structure_complexity}
  • Total Word Count: {total_words:,}
  • Average Load Time: {avg_load_time:.2f}s

{'='*80}
🌐 ENHANCED WEBSITE METADATA
{'='*80}

📄 Site Metadata:
  • Site Title: {enhanced_metadata.site_title or 'Not found'}
  • Meta Description: {enhanced_metadata.meta_description or 'Not found'}
  • Meta Keywords: {enhanced_metadata.meta_keywords or 'Not found'}
  • Logo URL: {enhanced_metadata.logo_url or 'Not found'}
  • Favicon URL: {enhanced_metadata.favicon_url or 'Not found'}

📖 About Us Information:
  • About Us URL: {enhanced_metadata.about_us_url or 'Not found'}
  • About Us Text: 
{enhanced_metadata.about_us_text or 'No about us information found'}

{'='*80}
📇 CONTACT INFORMATION
{'='*80}

📞 Phone Numbers:
  • Mobile Phone: {contact_info.mobile_phone or 'Not found'}
  • Corporate Phone: {contact_info.corporate_phone or 'Not found'}
  • Support Phone: {contact_info.support_phone or 'Not found'}
  • Company Phone: {contact_info.company_phone or 'Not found'}

📧 Digital Contact:
  • Email: {contact_info.email or 'Not found'}

🏢 Location:
  • Address: {contact_info.address or 'Not found'}
  • Company City: {contact_info.company_city or 'Not found'}
  • Company State: {contact_info.company_state or 'Not found'}

{'='*80}
🗺️ GOOGLE MAPS INTEGRATION - COMPREHENSIVE ANALYSIS
{'='*80}

📍 Maps Integration Status: {contact_info.google_maps_integration}
🔍 Total Google Maps Links Found: {len(contact_info.all_google_maps_links)}
🎯 Primary Google Maps Link: {contact_info.google_map or 'Not found'}

📋 Complete Google Maps Discovery:"""
        
        if contact_info.all_google_maps_links:
            for i, maps_link in enumerate(contact_info.all_google_maps_links, 1):
                report += f"\n  {i}. {maps_link}"
        else:
            report += "\n  No Google Maps links discovered across entire website"
        
        report += f"""

🔍 Google Maps Detection Methods Used:
  • Direct Links: {len(sitemap.google_maps_info.maps_links)} found
  • Iframe Embeds: {len(sitemap.google_maps_info.iframe_embeds)} found
  • JavaScript Maps: {len(sitemap.google_maps_info.javascript_maps)} found
  • Structured Data Maps: {len(sitemap.google_maps_info.structured_data_maps)} found
  • Contact Page Maps: {len(sitemap.google_maps_info.contact_page_maps)} found

📱 Social Media Presence:
  • Facebook: {contact_info.social_media.get('facebook') or 'Not found'}
  • Instagram: {contact_info.social_media.get('instagram') or 'Not found'}
  • TikTok: {contact_info.social_media.get('tiktok') or 'Not found'}
  • LinkedIn: {contact_info.social_media.get('linkedin') or 'Not found'}
  • Twitter: {contact_info.social_media.get('twitter') or 'Not found'}
  • Pinterest: {contact_info.social_media.get('pinterest') or 'Not found'}
  • YouTube: {contact_info.social_media.get('youtube') or 'Not found'}

{'='*80}
💼 BUSINESS METRICS & INTELLIGENCE
{'='*80}

📈 Company Metrics:
  • Employees: {business_metrics.employees or 'Unknown'}
  • Annual Revenue: {business_metrics.annual_revenue or 'Unknown'}
  • Segmentation: {business_metrics.segmentation} (Budget Classification)
  • Firmographic Score: {business_metrics.firmographic_score}/100
  • Engagement Score: {business_metrics.engagement_score}/100

🏗️ Website Analysis:
  • Website Structure Complexity: {business_metrics.website_structure_complexity}
  • Digital Presence Strength: {business_metrics.digital_presence_strength}
  • Contact Accessibility: {business_metrics.contact_accessibility}

🎯 Marketing Intelligence:
  • Instagram Handle: {marketing_intel.instagram_handle or 'Not found'}
  • IG Score: {marketing_intel.ig_score}/100
  • Worked With Creators: {'✅ Yes' if marketing_intel.worked_with_creators else '❌ No'}
  • Integrated Video Links: {len(marketing_intel.integrated_video_links)} found

📹 Video Content:"""
        
        if marketing_intel.integrated_video_links:
            for i, link in enumerate(marketing_intel.integrated_video_links[:5], 1):
                report += f"\n  {i}. {link}"
            if len(marketing_intel.integrated_video_links) > 5:
                report += f"\n  ... and {len(marketing_intel.integrated_video_links) - 5} more video links"
        else:
            report += "\n  No video content found"
        
        report += f"""

{'='*80}
🚀 WEBSITE FEATURES & CAPABILITIES
{'='*80}

D2C Presence: {'✅ Yes' if website_features.d2c_presence else '❌ No'}
E-Commerce Presence: {'✅ Yes' if website_features.ecommerce_presence else '❌ No'}
Social Media Presence: {'✅ Yes' if website_features.social_media_presence else '❌ No'}
Integrated Videos: {'✅ Yes' if website_features.video_presence else '❌ No'}
SaaS / Platform: {'✅ Yes' if website_features.saas_platform else '❌ No'}
Blog / Content: {'✅ Yes' if website_features.blog_presence else '❌ No'}
CTA Presence: {'✅ Yes' if website_features.cta_presence else '❌ No'}
Product Listings: {'✅ Yes' if website_features.product_listings else '❌ No'}
Contact Forms: {'✅ Yes' if website_features.contact_forms else '❌ No'}
Newsletter Signup: {'✅ Yes' if website_features.newsletter_signup else '❌ No'}

{'='*80}
⚙️ TECHNICAL DETAILS
{'='*80}

🔒 Security & Performance:
  • SSL Secure: {'✅ Yes' if website_features.ssl_secure else '❌ No'}
  • Mobile Responsive: {'✅ Yes' if website_features.mobile_responsive else '❌ No'}
  • Pages Analyzed: {summary_data['pages_analyzed']}
  • Average Load Time: {avg_load_time:.2f}s

🔗 Link Analysis:
  • Internal Links: {len(sitemap.internal_links)}
  • External Links: {len(sitemap.external_links)}
  • Social Media Links: {len(sitemap.social_links)}
  • Contact-Related Links: {len(sitemap.contact_links)}

{'='*80}
📊 COMPREHENSIVE DISCOVERY SUMMARY
{'='*80}

🔍 Complete Website Mapping Results:
This enhanced analysis performed comprehensive link discovery across the entire website structure, 
analyzing {sitemap.total_pages} pages at a maximum depth of {sitemap.crawl_depth_reached} levels. 
The system discovered {sitemap.total_links} total links and classified the website structure 
as {sitemap.website_structure_complexity.lower()}.

🗺️ Google Maps Integration Analysis:
The comprehensive Google Maps detection system used 4 different detection methods across all 
discovered pages, finding {len(contact_info.all_google_maps_links)} total Google Maps integrations. 
The maps integration status is: {contact_info.google_maps_integration}.

📈 Business Intelligence Quality:
- Company shows {'high' if business_metrics.engagement_score > 70 else 'moderate' if business_metrics.engagement_score > 40 else 'low'} digital engagement
- {'Strong' if business_metrics.firmographic_score > 70 else 'Moderate' if business_metrics.firmographic_score > 40 else 'Limited'} company data completeness
- Budget segment: {business_metrics.segmentation}
- Digital presence strength: {business_metrics.digital_presence_strength}
- Contact accessibility: {business_metrics.contact_accessibility}
- {'Active in creator economy' if marketing_intel.worked_with_creators else 'No evident creator collaborations'}

{'='*80}
🎯 ENHANCED ANALYSIS NOTES FOR SDR
{'='*80}

📊 Key Insights:
• Website Complexity: {sitemap.website_structure_complexity} structure with {sitemap.total_pages} discoverable pages
• Google Maps Integration: {contact_info.google_maps_integration} - {len(contact_info.all_google_maps_links)} maps links found
• Contact Accessibility: {business_metrics.contact_accessibility} - multiple contact methods available
• Digital Presence: {business_metrics.digital_presence_strength} social media presence across platforms
• Business Segment: {business_metrics.segmentation} market positioning

🎯 Recommended Approach:
Based on the comprehensive analysis, this company shows a {business_metrics.engagement_score}/100 
engagement score and {business_metrics.firmographic_score}/100 firmographic completeness. 
The {sitemap.website_structure_complexity.lower()} website structure and 
{business_metrics.digital_presence_strength.lower()} digital presence suggest 
{'a sophisticated' if business_metrics.engagement_score > 70 else 'a developing'} online operation.

{'='*80}
📋 COMPREHENSIVE ANALYSIS SUMMARY
{'='*80}

This enhanced comprehensive analysis extracted data from {summary_data['pages_analyzed']} web pages 
discovered through complete link mapping across {sitemap.crawl_depth_reached} levels of website depth. 
The analysis provides detailed business intelligence including contact information, business metrics, 
marketing strategies, technical capabilities, and complete Google Maps integration analysis.

Enhanced features include:
✅ Complete website link discovery and mapping
✅ Multi-method Google Maps detection across entire site
✅ Comprehensive business intelligence from all discovered pages
✅ Enhanced metadata extraction including complete about us content
✅ Quality metrics and scoring based on comprehensive site analysis
✅ Advanced link classification and relationship mapping

The company shows a {business_metrics.segmentation.lower()} market segment profile with 
{business_metrics.digital_presence_strength.lower()} digital presence and 
{business_metrics.contact_accessibility.lower()} contact accessibility.

Report generated by Enhanced Complete Website Analyzer v3.0
Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total Processing Time: {avg_load_time * len(pages_data):.1f} seconds
"""
        
        return report
    
    def _save_enhanced_summary_report(self, report: str, base_folder: Path, domain: str):
        """Save enhanced summary report to file"""
        report_file = base_folder / f"{domain}_comprehensive_summary.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        logger.info(f"Enhanced summary report saved: {report_file}")
    
    def _save_enhanced_master_json(self, summary_data: Dict[str, Any], base_folder: Path, domain: str):
        """Save enhanced master JSON with all data"""
        
        # Convert dataclasses to dictionaries for JSON serialization
        def convert_to_dict(obj):
            if hasattr(obj, '__dict__'):
                return obj.__dict__
            elif isinstance(obj, list):
                return [convert_to_dict(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: convert_to_dict(v) for k, v in obj.items()}
            else:
                return obj
        
        json_data = convert_to_dict(summary_data)
        
        master_file = base_folder / f"{domain}_master_enhanced.json"
        with open(master_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
        logger.info(f"Enhanced master JSON saved: {master_file}")

   # ============================================================================
    # Save unique JSON pages with duplicate prevention
    # ============================================================================
def save_unique_json_pages(pages_data: List, output_folder: Path):
    """
    Save page data as JSON files with duplicate prevention using content hashing.

    Args:
        pages_data (List): List of PageMetadata-like objects.
        output_folder (Path): Folder where JSON files should be saved.
    """
    seen_hashes = set()
    output_folder.mkdir(parents=True, exist_ok=True)

    for page in pages_data:
        try:
            page_data = {
                "url": page.url,
                "title": page.title,
                "content": page.content,
                "meta": page.meta_data,
            }

            json_str = json.dumps(page_data, sort_keys=True)
            content_hash = hashlib.md5(json_str.encode('utf-8')).hexdigest()

            if content_hash in seen_hashes:
                logger.info(f"Duplicate content found for {page.url}, skipping save.")
                continue

            seen_hashes.add(content_hash)

            # Generate a safe filename from the URL
            safe_filename = page.url.replace("https://", "").replace("http://", "").replace("/", "_")
            filename = f"{safe_filename}.json"
            filepath = output_folder / filename

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(json_str)
            
            logger.info(f"Saved: {filename}")

        except Exception as e:
            logger.error(f"Failed to save page {getattr(page, 'url', 'unknown')}: {e}")

# ============================================================================
# MAIN EXECUTION FUNCTION
# ============================================================================

def analyze_websites_comprehensive_enhanced(csv_file: str = 'websites.csv'):
    """
    Main function to analyze websites with comprehensive link discovery and Google Maps integration
    
    Args:
        csv_file: Path to CSV file containing website URLs
    """
    logger.info("Starting Enhanced Comprehensive Website Analysis v3.0")
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        return
    
    try:
        # Load websites from CSV
        df = pd.read_csv(csv_file)
        if 'url' not in df.columns:
            logger.error("'url' column missing in CSV")
            return
        
        urls = df['url'].dropna().unique().tolist()
        logger.info(f"Found {len(urls)} unique URLs to analyze comprehensively")
        
        # Initialize enhanced analyzer
        analyzer = EnhancedCompleteWebsiteAnalyzer()
        
        # Process websites with controlled parallelism
        completed = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            futures = [executor.submit(analyzer.analyze_website_comprehensive, url) for url in urls]
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        completed += 1
                        logger.info(f"✅ Progress: {completed}/{len(urls)} websites completed")
                    else:
                        failed += 1
                        logger.warning(f"⚠️ Website analysis returned empty result")
                except Exception as e:
                    failed += 1
                    logger.error(f"❌ Website analysis failed: {e}")
        
        logger.info(f"""
🎉 ENHANCED COMPREHENSIVE ANALYSIS COMPLETE!
✅ Successfully analyzed: {completed} websites
❌ Failed: {failed} websites
📁 Results saved in: analyzed/ directory
📊 Enhanced summary reports: *_comprehensive_summary.txt
🗃️ Raw data: html/, json/, images/, sitemap/ folders
🗺️ Complete sitemaps: sitemap/*_complete_sitemap.json
🌐 Google Maps integration: Complete detection across all pages
🔍 Link discovery: ALL clickable links mapped and analyzed
📈 Business intelligence: Comprehensive data from entire website structure
""")
        
    except Exception as e:
        logger.error(f"Critical error in enhanced comprehensive execution: {e}")

if __name__ == '__main__':
    analyze_websites_comprehensive_enhanced()

 