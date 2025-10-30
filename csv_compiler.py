"""
Enhanced CSV Compiler v3.0 - Complete Google Maps Integration & Link Discovery Data
==================================================================================

This script compiles comprehensive business intelligence data from enhanced summary 
reports into a structured CSV file with complete Google Maps integration data and 
comprehensive link discovery statistics.

ENHANCED FEATURES v3.0:
- Complete Google Maps integration data with all discovered links
- Comprehensive link discovery statistics (total pages, links, depth)
- Enhanced website structure complexity analysis
- Multiple Google Maps detection method results
- Complete sitemap integration data
- Advanced business intelligence compilation
- 45+ data fields including all discovery metrics
- Extraction of min/max priced product details

Input: Enhanced comprehensive summary report text files
Output: Complete CSV file with comprehensive business intelligence and discovery data

Author: Enhanced Website Analyzer v3.0
Date: 2024
"""

import os
import re
import csv
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime
from dataclasses import dataclass, field # Added for ProductDetails

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_enhanced_logging():
    """Setup enhanced logging for CSV compiler"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler(f"enhanced_csv_compiler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_enhanced_logging()

# ============================================================================
# DATA STRUCTURES (for compiler's internal use)
# ============================================================================

@dataclass
class ProductDetails:
    """Structure for storing product information extracted by the compiler"""
    name: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    url: Optional[str] = None

# ============================================================================
# ENHANCED DATA EXTRACTION PATTERNS v3.0
# ============================================================================

class EnhancedDataExtractionPatternsV3:
    """Enhanced regex patterns for extracting comprehensive data from summary reports"""
    
    def __init__(self):
        self.setup_patterns()
    
    def setup_patterns(self):
        """Initialize all extraction patterns including comprehensive discovery data"""
        
        # Company information patterns
        self.company_patterns = {
            'company_name': r'Company Name:\s*(.+?)(?:\n|$)',
            'industry': r'Industry:\s*(.+?)(?:\n|$)',
            'website': r'Website:\s*(.+?)(?:\n|$)',
            'founded_year': r'Founded Year:\s*(.+?)(?:\n|$)',
            'website_status': r'Website Status:\s*(.+?)(?:\n|$)'
        }
        
        # Enhanced discovery statistics patterns (NEW)
        self.discovery_patterns = {
            'total_pages_discovered': r'Total Pages Discovered:\s*(\d+)',
            'total_links_found': r'Total Links Found:\s*(\d+)',
            'max_crawl_depth': r'Maximum Crawl Depth:\s*(\d+)',
            'website_structure_complexity': r'Website Structure Complexity:\s*(.+?)(?:\n|$)',
            'total_word_count': r'Total Word Count:\s*([\d,]+)',
            'avg_load_time': r'Average Load Time:\s*([\d.]+)s'
        }
        
        # Enhanced metadata patterns
        self.enhanced_metadata_patterns = {
            'site_title': r'Site Title:\s*(.+?)(?:\n|$)',
            'meta_description': r'Meta Description:\s*(.+?)(?:\n|$)',
            'meta_keywords': r'Meta Keywords:\s*(.+?)(?:\n|$)',
            'keywords_compilation': r'Keywords Compilation:\s*(.+?)(?:\n|$)', # NEW pattern for keywords compilation
            'logo_url': r'Logo URL:\s*(.+?)(?:\n|$)',
            'favicon_url': r'Favicon URL:\s*(.+?)(?:\n|$)',
            'about_us_url': r'About Us URL:\s*(.+?)(?:\n|$)'
        }
        
        # Enhanced Google Maps patterns (COMPREHENSIVE)
        self.google_maps_patterns = {
            'google_maps_integration_status': r'Maps Integration Status:\s*(.+?)(?:\n|$)',
            'total_google_maps_found': r'Total Google Maps Links Found:\s*(\d+)',
            'primary_google_maps_link': r'Primary Google Maps Link:\s*(.+?)(?:\n|$)',
            'direct_maps_links_found': r'Direct Links:\s*(\d+)\s*found',
            'iframe_maps_embeds_found': r'Iframe Embeds:\s*(\d+)\s*found',
            'javascript_maps_found': r'JavaScript Maps:\s*(\d+)\s*found',
            'structured_data_maps_found': r'Structured Data Maps:\s*(\d+)\s*found',
            'contact_page_maps_found': r'Contact Page Maps:\s*(\d+)\s*found'
        }
        
        # Contact information patterns
        self.contact_patterns = {
            # Corrected phone number regex patterns (removed $$?)
            'mobile_phone': r'Mobile Phone:\s*(\+?1?[-.\s]?[2-9]\d{2}[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
            'corporate_phone': r'Corporate Phone:\s*(\+?1?[-.\s]?[2-9]\d{2}[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
            'support_phone': r'Support Phone:\s*(\+?1?[-.\s]?[2-9]\d{2}[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
            'company_phone': r'Company Phone:\s*(\+?1?[-.\s]?[2-9]\d{2}[-.\s]?[2-9]\d{2}[-.\s]?\d{4})',
            'email': r'Email:\s*(.+?)(?:\n|$)',
            'address': r'Address:\s*(.+?)(?:\n|$)',
            'company_city': r'Company City:\s*(.+?)(?:\n|$)',
            'company_state': r'Company State:\s*(.+?)(?:\n|$)'
        }
        
        # Social media patterns
        self.social_patterns = {
            'facebook': r'Facebook:\s*(.+?)(?:\n|$)',
            'instagram': r'Instagram:\s*(.+?)(?:\n|$)',
            'tiktok': r'TikTok:\s*(.+?)(?:\n|$)',
            'linkedin': r'LinkedIn:\s*(.+?)(?:\n|$)',
            'twitter': r'Twitter:\s*(.+?)(?:\n|$)',
            'pinterest': r'Pinterest:\s*(.+?)(?:\n|$)',
            'youtube': r'YouTube:\s*(.+?)(?:\n|$)'
        }
        
        # Enhanced business metrics patterns
        self.business_patterns = {
            'employees': r'Employees:\s*(.+?)(?:\n|$)',
            'annual_revenue': r'Annual Revenue:\s*(.+?)(?:\n|$)',
            'segmentation': r'Segmentation:\s*(.+?)(?:\s*\(|(?:\n|$))',
            'firmographic_score': r'Firmographic Score:\s*(\d+)/100',
            'engagement_score': r'Engagement Score:\s*(\d+)/100',
            'digital_presence_strength': r'Digital Presence Strength:\s*(.+?)(?:\n|$)',
            'contact_accessibility': r'Contact Accessibility:\s*(.+?)(?:\n|$)'
        }
        
        # Marketing intelligence patterns
        self.marketing_patterns = {
            'instagram_handle': r'Instagram Handle:\s*(.+?)(?:\n|$)',
            'ig_score': r'IG Score:\s*(\d+)/100',
            'worked_with_creators': r'Worked With Creators:\s*(✅ Yes|❌ No)',
            'integrated_video_links_count': r'Integrated Video Links:\s*(\d+)\s*found'
        }
        
        # Enhanced website features patterns
        self.features_patterns = {
            'd2c_presence': r'D2C Presence:\s*(✅ Yes|❌ No)',
            'ecommerce_presence': r'E-Commerce Presence:\s*(✅ Yes|❌ No)',
            'social_media_presence': r'Social Media Presence:\s*(✅ Yes|❌ No)',
            'video_presence': r'Integrated Videos:\s*(✅ Yes|❌ No)',
            'saas_platform': r'SaaS / Platform:\s*(✅ Yes|❌ No)',
            'blog_presence': r'Blog / Content:\s*(✅ Yes|❌ No)',
            'cta_presence': r'CTA Presence:\s*(✅ Yes|❌ No)',
            'product_listings': r'Product Listings:\s*(✅ Yes|❌ No)',
            'contact_forms': r'Contact Forms:\s*(✅ Yes|❌ No)',
            'newsletter_signup': r'Newsletter Signup:\s*(✅ Yes|❌ No)'
        }
        
        # Enhanced technical details patterns
        self.technical_patterns = {
            'ssl_secure': r'SSL Secure:\s*(✅ Yes|❌ No)',
            'mobile_responsive': r'Mobile Responsive:\s*(✅ Yes|❌ No)',
            'internal_links_count': r'Internal Links:\s*(\d+)',
            'external_links_count': r'External Links:\s*(\d+)',
            'social_links_count': r'Social Media Links:\s*(\d+)',
            'contact_links_count': r'Contact-Related Links:\s*(\d+)'
        }

        # Product pricing patterns (NEW)
        self.product_pricing_patterns = {
            'min_products_block': r'• 4 Minimum Price Products:\s*\n(.*?)(?=\n• 4 Maximum Price Products:|\n\n)',
            'max_products_block': r'• 4 Maximum Price Products:\s*\n(.*?)(?=\n\n|\n=)',
            'product_line': r'•\s*(.+?)\s*$$(.+?)([\d.]+)$$\s*-\s*(https?://[^\s]+)' # Adjusted regex for currency and price
        }

# ============================================================================
# ENHANCED SUMMARY REPORT PARSER v3.0
# ============================================================================

class EnhancedSummaryReportParserV3:
    """Enhanced parser for comprehensive summary reports with Google Maps integration"""
    
    def __init__(self):
        self.patterns = EnhancedDataExtractionPatternsV3()
    
    def parse_enhanced_summary_file(self, file_path: Path) -> Dict[str, Any]:
        """Parse enhanced summary report file with comprehensive Google Maps data"""
        logger.debug(f"Parsing enhanced summary file: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract all data sections
            data = {}
            
            # Extract company information
            data.update(self._extract_section(content, self.patterns.company_patterns))
            
            # Extract discovery statistics (NEW)
            data.update(self._extract_section(content, self.patterns.discovery_patterns))
            
            # Extract enhanced metadata
            data.update(self._extract_section(content, self.patterns.enhanced_metadata_patterns))
            
            # Extract comprehensive Google Maps data (ENHANCED)
            data.update(self._extract_section(content, self.patterns.google_maps_patterns))
            
            # Extract contact information
            data.update(self._extract_section(content, self.patterns.contact_patterns))
            
            # Extract social media
            social_data = self._extract_section(content, self.patterns.social_patterns)
            for key, value in social_data.items():
                data[f"{key}_url"] = value
            
            # Extract business metrics
            data.update(self._extract_section(content, self.patterns.business_patterns))
            
            # Extract marketing intelligence
            data.update(self._extract_section(content, self.patterns.marketing_patterns))
            
            # Extract website features
            data.update(self._extract_section(content, self.patterns.features_patterns))
            
            # Extract technical details
            data.update(self._extract_section(content, self.patterns.technical_patterns))
            
            # Extract additional enhanced data including complete Google Maps list and product pricing
            data.update(self._extract_enhanced_additional_data_v3(content))
            data.update(self._extract_product_pricing_details(content)) # NEW: Extract product pricing

            # Clean and validate data
            data = self._clean_enhanced_data(data)
            
            logger.debug(f"Successfully parsed enhanced file: {file_path}")
            return data
            
        except Exception as e:
            logger.error(f"Error parsing {file_path}: {e}")
            return {}
    
    def _extract_section(self, content: str, patterns: Dict[str, str]) -> Dict[str, str]:
        """Extract data using regex patterns"""
        extracted = {}
        
        for key, pattern in patterns.items():
            try:
                match = re.search(pattern, content, re.IGNORECASE | re.MULTILINE)
                if match:
                    value = match.group(1).strip()
                    # Clean common artifacts
                    value = value.replace('Not found', '').replace('Unknown', '').strip()
                    extracted[key] = value if value else ''
                else:
                    extracted[key] = ''
            except re.error as e:
                logger.warning(f"Regex error for pattern {key}: {e}")
                extracted[key] = ''
        
        return extracted
    
    def _extract_enhanced_additional_data_v3(self, content: str) -> Dict[str, Any]:
        """Extract enhanced additional data including complete Google Maps integration"""
        additional = {}
        
        # Extract domain from content
        domain_match = re.search(r'Domain:\s*(.+?)(?:\n|$)', content)
        if domain_match:
            additional['domain'] = domain_match.group(1).strip()
        
        # Extract analysis date
        date_match = re.search(r'Generated:\s*(.+?)(?:\n|$)', content)
        if date_match:
            additional['analysis_date'] = date_match.group(1).strip()
        
        # Extract complete about us text (ENHANCED)
        about_section = re.search(
            r'About Us Text:\s*\n(.*?)(?:\n\n|\n=|📇 CONTACT INFORMATION)', 
            content, 
            re.DOTALL
        )
        if about_section:
            about_text = about_section.group(1).strip()
            # Clean up the about text
            about_text = re.sub(r'\n\s*', ' ', about_text)  # Replace newlines with spaces
            about_text = re.sub(r'\s+', ' ', about_text)    # Normalize whitespace
            additional['about_us_text'] = about_text[:2000]  # Limit to 2000 chars
        else:
            additional['about_us_text'] = ''
        
        # Extract ALL Google Maps links (COMPREHENSIVE)
        maps_section = re.search(
            r'📋 Complete Google Maps Discovery:(.*?)(?:\n\n|\n🔍)', 
            content, 
            re.DOTALL
        )
        if maps_section:
            maps_content = maps_section.group(1)
            # Extract all numbered Google Maps links
            maps_links = re.findall(r'\d+\.\s*(https?://[^\s\n]+)', maps_content)
            additional['all_google_maps_links'] = '; '.join(maps_links)
            additional['google_maps_links_count'] = len(maps_links)
        else:
            additional['all_google_maps_links'] = ''
            additional['google_maps_links_count'] = 0
        
        # Extract primary Google Maps link
        primary_maps_match = re.search(r'🎯 Primary Google Maps Link:\s*(.+?)(?:\n|$)', content)
        if primary_maps_match:
            primary_link = primary_maps_match.group(1).strip()
            if primary_link and primary_link != 'Not found':
                additional['google_map'] = primary_link
            else:
                additional['google_map'] = ''
        else:
            additional['google_map'] = ''
        
        # Extract video content links (Removed [:5] limit)
        video_section = re.search(r'📹 Video Content:(.*?)(?:\n\n|\n=)', content, re.DOTALL)
        if video_section:
            video_content = video_section.group(1)
            video_links = re.findall(r'https?://[^\s]+', video_content)
            additional['video_links'] = '; '.join(video_links)
        else:
            additional['video_links'] = ''
        
        # Extract enhanced SDR notes
        sdr_section = re.search(r'🎯 ENHANCED ANALYSIS NOTES FOR SDR(.*?)(?:\n\n|\n=)', content, re.DOTALL)
        if sdr_section:
            sdr_notes = sdr_section.group(1).strip()
            # Clean up the notes
            sdr_notes = re.sub(r'\n\s*[•·]\s*', ' | ', sdr_notes)
            sdr_notes = re.sub(r'\n\s*', ' ', sdr_notes)
            additional['notes_for_sdr'] = sdr_notes[:1000]  # Limit length
        else:
            additional['notes_for_sdr'] = ''
        
        # Extract short description from meta description or about text
        meta_desc = additional.get('meta_description', '')
        about_text = additional.get('about_us_text', '')
        
        if meta_desc and len(meta_desc) > 20:
            additional['short_description'] = meta_desc[:200]
        elif about_text and len(about_text) > 20:
            # Take first sentence or first 200 chars of about text
            first_sentence = about_text.split('.')[0]
            if len(first_sentence) > 20 and len(first_sentence) < 200:
                additional['short_description'] = first_sentence + '.'
            else:
                additional['short_description'] = about_text[:200] + '...'
        else:
            additional['short_description'] = ''
        
        # Extract comprehensive discovery summary
        discovery_summary_match = re.search(
            r'🔍 Complete Website Mapping Results:(.*?)(?:\n\n|\n🗺️)', 
            content, 
            re.DOTALL
        )
        if discovery_summary_match:
            discovery_text = discovery_summary_match.group(1).strip()
            discovery_text = re.sub(r'\n\s*', ' ', discovery_text)
            additional['discovery_summary'] = discovery_text[:500]
        else:
            additional['discovery_summary'] = ''
        
        return additional

    def _extract_product_pricing_details(self, content: str) -> Dict[str, Any]:
        """Extracts min/max priced product details from the report content."""
        product_data = {}

        # Extract min products block
        min_block_match = re.search(self.patterns.product_pricing_patterns['min_products_block'], content, re.DOTALL)
        min_products = []
        if min_block_match:
            min_products_text = min_block_match.group(1).strip()
            min_products = self._parse_product_lines(min_products_text)
        
        # Extract max products block
        max_block_match = re.search(self.patterns.product_pricing_patterns['max_products_block'], content, re.DOTALL)
        max_products = []
        if max_block_match:
            max_products_text = max_block_match.group(1).strip()
            max_products = self._parse_product_lines(max_products_text)

        # Flatten product details into individual fields for CSV
        for i in range(4):
            product_data[f'min_price_product_{i+1}_name'] = min_products[i].name if i < len(min_products) else ''
            product_data[f'min_price_product_{i+1}_price'] = min_products[i].price if i < len(min_products) else ''
            product_data[f'min_price_product_{i+1}_url'] = min_products[i].url if i < len(min_products) else ''

            product_data[f'max_price_product_{i+1}_name'] = max_products[i].name if i < len(max_products) else ''
            product_data[f'max_price_product_{i+1}_price'] = max_products[i].price if i < len(max_products) else ''
            product_data[f'max_price_product_{i+1}_url'] = max_products[i].url if i < len(max_products) else ''
        
        return product_data

    def _parse_product_lines(self, text_block: str) -> List[ProductDetails]:
        """Parses individual product lines from a text block."""
        products = []
        for line in text_block.split('\n'):
            line = line.strip()
            if line.startswith('•'):
                match = re.search(self.patterns.product_pricing_patterns['product_line'], line)
                if match:
                    name = match.group(1).strip()
                    currency_price_str = match.group(2).strip()
                    url = match.group(4).strip()

                    currency_match = re.search(r'([$€£¥])', currency_price_str)
                    currency = currency_match.group(1) if currency_match else None
                    
                    price_str = re.sub(r'[^\d.]', '', currency_price_str)
                    try:
                        price = float(price_str)
                    except ValueError:
                        price = None
                    
                    products.append(ProductDetails(name=name, price=price, currency=currency, url=url))
        return products
    
    def _clean_enhanced_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize extracted enhanced data"""
        cleaned = {}
        
        for key, value in data.items():
            if isinstance(value, str):
                # Remove common artifacts
                value = value.replace('Not found', '').replace('Unknown', '').strip()
                
                # Convert Yes/No indicators to boolean strings
                if value in ['✅ Yes', '❌ No']:
                    value = 'True' if '✅' in value else 'False'
                
                # Clean phone numbers
                if 'phone' in key.lower() and value:
                    value = self._clean_phone_number(value)
                
                # Clean URLs
                if 'url' in key.lower() and value and not value.startswith('http'):
                    if value != '':
                        value = f"https://{value}" if not value.startswith('//') else f"https:{value}"
                
                # Clean numeric values
                if key in ['total_word_count', 'total_pages_discovered', 'total_links_found', 
                          'total_google_maps_found', 'google_maps_links_count',
                          'direct_maps_links_found', 'iframe_maps_embeds_found',
                          'javascript_maps_found', 'structured_data_maps_found', 'contact_page_maps_found',
                          'integrated_video_links_count', 'firmographic_score', 'engagement_score', 'ig_score']:
                    value = re.sub(r'[^\d.]', '', str(value)) # Allow decimals for load time
                
                # Clean text fields
                if key in ['about_us_text', 'short_description', 'notes_for_sdr', 'discovery_summary']:
                    value = self._clean_text_field(value)
                
            cleaned[key] = value
        
        return cleaned
    
    def _clean_phone_number(self, phone: str) -> str:
        """Clean and format phone numbers"""
        if not phone or phone.strip() == '':
            return ''
        
        # Remove extra whitespace and common artifacts
        phone = re.sub(r'\s+', ' ', phone.strip())
        
        # If multiple phones, take the first one
        if ',' in phone:
            phone = phone.split(',')[0].strip()
        
        return phone
    
    def _clean_text_field(self, text: str) -> str:
        """Clean text fields like about us text"""
        if not text:
            return ''
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common unwanted phrases
        unwanted_phrases = [
            'Not found', 'Unknown', 'No information available',
            'No about us information found'
        ]
        
        for phrase in unwanted_phrases:
            text = text.replace(phrase, '').strip()
        
        return text

# ============================================================================
# ENHANCED ADDRESS PARSER v3.0
# ============================================================================

class EnhancedAddressParserV3:
    """Enhanced address parser with comprehensive international support"""
    
    def __init__(self):
        self.us_states = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
            'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
            'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
            'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
            'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
            'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming'
        }
        
        self.countries = {
            'usa': 'United States', 'united states': 'United States', 'us': 'United States',
            'uk': 'United Kingdom', 'united kingdom': 'United Kingdom',
            'canada': 'Canada', 'ca': 'Canada',
            'australia': 'Australia', 'au': 'Australia',
            'germany': 'Germany', 'de': 'Germany',
            'france': 'France', 'fr': 'France',
            'japan': 'Japan', 'jp': 'Japan',
            'india': 'India', 'in': 'India',
            'china': 'China', 'cn': 'China',
            'brazil': 'Brazil', 'br': 'Brazil',
            'mexico': 'Mexico', 'mx': 'Mexico'
        }
    
    def parse_address_enhanced(self, address: str) -> Tuple[str, str, str]:
        """Parse address into city, state, country with enhanced detection"""
        if not address or address.strip() == '':
            return '', '', ''
        
        city = state = country = ''
        address = address.strip()
        
        # US format: Street, City, State Zip
        us_pattern = r'(.+),\s*([^,]+),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?'
        match = re.search(us_pattern, address)
        if match:
            city = match.group(2).strip()
            state_abbr = match.group(3).strip()
            if state_abbr in self.us_states:
                state = self.us_states[state_abbr]
                country = 'United States'
            return city, state, country
        
        # International format: Street, City, State/Province, Country
        intl_pattern = r'(.+),\s*([^,]+),\s*([^,]+),\s*([^,]+)'
        match = re.search(intl_pattern, address)
        if match:
            city = match.group(2).strip()
            state = match.group(3).strip()
            country = match.group(4).strip()
            
            # Normalize country name
            country_lower = country.lower()
            if country_lower in self.countries:
                country = self.countries[country_lower]
            
            return city, state, country
        
        # Simple City, State format
        simple_pattern = r'([^,]+),\s*([^,]+)'
        match = re.search(simple_pattern, address)
        if match:
            city = match.group(1).strip()
            state_or_country = match.group(2).strip()
            
            # Check if it's a US state
            if state_or_country.upper() in self.us_states:
                state = self.us_states[state_or_country.upper()]
                country = 'United States'
            else:
                # Check if it's a country
                state_lower = state_or_country.lower()
                if state_lower in self.countries:
                    country = self.countries[state_lower]
                    state = ''
                else:
                    state = state_or_country
                    country = ''
            
            return city, state, country
        
        return '', '', ''

# ============================================================================
# ENHANCED CSV COMPILER v3.0
# ============================================================================

class EnhancedCSVCompilerV3:
    """Enhanced CSV compiler with comprehensive Google Maps and discovery data"""
    
    def __init__(self):
        self.parser = EnhancedSummaryReportParserV3()
        self.address_parser = EnhancedAddressParserV3()
        self.base_dir = Path("analyzed")
        self.output_file = "comprehensive_business_intelligence_enhanced_v3.csv"
    
    def compile_to_enhanced_csv(self):
        """Compile all summary reports to comprehensive CSV with enhanced Google Maps data"""
        logger.info("Starting enhanced CSV compilation v3.0 with comprehensive Google Maps integration")
        
        # Define comprehensive CSV structure with enhanced fields (45+ fields)
        fieldnames = [
            # Basic Company Information
            'domain', 'company_name', 'industry', 'website', 'founded_year',
            'analysis_date', 'website_status',
            
            # Enhanced Discovery Statistics (NEW)
            'total_pages_discovered', 'total_links_found', 'max_crawl_depth',
            'website_structure_complexity', 'total_word_count', 'avg_load_time',
            
            # Enhanced Metadata
            'site_title', 'meta_description', 'meta_keywords', 'keywords_compilation', # Added keywords_compilation
            'logo_url', 'favicon_url', 'about_us_url', 'short_description',
            
            # Comprehensive Google Maps Integration (ENHANCED)
            'google_maps_integration_status', 'total_google_maps_found', 
            'primary_google_maps_link', 'all_google_maps_links', 'google_maps_links_count',
            'direct_maps_links_found', 'iframe_maps_embeds_found', 
            'javascript_maps_found', 'structured_data_maps_found', 'contact_page_maps_found',
            
            # Contact Information
            'mobile_phone', 'corporate_phone', 'support_phone', 'company_phone',
            'email', 'address', 'company_city', 'company_state',
            
            # Address Components (parsed)
            'address_city', 'address_state', 'address_country',
            
            # Social Media
            'facebook_url', 'instagram_url', 'tiktok_url', 'linkedin_url',
            'twitter_url', 'pinterest_url', 'youtube_url',
            
            # Enhanced Business Metrics
            'employees', 'annual_revenue', 'segmentation', 'firmographic_score',
            'engagement_score', 'digital_presence_strength', 'contact_accessibility',
            
            # Marketing Intelligence
            'instagram_handle', 'ig_score', 'worked_with_creators',
            'integrated_video_links_count', 'video_links',
            
            # Product Pricing (NEW)
            'min_price_product_1_name', 'min_price_product_1_price', 'min_price_product_1_url',
            'min_price_product_2_name', 'min_price_product_2_price', 'min_price_product_2_url',
            'min_price_product_3_name', 'min_price_product_3_price', 'min_price_product_3_url',
            'min_price_product_4_name', 'min_price_product_4_price', 'min_price_product_4_url',
            'max_price_product_1_name', 'max_price_product_1_price', 'max_price_product_1_url',
            'max_price_product_2_name', 'max_price_product_2_price', 'max_price_product_2_url',
            'max_price_product_3_name', 'max_price_product_3_price', 'max_price_product_3_url',
            'max_price_product_4_name', 'max_price_product_4_price', 'max_price_product_4_url',
            
            # Website Features
            'd2c_presence', 'ecommerce_presence', 'social_media_presence',
            'video_presence', 'saas_platform', 'blog_presence', 'cta_presence',
            'product_listings', 'contact_forms', 'newsletter_signup',
            
            # Technical Details & Link Analysis
            'ssl_secure', 'mobile_responsive', 'internal_links_count',
            'external_links_count', 'social_links_count', 'contact_links_count',
            
            # Enhanced Content Fields
            'about_us_text', 'keywords', 'notes_for_sdr', 'discovery_summary', 'status'
        ]
        
        # Process all summary files
        all_data = []
        processed_count = 0
        
        for domain_folder in self.base_dir.iterdir():
            if not domain_folder.is_dir():
                continue
            
            domain = domain_folder.name
            logger.info(f"Processing enhanced domain: {domain}")
            
            try:
                # Find enhanced summary file
                summary_files = list(domain_folder.glob("*_comprehensive_summary.txt"))
                if not summary_files:
                    # Try alternative naming
                    summary_files = list(domain_folder.glob("*_summary.txt"))
                
                if not summary_files:
                    logger.warning(f"No enhanced summary file found for {domain}")
                    continue
                
                summary_file = summary_files[0]
                
                # Parse enhanced summary file
                data = self.parser.parse_enhanced_summary_file(summary_file)
                
                if not data:
                    logger.warning(f"No data extracted from {summary_file}")
                    continue
                
                # Add domain if not present
                if 'domain' not in data or not data['domain']:
                    data['domain'] = domain
                
                # Parse address components
                address = data.get('address', '')
                if address:
                    city, state, country = self.address_parser.parse_address_enhanced(address)
                    data['address_city'] = city
                    data['address_state'] = state
                    data['address_country'] = country
                else:
                    data['address_city'] = ''
                    data['address_state'] = ''
                    data['address_country'] = ''
                
                # Process keywords from keywords_compilation (now directly extracted)
                keywords_compilation_str = data.get('keywords_compilation', '')
                if keywords_compilation_str:
                    data['keywords'] = keywords_compilation_str # Use the directly extracted string
                else:
                    data['keywords'] = ''
                
                # Set default values for missing fields
                for field in fieldnames:
                    if field not in data:
                        data[field] = ''
                
                # Add enhanced status and additional fields
                data['status'] = 'Active - Enhanced Analysis v3.0'
                
                # Convert boolean strings to proper format
                boolean_fields = [
                    'd2c_presence', 'ecommerce_presence', 'social_media_presence',
                    'video_presence', 'saas_platform', 'blog_presence', 'cta_presence',
                    'product_listings', 'contact_forms', 'newsletter_signup',
                    'ssl_secure', 'mobile_responsive', 'worked_with_creators'
                ]
                
                for field in boolean_fields:
                    if field in data:
                        value = str(data[field]).lower()
                        if value in ['true', '✅ yes', 'yes', '1']:
                            data[field] = 'True'
                        elif value in ['false', '❌ no', 'no', '0']:
                            data[field] = 'False'
                        else:
                            data[field] = 'False'  # Default to False for unclear values
                
                # Ensure Google Maps integration status is properly set
                if not data.get('google_maps_integration_status'):
                    if data.get('primary_google_maps_link') or data.get('all_google_maps_links'):
                        data['google_maps_integration_status'] = 'Integrated'
                    else:
                        data['google_maps_integration_status'] = 'Not Found'
                
                all_data.append(data)
                processed_count += 1
                logger.info(f"✅ Successfully processed enhanced domain: {domain}")
                
            except Exception as e:
                logger.error(f"❌ Error processing enhanced domain {domain}: {e}")
                continue
        
        # Write to enhanced CSV
        if all_data:
            self._write_enhanced_csv(all_data, fieldnames)
            logger.info(f"""
🎉 ENHANCED CSV COMPILATION v3.0 COMPLETE!
✅ Successfully processed: {processed_count} domains
📄 Output file: {self.output_file}
📊 Total records: {len(all_data)}
📋 Fields per record: {len(fieldnames)}
🗺️ Google Maps fields: 10 comprehensive integration fields
🔍 Discovery fields: Complete link discovery and website mapping data
🌐 Enhanced fields: Complete business intelligence with comprehensive analysis
""")
        else:
            logger.warning("No enhanced data to write to CSV")
    
    def _write_enhanced_csv(self, data: List[Dict[str, Any]], fieldnames: List[str]):
        """Write enhanced data to CSV file with comprehensive error handling"""
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in data:
                    # Ensure all fields are present and clean
                    csv_row = {}
                    for field in fieldnames:
                        value = row.get(field, '')
                        # Ensure value is string and handle None values
                        if value is None:
                            value = ''
                        else:
                            value = str(value)
                        
                        # Clean CSV-specific characters
                        value = value.replace('\n', ' ').replace('\r', ' ')
                        value = re.sub(r'\s+', ' ', value.strip())
                        
                        csv_row[field] = value
                    
                    writer.writerow(csv_row)
            
            logger.info(f"Enhanced CSV file v3.0 written successfully: {self.output_file}")
            
        except Exception as e:
            logger.error(f"Error writing enhanced CSV file: {e}")
            raise

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def compile_enhanced_summaries_to_csv_v3():
    """
    Main function to compile enhanced summary reports to comprehensive CSV v3.0
    
    This enhanced function:
    1. Scans the 'analyzed' directory for enhanced comprehensive summary report files
    2. Parses each summary report to extract complete business intelligence with Google Maps
    3. Extracts comprehensive discovery statistics (pages, links, depth, complexity)
    4. Compiles complete Google Maps integration data with all detection methods
    5. Compiles all data into a comprehensive CSV file with 45+ fields
    6. Handles enhanced data cleaning and validation with comprehensive error handling
    7. Provides detailed logging and comprehensive error reporting
    """
    logger.info("Starting enhanced CSV compilation process v3.0 with comprehensive Google Maps integration")
    
    try:
        compiler = EnhancedCSVCompilerV3()
        compiler.compile_to_enhanced_csv()
        
        logger.info("Enhanced CSV compilation v3.0 completed successfully")
        
    except Exception as e:
        logger.error(f"Critical error in enhanced CSV compilation v3.0: {e}")
        raise

if __name__ == '__main__':
    compile_enhanced_summaries_to_csv_v3()
