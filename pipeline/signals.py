"""
Signal extraction module for lead enrichment.

Extracts actionable signals from:
- Place Details data (phone, reviews)
- Website analysis (lightweight, no headless browser)

Design Philosophy:
- Observer, not actor (never submit forms, never execute JS)
- Accuracy > perfection (reduce false negatives)
- Cheap, deterministic signals > deep audits
- 1 GET request per website (+ optional HTTP fallback for SSL issues)

Cost Optimization:
- Uses simple HTTP requests, no Lighthouse or Puppeteer
- Single GET request per website extracts all signals
- Timeout aggressively on slow sites (they're likely low quality anyway)
"""

import re
import time
import logging
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timezone
from urllib.parse import urlparse
import requests

logger = logging.getLogger(__name__)

# Website analysis configuration
WEBSITE_TIMEOUT = 10  # Aggressive timeout - slow sites = low quality signal
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Automated scheduling system patterns
# These are THIRD-PARTY scheduling tools that indicate operational maturity.
# For HVAC: Phone + contact forms are the PRIMARY booking mechanisms.
# Automated scheduling is an ops maturity signal, NOT a booking requirement.
#
# Interpretation:
#   has_automated_scheduling = true  → Operationally mature, possibly saturated
#   has_automated_scheduling = false → Manual ops, higher optimization opportunity
AUTOMATED_SCHEDULING_PATTERNS = [
    # Field service management (HVAC-specific)
    r'servicetitan\.com',
    r'housecallpro\.com', 
    r'jobber\.com',
    r'fieldedge\.com',
    r'successware\.com',
    
    # General scheduling tools
    r'calendly\.com',
    r'acuityscheduling\.com',
    r'squareup\.com/appointments',
    r'square\.site',
    r'setmore\.com',
    r'schedulicity\.com',
    r'simplybook\.me',
    r'appointy\.com',
    # Dental / healthcare booking
    r'zocdoc\.com',
    r'nexhealth\.com',
    r'localmed\.com',
    r'solutionreach\.com',
    r'doctible\.com',
    r'patientpop\.com',
    r'lumahealth\.io',
    r'weave\.com',
    r'demandforce\.com',
    r'dentrix\.com',
    r'curvedental\.com',
    r'opendental\.com',
    r'denticon\.com',
    r'carestack\.com',
    r'tab32\.com',
    r'revenuewell\.com',
    r'yapi\.com',
    r'lighthouse360\.com',
    r'pb\.dental',
    r'flexbook\.me',
    r'getweave\.com',
    r'modento\.io',
    r'kleer\.com',
    r'dentalhq\.com',
    
    # Booking platforms (general)
    r'booksy\.com',
    r'vagaro\.com',
    r'mindbody\.com',
    r'mindbodyonline\.com',
    r'jane\.app',
    r'intakeq\.com',
]

# Booking conversion path (dentist-realistic): detect path type from page
BOOKING_CONVERSION_PATH_FULL = [
    r"book\s*now",
    r"book\s*online",
    r"book\s*an?\s*appointment",
    r"schedule\s*now",
    r"schedule\s*online",
    r"appointment\s*request",  # often leads to scheduler
]
BOOKING_CONVERSION_PATH_REQUEST = [
    r"request\s*appointment",
    r"request\s*a\s*visit",
    r"schedule\s*request",
    r"contact\s*us\s*for\s*appointment",
]
BOOKING_CONVERSION_PATH_PHONE_ONLY = [
    r"call\s*to\s*schedule",
    r"phone\s*only",
    r"call\s*for\s*appointment",
]

# =============================================================================
# CONTACT FORM DETECTION (AGENCY-SAFE)
# =============================================================================
# Philosophy: false negatives destroy agency trust.
# Only emit false when we can PROVE absence (extremely rare).
# Default to null (unknown) when uncertain.
#
# Tri-State Semantics:
#   true  = Confidently observed (human can clearly submit a request)
#   null  = Unknown / cannot be determined
#   false = Explicit evidence of absence ONLY (e.g., "phone only")
# =============================================================================

# Strong HTML evidence - if ANY of these exist, has_contact_form = true
CONTACT_FORM_HTML_PATTERNS = [
    # Form elements
    r'<form\b',                          # Form tag (word boundary)
    r'<input[^>]*type=["\']email["\']',  # Email input field
    r'<input[^>]*type=["\']tel["\']',    # Phone input field
    r'<textarea\b',                      # Text area
    
    # Known form plugins (very high confidence)
    r'wpcf7',                            # WordPress Contact Form 7
    r'wpforms',                          # WPForms
    r'elementor-form',                   # Elementor forms
    r'gravity[-_]?forms?',               # Gravity Forms
    r'ninja[-_]?forms?',                 # Ninja Forms
    r'formidable',                       # Formidable Forms
    r'contact[-_]?form[-_]?7',
    r'formspree',
    r'formstack',
    r'jotform',
    r'typeform',
    r'hubspot.*form',
    r'mailchimp.*form',
    r'netlify-form',
    r'data-form',
    r'quote[-_]?form',
]

# Strong CTA text evidence - BUSINESS CRITICAL
# If a human can clearly see lead-capture intent → true
# These patterns indicate the business WANTS inbound contact
#
# RULE: If a human viewing the page would understand they can submit a request,
#       this MUST be true. This overrides lack of <form> tags.
CONTACT_FORM_TEXT_PATTERNS = [
    # ==========================================================================
    # CALLBACK REQUESTS (extremely strong signal for HVAC)
    # ==========================================================================
    r'request\s+a\s+call\s*back',       # "request a call back"
    r'request\s+a\s+callback',          # "request a callback" (one word)
    r'request\s+call\s*back',           # "request callback"
    r'call\s*back\s+(request|form)',    # "callback request", "callback form"
    r'we\'?ll\s+call\s+you',            # "we'll call you"
    r'have\s+us\s+call',                # "have us call"
    r'call\s+you\s+back',               # "call you back"
    
    # ==========================================================================
    # QUOTE / ESTIMATE REQUESTS (very common for HVAC)
    # ==========================================================================
    r'free\s+quote',                    # "free quote"
    r'get\s+(a\s+)?quote',              # "get a quote", "get quote"
    r'request\s+(a\s+)?quote',          # "request a quote"
    r'for\s+a\s+free\s+quote',          # "for a free quote"
    r'free\s+estimate',                 # "free estimate"
    r'get\s+(a\s+)?free\s+estimate',    # "get a free estimate"
    r'request\s+(an?\s+)?estimate',     # "request an estimate"
    r'instant\s+quote',                 # "instant quote"
    r'online\s+quote',                  # "online quote"
    r'quick\s+quote',                   # "quick quote"
    r'no[- ]?obligation\s+quote',       # "no-obligation quote"
    
    # ==========================================================================
    # FORM SUBMISSION LANGUAGE (explicit form reference)
    # ==========================================================================
    r'fill\s+(out|in)\s+(the|our|this|an?)?\s*(online\s+)?form',  # "fill out our online form"
    r'submit\s+(the\s+|your\s+)?(form|request|inquiry)',          # "submit form"
    r'complete\s+(the|this|our)\s+form',                          # "complete the form"
    r'online\s+form',                   # "online form"
    r'contact\s+form',                  # "contact form"
    r'quote\s+form',                    # "quote form"
    r'request\s+form',                  # "request form"
    
    # ==========================================================================
    # SERVICE SCHEDULING
    # ==========================================================================
    r'schedule\s+(a\s+)?service',           # "schedule a service"
    r'schedule\s+(an?\s+)?appointment',     # "schedule an appointment"
    r'schedule\s+(a\s+)?consultation',      # "schedule a consultation"
    r'schedule\s+(your\s+)?visit',          # "schedule your visit"
    r'book\s+(an?\s+)?appointment',         # "book an appointment"
    r'book\s+(a\s+)?service',               # "book a service"
    r'book\s+online',                       # "book online"
    r'request\s+(a\s+)?service',            # "request a service"
    r'request\s+(a\s+)?consultation',       # "request a consultation"
    r'request\s+an?\s+appointment',         # "request an appointment"
    
    # ==========================================================================
    # CONTACT INTENT LANGUAGE
    # ==========================================================================
    r'contact\s+us',                    # "contact us"
    r'get\s+in\s+touch',                # "get in touch"
    r'send\s+(us\s+)?(a\s+)?message',   # "send us a message"
    r'reach\s+out',                     # "reach out"
    r'let\'?s\s+talk',                  # "let's talk"
    r'drop\s+us\s+a\s+line',            # "drop us a line"
    r'inquiry\s+form',                  # "inquiry form"
    r'message\s+us',                    # "message us"
    r'write\s+to\s+us',                 # "write to us"
    r'email\s+us',                      # "email us"
    
    # ==========================================================================
    # HVAC-SPECIFIC CTAs
    # ==========================================================================
    r'call\s+(us\s+)?(today|now|for)',          # "call us today"
    r'call\s+for\s+(immediate|emergency|same[- ]?day)',  # "call for immediate service"
    r'24[/-]?7\s+(service|emergency|available)', # "24/7 service"
    r'speak\s+(to|with)\s+(a|an)\s+\w+',        # "speak with a technician"
    r'talk\s+to\s+(a|an)\s+\w+',                # "talk to a specialist"
    r'free\s+consultation',                     # "free consultation"
    r'free\s+inspection',                       # "free inspection"
]

# Explicit absence evidence - ONLY set false if these are found
# These are rare and indicate the business explicitly does NOT want online contact
CONTACT_FORM_ABSENCE_PATTERNS = [
    r'phone\s+(inquiries?|calls?)\s+only',
    r'call\s+only',
    r'no\s+online\s+(forms?|requests?|inquiries?)',
    r'no\s+email',
    r'phone\s+only\s*[-–—]\s*no\s+(email|online|web)',
]

# Email extraction patterns
EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
MAILTO_PATTERN = r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'

# Trust badge patterns - indicates established business
TRUST_BADGE_PATTERNS = [
    r'bbb\.org',                         # Better Business Bureau
    r'better\s+business\s+bureau',
    r'homeadvisor\.com',
    r'home\s*advisor',
    r'angi\.com',                        # Angi (formerly Angie's List)
    r'angie\'?s?\s*list',
    r'thumbtack\.com',
    r'thumbtack',
    r'yelp\.com/biz',
    r'google\s*reviews?',
    r'facebook\.com/.*reviews',
    r'elite\s+service',
    r'top[- ]?rated',
    r'screened\s+(&|and)\s+approved',
    r'licensed\s+(&|and)\s+insured',
    r'bonded\s+(&|and)\s+insured',
    r'nate[- ]?certified',               # HVAC-specific certification
    r'epa[- ]?certified',
    r'energy\s+star\s+partner',
]


# =============================================================================
# PAID ADVERTISING DETECTION (Budget Signal)
# =============================================================================
# Detecting ad spend = business has budget and is actively investing.
# MVP: boolean + channel identification from HTML scripts/pixels.
#
# Tri-State: true = ad pixel/tag found, false = page loaded & none found, null = unknown
# =============================================================================

PAID_ADS_PATTERNS = {
    "google": [
        r'googleads\.g\.doubleclick\.net',
        r'google_conversion',
        r'gtag\s*\(\s*["\']config["\']',      # gtag('config', 'AW-...')
        r'AW-\d{5,}',                          # Google Ads conversion ID
        r'google[-_]?ads',
        r'adwords',
        r'googlesyndication\.com',
        r'googleadservices\.com',
        r'gads',
    ],
    "meta": [
        r'connect\.facebook\.net.*fbevents',   # Meta Pixel
        r'fbq\s*\(',                           # fbq('track', ...)
        r'facebook[-_]?pixel',
        r'fb[-_]?pixel',
        r'meta[-_]?pixel',
        r'_fbp',                                # Facebook browser cookie
    ],
    "bing": [
        r'bat\.bing\.com',
        r'uetag',                              # Bing UET tag
    ],
    "other": [
        r'adroll\.com',
        r'ads\.linkedin\.com',
        r'snap\.licdn\.com',
        r'ads\.twitter\.com',
        r'analytics\.tiktok\.com',
    ],
}

# =============================================================================
# HIRING / GROWTH DETECTION (Timing Signal)
# =============================================================================
# Active hiring = growth phase = budget available + potential pain points.
# MVP: detect careers/hiring content from website HTML.
#
# Tri-State: true = hiring evidence found, false = page loaded & none, null = unknown
# =============================================================================

HIRING_LINK_PATTERNS = [
    r'href=["\'][^"\']*(/careers|/jobs|/hiring|/join[- ]us|/work[- ]with[- ]us|/employment|/openings)',
    r'href=["\'][^"\']*indeed\.com',
    r'href=["\'][^"\']*glassdoor\.com',
    r'href=["\'][^"\']*linkedin\.com/company/[^"\']+/jobs',
]

HIRING_TEXT_PATTERNS = [
    r'we\'?re\s+hiring',
    r'now\s+hiring',
    r'join\s+our\s+team',
    r'career\s+opportunities',
    r'open\s+positions?',
    r'job\s+openings?',
    r'apply\s+(now|today|here)',
    r'looking\s+for\s+(a|an)\s+(technician|installer|dispatcher|office|sales|marketing|service)',
    r'hiring\s+(a|an)\s+\w+',
    r'positions?\s+available',
    r'employment\s+opportunities',
]

HIRING_ROLE_PATTERNS = {
    "technician": [r'(hvac|service)\s+technic', r'installer', r'field\s+tech'],
    "front_desk": [r'(front\s+desk|receptionist|dispatcher|office\s+admin|customer\s+service\s+rep)'],
    "marketing": [r'marketing\s+(specialist|coordinator|manager)', r'digital\s+market'],
    "sales": [r'sales\s+(rep|representative|manager|associate)', r'estimator', r'account\s+exec'],
    "management": [r'(general|operations?|service)\s+manager', r'supervisor'],
}


def normalize_domain(url: str) -> str:
    """
    Extract and normalize domain from URL.
    
    Strips protocol, www prefix, and trailing paths.
    
    Args:
        url: Full URL string
    
    Returns:
        Normalized domain (e.g., "example.com")
    """
    if not url:
        return ""
    
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split('/')[0]
        
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # Remove port if present
        domain = domain.split(':')[0]
        
        return domain.lower()
    except Exception:
        return ""


def normalize_phone(
    formatted_phone: Optional[str],
    international_phone: Optional[str]
) -> Tuple[bool, Optional[str]]:
    """
    Normalize phone number, preferring international format.
    
    Args:
        formatted_phone: Local formatted phone (e.g., "(408) 555-1234")
        international_phone: International format (e.g., "+1 408-555-1234")
    
    Returns:
        Tuple of (has_phone: bool, normalized_phone: str or None)
    """
    # Prefer international format
    phone = international_phone or formatted_phone
    
    if not phone:
        return False, None
    
    # Basic normalization: keep only digits and leading +
    cleaned = phone.strip()
    
    # If it starts with +, keep it
    if cleaned.startswith('+'):
        prefix = '+'
        digits = re.sub(r'[^\d]', '', cleaned[1:])
        normalized = prefix + digits
    else:
        digits = re.sub(r'[^\d]', '', cleaned)
        # Add +1 for US numbers if 10 digits
        if len(digits) == 10:
            normalized = '+1' + digits
        elif len(digits) == 11 and digits.startswith('1'):
            normalized = '+' + digits
        else:
            normalized = digits
    
    return True, normalized


def calculate_days_since_review(reviews: List[Dict]) -> Optional[int]:
    """
    Calculate days since the most recent review.
    
    Args:
        reviews: List of review dicts from Place Details API
    
    Returns:
        Days since last review, or None if no reviews
    """
    if not reviews:
        return None
    
    latest_timestamp = None
    
    for review in reviews:
        # Google provides 'time' as Unix timestamp
        review_time = review.get('time')
        if review_time:
            if latest_timestamp is None or review_time > latest_timestamp:
                latest_timestamp = review_time
    
    if latest_timestamp is None:
        return None
    
    # Calculate days ago
    review_date = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc)
    now = datetime.now(tz=timezone.utc)
    delta = now - review_date
    
    return delta.days


def _fetch_website_html(url: str, headers: Dict) -> Tuple[Optional[str], int, bool, str]:
    """
    Fetch website HTML with SSL fallback.
    
    If HTTPS fails with SSL error, retries once with HTTP.
    This handles small-business sites with misconfigured certs.
    
    Args:
        url: URL to fetch
        headers: Request headers
    
    Returns:
        Tuple of (html_content, load_time_ms, has_ssl, final_url)
        html_content is None if fetch failed
    """
    # Normalize URL - ensure it has a protocol
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Try HTTPS first
    try:
        start_time = time.time()
        response = requests.get(
            url,
            headers=headers,
            timeout=WEBSITE_TIMEOUT,
            allow_redirects=True
        )
        load_time_ms = int((time.time() - start_time) * 1000)
        
        if response.status_code == 200:
            final_url = response.url
            has_ssl = final_url.startswith('https')
            return response.text, load_time_ms, has_ssl, final_url
        else:
            logger.debug(f"Website returned {response.status_code}: {url}")
            return None, load_time_ms, url.startswith('https'), url
            
    except requests.exceptions.SSLError:
        # SSL verification failed - try HTTP fallback
        logger.debug(f"SSL error for {url}, trying HTTP fallback")
        
        # Only try HTTP fallback if we were on HTTPS
        if url.startswith('https://'):
            http_url = url.replace('https://', 'http://', 1)
            try:
                start_time = time.time()
                response = requests.get(
                    http_url,
                    headers=headers,
                    timeout=WEBSITE_TIMEOUT,
                    allow_redirects=True
                )
                load_time_ms = int((time.time() - start_time) * 1000)
                
                if response.status_code == 200:
                    final_url = response.url
                    # Site works but SSL is broken
                    has_ssl = final_url.startswith('https')
                    return response.text, load_time_ms, has_ssl, final_url
                    
            except requests.exceptions.RequestException:
                pass
        
        # Both HTTPS and HTTP failed
        return None, 0, False, url
        
    except requests.exceptions.Timeout:
        logger.debug(f"Website timeout: {url}")
        return None, WEBSITE_TIMEOUT * 1000, url.startswith('https'), url
        
    except requests.exceptions.ConnectionError:
        logger.debug(f"Connection error for {url}")
        return None, 0, url.startswith('https'), url
        
    except requests.exceptions.RequestException as e:
        logger.debug(f"Request error for {url}: {e}")
        return None, 0, url.startswith('https'), url


def _is_low_quality_html(html: str | None) -> bool:
    """
    Conservative quality gate for initial HTML extraction.

    Returns True when content is missing or likely an unrendered JS shell.
    """
    if html is None:
        return True

    html_lower = html.lower()
    if len(html) < 500:
        return True
    if "<body" not in html_lower:
        return True

    js_shell_markers = [
        "enable javascript",
        "__next_data__",
        "app-root",
        "<noscript>",
        "window.__initial_state__",
    ]
    return any(marker in html_lower for marker in js_shell_markers)


def _extract_emails(html: str) -> List[str]:
    """
    Extract email addresses from HTML content.
    
    Sources:
    - mailto: links
    - Visible email text in HTML
    
    Args:
        html: Raw HTML content
    
    Returns:
        List of unique email addresses found
    """
    emails = set()
    
    # Extract from mailto: links (highest confidence)
    mailto_matches = re.findall(MAILTO_PATTERN, html, re.IGNORECASE)
    emails.update(mailto_matches)
    
    # Extract visible email addresses
    email_matches = re.findall(EMAIL_PATTERN, html)
    for email in email_matches:
        # Filter out common false positives
        email_lower = email.lower()
        if not any(fp in email_lower for fp in [
            'example.com', 'domain.com', 'email.com', 'test.com',
            'yoursite.com', 'website.com', 'company.com',
            '.png', '.jpg', '.gif', '.css', '.js'
        ]):
            emails.add(email)
    
    return list(emails)


def _detect_schema_microdata(html: str, html_lower: str, has_substantial_html: bool) -> Tuple[Optional[bool], Optional[List[str]]]:
    """
    Detect Organization / LocalBusiness schema in ld+json or itemtype (Phase 0).
    
    Returns:
        (has_schema_microdata, schema_types)
        - has_schema_microdata: True if any Organization/LocalBusiness found, False if page analyzed and none, None if unknown
        - schema_types: list of type names e.g. ["Organization", "LocalBusiness"], or None
    """
    import json
    types_found = []
    
    # 1) application/ld+json
    ld_json_blocks = re.findall(
        r'<script[^>]*type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL | re.IGNORECASE
    )
    for block in ld_json_blocks:
        block = block.strip()
        if not block:
            continue
        try:
            data = json.loads(block)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(data, dict):
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        t = item.get("@type")
                        if t:
                            types_found.extend(t if isinstance(t, list) else [t])
            continue
        at_type = data.get("@type")
        if at_type:
            types_found.extend(at_type if isinstance(at_type, list) else [at_type])
    
    # 2) itemtype (microdata) e.g. itemtype="https://schema.org/LocalBusiness"
    itemtype_matches = re.findall(
        r'itemtype\s*=\s*["\'](?:https?:)?//schema\.org/([^"\'>\s]+)["\']',
        html_lower
    )
    types_found.extend(itemtype_matches)
    
    # Normalize: "Organization", "LocalBusiness" (strip URL prefix if any)
    wanted = {"organization", "localbusiness"}
    found_normalized = []
    for t in types_found:
        if not t:
            continue
        name = (t.split("/")[-1] if "/" in t else t).strip().lower()
        if name in wanted and name not in found_normalized:
            found_normalized.append(name)
    
    if found_normalized:
        schema_types = [s.title() for s in found_normalized]
        return True, schema_types
    if has_substantial_html:
        return False, []
    return None, None


# =============================================================================
# PHASE 0.1: Social links, phone/address in HTML
# =============================================================================

# Social platform URL patterns (href or plain URL on page)
SOCIAL_URL_PATTERNS = {
    "facebook": r'facebook\.com',
    "instagram": r'instagram\.com',
    "linkedin": r'linkedin\.com',
    "twitter": r'(?:twitter\.com|x\.com)',
    "yelp": r'yelp\.com',
    "youtube": r'youtube\.com',
    "tiktok": r'tiktok\.com',
    "pinterest": r'pinterest\.com',
}

# US-style phone: (xxx) xxx-xxxx, xxx-xxx-xxxx, xxx.xxx.xxxx, 10+ digits
PHONE_IN_HTML_PATTERNS = [
    r'<a[^>]*href\s*=\s*["\']tel:[^"\']+["\']',  # tel: link
    r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',             # (123) 456-7890
    r'\d{3}[-.\s]\d{3}[-.\s]\d{4}',               # 123-456-7890
]

# Address in HTML: schema.org or simple street pattern
ADDRESS_IN_HTML_PATTERNS = [
    r'streetAddress["\']\s*:\s*["\']',            # ld+json
    r'["\']streetAddress["\']',                    # schema
    r'\d{1,6}\s+[\w\s]+(?:street|st|avenue|ave|blvd|road|rd|drive|dr|way|lane|ln)[\s,.]',  # 123 Main St
    r'suite\s+\d+',                                # Suite 100
]


# LinkedIn company page: linkedin.com/company/slug
LINKEDIN_COMPANY_PATTERN = re.compile(
    r'https?://(?:www\.)?linkedin\.com/company/([a-zA-Z0-9_-]+)',
    re.IGNORECASE
)


def _extract_linkedin_company_url(html: str) -> Optional[str]:
    """
    Extract first LinkedIn company page URL from HTML.
    Returns full URL (https://www.linkedin.com/company/slug) or None.
    """
    if not html:
        return None
    m = LINKEDIN_COMPANY_PATTERN.search(html)
    if m:
        slug = m.group(1)
        return f"https://www.linkedin.com/company/{slug}"
    return None


def _detect_social_links(html_lower: str, has_substantial_html: bool) -> Tuple[Optional[bool], Optional[List[str]]]:
    """
    Detect social profile links in HTML (Phase 0.1).
    Returns (has_social_links, social_platforms).
    """
    found = []
    for platform, pattern in SOCIAL_URL_PATTERNS.items():
        if re.search(pattern, html_lower):
            found.append(platform)
    if found:
        return True, found
    if has_substantial_html:
        return False, []
    return None, None


def _detect_phone_in_html(html: str, html_lower: str, has_substantial_html: bool) -> Optional[bool]:
    """Detect phone number on page (tel: link or US-style number). Phase 0.1."""
    for pattern in PHONE_IN_HTML_PATTERNS:
        if re.search(pattern, html_lower, re.IGNORECASE):
            return True
    if has_substantial_html:
        return False
    return None


def _detect_phone_clickable(html_lower: str, has_substantial_html: bool) -> Optional[bool]:
    if re.search(r'href\s*=\s*["\']tel:[^"\']+["\']', html_lower, re.IGNORECASE):
        return True
    if has_substantial_html:
        return False
    return None


def _count_cta_elements(html_lower: str) -> int:
    """
    Count approximate CTA elements by text and class/id markers.
    Deterministic, lightweight proxy (no DOM execution).
    """
    text_patterns = [
        r'>\s*book(?:\s+now|\s+online|\s+appointment)?\s*<',
        r'>\s*schedule(?:\s+now|\s+online|\s+appointment)?\s*<',
        r'>\s*contact(?:\s+us)?\s*<',
        r'>\s*call(?:\s+now|\s+us)?\s*<',
        r'>\s*request(?:\s+appointment|\s+quote|\s+service)?\s*<',
    ]
    attr_patterns = [
        r'class\s*=\s*["\'][^"\']*(?:cta|button|btn|appointment|book)[^"\']*["\']',
        r'id\s*=\s*["\'][^"\']*(?:cta|button|btn|appointment|book)[^"\']*["\']',
    ]
    count = 0
    for p in text_patterns + attr_patterns:
        count += len(re.findall(p, html_lower, re.IGNORECASE))
    return min(count, 100)


def _detect_form_step_type(html_lower: str, has_substantial_html: bool) -> str:
    if not has_substantial_html:
        return "unknown"
    # Heuristic: explicit step labels / wizard indicators imply multi-step.
    if re.search(r'(multi[- ]?step|step\s*1|step\s*2|next\s*step|progress[- ]?bar|wizard)', html_lower, re.IGNORECASE):
        return "multi_step"
    if "<form" in html_lower:
        return "single_step"
    return "unknown"


def _detect_address_in_html(html_lower: str, has_substantial_html: bool) -> Optional[bool]:
    """Detect physical address on page (schema or street pattern). Phase 0.1."""
    for pattern in ADDRESS_IN_HTML_PATTERNS:
        if re.search(pattern, html_lower):
            return True
    if has_substantial_html:
        return False
    return None


def _analyze_html_content(html: str) -> Dict:
    """
    Analyze HTML content for signals using AGENCY-SAFE tri-state semantics.
    
    CRITICAL: False negatives destroy agency trust.
    
    Tri-State Signal Semantics:
    - true  = Confidently observed (human can clearly see it)
    - null  = Unknown / cannot be determined (DEFAULT for uncertainty)
    - false = Explicit evidence of ABSENCE only (extremely rare)
    
    Contact Form Logic (AGENCY-SAFE):
    - true  → Form HTML OR strong CTA text found
    - null  → Cannot determine (no evidence either way)
    - false → ONLY if explicit absence (e.g., "phone only, no forms")
    
    Email Logic:
    - true  → Email found in HTML
    - null  → No email found (but may exist elsewhere)
    - NEVER false (email may exist on other pages, Google listing, etc.)
    
    Args:
        html: HTML content
    
    Returns:
        Dictionary with tri-state signals
    """
    html_lower = html.lower()
    
    # Check if we have substantial HTML content
    has_substantial_html = len(html_lower) > 500 and '<body' in html_lower
    
    # =========================================================================
    # MOBILE-FRIENDLY: Viewport meta tag detection
    # =========================================================================
    has_viewport = bool(
        re.search(r'<meta[^>]*name=["\']viewport["\']', html_lower) or
        re.search(r'<meta[^>]*viewport[^>]*width\s*=\s*device-width', html_lower) or
        re.search(r'<meta[^>]*content=[^>]*width\s*=\s*device-width', html_lower)
    )
    mobile_friendly = has_viewport  # true/false based on tag presence
    
    # =========================================================================
    # CONTACT FORM: Agency-safe detection
    # =========================================================================
    # Check for strong HTML evidence
    has_form_html = any(
        re.search(pattern, html_lower, re.IGNORECASE)
        for pattern in CONTACT_FORM_HTML_PATTERNS
    )
    
    # Check for strong CTA text evidence
    # If a human can clearly see lead-capture intent → true
    has_form_text = any(
        re.search(pattern, html_lower, re.IGNORECASE)
        for pattern in CONTACT_FORM_TEXT_PATTERNS
    )
    
    # Check for explicit absence evidence (very rare)
    has_explicit_absence = any(
        re.search(pattern, html_lower, re.IGNORECASE)
        for pattern in CONTACT_FORM_ABSENCE_PATTERNS
    )
    
    # Determine has_contact_form with AGENCY-SAFE logic
    if has_form_html or has_form_text:
        # Confidently observed - human can clearly submit a request
        has_contact_form = True
    elif has_explicit_absence:
        # Explicit evidence of absence - defensible to reviewer
        has_contact_form = False
    else:
        # Unknown - could be JS-rendered, iframe, multi-page flow
        # DEFAULT TO NULL, NOT FALSE
        has_contact_form = None
    
    # =========================================================================
    # EMAIL: Extract and signal
    # =========================================================================
    emails = _extract_emails(html)
    
    if emails:
        has_email = True
        email_address = emails[0]  # Return first found (usually primary)
    else:
        # No email found, but may exist elsewhere (Google listing, other pages)
        # NEVER set false - email may exist, we just can't see it
        has_email = None
        email_address = None
    
    # =========================================================================
    # AUTOMATED SCHEDULING: Online booking detection
    # =========================================================================
    has_scheduling_platform = any(
        re.search(pattern, html_lower, re.IGNORECASE)
        for pattern in AUTOMATED_SCHEDULING_PATTERNS
    )

    # Detect booking CTAs and links to booking subdomains/pages
    has_full_cta = any(re.search(p, html_lower) for p in BOOKING_CONVERSION_PATH_FULL) if has_substantial_html else False
    has_request_cta = any(re.search(p, html_lower) for p in BOOKING_CONVERSION_PATH_REQUEST) if has_substantial_html else False
    has_phone_only_cta = any(re.search(p, html_lower) for p in BOOKING_CONVERSION_PATH_PHONE_ONLY) if has_substantial_html else False

    # Check for booking links to subdomains (book.*, schedule.*, appointment.*)
    has_booking_subdomain_link = False
    if has_substantial_html:
        booking_link_patterns = [
            r'href\s*=\s*["\']https?://book\.',
            r'href\s*=\s*["\']https?://schedule\.',
            r'href\s*=\s*["\']https?://appointment\.',
            r'href\s*=\s*["\']https?://booking\.',
            r'href\s*=\s*["\']https?://app\.[^"\']*(?:book|schedul|appoint)',
        ]
        has_booking_subdomain_link = any(
            re.search(p, html_lower, re.IGNORECASE)
            for p in booking_link_patterns
        )

    # Online booking = recognized platform OR booking CTA with link evidence
    has_automated_scheduling: bool | None
    if has_scheduling_platform:
        has_automated_scheduling = True
    elif has_full_cta and has_booking_subdomain_link:
        has_automated_scheduling = True
    elif has_full_cta:
        has_automated_scheduling = True
    elif has_substantial_html:
        has_automated_scheduling = False
    else:
        has_automated_scheduling = None

    # =========================================================================
    # BOOKING CONVERSION PATH (dentist-realistic: Phone-only | Request form | Online booking limited/full)
    # =========================================================================
    booking_conversion_path = None
    if has_substantial_html:
        if has_automated_scheduling and has_full_cta:
            booking_conversion_path = "Online booking (full)"
        elif has_automated_scheduling:
            booking_conversion_path = "Online booking (limited)"
        elif has_request_cta or (has_contact_form and not has_automated_scheduling):
            booking_conversion_path = "Request form"
        elif has_phone_only_cta or (not has_contact_form and not has_automated_scheduling):
            booking_conversion_path = "Phone-only"
    
    # =========================================================================
    # TRUST BADGES: Established business indicator
    # =========================================================================
    has_badge_evidence = any(
        re.search(pattern, html_lower, re.IGNORECASE)
        for pattern in TRUST_BADGE_PATTERNS
    )
    
    if has_badge_evidence:
        has_trust_badges = True
    elif has_substantial_html:
        has_trust_badges = False
    else:
        has_trust_badges = None
    
    # =========================================================================
    # PAID ADVERTISING: Budget signal
    # =========================================================================
    detected_ad_channels = []
    for channel, patterns in PAID_ADS_PATTERNS.items():
        if any(re.search(p, html_lower, re.IGNORECASE) for p in patterns):
            detected_ad_channels.append(channel)
    
    if detected_ad_channels:
        runs_paid_ads = True
    elif has_substantial_html:
        runs_paid_ads = False
    else:
        runs_paid_ads = None
    
    # =========================================================================
    # HIRING / GROWTH: Timing signal
    # =========================================================================
    has_hiring_links = any(
        re.search(p, html_lower, re.IGNORECASE) for p in HIRING_LINK_PATTERNS
    )
    has_hiring_text = any(
        re.search(p, html_lower, re.IGNORECASE) for p in HIRING_TEXT_PATTERNS
    )
    # Link-only rule: href to /careers or /jobs or /hiring → hiring_active = True (no second fetch)
    if has_hiring_links or has_hiring_text:
        hiring_active = True
        hiring_signal_source = "careers_link_only" if (has_hiring_links and not has_hiring_text) else None
    elif has_substantial_html:
        hiring_active = False
        hiring_signal_source = None
    else:
        hiring_active = None
        hiring_signal_source = None
    
    # Detect specific roles being hired (only when we have hiring text to scan)
    detected_roles = []
    if hiring_active:
        for role, patterns in HIRING_ROLE_PATTERNS.items():
            if any(re.search(p, html_lower, re.IGNORECASE) for p in patterns):
                detected_roles.append(role)
    
    # =========================================================================
    # SCHEMA / MICRODATA: Organization, LocalBusiness (Phase 0)
    # =========================================================================
    has_schema_microdata, schema_types = _detect_schema_microdata(html, html_lower, has_substantial_html)
    
    # =========================================================================
    # PHASE 0.1: Social links, phone/address in HTML, LinkedIn company
    # =========================================================================
    has_social_links, social_platforms = _detect_social_links(html_lower, has_substantial_html)
    has_phone_in_html = _detect_phone_in_html(html, html_lower, has_substantial_html)
    phone_clickable = _detect_phone_clickable(html_lower, has_substantial_html)
    cta_count = _count_cta_elements(html_lower) if has_substantial_html else 0
    form_step_type = _detect_form_step_type(html_lower, has_substantial_html)
    has_address_in_html = _detect_address_in_html(html_lower, has_substantial_html)
    linkedin_company_url = _extract_linkedin_company_url(html)
    
    return {
        "mobile_friendly": mobile_friendly,
        "has_contact_form": has_contact_form,
        "has_email": has_email,
        "email_address": email_address,
        "has_automated_scheduling": has_automated_scheduling,
        "booking_conversion_path": booking_conversion_path,
        "has_trust_badges": has_trust_badges,
        # New signal families
        "runs_paid_ads": runs_paid_ads,
        "paid_ads_channels": detected_ad_channels if detected_ad_channels else None,
        "hiring_active": hiring_active,
        "hiring_roles": detected_roles if detected_roles else None,
        "hiring_signal_source": hiring_signal_source,
        "has_schema_microdata": has_schema_microdata,
        "schema_types": schema_types,
        # Phase 0.1
        "has_social_links": has_social_links,
        "social_platforms": social_platforms if social_platforms else None,
        "has_phone_in_html": has_phone_in_html,
        "phone_clickable": phone_clickable,
        "cta_count": cta_count,
        "form_single_or_multi_step": form_step_type,
        "has_address_in_html": has_address_in_html,
        "linkedin_company_url": linkedin_company_url,
        "_has_substantial_html": has_substantial_html,
    }


def analyze_website(url: str) -> Dict:
    """
    Perform lightweight website analysis with tri-state signal semantics.
    
    Tri-State Signal Semantics:
    - true  = Confidently observed (evidence found)
    - false = Confidently absent (page analyzed, no evidence)
    - null  = Unknown / not determinable (page inaccessible, JS-rendered, etc.)
    
    Design:
    - Primary HTTP GET (+ 1 retry on SSL failure via HTTP)
    - One guarded headless retry only when extraction quality is low
      or critical signals are all inconclusive
    - Unknown ≠ False (epistemically honest)
    - SSL errors don't block analysis - we try HTTP fallback
    
    Args:
        url: Website URL to analyze
    
    Returns:
        Dictionary of website signals with tri-state values
    """
    # Initialize signals with NULL defaults (unknown state)
    # Only set true/false when we have confident evidence
    # AGENCY-SAFE: Never default to false for contact form or email
    signals = {
        "has_website": True,
        "website_url": url,
        "domain": normalize_domain(url),
        "has_ssl": None,               # Unknown until we try to connect
        "mobile_friendly": None,       # Unknown until HTML analyzed
        "has_contact_form": None,      # Unknown until HTML analyzed (NEVER default false)
        "has_email": None,             # Unknown until HTML analyzed (NEVER false)
        "email_address": None,
        "has_automated_scheduling": None,
        "booking_conversion_path": None,
        "has_trust_badges": None,
        "page_load_time_ms": None,
        "website_accessible": None,
        # New signal families
        "runs_paid_ads": None,
        "paid_ads_channels": None,
        "hiring_active": None,
        "hiring_roles": None,
        "hiring_signal_source": None,
        "has_schema_microdata": None,
        "schema_types": None,
        "has_social_links": None,
        "social_platforms": None,
        "has_phone_in_html": None,
        "phone_clickable": None,
        "cta_count": 0,
        "form_single_or_multi_step": "unknown",
        "has_address_in_html": None,
        "linkedin_company_url": None,
        "extraction_method": "http",
        "extraction_retry_count": 0,
        "extraction_notes": None,
    }
    
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    
    # Fetch HTML with SSL fallback
    html, load_time_ms, has_ssl, final_url = _fetch_website_html(url, headers)
    
    # Update connection-based signals (these we can determine from the attempt)
    signals["page_load_time_ms"] = load_time_ms if load_time_ms > 0 else None
    signals["has_ssl"] = has_ssl  # This is determined from the connection
    signals["website_url"] = final_url

    # Primary analysis (if HTML is present).
    content_signals = None
    if html:
        signals["website_accessible"] = True
        content_signals = _analyze_html_content(html)
        signals["mobile_friendly"] = content_signals["mobile_friendly"]
        signals["has_contact_form"] = content_signals["has_contact_form"]
        signals["has_email"] = content_signals["has_email"]
        signals["email_address"] = content_signals["email_address"]
        signals["has_automated_scheduling"] = content_signals["has_automated_scheduling"]
        signals["booking_conversion_path"] = content_signals.get("booking_conversion_path")
        signals["has_trust_badges"] = content_signals["has_trust_badges"]
        signals["runs_paid_ads"] = content_signals["runs_paid_ads"]
        signals["paid_ads_channels"] = content_signals["paid_ads_channels"]
        signals["hiring_active"] = content_signals["hiring_active"]
        signals["hiring_roles"] = content_signals["hiring_roles"]
        signals["hiring_signal_source"] = content_signals.get("hiring_signal_source")
        signals["has_schema_microdata"] = content_signals["has_schema_microdata"]
        signals["schema_types"] = content_signals["schema_types"]
        signals["has_social_links"] = content_signals["has_social_links"]
        signals["social_platforms"] = content_signals["social_platforms"]
        signals["has_phone_in_html"] = content_signals["has_phone_in_html"]
        signals["phone_clickable"] = content_signals["phone_clickable"]
        signals["cta_count"] = content_signals["cta_count"]
        signals["form_single_or_multi_step"] = content_signals["form_single_or_multi_step"]
        signals["has_address_in_html"] = content_signals["has_address_in_html"]
        signals["linkedin_company_url"] = content_signals["linkedin_company_url"]
    else:
        # Primary fetch failed; content signals remain unknown (None defaults).
        signals["website_accessible"] = False

    # Recovery ladder: headless retry only when extraction quality is weak.
    critical_fields = [
        "has_contact_form",
        "has_automated_scheduling",
        "runs_paid_ads",
        "hiring_active",
    ]
    low_quality_html = _is_low_quality_html(html)
    critical_all_none = all(signals.get(field) is None for field in critical_fields)
    should_try_headless = low_quality_html or critical_all_none

    if should_try_headless:
        try:
            from pipeline.headless_browser import render_page

            rendered_html, rendered_load_ms = render_page(final_url)
            if rendered_html and not _is_low_quality_html(rendered_html):
                rendered_signals = _analyze_html_content(rendered_html)
                signals["website_accessible"] = True
                signals["mobile_friendly"] = rendered_signals["mobile_friendly"]
                signals["has_contact_form"] = rendered_signals["has_contact_form"]
                signals["has_email"] = rendered_signals["has_email"]
                signals["email_address"] = rendered_signals["email_address"]
                signals["has_automated_scheduling"] = rendered_signals["has_automated_scheduling"]
                signals["booking_conversion_path"] = rendered_signals.get("booking_conversion_path")
                signals["has_trust_badges"] = rendered_signals["has_trust_badges"]
                signals["runs_paid_ads"] = rendered_signals["runs_paid_ads"]
                signals["paid_ads_channels"] = rendered_signals["paid_ads_channels"]
                signals["hiring_active"] = rendered_signals["hiring_active"]
                signals["hiring_roles"] = rendered_signals["hiring_roles"]
                signals["hiring_signal_source"] = rendered_signals.get("hiring_signal_source")
                signals["has_schema_microdata"] = rendered_signals["has_schema_microdata"]
                signals["schema_types"] = rendered_signals["schema_types"]
                signals["has_social_links"] = rendered_signals["has_social_links"]
                signals["social_platforms"] = rendered_signals["social_platforms"]
                signals["has_phone_in_html"] = rendered_signals["has_phone_in_html"]
                signals["phone_clickable"] = rendered_signals["phone_clickable"]
                signals["cta_count"] = rendered_signals["cta_count"]
                signals["form_single_or_multi_step"] = rendered_signals["form_single_or_multi_step"]
                signals["has_address_in_html"] = rendered_signals["has_address_in_html"]
                signals["linkedin_company_url"] = rendered_signals["linkedin_company_url"]

                if signals["page_load_time_ms"] is None and rendered_load_ms > 0:
                    signals["page_load_time_ms"] = rendered_load_ms
                signals["extraction_method"] = "headless"
                signals["extraction_retry_count"] = 1
                signals["extraction_notes"] = "Recovered via headless render"
            else:
                signals["extraction_notes"] = "Headless fallback failed"
        except Exception as exc:
            logger.debug("Headless fallback skipped for %s: %s", final_url, exc)
            signals["extraction_notes"] = "Headless fallback failed"
    
    return signals


def _calculate_review_trends(reviews: List[Dict], review_count: int) -> Dict:
    """
    Calculate review direction signals from available review data.
    
    Uses the up to 5 reviews returned by Google Places to estimate:
    - review_velocity_30d: approximate reviews per 30 days
    - rating_delta_60d: rating trend (positive = improving, negative = declining)
    
    These are estimates from limited data. Accuracy > completeness.
    
    Args:
        reviews: List of review dicts from Place Details API
        review_count: Total review count from Google
    
    Returns:
        Dictionary with trend signals
    """
    now = datetime.now(tz=timezone.utc)
    
    if not reviews:
        return {
            "review_velocity_30d": None,
            "rating_delta_60d": None,
        }
    
    # Parse review timestamps and ratings
    parsed_reviews = []
    for review in reviews:
        ts = review.get("time")
        rating = review.get("rating")
        if ts and rating is not None:
            review_date = datetime.fromtimestamp(ts, tz=timezone.utc)
            days_ago = (now - review_date).days
            parsed_reviews.append({"days_ago": days_ago, "rating": rating})
    
    if not parsed_reviews:
        return {
            "review_velocity_30d": None,
            "rating_delta_60d": None,
        }
    
    # --- Review Velocity (30d) ---
    # Count reviews in last 30 days from available sample
    # This is a lower-bound estimate (Google returns "most relevant", not all)
    recent_30d = sum(1 for r in parsed_reviews if r["days_ago"] <= 30)
    review_velocity_30d = recent_30d
    
    # --- Rating Delta (60d) ---
    # Compare average rating of reviews in last 60 days vs older
    recent_60d = [r for r in parsed_reviews if r["days_ago"] <= 60]
    older = [r for r in parsed_reviews if r["days_ago"] > 60]
    
    if recent_60d and older:
        recent_avg = sum(r["rating"] for r in recent_60d) / len(recent_60d)
        older_avg = sum(r["rating"] for r in older) / len(older)
        rating_delta_60d = round(recent_avg - older_avg, 2)
    else:
        # Not enough data to compute trend
        rating_delta_60d = None
    
    return {
        "review_velocity_30d": review_velocity_30d,
        "rating_delta_60d": rating_delta_60d,
    }


def extract_signals(lead: Dict) -> Dict:
    """
    Extract all signals from an enriched lead.
    
    Combines Place Details data and website analysis into
    a structured LeadSignals object.
    
    Args:
        lead: Lead dict with '_place_details' from enrichment
    
    Returns:
        LeadSignals dictionary
    """
    # Get Place Details enrichment data
    details = lead.get("_place_details", {})
    
    # Extract phone signals
    has_phone, phone_number = normalize_phone(
        details.get("formatted_phone_number"),
        details.get("international_phone_number")
    )
    
    # Extract review signals
    reviews = details.get("reviews", [])
    urt = lead.get("user_ratings_total")
    review_count = int(urt) if urt is not None and urt != 0 else len(reviews)
    rating = lead.get("rating")
    last_review_days_ago = calculate_days_since_review(reviews)
    
    # Extract website signals
    website_url = details.get("website")
    
    if website_url:
        website_signals = analyze_website(website_url)
    else:
        # No website listed in Place Details
        # has_website = False (we KNOW they don't have one listed)
        # Other signals = null (unknown - we can't analyze what doesn't exist)
        # 
        # AGENCY-SAFE Tri-state semantics:
        # - has_website: false (confidently absent from Google listing)
        # - Other signals: null (unknown, not determinable, NEVER false)
        website_signals = {
            "has_website": False,        # Confidently absent from listing
            "website_url": None,
            "domain": None,
            "has_ssl": None,             # Unknown
            "mobile_friendly": None,     # Unknown
            "has_contact_form": None,    # Unknown (NEVER false)
            "has_email": None,           # Unknown (NEVER false)
            "email_address": None,
            "has_automated_scheduling": None,
            "booking_conversion_path": None,
            "has_trust_badges": None,
            "page_load_time_ms": None,
            "website_accessible": None,
            "runs_paid_ads": None,
            "paid_ads_channels": None,
            "hiring_active": None,
            "hiring_roles": None,
            "hiring_signal_source": None,
            "has_schema_microdata": None,
            "schema_types": None,
            "has_social_links": None,
            "social_platforms": None,
            "has_phone_in_html": None,
            "phone_clickable": None,
            "cta_count": 0,
            "form_single_or_multi_step": "unknown",
            "has_address_in_html": None,
            "linkedin_company_url": None,
        }
    
    # Calculate review trends from available review data
    review_trends = _calculate_review_trends(reviews, review_count)
    
    # Review context: summary + themes from review text (optional LLM)
    from pipeline.review_context import build_review_context
    review_context = build_review_context(reviews, rating=rating, review_count=review_count)
    
    # Build final signals object with AGENCY-SAFE TRI-STATE SEMANTICS
    # 
    # Tri-State Values:
    #   true  = Confidently observed (evidence found)
    #   null  = Unknown / not determinable (DEFAULT for uncertainty)
    #   false = Confidently absent (RARE - only when defensible)
    #
    # AGENCY-SAFE Rules:
    # - has_contact_form: NEVER default to false (destroys trust)
    #   * true  = Form HTML or strong CTA text found
    #   * null  = Unknown (JS-rendered, iframe, etc.)
    #   * false = ONLY explicit absence ("phone only", etc.)
    # - has_email: NEVER false (email may exist elsewhere)
    #   * true  = Email found in HTML
    #   * null  = Not found (but may exist on Google listing, etc.)
    # - has_automated_scheduling: Can be false (scheduling tools are explicit)
    #   * true  = Scheduling platform detected
    #   * false = Page analyzed, none found (manual ops = opportunity)
    #   * null  = Page not analyzable
    #
    signals = {
        "place_id": lead.get("place_id"),
        
        # Phone signals - PRIMARY booking mechanism for HVAC
        "has_phone": has_phone,
        "phone_number": phone_number,
        
        # Website signals
        "has_website": website_signals["has_website"],
        "website_url": website_signals["website_url"],
        "domain": website_signals["domain"],
        "has_ssl": website_signals["has_ssl"],
        "mobile_friendly": website_signals["mobile_friendly"],
        
        # Inbound readiness - AGENCY-SAFE (never default false)
        "has_contact_form": website_signals["has_contact_form"],
        
        # Email reachability - NEVER false
        "has_email": website_signals["has_email"],
        "email_address": website_signals["email_address"],
        
        # Operational maturity - can be false (explicit signal)
        "has_automated_scheduling": website_signals["has_automated_scheduling"],
        "booking_conversion_path": website_signals.get("booking_conversion_path"),
        
        # Trust/reputation signals
        "has_trust_badges": website_signals["has_trust_badges"],
        
        "page_load_time_ms": website_signals["page_load_time_ms"],
        "website_accessible": website_signals["website_accessible"],
        
        # Paid advertising signals (BUDGET indicator)
        "runs_paid_ads": website_signals["runs_paid_ads"],
        "paid_ads_channels": website_signals["paid_ads_channels"],
        
        # Hiring/growth signals (TIMING indicator)
        "hiring_active": website_signals["hiring_active"],
        "hiring_roles": website_signals["hiring_roles"],
        "hiring_signal_source": website_signals.get("hiring_signal_source"),
        
        # Schema/microdata (Phase 0)
        "has_schema_microdata": website_signals.get("has_schema_microdata"),
        "schema_types": website_signals.get("schema_types"),
        # Phase 0.1: social, phone/address in HTML
        "has_social_links": website_signals.get("has_social_links"),
        "social_platforms": website_signals.get("social_platforms"),
        "has_phone_in_html": website_signals.get("has_phone_in_html"),
        "has_address_in_html": website_signals.get("has_address_in_html"),
        "linkedin_company_url": website_signals.get("linkedin_company_url"),
        
        # Review signals - business activity indicator
        "rating": rating,
        "review_count": review_count,
        "last_review_days_ago": last_review_days_ago,
        
        # Review direction signals (PAIN indicator)
        "review_velocity_30d": review_trends["review_velocity_30d"],
        "rating_delta_60d": review_trends["rating_delta_60d"],
        # Review context (summary + themes from text)
        "review_summary_text": review_context.get("review_summary"),
        "review_themes": review_context.get("review_themes") or [],
        "review_sample_snippets": review_context.get("review_sample_snippets") or [],
        "review_sample_size": review_context.get("review_sample_size") or 0,
        "review_service_mentions": review_context.get("service_mentions") or {},
        "review_complaint_themes": review_context.get("complaint_themes") or {},
        "review_intelligence": review_context,
        # Observable conversion-structure details
        "phone_clickable": website_signals.get("phone_clickable"),
        "cta_count": website_signals.get("cta_count"),
        "form_single_or_multi_step": website_signals.get("form_single_or_multi_step"),
    }
    
    return signals


def extract_signals_batch(
    leads: List[Dict],
    progress_interval: int = 10
) -> List[Dict]:
    """
    Extract signals from multiple leads.
    
    Args:
        leads: List of enriched lead dictionaries
        progress_interval: Log progress every N leads
    
    Returns:
        List of LeadSignals dictionaries
    """
    signals_list = []
    total = len(leads)
    websites_analyzed = 0
    
    for i, lead in enumerate(leads, 1):
        signals = extract_signals(lead)
        signals_list.append(signals)
        
        if signals["has_website"]:
            websites_analyzed += 1
        
        if i % progress_interval == 0:
            logger.info(
                f"Extracted signals for {i}/{total} leads "
                f"({websites_analyzed} websites analyzed)"
            )
    
    logger.info(
        f"Signal extraction complete: {total} leads, "
        f"{websites_analyzed} websites analyzed"
    )
    
    return signals_list


def merge_signals_into_lead(lead: Dict, signals: Dict) -> Dict:
    """
    Merge extracted signals back into the lead record.
    
    Creates a flat structure suitable for database storage.
    
    Args:
        lead: Original lead dictionary
        signals: Extracted signals dictionary
    
    Returns:
        Lead dictionary with signals merged in
    """
    merged = lead.copy()
    
    # Remove internal enrichment data
    if "_place_details" in merged:
        del merged["_place_details"]
    
    # Add signal fields with 'signal_' prefix to avoid conflicts
    for key, value in signals.items():
        if key != "place_id":  # Don't duplicate place_id
            merged[f"signal_{key}"] = value
    
    return merged
