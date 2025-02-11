import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from collections import defaultdict
import urllib.robotparser
import math
from threading import Lock
import time

# Analytics tracking with thread safety
stats_lock = Lock()
last_save_time = time.time()
SAVE_INTERVAL = 300  # Save every 5 minutes

# Global statistics tracking
word_frequencies = defaultdict(int)  # Track word frequencies
page_word_counts = {}  # Track page lengths
subdomain_counts = defaultdict(int)  # Track subdomains
unique_page_count = set()  # Track unique URLs

# Trap detection
url_patterns = defaultdict(int)  # Track URL patterns
content_fingerprints = []  # Store document fingerprints
robots_cache = {}  # Cache for robots.txt parsers
visited_urls = set()  # Track already visited URLs

def save_stats_if_needed():
    """
    Saves statistics to file if enough time has passed since last save
    """
    global last_save_time
    current_time = time.time()
    
    if current_time - last_save_time >= SAVE_INTERVAL:
        with stats_lock:
            with open('crawler_progress.txt', 'w') as f:
                f.write(get_analytics())
            last_save_time = current_time

def scraper(url, resp):
    """
    Extracts and returns valid links from a webpage while analyzing content.
    Args:
        url: The URL being scraped
        resp: Response object containing page content
    Returns:
        List of valid URLs found on the page
    """
    # Skip if already visited
    if url in visited_urls:
        return []
    visited_urls.add(url)
    
    if not is_allowed_by_robots(url):
        return []
        
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    """
    Processes page content and extracts links.
    """
    if not resp.raw_response or resp.status != 200:
        return []
    
    try:
        # Parse with BeautifulSoup for better HTML handling
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        
        # Remove script and style elements
        for element in soup(['script', 'style', 'meta', 'link']):
            element.decompose()
            
        # Extract text content
        text = soup.get_text()
        
        # Check for traps and similar content
        if is_trap(url) or is_similar_content(text):
            return []
        
        # Process page content for analytics with thread safety
        with stats_lock:
            process_content(url, text)
        
        # Extract links
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            if href and not href.startswith(('javascript:', 'mailto:', 'tel:')):
                # Convert relative URLs to absolute
                absolute_url = urljoin(url, href)
                # Remove fragments and query parameters
                clean_url = absolute_url.split('#')[0].split('?')[0]
                links.append(clean_url)
        
        return links
        
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return []

def process_content(url, text):
    """
    Analyzes page content for statistics tracking.
    """
    # Clean and normalize text
    text = ' '.join(text.split())
    
    # Skip if page has too little content
    if len(text) < 50:
        return
    
    # Process words
    words = text.lower().split()
    
    # Update word frequencies (excluding stopwords)
    for word in words:
        if len(word) > 2 and not is_stopword(word):
            word_frequencies[word] += 1
    
    # Track page length
    page_word_counts[url] = len(words)
    
    # Track subdomains for ics.uci.edu
    parsed_url = urlparse(url)
    if 'ics.uci.edu' in parsed_url.netloc:
        subdomain_counts[parsed_url.netloc] += 1
    
    # Track unique URLs
    unique_page_count.add(url)
    
    # Save stats periodically
    save_stats_if_needed()

def is_valid(url):
    """
    Determines if a URL should be crawled.
    """
    try:
        parsed = urlparse(url)
        
        # Check scheme
        if parsed.scheme not in {'http', 'https'}:
            return False
            
        # Strict domain checking
        allowed_domains = {
            'ics.uci.edu', 'cs.uci.edu',
            'informatics.uci.edu', 'stat.uci.edu'
        }
        
        domain = parsed.netloc.lower()
        if not any(domain.endswith(d) for d in allowed_domains):
            return False
            
        # File type filtering
        invalid_extensions = {
            # Documents
            'pdf', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx', 'txt', 'rtf',
            'odt', 'ods', 'odp', 'tex', 'ps', 'eps',
            # Images
            'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'ico', 'svg', 'webp',
            # Audio/Video
            'mp3', 'mp4', 'wav', 'avi', 'mov', 'wmv', 'flv', 'mkv', 'webm',
            # Archives
            'zip', 'rar', 'gz', 'tar', '7z', 'bz2', 'xz',
            # Web assets
            'css', 'js', 'json', 'xml', 'rss', 'atom',
            # Other
            'exe', 'dll', 'so', 'dmg', 'iso', 'bin', 'apk',
            'swf', 'woff', 'woff2', 'eot', 'ttf'
        }
        
        # Check file extension
        path = parsed.path.lower()
        if '.' in path:
            ext = path.split('.')[-1]
            if ext in invalid_extensions:
                return False
                
        # Avoid calendar traps
        if re.search(r'/calendar/|/events?/|/archive/', path):
            return False
            
        # Avoid specific problematic paths
        if any(x in path for x in ['/login', '/logout', '/search', '/print/', '/download/']):
            return False
            
        # Avoid URLs that are too long
        if len(url) > 200:
            return False
            
        return True

    except Exception as e:
        print(f"Error validating {url}: {e}")
        return False

def is_stopword(word):
    """
    Checks if a word is a stopword.
    """
    try:
        with open('stopwords.txt', 'r') as f:
            stopwords = {line.strip() for line in f}
        return word in stopwords
    except:
        # Basic stopwords if file not found
        basic_stopwords = {'a', 'an', 'the', 'in', 'on', 'at', 'for', 'to', 'of', 'with'}
        return word in basic_stopwords

def get_analytics():
    """
    Returns current analytics data.
    """
    return {
        'unique_pages': len(unique_page_count),
        'longest_page': max(page_word_counts.items(), key=lambda x: x[1]) if page_word_counts else None,
        'most_common_words': sorted(word_frequencies.items(), key=lambda x: x[1], reverse=True)[:50],
        'subdomains': dict(subdomain_counts)
    }

def is_trap(url):
    """
    Detects URL patterns that might indicate a trap.
    """
    parsed = urlparse(url)
    path = parsed.path.lower()
    
    # Extract pattern by replacing numbers with *
    pattern = re.sub(r'\d+', '*', path)
    
    # Count pattern occurrences
    url_patterns[pattern] += 1
    
    # Check for common trap patterns
    if any([
        # Too many numbers in path
        len(re.findall(r'\d+', path)) > 4,
        # Deep directory structure
        path.count('/') > 6,
        # Same pattern repeated too many times
        url_patterns[pattern] > 50,
        # Calendar-like patterns
        re.search(r'/\d{4}/\d{2}/\d{2}/', path),
        # Repeated directory names
        len(set(path.split('/'))) < path.count('/') - 2
    ]):
        return True
        
    return False

def create_fingerprint(text, k=5):
    """
    Creates a document fingerprint using k-shingles.
    """
    # Create k-shingles (character level)
    shingles = set()
    text = ' '.join(text.split())  # Normalize whitespace
    for i in range(len(text) - k + 1):
        shingles.add(text[i:i+k])
    
    # Convert shingles to binary vector
    vector = []
    for i in range(32):  # Use 32 bits for fingerprint
        # Simple hash function
        hash_val = sum(hash(shingle) * (i + 1) for shingle in shingles)
        vector.append(1 if hash_val % 2 else 0)
    
    return vector

def calculate_similarity(vec1, vec2):
    """
    Calculates cosine similarity between two binary vectors.
    """
    if not vec1 or not vec2:
        return 0
        
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(x * x for x in vec1))
    norm2 = math.sqrt(sum(x * x for x in vec2))
    
    if norm1 == 0 or norm2 == 0:
        return 0
        
    return dot_product / (norm1 * norm2)

def is_similar_content(text, threshold=0.85):
    """
    Checks if content is too similar to previously seen pages.
    """
    # Create fingerprint for current page
    current_fingerprint = create_fingerprint(text)
    
    # Compare with existing fingerprints
    for fp in content_fingerprints:
        if calculate_similarity(current_fingerprint, fp) > threshold:
            return True
    
    # Add current fingerprint to collection
    content_fingerprints.append(current_fingerprint)
    if len(content_fingerprints) > 1000:  # Limit memory usage
        content_fingerprints.pop(0)
    
    return False

def is_allowed_by_robots(url):
    """
    Checks if URL is allowed by robots.txt
    """
    try:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        # Check cache first
        if base_url not in robots_cache:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"{base_url}/robots.txt")
            try:
                rp.read()
                robots_cache[base_url] = rp
            except:
                # If robots.txt can't be read, assume allowed
                return True
        
        return robots_cache[base_url].can_fetch("*", url)
        
    except:
        # In case of any error, assume allowed
        return True

