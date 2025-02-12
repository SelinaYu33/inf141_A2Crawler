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
total_urls_found = 0
total_urls_crawled = 0
urls_per_domain = defaultdict(int)
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

class SimHash:
    """
    Implementation of SimHash algorithm for near-duplicate detection
    """
    def __init__(self, text, hash_bits=64):
        self.hash_bits = hash_bits
        self.hash_value = self._generate_hash(text)

    def _preprocess_text(self, text):
        """
        Process text into features (words) with frequencies
        """
        # Normalize text
        text = text.lower()
        # Remove special characters and extra spaces
        text = re.sub(r'[^\w\s]', ' ', text)
        text = ' '.join(text.split())
        
        # Get word frequencies
        words = text.split()
        word_freq = defaultdict(int)
        for word in words:
            if len(word) > 2:  # Skip very short words
                word_freq[word] += 1
        return word_freq

    def _hash_function(self, word):
        """
        Generate a b-bit hash value for a word using basic hash function
        """
        hash_val = 0
        for char in word.encode('utf-8'):
            hash_val = (hash_val * 31 + char) & ((1 << self.hash_bits) - 1)
        return hash_val

    def _generate_hash(self, text):
        """
        Generate SimHash value for text
        """
        # Step 1: Get features (words) with weights (frequencies)
        features = self._preprocess_text(text)
        
        # Step 3: Initialize b-dimensional vector V
        v = [0] * self.hash_bits
        
        # Process each word
        for word, freq in features.items():
            # Step 2: Generate hash for word
            hash_val = self._hash_function(word)
            
            # Step 3: Update vector V
            for i in range(self.hash_bits):
                bitmask = 1 << i
                if hash_val & bitmask:
                    v[i] += freq  # Add weight if bit is 1
                else:
                    v[i] -= freq  # Subtract weight if bit is 0
        
        # Step 4: Generate final fingerprint
        fingerprint = 0
        for i in range(self.hash_bits):
            if v[i] > 0:
                fingerprint |= 1 << i
        
        return fingerprint

    def distance(self, other):
        """
        Calculate Hamming distance between two SimHash values
        """
        x = self.hash_value ^ other.hash_value
        diff_bits = 0
        while x:
            diff_bits += 1
            x &= (x - 1)  # Clear the least significant bit
        return diff_bits

def save_stats_if_needed():
    """
    Saves statistics to file if enough time has passed since last save
    """
    global last_save_time
    current_time = time.time()
    
    if current_time - last_save_time >= SAVE_INTERVAL:
        with stats_lock:
            stats = get_analytics()
            with open('crawler_progress.txt', 'w') as f:
                f.write(f"""Crawler Progress Report
Time: {time.strftime('%Y-%m-%d %H:%M:%S')}

Overall Progress:
Total URLs Found: {total_urls_found}
Total URLs Crawled: {total_urls_crawled}

Analytics:
Unique Pages: {stats['unique_pages']}
Longest Page: {stats['longest_page'][0]} ({stats['longest_page'][1]} words)

Top 10 Most Common Words:
{format_common_words(stats['most_common_words'][:10])}

Recent Subdomains:
{format_subdomains(list(stats['subdomains'].items())[-5:])}

Notes:
- Traps detected and avoided: {len(url_patterns)}
- Similar pages skipped: {len(content_fingerprints)}
- Robots.txt entries cached: {len(robots_cache)}
""")
            last_save_time = current_time

def format_common_words(words):
    return '\n'.join(f"  {word}: {count}" for word, count in words)

def format_subdomains(subdomains):
    return '\n'.join(f"  {domain}: {count} pages" for domain, count in subdomains)

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
    
    # Handle redirects
    final_url = url
    if resp.status in (301, 302, 303, 307, 308):  # Redirect status codes
        if resp.raw_response and resp.raw_response.url:
            final_url = resp.raw_response.url
            if not is_valid(final_url) or not is_allowed_by_robots(final_url):
                return []
            print(f"Following redirect: {url} -> {final_url}")
    
    links = extract_next_links(final_url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    """
    Processes page content and extracts links.
    """
    if not resp.raw_response:
        return []
    
    # Accept both 200 and redirect status codes
    if resp.status not in (200, 301, 302, 303, 307, 308):
        return []
    
    try:
        # Parse with BeautifulSoup for better HTML handling
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        
        # Check content size
        content_size = len(resp.raw_response.content)
        if content_size > 5 * 1024 * 1024:  # Skip files larger than 5MB
            print(f"Skipping large file: {url} ({content_size} bytes)")
            return []
        
        # Remove script and style elements
        for element in soup(['script', 'style', 'meta', 'link']):
            element.decompose()
            
        # Extract text content
        text = soup.get_text()
        
        # Skip pages with too little content
        if len(text.split()) < 50:  # Skip pages with fewer than 50 words
            print(f"Skipping low content page: {url}")
            return []
        
        # Check for traps and similar content
        if is_trap(url) or is_similar_content(text, url):
            print(f"Detected trap or similar content: {url}")
            return []
        
        # Process page content for analytics with thread safety
        with stats_lock:
            process_content(url, text)
            save_stats_if_needed()
        
        # Extract links
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            if href and not href.startswith(('javascript:', 'mailto:', 'tel:')):
                try:
                    # Convert relative URLs to absolute
                    absolute_url = urljoin(url, href)
                    # Remove fragments and query parameters
                    clean_url = absolute_url.split('#')[0]
                    # Ensure URL is ASCII-only
                    clean_url = clean_url.encode('ascii', errors='ignore').decode()
                    if clean_url:
                        links.append(clean_url)
                except:
                    continue
        
        return links
        
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return []

def process_content(url, text):
    """
    Analyzes page content for statistics tracking.
    """
    global total_urls_crawled
    
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
    
    # Update counters
    total_urls_crawled += 1
    urls_per_domain[parsed_url.netloc] += 1
    
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
        len(set(path.split('/'))) < path.count('/') - 2,
        # Long query strings
        len(parsed.query) > 100,
        # Too many parameters
        parsed.query.count('&') > 5,
        # Infinite redirects
        re.search(r'/(redir|goto|redirect|link)/', path),
        # Session IDs in URL
        re.search(r'(sess|session|sid|uid)=', parsed.query)
    ]):
        print(f"Detected URL trap: {url}")
        return True
        
    return False

def is_similar_content(text, url, threshold=0.1):
    """
    Check if content is too similar to previously seen pages using SimHash
    """
    current_hash = SimHash(text)
    
    # Compare with existing fingerprints
    for stored_url, stored_hash in content_fingerprints:
        if current_hash.distance(stored_hash) < threshold:
            print(f"Similar content detected: {url} is similar to {stored_url}")
            return True
    
    # Add current fingerprint to collection
    content_fingerprints.append((url, current_hash))
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

