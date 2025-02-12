import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from collections import defaultdict
import urllib.robotparser
import math
from threading import Lock
import time
import os

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
unique_page_count = set()  # Track unique URLs

# Trap detection
url_patterns = defaultdict(int)  # Track URL patterns
content_fingerprints = []  # Store document fingerprints
robots_cache = {}  # Cache for robots.txt parsers
visited_urls = set()  # Track already visited URLs

# Load stopwords
STOPWORDS = set()
try:
    with open('stopwords.txt', 'r') as f:
        STOPWORDS = set(word.strip().lower() for word in f)
except Exception as e:
    print(f"Warning: Could not load stopwords.txt: {e}")

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



def scraper(url, resp):
    """
    Extracts and returns valid links from a webpage while analyzing content.
    Args:
        url: The URL being scraped
        resp: Response object containing page content
    Returns:
        List of valid URLs found on the page
    """
    clean_url = url.split('#')[0]
    if clean_url in visited_urls:
        return []
    visited_urls.add(clean_url)
    
    if not resp.raw_response:
        return []
    

    if resp.status not in (200, 301, 302, 303, 307, 308):
        return []
    
    try:
        # Parse with BeautifulSoup
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        
        # Remove script and style elements
        for element in soup(['script', 'style', 'meta', 'link']):
            element.decompose()
            
        # Extract text content
        text = soup.get_text()
        
        # Check for traps and similar content
        if is_trap(url) or is_similar_content(text, url):
            return []
        
        # Process page content for analytics
        process_content(url, text)
        
        # Extract links
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            if href and not href.startswith(('javascript:', 'mailto:', 'tel:')):
                try:
                    # Convert relative URLs to absolute
                    absolute_url = urljoin(url, href)
                    # Remove fragments
                    clean_url = absolute_url.split('#')[0]
                    # Ensure URL is ASCII-only
                    clean_url = clean_url.encode('ascii', errors='ignore').decode()
                    if clean_url:
                        links.append(clean_url)
                except:
                    continue
        
        # Check robots.txt before returning links
        return [link for link in links if is_valid(link) and is_allowed_by_robots(link)]
        
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return []

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

def save_stats_if_needed():
    """
    Saves statistics to file if enough time has passed since last save
    """
    global last_save_time
    current_time = time.time()
    
    if current_time - last_save_time >= SAVE_INTERVAL:
        stats = get_analytics()
        with open('crawler_analytics.txt', 'w') as f:
            f.write(f"""Web Crawler Analytics Report
Time: {time.strftime('%Y-%m-%d %H:%M:%S')}

1. Unique Pages: {stats['unique_pages']}
(URL uniqueness determined by ignoring fragments)

2. Longest Page: 
URL: {stats['longest_page'][0]}
Word Count: {stats['longest_page'][1]} words
(HTML markup not counted)

3. Top 50 Most Common Words:
(Excluding stopwords)
{format_word_frequencies(stats['most_common_words'][:50])}

4. ICS Subdomains:
(Sorted alphabetically with unique page counts)
{format_subdomains(stats['subdomains'])}

Additional Statistics:
- Total URLs Found: {total_urls_found}
- Total URLs Crawled: {total_urls_crawled}
- Domains Crawled: {len(urls_per_domain)}
""")
            last_save_time = current_time

def format_word_frequencies(word_freq_list):
    """
    Format word frequencies for output, ensuring ASCII-only content
    """
    output = []
    rank = 1
    for word, freq in word_freq_list:
        if word.isascii() and word.isalnum():
            output.append(f"{rank}. {word}: {freq}")
            rank += 1
    return '\n'.join(output)

def format_subdomains(subdomains):
    """Format subdomains list with full URLs"""
    return '\n'.join(f"{url}, {count}" 
                    for url, count in subdomains.items())

def process_content(url, text):
    """
    Analyzes page content for statistics tracking.
    Ensures proper text encoding and filtering.
    """
    global total_urls_crawled
    print("Processing Content: " + url)
    
    try:
        # Ensure text is properly decoded and normalized
        if isinstance(text, bytes):
            text = text.decode('utf-8', errors='ignore')
        
        # Remove non-ASCII characters and normalize spaces
        text = ''.join(char for char in text if ord(char) < 128)
        text = ' '.join(text.split())
        
        # Process words (only ASCII characters)
        words = [word.lower() for word in text.split() 
                if word.isascii() and len(word) > 2 and not is_stopword(word)]
        
        # Update word frequencies
        with stats_lock:
            for word in words:
                if word.isalnum():  # Only count alphanumeric words
                    word_frequencies[word] += 1
            
            # Track page length
            page_word_counts[url] = len(words)
            
            # Track unique URLs
            unique_page_count.add(url)
            total_urls_crawled += 1
            
            # Update domain statistics
            parsed = urlparse(url)
            urls_per_domain[parsed.netloc] += 1
            
            save_stats_if_needed()
            
    except Exception as e:
        print(f"Error processing content for {url}: {e}")

def get_analytics():
    """
    Get current analytics data
    """
    # Get unique pages (ignoring fragments)
    unique_urls = set()
    for url in unique_page_count:
        base_url = url.split('#')[0]  # Remove fragments
        unique_urls.add(base_url)
    
    # Find longest page
    longest_page = ('', 0)
    for url, count in page_word_counts.items():
        if count > longest_page[1]:
            longest_page = (url, count)
    
    # Get most common words (excluding stopwords)
    word_list = [(word, count) for word, count in word_frequencies.items()]
    word_list.sort(key=lambda x: x[1], reverse=True)
    
    # Get subdomains for ics.uci.edu with full URLs
    ics_subdomains = defaultdict(int)
    for url in unique_urls:
        parsed = urlparse(url)
        if 'ics.uci.edu' in parsed.netloc:
            # Construct base URL with scheme
            subdomain_url = f"{parsed.scheme}://{parsed.netloc}"
            ics_subdomains[subdomain_url] += 1
    
    return {
        'unique_pages': len(unique_urls),
        'longest_page': longest_page,
        'most_common_words': word_list,
        'subdomains': dict(sorted(ics_subdomains.items()))  # Sort alphabetically
    }

def is_trap(url):
    """
    Detects URL patterns that might indicate a trap.
    """
    parsed = urlparse(url)
    path = parsed.path.lower()
    query = parsed.query.lower()
    
    # Extract pattern by replacing numbers with *
    pattern = re.sub(r'\d+', '*', path)
    
    # Count pattern occurrences
    url_patterns[pattern] += 1
    
    # Check for common trap patterns
    if any([
        # Existing checks
        len(re.findall(r'\d+', path)) > 4,
        path.count('/') > 6,
        url_patterns[pattern] > 50,
        re.search(r'/\d{4}/\d{2}/\d{2}/', path),
        len(set(path.split('/'))) < path.count('/') - 2,
        len(parsed.query) > 100,
        parsed.query.count('&') > 5,
        
        # Additional checks
        # Wiki-related traps
        re.search(r'(timeline|history|revisions|diff|changes)', path),
        re.search(r'\?do=(index|revisions|diff|backlink)', query),
        
        # Timestamp-related traps
        re.search(r'from=\d{4}-\d{2}-\d{2}', query),
        re.search(r'precision=(second|minute|hour)', query),
        
        # File download traps
        re.search(r'action=download', query),
        re.search(r'\.(zip|rar|tar|gz)$', path),
        
        # Special directory traps
        re.search(r'/(virus|malware|spam)/', path),
        path.count('wiki') > 1,
        
        # Dynamic parameter traps
        re.search(r'[?&](page|offset|start|limit)=\d+', url),
        re.search(r'[?&](sort|order|filter)=', url),
        
        # Duplicate parameter checks
        len(re.findall(r'do=', query)) > 1,
        len(re.findall(r'from=', query)) > 1
    ]):
        print(f"Detected URL trap: {url}")
        return True
        
    return False

def is_similar_content(text, url, threshold=0.1):
    """
    Check if content is too similar to previously seen pages using SimHash
    """
    # Skip similarity check for faculty pages
    parsed = urlparse(url)
    if '~' in parsed.path:  # Faculty/staff personal pages
        return False
        
    # Skip similarity check for important pages
    important_paths = {
        '/explore/', '/about/', '/faculty/', '/staff/',
        '/research/', '/grad/', '/phd/', '/courses/',
        '/people/', '/contact/', '/news/'
    }
    if any(path in parsed.path.lower() for path in important_paths):
        return False
    
    # Only compare with pages from the same domain
    domain = parsed.netloc
    
    current_hash = SimHash(text)
    similar_count = 0
    
    # Compare with existing fingerprints
    for stored_url, stored_hash in content_fingerprints:
        if urlparse(stored_url).netloc == domain:  # Same domain comparison
            if current_hash.distance(stored_hash) < threshold:
                similar_count += 1
                if similar_count >= 3:  # Require multiple similar pages
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



def is_valid(url):
    """
    Strictly validates URLs against allowed domains and paths.
    Only allows *.ics.uci.edu/*, *.cs.uci.edu/*, *.informatics.uci.edu/*, *.stat.uci.edu/*
    """
    try:
        parsed = urlparse(url)
        
        # Check scheme
        if parsed.scheme not in {'http', 'https'}:
            return False
            
        # Strict domain validation
        domain = parsed.netloc.lower()
        allowed_domains = {
            'ics.uci.edu',
            'cs.uci.edu',
            'informatics.uci.edu',
            'stat.uci.edu'
        }
        
        # Check if domain matches any of the allowed patterns
        is_allowed = False
        for allowed_domain in allowed_domains:
            # Check exact match or subdomain
            if domain == allowed_domain or domain.endswith('.' + allowed_domain):
                is_allowed = True
                break
                
        if not is_allowed:
            return False
            
        # File extension filtering
        invalid_extensions = {
            # Documents
            'pdf', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx', 'txt', 'rtf',
            'odt', 'ods', 'odp', 'tex', 'ps', 'eps', 'ppsx',
            # Images
            'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'ico', 'svg', 'webp',
            # Audio/Video
            'mp3', 'mp4', 'wav', 'avi', 'mov', 'wmv', 'flv', 'mkv', 'webm',
            'mpg', 'mpeg', 'm4v', '3gp', 'ogg', 'ogv', 'MPG',
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
            ext = path.split('.')[-1].lower()
            if ext in invalid_extensions:
                return False
        
        # Filter out resource directories
        if any(x in path.lower() for x in [
            '/images/', '/img/', '/media/', 
            '/video/', '/audio/', '/download/',
            '/css/', '/js/', '/assets/', '/fonts/',
            '/static/', '/uploads/', '/files/'
        ]):
            return False
                
        # Avoid calendar and event traps
        if re.search(r'/calendar/|/events?/|/archive/', path):
            return False
            
        # Avoid specific problematic paths
        if any(x in path for x in [
            '/login', '/logout', '/search', '/print/',
            '/feed', '/rss', '/atom', '/api/', '/ajax/',
            '/cgi-bin/', '/wp-admin/', '/wp-content/',
            '/admin/', '/backup/', '/raw/'
        ]):
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
    Check if a word is a stopword
    """
    return word.lower() in STOPWORDS