import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import hashlib
from collections import defaultdict
import hashlib

# Global variables to store analytics
word_counts = defaultdict(int)  # For tracking word frequencies
page_lengths = {}  # For tracking page lengths
subdomains = defaultdict(int)  # For tracking subdomains
unique_urls = set()  # For tracking unique URLs
page_checksums = set()  # For duplicate detection

def scraper(url, resp):
    """
    Scrapes a webpage and returns a list of valid URLs found on the page.
    Also processes page content for analytics.
    """
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    """
    Extracts links from the response and processes page content.
    Returns a list of normalized URLs.
    """
    if not resp.raw_response or resp.status != 200:
        return []
    
    try:
        content = resp.raw_response.content.decode('utf-8')
        
        # Process page content for analytics
        process_page_content(url, content)
        
        # Extract and normalize links using regex
        links = []
        href_pattern = re.compile(r'href=[\'"]?([^\'" >]+)')
        for match in href_pattern.finditer(content):
            href = match.group(1)
            if href:
                # Convert relative URLs to absolute URLs
                absolute_url = urljoin(url, href)
                # Remove fragments
                defragmented_url = absolute_url.split('#')[0]
                links.append(defragmented_url)
        
        return links
    
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return []

def process_page_content(url, content):
    """
    Processes page content for analytics:
    - Word count
    - Subdomain tracking
    - Duplicate detection
    """
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', content)
    # Remove scripts and style content
    text = re.sub(r'(?is)<script[^>]*>.*?</script>', ' ', text)
    text = re.sub(r'(?is)<style[^>]*>.*?</style>', ' ', text)
    # Remove special characters and extra whitespace
    text = re.sub(r'[^\w\s]', ' ', text)
    text = ' '.join(text.split())
    
    # Calculate checksum for duplicate detection
    content_hash = hashlib.md5(text.encode()).hexdigest()
    if content_hash in page_checksums:
        return  # Skip duplicate content
    page_checksums.add(content_hash)
    
    # Process words
    words = text.lower().split()
    
    # Update word counts (excluding stopwords)
    for word in words:
        if not is_stopword(word):
            word_counts[word] += 1
    
    # Update page length
    page_lengths[url] = len(words)
    
    # Track subdomains for ics.uci.edu
    parsed_url = urlparse(url)
    if 'ics.uci.edu' in parsed_url.netloc:
        subdomains[parsed_url.netloc] += 1
    
    # Track unique URLs
    unique_urls.add(url)

def is_valid(url):
    """
    Determines if a URL should be crawled.
    Returns True for valid URLs, False otherwise.
    """
    try:
        parsed = urlparse(url)
        
        # Check scheme
        if parsed.scheme not in {'http', 'https'}:
            return False
            
        # Check allowed domains
        allowed_domains = {
            'ics.uci.edu', 'cs.uci.edu', 
            'informatics.uci.edu', 'stat.uci.edu'
        }
        
        if not any(domain in parsed.netloc for domain in allowed_domains):
            return False
            
        # Check file extensions to avoid
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False
            
        return True

    except TypeError:
        print(f"TypeError for {url}")
        return False

def is_stopword(word):
    """
    Checks if a word is a stopword.
    Returns True for stopwords, False otherwise.
    """
    # Load stopwords from file
    try:
        with open('stopwords.txt', 'r') as f:
            stopwords = {line.strip() for line in f}
        return word in stopwords
    except:
        # Fallback to basic stopwords if file not found
        basic_stopwords = {'a', 'an', 'the', 'in', 'on', 'at', 'for', 'to', 'of', 'with'}
        return word in basic_stopwords
