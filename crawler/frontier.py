import os
import shelve
from threading import Thread, RLock
from collections import defaultdict
import time
from urllib.parse import urlparse
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier:
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        
        # Thread safety
        self._lock = RLock()
        
        # Track queues and timing for main domains (e.g., ics.uci.edu, cs.uci.edu)
        self.main_domain_queues = defaultdict(list)  # URLs for each main domain
        self.main_domain_last_access = defaultdict(float)  # Last access time for each main domain
        
        # Global tracking
        self.in_progress = set()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
            
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def get_main_domain(self, url):
        """
        Extract main domain from URL
        e.g., www.ics.uci.edu -> ics.uci.edu
        """
        parsed = urlparse(url)
        domain_parts = parsed.netloc.split('.')
        # Handle domains like ics.uci.edu, cs.uci.edu
        if len(domain_parts) >= 3:
            return '.'.join(domain_parts[-3:])
        return parsed.netloc

    def get_tbd_url(self):
        """Get next URL respecting politeness delay at main domain level"""
        current_time = time.time()
        
        with self._lock:
            # Check each main domain queue
            for main_domain, urls in self.main_domain_queues.items():
                if not urls:
                    continue
                    
                # Enforce 500ms delay for main domain and all its subdomains
                time_since_last = current_time - self.main_domain_last_access[main_domain]
                if time_since_last < 0.5:  # Strict 500ms politeness delay
                    continue
                    
                # Get first non-in-progress URL
                for i, url in enumerate(urls):
                    if url not in self.in_progress:
                        urls.pop(i)
                        self.in_progress.add(url)
                        # Update last access time for the main domain
                        self.main_domain_last_access[main_domain] = current_time
                        self.logger.info(f"Assigning URL {url} from domain {main_domain} (waited {time_since_last:.2f}s)")
                        return url
            
            return None

    def add_url(self, url):
        """Add URL to frontier"""
        if not url:
            return
            
        url = normalize(url)
        if not is_valid(url):
            return
            
        with self._lock:
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                # Add URL to its main domain queue
                main_domain = self.get_main_domain(url)
                self.main_domain_queues[main_domain].append(url)

    def mark_url_complete(self, url):
        """Mark URL as completed"""
        if not url:
            return
            
        with self._lock:
            self.in_progress.discard(url)
            urlhash = get_urlhash(url)
            if urlhash in self.save:
                self.save[urlhash] = (url, True)
                self.save.sync()

    def _parse_save_file(self):
        """Load URLs from save file"""
        with self._lock:
            for url, completed in self.save.values():
                if not completed and is_valid(url):
                    # Add to appropriate main domain queue
                    main_domain = self.get_main_domain(url)
                    self.main_domain_queues[main_domain].append(url)

    def __del__(self):
        self.save.close()
