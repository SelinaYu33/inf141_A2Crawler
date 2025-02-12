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
        self._domain_locks = defaultdict(RLock)
        
        # Domain queues and timing
        self.domain_queues = defaultdict(list)
        self.domain_last_access = defaultdict(float)
        
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

    def get_tbd_url(self):
        """Get next URL respecting politeness delay"""
        current_time = time.time()
        
        with self._lock:
            for domain, urls in self.domain_queues.items():
                if not urls:
                    continue
                    
                with self._domain_locks[domain]:
                    # Strict 500ms politeness delay
                    time_since_last = current_time - self.domain_last_access[domain]
                    if time_since_last < 0.5:
                        continue
                        
                    # Get first non-in-progress URL
                    for i, url in enumerate(urls):
                        if url not in self.in_progress:
                            urls.pop(i)
                            self.in_progress.add(url)
                            self.domain_last_access[domain] = current_time
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
                domain = urlparse(url).netloc
                with self._domain_locks[domain]:
                    self.domain_queues[domain].append(url)

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
                    domain = urlparse(url).netloc
                    with self._domain_locks[domain]:
                        self.domain_queues[domain].append(url)

    def __del__(self):
        self.save.close()
