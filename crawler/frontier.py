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
        
        # Enhanced thread safety
        self._lock = RLock()
        self._domain_locks = defaultdict(RLock)  # Per-domain locks
        
        # Domain-based queues and timing
        self.domain_queues = defaultdict(list)  # URLs per domain
        self.domain_last_access = defaultdict(float)  # Last access time per domain
        self.domain_in_progress = defaultdict(set)  # In-progress URLs per domain
        
        # Global tracking
        self.global_in_progress = set()  # All in-progress URLs
        
        # Initialize persistent storage
        if restart and os.path.exists(self.config.save_file):
            os.remove(self.config.save_file)
            
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._load_save_file()

    def get_tbd_url(self):
        """Thread-safe method to get next URL respecting politeness delay"""
        current_time = time.time()
        
        with self._lock:
            # Find domain with available URLs and sufficient delay
            for domain, urls in self.domain_queues.items():
                if not urls:
                    continue
                    
                with self._domain_locks[domain]:
                    # Strict politeness check with 500ms delay
                    time_since_last = current_time - self.domain_last_access[domain]
                    if time_since_last < 0.5:  # 500ms fixed delay
                        continue
                    # Get first non-in-progress URL
                    for i, url in enumerate(urls):
                        if url not in self.global_in_progress:
                            # Remove URL from queue
                            urls.pop(i)
                            # Mark as in progress
                            self.global_in_progress.add(url)
                            self.domain_in_progress[domain].add(url)
                            # Update last access time
                            self.domain_last_access[domain] = current_time
                            
                            return url
            
            return None

    def add_url(self, url):
        """Thread-safe method to add URL"""
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
        """Thread-safe method to mark URL as completed"""
        if not url:
            return
            
        domain = urlparse(url).netloc
        
        with self._lock:
            # Remove from tracking sets
            self.global_in_progress.discard(url)
            with self._domain_locks[domain]:
                self.domain_in_progress[domain].discard(url)
            
            # Update persistent storage
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                self.logger.error(f"Completed url {url}, but have not seen it before.")
                return
            
            self.save[urlhash] = (url, True)
            self.save.sync()

    def _load_save_file(self):
        """Load and organize URLs from save file"""
        with self._lock:
            total_urls = len(self.save)
            pending_urls = 0
            
            for url, completed in self.save.values():
                if not completed and is_valid(url):
                    domain = urlparse(url).netloc
                    with self._domain_locks[domain]:
                        self.domain_queues[domain].append(url)
                    pending_urls += 1
                    
            if not self.save:  # Empty save file
                for url in self.config.seed_urls:
                    self.add_url(url)
                    
            self.logger.info(
                f"Found {pending_urls} urls to be downloaded from {total_urls} "
                f"total urls discovered.")

    def __del__(self):
        self.save.close()
