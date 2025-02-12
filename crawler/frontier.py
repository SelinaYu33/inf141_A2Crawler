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
        
        # Add priority queue for domains
        self.domain_priorities = {}  # Track next available time for each domain
        self.last_domain_check = {}  # Track when we last checked each domain
        
        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        
        # Initialize persistent storage
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
            # Find available domain with shortest wait time
            next_domain = None
            min_wait_time = float('inf')
            
            for domain, urls in self.domain_queues.items():
                if not urls:
                    continue
                    
                # Only check domains we haven't checked recently
                if domain in self.last_domain_check:
                    if current_time - self.last_domain_check[domain] < self.config.time_delay / 2:
                        continue
                
                wait_time = 0
                if domain in self.domain_priorities:
                    wait_time = max(0, self.domain_priorities[domain] - current_time)
                
                if wait_time < min_wait_time:
                    next_domain = domain
                    min_wait_time = wait_time
            
            if not next_domain:
                return None
                
            # Get domain lock for atomic operations
            with self._domain_locks[next_domain]:
                # Double check politeness after getting lock
                current_time = time.time()
                if current_time < self.domain_priorities.get(next_domain, 0):
                    return None
                
                # Get first non-in-progress URL
                urls = self.domain_queues[next_domain]
                for i, url in enumerate(urls):
                    if url not in self.global_in_progress:
                        # Remove URL from queue
                        urls.pop(i)
                        # Mark as in progress
                        self.global_in_progress.add(url)
                        self.domain_in_progress[next_domain].add(url)
                        
                        # Update timing information
                        self.domain_priorities[next_domain] = current_time + self.config.time_delay
                        self.last_domain_check[next_domain] = current_time
                        
                        self.logger.info(
                            f"Assigning URL {url} from domain {next_domain}")
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
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
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
