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
        self._domain_locks = defaultdict(RLock)
        
        # Domain-based queues for politeness
        self.domain_queues = defaultdict(list)
        self.domain_last_access = defaultdict(float)
        
        # Track visited and in-progress URLs
        self.in_progress = set()
        
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

    def _load_save_file(self):
        """Load and organize URLs from save file"""
        with self._lock:
            total_urls = len(self.save)
            pending_urls = 0
            
            for url, completed in self.save.values():
                if not completed and is_valid(url):
                    domain = urlparse(url).netloc
                    self.domain_queues[domain].append(url)
                    pending_urls += 1
                    
            if not self.save:  # Empty save file
                for url in self.config.seed_urls:
                    self.add_url(url)
                    
            self.logger.info(
                f"Found {pending_urls} urls to be downloaded from {total_urls} "
                f"total urls discovered.")

    def get_tbd_url(self):
        """Thread-safe method to get next URL respecting politeness delay"""
        with self._lock:
            current_time = time.time()
            
            # Check all domain queues
            for domain, urls in list(self.domain_queues.items()):
                if not urls:
                    continue
                    
                # Check politeness delay requirements
                with self._domain_locks[domain]:
                    last_access = self.domain_last_access[domain]
                    if current_time - last_access >= self.config.time_delay:
                        url = urls[0]
                        
                        # Skip URLs that are being processed
                        if url in self.in_progress:
                            continue
                            
                        # Remove from queue and mark as in progress
                        urls.pop(0)
                        self.in_progress.add(url)
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
            
        with self._lock:
            self.in_progress.discard(url)
            
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
                return
            
            self.save[urlhash] = (url, True)
            self.save.sync()

    def __del__(self):
        self.save.close()
