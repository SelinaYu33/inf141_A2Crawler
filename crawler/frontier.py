import os
import shelve
from threading import Thread, RLock
from collections import defaultdict
import time
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid
from urllib.parse import urlparse

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        
        # Remove unused list since we're using domain_queues
        # self.to_be_downloaded = list()
        
        # Add thread-safe lock
        self._lock = RLock()
        
        # Track last access time for each domain
        self.domain_last_access = defaultdict(float)
        self.domain_queues = defaultdict(list)
        
        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
            
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        total_count = len(self.save)
        tbd_count = 0
        with self._lock:
            for url, completed in self.save.values():
                if not completed and is_valid(url):
                    domain = urlparse(url).netloc
                    self.domain_queues[domain].append(url)
                    tbd_count += 1
            self.logger.info(
                f"Found {tbd_count} urls to be downloaded from {total_count} "
                f"total urls discovered.")

    def get_tbd_url(self):
        """Thread-safe method to get next URL respecting politeness delay"""
        with self._lock:
            current_time = time.time()
            
            # Check all domain queues
            for domain, urls in list(self.domain_queues.items()):  # Create a copy of items
                if not urls:
                    continue
                    
                # Check if politeness requirement is met
                last_access = self.domain_last_access[domain]
                if current_time - last_access >= self.config.time_delay:
                    url = urls.pop(0)
                    self.domain_last_access[domain] = current_time
                    return url
            
            # If all domains are recently accessed, return None
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
                self.domain_queues[domain].append(url)
    
    def mark_url_complete(self, url):
        """Thread-safe method to mark URL as completed"""
        if not url:
            return
            
        with self._lock:
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
                return

            self.save[urlhash] = (url, True)
            self.save.sync()

    def __del__(self):
        self.save.close()
