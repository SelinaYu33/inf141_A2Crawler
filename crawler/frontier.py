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
        self.to_be_downloaded = list()
        
        # Add thread-safe lock
        self._lock = RLock()
        
        # Track last access time for each domain
        self.domain_last_access = defaultdict(float)
        self.domain_queues = defaultdict(list)
        
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
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
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
            
            # 检查所有域名队列
            for domain, urls in self.domain_queues.items():
                if not urls:
                    continue
                    
                # 检查是否满足politeness要求
                last_access = self.domain_last_access[domain]
                if current_time - last_access >= self.config.time_delay:
                    url = urls.pop(0)
                    self.domain_last_access[domain] = current_time
                    return url
                    
            return None

    def add_url(self, url):
        """Thread-safe method to add URL"""
        url = normalize(url)
        with self._lock:
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                domain = urlparse(url).netloc
                self.domain_queues[domain].append(url)
    
    def mark_url_complete(self, url):
        """Thread-safe method to mark URL as completed"""
        with self._lock:
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, True)
            self.save.sync()
