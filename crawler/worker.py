from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.worker_id = worker_id
        self.config = config
        self.frontier = frontier
        self.logger = get_logger(f"Worker-{worker_id}", f"Worker")
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        """Main worker loop"""
        while True:
            try:
                # Get next URL
                url = self.frontier.get_tbd_url()
                if not url:
                    time.sleep(0.1)  # Short sleep if no URLs
                    continue
                
                if not scraper.is_valid(url) or scraper.is_trap(url) or not scraper.is_allowed_by_robots(url, self.config):
                    self.logger.info(f"Invalid URL or trap URL or not allowed by robots.txt: {url}")
                    self.frontier.mark_url_complete(url)
                    continue
                
                # Download page
                resp = download(url, self.config, self.logger)
                
                self.logger.info(
                    f"Downloaded {url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                
                # Extract and add new URLs
                new_urls = scraper.scraper(url, resp)
                
                # Filter URLs by robots.txt and valid urls and traps
                filtered_urls = []
                for new_url in new_urls:
                    if scraper.is_allowed_by_robots(new_url, self.config) and not scraper.is_trap(new_url) and scraper.is_valid(new_url):
                        filtered_urls.append(new_url)
                
                for new_url in filtered_urls:
                    self.frontier.add_url(new_url)
                    
                # Mark current URL complete
                self.frontier.mark_url_complete(url)
                
            except Exception as e:
                self.logger.error(f"Error processing {url}: {e}")
                if url:
                    self.frontier.mark_url_complete(url)
