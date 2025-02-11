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
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        """Main worker loop"""
        while True:
            try:
                # Get next URL to process
                url = self.frontier.get_tbd_url()
                
                if not url:
                    # No URLs available, wait before retrying
                    self.logger.info(
                        f"Worker-{self.worker_id}: No URLs available. Waiting...")
                    time.sleep(self.config.time_delay)
                    continue
                
                # Download page
                resp = download(url, self.config, self.logger)
                
                if resp.status != 200:
                    self.logger.error(
                        f"Worker-{self.worker_id} failed to download {url}, "
                        f"status <{resp.status}>")
                    self.frontier.mark_url_complete(url)
                    continue
                
                self.logger.info(
                    f"Worker-{self.worker_id} downloaded {url}, "
                    f"status <{resp.status}>, using cache {self.config.cache_server}.")
                
                # Extract and add new URLs
                new_urls = scraper.scraper(url, resp)
                for new_url in new_urls:
                    self.frontier.add_url(new_url)
                
                # Mark current URL as completed
                self.frontier.mark_url_complete(url)
                
            except Exception as e:
                self.logger.error(
                    f"Worker-{self.worker_id} error processing {url}: {str(e)}")
                if url:
                    self.frontier.mark_url_complete(url)
