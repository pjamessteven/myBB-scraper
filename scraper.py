import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime
from urllib.parse import urljoin
import config
from database import Database

class ForumScraper:
    def __init__(self):
        self.db = Database()
        self.session = requests.Session()
        self.session.headers.update(config.HEADERS)
        
    def get_soup(self, url):
        """Fetch a URL and return BeautifulSoup object"""
        for attempt in range(config.MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=config.TIMEOUT)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'lxml')
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"Max retries exceeded for {url}")
                    return None
        return None
    
    def extract_number_of_pages(self, soup):
        """Extract the total number of pages from the first page of a thread"""
        pagination = soup.find('div', class_='pagination')
        if pagination:
            page_links = pagination.find_all('a')
            page_numbers = []
            for link in page_links:
                text = link.get_text()
                if text.isdigit():
                    page_numbers.append(int(text))
            if page_numbers:
                return max(page_numbers)
        # If no pagination found, assume only one page
        return 1
    
    def parse_user_info(self, post_element):
        """Extract user information from a post element"""
        # This will need to be customized based on the actual HTML structure
        # For now, let's assume a basic structure
        username = None
        num_posts = None
        num_threads = None
        joined_date = None
        
        # Find username
        username_elem = post_element.find('a', class_='username')
        if username_elem:
            username = username_elem.get_text(strip=True)
        
        # These selectors need to be adjusted based on actual forum structure
        # For demonstration, we'll use placeholder logic
        return username, num_posts, num_threads, joined_date
    
    def parse_post(self, post_element, thread_id):
        """Parse individual post from a post element"""
        # Extract post ID
        post_id = None
        post_id_elem = post_element.get('id')
        if post_id_elem:
            # Try to extract numeric ID
            match = re.search(r'(\d+)', post_id_elem)
            if match:
                post_id = int(match.group(1))
        
        # Extract post date
        post_date = None
        date_elem = post_element.find('span', class_='post_date')
        if date_elem:
            date_text = date_elem.get_text(strip=True)
            # Try to parse date - this will need to be adjusted
            try:
                post_date = datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
            except:
                pass
        
        # Extract post text
        post_text = None
        text_elem = post_element.find('div', class_='post_body')
        if text_elem:
            post_text = text_elem.get_text(strip=True)
        
        # Extract username
        username = None
        user_elem = post_element.find('a', class_='username')
        if user_elem:
            username = user_elem.get_text(strip=True)
        
        return post_id, post_date, post_text, username
    
    def scrape_thread_page(self, thread_id, page_num):
        """Scrape a single page of a thread"""
        url = config.THREAD_URL_TEMPLATE.format(tid=thread_id, page=page_num)
        print(f"Scraping {url}")
        
        soup = self.get_soup(url)
        if not soup:
            print(f"Failed to retrieve {url}")
            return False
        
        # On first page, extract thread info and number of pages
        if page_num == 1:
            # Extract thread title
            thread_title = None
            title_elem = soup.find('title')
            if title_elem:
                thread_title = title_elem.get_text(strip=True)
            
            # Extract board name
            board_name = None
            breadcrumb = soup.find('div', class_='breadcrumb')
            if breadcrumb:
                links = breadcrumb.find_all('a')
                if len(links) > 1:
                    board_name = links[-2].get_text(strip=True)
            
            # Extract thread date (from first post)
            thread_date = None
            
            # Get total pages
            total_pages = self.extract_number_of_pages(soup)
            
            # Insert thread info (placeholder date for now)
            if thread_title:
                self.db.insert_thread(thread_id, thread_title, board_name, thread_date)
                print(f"Thread {thread_id}: {thread_title} (Board: {board_name}) - {total_pages} pages")
        
        # Find all posts on the page
        posts = soup.find_all('div', class_='post')
        if not posts:
            # Try alternative class names
            posts = soup.find_all('div', id=re.compile(r'post_\d+'))
        
        for post in posts:
            post_id, post_date, post_text, username = self.parse_post(post, thread_id)
            if post_id and username:
                # Insert user with placeholder info
                self.db.insert_user(username, None, None, None)
                # Insert post
                self.db.insert_post(post_id, post_date, post_text, username, thread_id)
        
        time.sleep(config.DELAY_BETWEEN_REQUESTS)
        return True
    
    def scrape_thread(self, thread_id):
        """Scrape all pages of a thread"""
        # Check if thread already exists
        if self.db.thread_exists(thread_id):
            print(f"Thread {thread_id} already exists in database, skipping")
            return True
        
        # First, get the first page to know total number of pages
        url = config.THREAD_URL_TEMPLATE.format(tid=thread_id, page=1)
        soup = self.get_soup(url)
        if not soup:
            print(f"Thread {thread_id} might not exist or is inaccessible")
            return False
        
        # Check if thread exists by looking for error messages or empty content
        # This is forum-specific
        error_msg = soup.find('div', class_='error')
        if error_msg and 'not found' in error_msg.get_text().lower():
            print(f"Thread {thread_id} not found")
            return False
        
        total_pages = self.extract_number_of_pages(soup)
        
        # Scrape each page
        for page_num in range(1, total_pages + 1):
            success = self.scrape_thread_page(thread_id, page_num)
            if not success:
                print(f"Failed to scrape page {page_num} of thread {thread_id}")
                break
        
        return True
    
    def scrape_range(self, start_tid, end_tid):
        """Scrape a range of thread IDs"""
        for thread_id in range(start_tid, end_tid + 1):
            print(f"\nProcessing thread ID: {thread_id}")
            self.scrape_thread(thread_id)
            # Add a small delay between threads
            time.sleep(config.DELAY_BETWEEN_REQUESTS)
    
    def close(self):
        """Clean up resources"""
        self.db.close()
        self.session.close()
