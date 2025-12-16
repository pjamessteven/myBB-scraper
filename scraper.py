import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime
from urllib.parse import urljoin, parse_qs, urlparse
import config
from database import Database

class ForumScraper:
    def __init__(self):
        self.db = Database()
        self.session = requests.Session()
        self.session.headers.update(config.HEADERS)
        
        # Add cookies from config
        if hasattr(config, 'COOKIES') and config.COOKIES:
            # Build Cookie header string
            cookie_parts = []
            for name, value in config.COOKIES.items():
                cookie_parts.append(f"{name}={value}")
            cookie_header = '; '.join(cookie_parts)
            self.session.headers.update({'Cookie': cookie_header})
        # Add cookies from config
        if hasattr(config, 'COOKIES'):
            for cookie_name, cookie_value in config.COOKIES.items():
                self.session.cookies.set(cookie_name, cookie_value)
        # Add cookies to the session
        if hasattr(config, 'COOKIES'):
            # Update the session's cookies
            for name, value in config.COOKIES.items():
                self.session.cookies.set(name, value)
        
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
    
    def extract_number_of_pages(self, soup, thread_id=None):
        """Extract the total number of pages from the first page of a thread"""
        # First, try to find pagination specific to this thread
        # Look for links that point to showthread.php with this thread_id
        page_numbers = set()
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Check if it's a showthread.php link for this thread
            if 'showthread.php' in href and f'tid={thread_id}' in href:
                # Extract page number from the href
                # Look for page= parameter
                if 'page=' in href:
                    # Parse the page number
                    import re
                    match = re.search(r'page=(\d+)', href)
                    if match:
                        try:
                            page_num = int(match.group(1))
                            page_numbers.add(page_num)
                        except ValueError:
                            pass
                # Also check if the link text is a number (for numbered pagination)
                text = link.get_text(strip=True)
                if text.isdigit():
                    try:
                        page_num = int(text)
                        page_numbers.add(page_num)
                    except ValueError:
                        pass
        
        # Also look for traditional pagination div as a fallback,
        # but exclude pagination that is inside navigation breadcrumb (forum pagination)
        if not page_numbers:
            pagination_divs = soup.find_all('div', class_='pagination')
            for pagination in pagination_divs:
                # Skip if inside navigation breadcrumb
                if pagination.find_parent('div', class_='navigation'):
                    continue
                page_links = pagination.find_all('a')
                for link in page_links:
                    text = link.get_text(strip=True)
                    if text.isdigit():
                        try:
                            page_numbers.add(int(text))
                        except ValueError:
                            pass
                # If we found any page numbers from this pagination div, break
                if page_numbers:
                    break
        
        # Include page 1 always
        page_numbers.add(1)
        
        if page_numbers:
            return max(page_numbers)
        else:
            return 1
    
    def parse_user_info(self, post_element):
        """Extract user information from a post element"""
        username = None
        num_posts = None
        num_threads = None
        joined_date = None
        
        # Find username from largetext
        username_elem = post_element.find('span', class_='largetext')
        if username_elem:
            # First try to find an <a> tag (for registered users)
            username_link = username_elem.find('a')
            if username_link:
                username = username_link.get_text(strip=True)
            else:
                # If no <a> tag, get the text directly (for unregistered users)
                username = username_elem.get_text(strip=True)
        
        # Find author_statistics div
        stats_div = post_element.find('div', class_='author_statistics')
        if stats_div:
            stats_text = stats_div.get_text()
            # Parse Posts: X
            posts_match = re.search(r'Posts:\s*(\d+)', stats_text)
            if posts_match:
                num_posts = int(posts_match.group(1))
            # Parse Threads: X
            threads_match = re.search(r'Threads:\s*(\d+)', stats_text)
            if threads_match:
                num_threads = int(threads_match.group(1))
            # Parse Joined: MMM YYYY
            joined_match = re.search(r'Joined:\s*(\w+\s+\d{4})', stats_text)
            if joined_match:
                joined_str = joined_match.group(1)
                try:
                    # Convert "Dec 2021" to datetime (day set to 1)
                    joined_date = datetime.strptime(joined_str, '%b %Y')
                except Exception as e:
                    # Try alternative format maybe with extra spaces
                    joined_str_clean = joined_str.strip()
                    try:
                        joined_date = datetime.strptime(joined_str_clean, '%b %Y')
                    except Exception:
                        # Debug output
                        print(f"Debug: Could not parse joined date '{joined_str}' for user {username}")
                        pass
        
        return username, num_posts, num_threads, joined_date
    
    def parse_post(self, post_element, thread_id):
        """Parse individual post from a post element"""
        # Extract post ID
        post_id = None
        post_id_elem = post_element.get('id')
        if post_id_elem:
            match = re.search(r'(\d+)', post_id_elem)
            if match:
                post_id = int(match.group(1))
        if not post_id:
            print(f"Debug: Could not extract post_id from {post_id_elem}")
        
        # Extract post date
        post_date = None
        date_elem = post_element.find('span', class_='post_date')
        if date_elem:
            date_text = date_elem.get_text(strip=True)
            # Remove any trailing edited indicator
            # Example: "09-Dec-2021, 10:06 PM " or "09-Dec-2021, 10:06 PM (This post was last modified: ...)"
            # We'll try to parse the part before the first '('
            if '(' in date_text:
                date_text = date_text.split('(')[0].strip()
            # Try common formats seen in the sample
            for fmt in ('%d-%b-%Y, %I:%M %p', '%d-%b-%Y, %H:%M'):
                try:
                    post_date = datetime.strptime(date_text, fmt)
                    break
                except:
                    continue
        if not post_date:
            print(f"Debug: Could not parse date from element {date_elem}")
        
        # Extract post text
        post_text = None
        text_elem = post_element.find('div', class_='post_body')
        if text_elem:
            # Remove blockquote elements to get clean post text
            # We'll make a copy to avoid modifying the original
            text_elem_copy = BeautifulSoup(str(text_elem), 'lxml')
            for blockquote in text_elem_copy.find_all('blockquote', class_='mycode_quote'):
                blockquote.decompose()
            # Get text with newlines preserved
            # First, get all text nodes with proper spacing
            # We'll use get_text with separator and then clean up
            raw_text = text_elem_copy.get_text(separator='\n', strip=False)
            # Clean up the text: remove excessive newlines and spaces
            # Split by newline, strip each line, and filter out empty lines
            lines = [line.strip() for line in raw_text.split('\n')]
            # Join non-empty lines with a single newline between them
            post_text = '\n'.join(line for line in lines if line)
            # If post_text is empty after processing, try a different approach
            if not post_text:
                # Fallback to original method
                post_text = text_elem_copy.get_text(strip=True)
        if not post_text:
            print(f"Debug: Could not find post_body in post {post_id}")
        
        # Extract username (same as in parse_user_info)
        username = None
        username_elem = post_element.find('span', class_='largetext')
        if username_elem:
            # First try to find an <a> tag (for registered users)
            username_link = username_elem.find('a')
            if username_link:
                username = username_link.get_text(strip=True)
            else:
                # If no <a> tag, get the text directly (for unregistered users)
                username = username_elem.get_text(strip=True)
        if not username:
            print(f"Debug: Could not find username in post {post_id}")
        
        # Extract replies_to from blockquote
        replies_to = None
        # Find the post_body element
        post_body = post_element.find('div', class_='post_body')
        if post_body:
            # Look for blockquote elements that are direct children of post_body
            # We want the top-level reply, which should be the first direct child blockquote
            blockquotes = post_body.find_all('blockquote', class_='mycode_quote', recursive=False)
            if blockquotes:
                # Take the first top-level blockquote
                blockquote = blockquotes[0]
                # Find the link inside the blockquote's cite
                cite = blockquote.find('cite')
                if cite:
                    # Look for a link
                    link = cite.find('a')
                    if link:
                        href = link.get('href', '')
                        # Parse the URL to extract the pid parameter
                        try:
                            parsed = urlparse(href)
                            query_params = parse_qs(parsed.query)
                            if 'pid' in query_params:
                                pid_value = query_params['pid'][0]
                                if pid_value.isdigit():
                                    replies_to = int(pid_value)
                        except Exception as e:
                            print(f"Debug: Could not parse href {href}: {e}")
        
        return post_id, post_date, post_text, username, replies_to
    
    def scrape_thread_page(self, thread_id, page_num):
        """Scrape a single page of a thread"""
        url = config.THREAD_URL_TEMPLATE.format(tid=thread_id, page=page_num)
        print(f"Scraping {url}")
        
        soup = self.get_soup(url)
        if not soup:
            print(f"Failed to retrieve {url}")
            return False
        
        # On first page, extract thread info and insert it before processing posts
        if page_num == 1:
            # Extract thread title
            thread_title = None
            title_elem = soup.find('title')
            if title_elem:
                thread_title = title_elem.get_text(strip=True)
            
            # Extract board name from navigation breadcrumb
            board_name = None
            nav_div = soup.find('div', class_='navigation')
            if nav_div:
                # Find all <a> tags that are not part of pagination
                links = []
                for a in nav_div.find_all('a'):
                    # Skip pagination links
                    if a.get('class') and any(cls.startswith('pagination_') for cls in a.get('class')):
                        continue
                    # Also skip if inside a div with class 'pagination'
                    parent_div = a.find_parent('div', class_='pagination')
                    if parent_div:
                        continue
                    links.append(a)
                if links:
                    # Combine all link texts to form full breadcrumb path
                    link_texts = [link.get_text(strip=True) for link in links]
                    board_name = ' › '.join(link_texts)
            
            # Extract thread date (from first post's date)
            thread_date = None
            first_post = soup.find('div', class_='post')
            if first_post:
                date_elem = first_post.find('span', class_='post_date')
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    if '(' in date_text:
                        date_text = date_text.split('(')[0].strip()
                    for fmt in ('%d-%b-%Y, %I:%M %p', '%d-%b-%Y, %H:%M'):
                        try:
                            thread_date = datetime.strptime(date_text, fmt)
                            break
                        except:
                            continue
            
            # Get total pages
            total_pages = self.extract_number_of_pages(soup, thread_id)
            
            # Insert thread info if we have a title
            if thread_title:
                self.db.insert_thread(thread_id, thread_title, board_name, thread_date)
                print(f"Thread {thread_id}: {thread_title} (Board: {board_name}) - {total_pages} pages")
            else:
                print(f"Thread {thread_id}: Could not extract thread title, not saving thread to database")
                # Without a thread title, we can't insert the thread, so we shouldn't process posts
                return False
        
        # Find all posts on the page by their id pattern (more reliable than class)
        posts = soup.find_all('div', id=re.compile(r'post_\d+'))
        print(f"Found {len(posts)} posts on page {page_num}")
        
        # Track if we found at least one valid post
        found_valid_posts = False
        
        for post in posts:
            post_id, post_date, post_text, username, replies_to = self.parse_post(post, thread_id)
            if post_id and username:
                found_valid_posts = True
                # Parse user info (posts, threads, joined date)
                user_info = self.parse_user_info(post)
                username_found, num_posts, num_threads, joined_date = user_info
                # Ensure username matches
                if username_found and username_found != username:
                    # Use the one from parse_user_info
                    username = username_found
                # Insert user with extracted info
                self.db.insert_user(username, num_posts, num_threads, joined_date)
                # Insert post
                self.db.insert_post(post_id, post_date, post_text, username, thread_id, replies_to)
            else:
                # Debug: print why post wasn't parsed
                print(f"Warning: Failed to parse post from element {post.get('id')}")
        
        time.sleep(config.DELAY_BETWEEN_REQUESTS)
        return found_valid_posts
    
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
        
        total_pages = self.extract_number_of_pages(soup, thread_id)
        
        # Track if any valid posts were found across all pages
        any_valid_posts_found = False
        
        # Scrape each page
        for page_num in range(1, total_pages + 1):
            success = self.scrape_thread_page(thread_id, page_num)
            # If scrape_thread_page returns True, it means it found valid posts on that page
            if success:
                any_valid_posts_found = True
            elif not success and page_num == 1:
                # If the first page has no valid posts, there's no point in continuing
                print(f"No valid posts found on first page of thread {thread_id}, stopping")
                break
            else:
                print(f"Failed to scrape page {page_num} of thread {thread_id}")
                break
        
        # Return True only if we found at least one valid post
        # This helps track whether the thread was actually saved to the database
        return any_valid_posts_found
    
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
