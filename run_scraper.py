import sys
import config
from database import Database
from scraper import ForumScraper

def main():
    # First, ensure database tables exist
    print("Setting up database...")
    db = Database()
    db.create_tables()
    db.close()
    
    # Start scraping
    print("Starting scraper...")
    scraper = ForumScraper()
    
    try:
        scraper.scrape_range(config.START_TID, config.END_TID)
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        scraper.close()
        print("Scraping completed")

if __name__ == "__main__":
    main()
