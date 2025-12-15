import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import config

class Database:
    def __init__(self):
        self.conn = None
        self.connect()
        
    def connect(self):
        """Establish a connection to the database"""
        try:
            self.conn = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                database=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD
            )
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            print("Connected to database successfully")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def create_tables(self):
        """Create the necessary tables if they don't exist"""
        create_tables_queries = [
            """
            CREATE TABLE IF NOT EXISTS users (
                username VARCHAR(255) PRIMARY KEY,
                num_posts INTEGER,
                num_threads INTEGER,
                joined_date TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS threads (
                thread_id INTEGER PRIMARY KEY,
                thread_title VARCHAR(500),
                board_name VARCHAR(255),
                date_posted TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS posts (
                post_id INTEGER PRIMARY KEY,
                post_date TIMESTAMP,
                post_text TEXT,
                username VARCHAR(255) REFERENCES users(username),
                thread_id INTEGER REFERENCES threads(thread_id),
                replies_to INTEGER
            )
            """
        ]
        
        cursor = self.conn.cursor()
        for query in create_tables_queries:
            try:
                cursor.execute(query)
                print("Table created or already exists")
            except Exception as e:
                print(f"Error creating table: {e}")
                self.conn.rollback()
                raise
        
        # Add the replies_to column if it doesn't exist (for existing tables)
        try:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='posts' and column_name='replies_to'
            """)
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE posts ADD COLUMN replies_to INTEGER")
                print("Added replies_to column to posts table")
        except Exception as e:
            print(f"Error checking/adding replies_to column: {e}")
            self.conn.rollback()
        
        # Try to drop any existing foreign key constraint on replies_to
        try:
            # Find the constraint name
            cursor.execute("""
                SELECT conname
                FROM pg_constraint 
                WHERE conrelid = 'posts'::regclass 
                AND contype = 'f' 
                AND conname LIKE '%replies_to%'
            """)
            constraint = cursor.fetchone()
            if constraint:
                constraint_name = constraint[0]
                cursor.execute(f"ALTER TABLE posts DROP CONSTRAINT {constraint_name}")
                print(f"Dropped foreign key constraint {constraint_name} on replies_to")
        except Exception as e:
            print(f"Error dropping foreign key constraint on replies_to (may not exist): {e}")
            self.conn.rollback()
        
        # Try to drop any existing foreign key constraint on replies_to
        try:
            # Find the constraint name for foreign key on replies_to column
            cursor.execute("""
                SELECT tc.constraint_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = 'posts'
                  AND tc.constraint_type = 'FOREIGN KEY'
                  AND kcu.column_name = 'replies_to'
            """)
            constraint = cursor.fetchone()
            if constraint:
                constraint_name = constraint[0]
                # Use the constraint name to drop it
                drop_query = sql.SQL("ALTER TABLE posts DROP CONSTRAINT {}").format(
                    sql.Identifier(constraint_name)
                )
                cursor.execute(drop_query)
                print(f"Dropped foreign key constraint {constraint_name} on replies_to")
        except Exception as e:
            print(f"Error dropping foreign key constraint on replies_to (may not exist): {e}")
            self.conn.rollback()
        
        cursor.close()
    
    def insert_user(self, username, num_posts, num_threads, joined_date):
        """Insert or update a user"""
        cursor = self.conn.cursor()
        query = """
        INSERT INTO users (username, num_posts, num_threads, joined_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (username) DO UPDATE SET
            num_posts = EXCLUDED.num_posts,
            num_threads = EXCLUDED.num_threads,
            joined_date = EXCLUDED.joined_date
        """
        try:
            cursor.execute(query, (username, num_posts, num_threads, joined_date))
        except Exception as e:
            print(f"Error inserting user {username}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def insert_thread(self, thread_id, thread_title, board_name, date_posted):
        """Insert or update a thread"""
        cursor = self.conn.cursor()
        query = """
        INSERT INTO threads (thread_id, thread_title, board_name, date_posted)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (thread_id) DO UPDATE SET
            thread_title = EXCLUDED.thread_title,
            board_name = EXCLUDED.board_name,
            date_posted = EXCLUDED.date_posted
        """
        try:
            cursor.execute(query, (thread_id, thread_title, board_name, date_posted))
        except Exception as e:
            print(f"Error inserting thread {thread_id}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def insert_post(self, post_id, post_date, post_text, username, thread_id, replies_to=None):
        """Insert or update a post"""
        cursor = self.conn.cursor()
        query = """
        INSERT INTO posts (post_id, post_date, post_text, username, thread_id, replies_to)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (post_id) DO UPDATE SET
            post_date = EXCLUDED.post_date,
            post_text = EXCLUDED.post_text,
            username = EXCLUDED.username,
            thread_id = EXCLUDED.thread_id,
            replies_to = EXCLUDED.replies_to
        """
        try:
            cursor.execute(query, (post_id, post_date, post_text, username, thread_id, replies_to))
        except Exception as e:
            print(f"Error inserting post {post_id}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def thread_exists(self, thread_id):
        """Check if a thread exists in the database"""
        cursor = self.conn.cursor()
        query = "SELECT 1 FROM threads WHERE thread_id = %s"
        try:
            cursor.execute(query, (thread_id,))
            exists = cursor.fetchone() is not None
            return exists
        except Exception as e:
            print(f"Error checking thread existence: {e}")
            return False
        finally:
            cursor.close()
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed")
