import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import concurrent.futures
import sqlite3
from dataclasses import dataclass
from typing import Optional
import hashlib

@dataclass
class DatabaseConfig:
    host: str
    user: str
    password: str
    database: str
    port: int = 3306

class WebCrawler:
    def __init__(self, start_url, db_config: DatabaseConfig, delay=1, max_workers=5):
        self.start_url = start_url
        self.visited = set()
        self.delay = delay
        self.domain = urlparse(start_url).netloc
        self.max_workers = max_workers
        self.db_config = db_config
        self.setup_database()

    def setup_database(self):
        """Initialise la base de données"""
        self.conn = sqlite3.connect('crawler.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # Création des tables
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS crawl_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            title TEXT,
            content_hash TEXT UNIQUE,
            ip_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS crawl_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            status TEXT,
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        self.conn.commit()

    def get_content_hash(self, content: str) -> str:
        """Calcule le hash du contenu pour détecter les doublons"""
        return hashlib.md5(content.encode()).hexdigest()

    def is_duplicate_content(self, content_hash: str) -> bool:
        """Vérifie si le contenu existe déjà"""
        self.cursor.execute('SELECT id FROM crawl_results WHERE content_hash = ?', (content_hash,))
        return self.cursor.fetchone() is not None

    def save_result(self, url: str, title: str, content: str, ip: Optional[str] = None):
        """Sauvegarde les résultats dans la base de données"""
        content_hash = self.get_content_hash(content)
        
        if not self.is_duplicate_content(content_hash):
            try:
                self.cursor.execute('''
                INSERT INTO crawl_results (url, title, content_hash, ip_address)
                VALUES (?, ?, ?, ?)
                ''', (url, title, content_hash, ip))
                self.conn.commit()
            except sqlite3.IntegrityError:
                print(f"URL déjà crawlée: {url}")

    def log_crawl(self, url: str, status: str, error: Optional[str] = None):
        """Enregistre les logs de crawling"""
        self.cursor.execute('''
        INSERT INTO crawl_logs (url, status, error)
        VALUES (?, ?, ?)
        ''', (url, status, error))
        self.conn.commit()

    def crawl_page(self, url: str):
        """Crawl une seule page"""
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.title.string if soup.title else "No title"
            
            # Extraction de l'IP (simulation)
            ip = urlparse(url).netloc
            
            # Sauvegarde des résultats
            self.save_result(url, title, response.text, ip)
            self.log_crawl(url, "success")
            
            # Récupération des nouveaux liens
            links = set()
            for link in soup.find_all('a'):
                href = link.get('href')
                if href:
                    absolute_url = urljoin(url, href)
                    if self.is_valid_url(absolute_url):
                        links.add(absolute_url)
            
            return links
        
        except Exception as e:
            self.log_crawl(url, "error", str(e))
            return set()

    def crawl(self, max_pages=10):
        """Lance le crawling en parallèle"""
        queue = [self.start_url]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            while queue and len(self.visited) < max_pages:
                # Prépare le batch de pages à crawler
                batch = []
                while queue and len(batch) < self.max_workers:
                    url = queue.pop(0)
                    if url not in self.visited:
                        self.visited.add(url)
                        batch.append(url)
                
                if not batch:
                    break
                
                # Lance le crawling en parallèle
                future_to_url = {executor.submit(self.crawl_page, url): url for url in batch}
                
                # Récupère les résultats
                for future in concurrent.futures.as_completed(future_to_url):
                    new_links = future.result()
                    queue.extend(link for link in new_links if link not in self.visited)
                
                time.sleep(self.delay)

    def close(self):
        """Ferme la connexion à la base de données"""
        self.conn.close()

# Exemple d'utilisation
if __name__ == "__main__":
    db_config = DatabaseConfig(
        host="localhost",
        user="crawler_user",
        password="secure_password",
        database="crawler_db"
    )
    
    crawler = WebCrawler(
        start_url="https://example.com",
        db_config=db_config,
        delay=1,
        max_workers=5
    )
    
    try:
        crawler.crawl(max_pages=20)
    finally:
        crawler.close()