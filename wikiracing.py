from typing import List, Optional
import requests
from bs4 import BeautifulSoup
import urllib.parse
import time
import collections
import psycopg2
from psycopg2 import pool


requests_per_minute = 100
links_per_page = 200


class DatabaseOperation:
    # conn_pool = None

    def __init__(self):
        self.connection = self.conn_pool.getconn()
        self.cursor = self.connection.cursor()

    def create_pool(self):
        db_config = {
            'host': '127.0.0.1',
            'user': 'postgres',
            'password': 'postgres',
            'database': 'postgres'
        }
        try:
            self.conn_pool = psycopg2.pool.SimpleConnectionPool(1, 5, **db_config)
        except(Exception, psycopg2.DatabaseError) as err:
            print('Error while connecting PostgreSQL', err)

    def close_all_connections(self):
        if self.conn_pool:
            self.conn_pool.closeall

    def create_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS wikiracer(
                id serial PRIMARY KEY,
                source VARCHAR(1000),
                target VARCHAR(1000)
            )"""
        self.cursor.execute(create_table_query)
        self.connection.commit()

    def insert_values(self, page_name: str, page_name_link: str):
        # single quote escape in page names
        page_name = page_name.replace("'", "''")
        page_name_link = page_name_link.replace("'", "''")
        insert_values = f"INSERT INTO wikiracer(source, target) VALUES ('{page_name}', '{page_name_link}')"
        self.cursor.execute(insert_values)
        self.connection.commit()

    def check_value_in_table(self, page_name: str) -> Optional[str]:
        self.create_table()
        page_name = page_name.replace("'", "''")
        check_value = f"SELECT DISTINCT source FROM wikiracer WHERE source = '{page_name}'"
        self.cursor.execute(check_value)
        return self.cursor.fetchone()


class WikiRacer:

    def get_sublinks(self, page_name: str) -> List[str]:
        page_name_check_in_db = DatabaseOperation().check_value_in_table(page_name)
        if not page_name_check_in_db:
            return self.get_page_links(page_name)
        print('already in database')

    @staticmethod
    def crawl_page(start: str) -> str:
        retries = 3
        attempts = 0
        while attempts < retries:
            try:
                resp = requests.get('https://uk.wikipedia.org/wiki/' + start)
                time.sleep(60 / requests_per_minute)
                return resp.text
            except requests.exceptions.RequestException as err:
                print('err', err)
                time.sleep(1)
                attempts += 1

    def get_page_links(self, start: str) -> List[str]:
        links = []
        resp = self.crawl_page(start)
        if not resp:
            return []
        soup = BeautifulSoup(resp, 'lxml')
        urls = soup.find('div', class_='mw-parser-output').find_all('a')

        for url in urls:
            href = url.get('href', ' ')
            # separate valid internal links from outer and other garbage links
            internal_link = href.startswith('/wiki/') and ('#' not in href and ':' not in href)
            page_name_raw = href.split('/')[-1]
            page_name = ' '.join(page_name_raw.split('_'))
            if internal_link:
                if len(links) < links_per_page and page_name not in links:
                    links.append(urllib.parse.unquote(page_name))
        return links

    def find_path(self, start: str, finish: str) -> List[str]:
        path = {start: [start]}
        queue = collections.deque([start])

        while len(queue) != 0:
            vertex = queue.popleft()
            links = self.get_sublinks(vertex)
            if not links:
                continue
            for link in links:
                DatabaseOperation().insert_values(vertex, link)
                if finish in links:
                    return path[vertex] + [finish]
                if (link not in path) and (link != vertex):
                    path[link] = path[vertex] + [link]
                    queue.append(link)
        return []


if __name__ == '__main__':
    s = WikiRacer()
    s.find_path('Дружба', 'Рим')
