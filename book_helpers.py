import os
import sqlite3
from datetime import datetime, timezone

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, 'booksapi.txt')) as f:
    BOOKS_API_KEY = f.read().strip()

BOOKS_URL = 'https://www.googleapis.com/books/v1/volumes'


def setup_books_db(db_path='tmdb_analytics.db'):
    """Create book tables. Safe to re-run."""
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS books (
            id             TEXT PRIMARY KEY,
            title          TEXT,
            authors        TEXT,
            publisher      TEXT,
            published_date TEXT,
            page_count     INTEGER,
            description    TEXT,
            cover_url      TEXT,
            status         TEXT DEFAULT 'read',
            date_read      TEXT,
            datetime_added TEXT,
            notes          TEXT
        );
        CREATE TABLE IF NOT EXISTS book_genres (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            book_id TEXT REFERENCES books(id),
            name    TEXT
        );
    ''')
    conn.commit()
    conn.close()
    print('Books DB ready.')


def _parse_volume(v):
    """Extract fields from a Google Books volume dict."""
    info = v.get('volumeInfo', {})
    images = info.get('imageLinks', {})
    cover = images.get('thumbnail') or images.get('smallThumbnail')
    if cover:
        cover = cover.replace('http://', 'https://')
    return {
        'id':             v['id'],
        'title':          info.get('title', 'Unknown'),
        'authors':        ', '.join(info.get('authors', [])),
        'publisher':      info.get('publisher'),
        'published_date': info.get('publishedDate'),
        'page_count':     info.get('pageCount'),
        'description':    info.get('description'),
        'cover_url':      cover,
        'categories':     info.get('categories', []),
    }


def search_book(query):
    """Search Google Books, return top 5 results."""
    params = {'q': query, 'maxResults': 5, 'key': BOOKS_API_KEY}
    resp = requests.get(BOOKS_URL, params=params, timeout=15)
    resp.raise_for_status()
    items = resp.json().get('items', [])
    if not items:
        raise ValueError(f'No results found for "{query}"')
    return [_parse_volume(v) for v in items]


def fetch_and_store_book(book_id, db_path='tmdb_analytics.db', status='read', date_read=None):
    """Fetch full book details from Google Books and store in the DB."""
    resp = requests.get(f'{BOOKS_URL}/{book_id}', params={'key': BOOKS_API_KEY}, timeout=15)
    resp.raise_for_status()
    b = _parse_volume(resp.json())

    now = datetime.now(timezone.utc).isoformat()
    date_read_ts = date_read or (now if status in ('read', 'dropped') else None)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute('INSERT OR IGNORE INTO books (id, datetime_added) VALUES (?, ?)', (b['id'], now))
    cur.execute('''
        UPDATE books SET title=?, authors=?, publisher=?, published_date=?,
                         page_count=?, description=?, cover_url=?, status=?, date_read=?
        WHERE id=?
    ''', (
        b['title'], b['authors'], b['publisher'], b['published_date'],
        b['page_count'], b['description'], b['cover_url'], status, date_read_ts,
        b['id'],
    ))

    cur.execute('DELETE FROM book_genres WHERE book_id=?', (b['id'],))
    for cat in b['categories']:
        cur.execute('INSERT INTO book_genres (book_id, name) VALUES (?,?)', (b['id'], cat))

    conn.commit()
    conn.close()
    print(f"Stored: {b['title']} by {b['authors']}")
    return b['id']


def add_book(query, status='read', date_read=None, db_path='tmdb_analytics.db'):
    """Search for a book, prompt user to pick, then fetch and store."""
    results = search_book(query)

    print('Search results:')
    for i, r in enumerate(results):
        year = r['published_date'][:4] if r['published_date'] else '????'
        print(f'  [{i + 1}] {r["title"]} by {r["authors"]} ({year})')

    while True:
        choice = input(f'\nSelect a book [1-{len(results)}], or 0 / q to cancel: ').strip().lower()
        if choice in ('0', 'q', ''):
            print('Cancelled.')
            return None
        try:
            idx = int(choice)
            if 1 <= idx <= len(results):
                break
        except ValueError:
            pass

    selected = results[idx - 1]
    print(f'\nAdding: {selected["title"]}')
    return fetch_and_store_book(selected['id'], db_path, status=status, date_read=date_read)


def view_books(db_path='tmdb_analytics.db'):
    """Display all books in the DB."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        'SELECT title, authors, published_date, status, date_read FROM books ORDER BY datetime_added DESC'
    ).fetchall()
    conn.close()

    if not rows:
        print('No books stored yet.')
        return

    print(f'{"#":<5} {"Title":<40} {"Author":<25} {"Year":<6} {"Status":<12} {"Read"}')
    print('-' * 95)
    for i, (title, authors, pub_date, status, date_read) in enumerate(rows, 1):
        year = pub_date[:4] if pub_date else '????'
        read = date_read[:10] if date_read else '—'
        print(f'{i:<5} {title:<40} {authors:<25} {year:<6} {status:<12} {read}')
