import os
import sqlite3
from datetime import datetime, timezone

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, 'tmdbtoken.txt')) as f:
    TMDB_TOKEN = f.read().strip()

TMDB_HEADERS = {'Authorization': f'Bearer {TMDB_TOKEN}', 'accept': 'application/json'}


def setup_tv_db(db_path='tmdb_analytics.db'):
    """Create TV show tables. Safe to re-run."""
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS tv_shows (
            id                INTEGER PRIMARY KEY,
            name              TEXT,
            first_air_date    TEXT,
            poster_path       TEXT,
            overview          TEXT,
            vote_average      REAL,
            tmdb_status       TEXT,
            number_of_seasons INTEGER,
            watch_status      TEXT DEFAULT 'watching',
            datetime_added    TEXT,
            last_refreshed    TEXT,
            notes             TEXT
        );
        CREATE TABLE IF NOT EXISTS tv_show_genres (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER REFERENCES tv_shows(id),
            name    TEXT
        );
        CREATE TABLE IF NOT EXISTS tv_show_networks (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER REFERENCES tv_shows(id),
            name    TEXT
        );
        CREATE TABLE IF NOT EXISTS tv_show_creators (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER REFERENCES tv_shows(id),
            name    TEXT
        );
        CREATE TABLE IF NOT EXISTS tv_seasons (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id        INTEGER REFERENCES tv_shows(id),
            season_number  INTEGER,
            season_name    TEXT,
            episode_count  INTEGER,
            air_date       TEXT,
            date_completed TEXT,
            UNIQUE(show_id, season_number)
        );
    ''')
    conn.commit()
    conn.close()
    print('TV DB ready.')


def search_show(title):
    """Search TMDB for TV shows, return top 5."""
    resp = requests.get(
        'https://api.themoviedb.org/3/search/tv',
        headers=TMDB_HEADERS,
        params={'query': title},
        timeout=15,
    )
    resp.raise_for_status()
    results = resp.json().get('results', [])
    if not results:
        raise ValueError(f'No results found for "{title}"')
    return results[:5]


def fetch_and_store_show(tmdb_id, db_path='tmdb_analytics.db', watch_status='watching',
                         refresh_only=False):
    """Fetch full show details from TMDB and store in the DB."""
    resp = requests.get(
        f'https://api.themoviedb.org/3/tv/{tmdb_id}',
        headers=TMDB_HEADERS,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute('INSERT OR IGNORE INTO tv_shows (id, datetime_added) VALUES (?, ?)', (tmdb_id, now))
    if refresh_only:
        cur.execute('''
            UPDATE tv_shows SET name=?, first_air_date=?, poster_path=?, overview=?,
                                vote_average=?, tmdb_status=?, number_of_seasons=?,
                                last_refreshed=?
            WHERE id=?
        ''', (
            data.get('name'), data.get('first_air_date'), data.get('poster_path'),
            data.get('overview'), data.get('vote_average'), data.get('status'),
            data.get('number_of_seasons'), now, tmdb_id,
        ))
    else:
        cur.execute('''
            UPDATE tv_shows SET name=?, first_air_date=?, poster_path=?, overview=?,
                                vote_average=?, tmdb_status=?, number_of_seasons=?, watch_status=?,
                                last_refreshed=?
            WHERE id=?
        ''', (
            data.get('name'), data.get('first_air_date'), data.get('poster_path'),
            data.get('overview'), data.get('vote_average'), data.get('status'),
            data.get('number_of_seasons'), watch_status, now, tmdb_id,
        ))

    cur.execute('DELETE FROM tv_show_genres WHERE show_id=?', (tmdb_id,))
    for g in data.get('genres', []):
        cur.execute('INSERT INTO tv_show_genres (show_id, name) VALUES (?,?)', (tmdb_id, g['name']))

    cur.execute('DELETE FROM tv_show_networks WHERE show_id=?', (tmdb_id,))
    for n in data.get('networks', []):
        cur.execute('INSERT INTO tv_show_networks (show_id, name) VALUES (?,?)', (tmdb_id, n['name']))

    cur.execute('DELETE FROM tv_show_creators WHERE show_id=?', (tmdb_id,))
    for c in data.get('created_by', []):
        cur.execute('INSERT INTO tv_show_creators (show_id, name) VALUES (?,?)', (tmdb_id, c['name']))

    # Store seasons (skip season 0 = Specials)
    for season in data.get('seasons', []):
        if season.get('season_number', 0) == 0:
            continue
        cur.execute('''
            INSERT INTO tv_seasons (show_id, season_number, season_name, episode_count, air_date)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(show_id, season_number) DO UPDATE SET
                season_name=excluded.season_name,
                episode_count=excluded.episode_count,
                air_date=excluded.air_date
        ''', (
            tmdb_id,
            season.get('season_number'),
            season.get('name'),
            season.get('episode_count'),
            season.get('air_date'),
        ))

    conn.commit()
    conn.close()
    print(f"Stored: {data.get('name')} ({data.get('number_of_seasons')} seasons)")
    return tmdb_id


def add_show(title, watch_status='watching', db_path='tmdb_analytics.db'):
    """Search for a show, prompt user to pick, then fetch and store."""
    results = search_show(title)

    print('Search results:')
    for i, r in enumerate(results):
        year = r.get('first_air_date', '')[:4] or '????'
        print(f'  [{i + 1}] {r["name"]} ({year})')

    while True:
        choice = input(f'\nSelect a show [1-{len(results)}], or 0 / q to cancel: ').strip().lower()
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
    print(f'\nAdding: {selected["name"]}')
    return fetch_and_store_show(selected['id'], db_path, watch_status=watch_status)


def view_shows(db_path='tmdb_analytics.db'):
    """Display all TV shows in the DB."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        'SELECT name, first_air_date, watch_status, number_of_seasons FROM tv_shows ORDER BY datetime_added DESC'
    ).fetchall()
    conn.close()
    if not rows:
        print('No TV shows stored yet.')
        return
    print(f'{"#":<5} {"Title":<40} {"Year":<6} {"Status":<15} {"Seasons"}')
    print('-' * 75)
    for i, (name, air_date, status, seasons) in enumerate(rows, 1):
        year = air_date[:4] if air_date else '????'
        print(f'{i:<5} {name:<40} {year:<6} {status:<15} {seasons or "?"}')
