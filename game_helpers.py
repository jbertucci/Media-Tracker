import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, 'igdb_credentials.txt')) as f:
    lines = f.read().strip().splitlines()
    IGDB_CLIENT_ID = lines[0].strip()
    IGDB_CLIENT_SECRET = lines[1].strip()

TOKEN_CACHE_PATH = os.path.join(BASE_DIR, 'igdb_token_cache.json')
WIKIDATA_SPARQL = 'https://query.wikidata.org/sparql'
WIKIDATA_HEADERS = {'User-Agent': 'MediaProjectAnalytics/1.0 (jbertucci131@gmail.com)'}


def _igdb_token():
    if os.path.exists(TOKEN_CACHE_PATH):
        with open(TOKEN_CACHE_PATH) as f:
            cache = json.load(f)
        if cache.get('expires_at', 0) > time.time() + 60:
            return cache['access_token']

    resp = requests.post(
        'https://id.twitch.tv/oauth2/token',
        params={
            'client_id': IGDB_CLIENT_ID,
            'client_secret': IGDB_CLIENT_SECRET,
            'grant_type': 'client_credentials',
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    cache = {'access_token': data['access_token'], 'expires_at': time.time() + data['expires_in']}
    with open(TOKEN_CACHE_PATH, 'w') as f:
        json.dump(cache, f)
    return cache['access_token']


def _igdb_headers():
    return {
        'Client-ID': IGDB_CLIENT_ID,
        'Authorization': f'Bearer {_igdb_token()}',
    }


def setup_games_db(db_path='tmdb_analytics.db'):
    """Create game tables. Safe to re-run."""
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS games (
            id                 INTEGER PRIMARY KEY,
            name               TEXT,
            first_release_date TEXT,
            status             TEXT DEFAULT 'completed',
            date_completed     TEXT,
            datetime_added     TEXT,
            summary            TEXT,
            cover_image_id     TEXT,
            lead_writer        TEXT,
            composer           TEXT,
            rating             REAL,
            completed_fully    INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS game_genres (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER REFERENCES games(id),
            name    TEXT
        );
        CREATE TABLE IF NOT EXISTS game_developers (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER REFERENCES games(id),
            name    TEXT
        );
        CREATE TABLE IF NOT EXISTS game_publishers (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER REFERENCES games(id),
            name    TEXT
        );
        CREATE TABLE IF NOT EXISTS game_perspectives (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER REFERENCES games(id),
            name    TEXT
        );
    ''')
    try:
        conn.execute('ALTER TABLE games ADD COLUMN completed_fully INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()
    print('Games DB ready.')


def search_game(title):
    """Search IGDB by title, return top 5 results."""
    escaped = title.replace('"', '\\"')
    resp = requests.post(
        'https://api.igdb.com/v4/games',
        headers=_igdb_headers(),
        data=f'fields name, first_release_date, cover.image_id, genres.name, summary; '
             f'search "{escaped}"; limit 5;',
        timeout=15,
    )
    resp.raise_for_status()
    results = resp.json()
    if not results:
        raise ValueError(f'No results found for "{title}"')
    return results


def fetch_and_store_game(igdb_id, db_path='tmdb_analytics.db', status='completed', date_completed=None, completed_fully=False):
    """Fetch full game details from IGDB and store in the DB."""
    resp = requests.post(
        'https://api.igdb.com/v4/games',
        headers=_igdb_headers(),
        data=f'fields name, first_release_date, cover.image_id, genres.name, summary, rating, '
             f'involved_companies.developer, involved_companies.publisher, involved_companies.company.name, '
             f'player_perspectives.name; '
             f'where id = {igdb_id};',
        timeout=15,
    )
    resp.raise_for_status()
    results = resp.json()
    if not results:
        raise ValueError(f'No game found with IGDB id {igdb_id}')
    data = results[0]

    now = datetime.now(timezone.utc).isoformat()

    release_date = None
    if 'first_release_date' in data:
        release_date = datetime.fromtimestamp(
            data['first_release_date'], tz=timezone.utc
        ).strftime('%Y-%m-%d')

    completed_ts = date_completed or (now if status in ('completed', 'dropped') else None)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute('INSERT OR IGNORE INTO games (id, datetime_added) VALUES (?, ?)', (igdb_id, now))
    cur.execute('''
        UPDATE games SET name=?, first_release_date=?, status=?, date_completed=?,
                         summary=?, cover_image_id=?, rating=?, completed_fully=?
        WHERE id=?
    ''', (
        data.get('name'), release_date, status, completed_ts,
        data.get('summary'), data.get('cover', {}).get('image_id'),
        data.get('rating'), 1 if completed_fully else 0, igdb_id,
    ))

    cur.execute('DELETE FROM game_genres WHERE game_id=?', (igdb_id,))
    for g in data.get('genres', []):
        cur.execute('INSERT INTO game_genres (game_id, name) VALUES (?,?)', (igdb_id, g['name']))

    cur.execute('DELETE FROM game_developers WHERE game_id=?', (igdb_id,))
    cur.execute('DELETE FROM game_publishers WHERE game_id=?', (igdb_id,))
    for ic in data.get('involved_companies', []):
        name = ic.get('company', {}).get('name')
        if not name:
            continue
        if ic.get('developer'):
            cur.execute('INSERT INTO game_developers (game_id, name) VALUES (?,?)', (igdb_id, name))
        if ic.get('publisher'):
            cur.execute('INSERT INTO game_publishers (game_id, name) VALUES (?,?)', (igdb_id, name))

    cur.execute('DELETE FROM game_perspectives WHERE game_id=?', (igdb_id,))
    for p in data.get('player_perspectives', []):
        cur.execute('INSERT INTO game_perspectives (game_id, name) VALUES (?,?)', (igdb_id, p['name']))

    conn.commit()
    conn.close()
    print(f"Stored: {data.get('name')} ({release_date[:4] if release_date else '????'})")
    return igdb_id


def fetch_wikidata_game_enrichment(game_id, db_path='tmdb_analytics.db'):
    """Look up composer and lead writer for a game via its IGDB ID on Wikidata."""
    query = f'''
        SELECT (GROUP_CONCAT(DISTINCT ?composerLabel; separator=", ") AS ?composers)
               (GROUP_CONCAT(DISTINCT ?writerLabel;   separator=", ") AS ?writers)
        WHERE {{
            ?game wdt:P5794 "{game_id}" .
            OPTIONAL {{
                ?game wdt:P86 ?composer .
                ?composer rdfs:label ?composerLabel .
                FILTER(LANG(?composerLabel) = "en")
            }}
            OPTIONAL {{
                ?game wdt:P58 ?writer .
                ?writer rdfs:label ?writerLabel .
                FILTER(LANG(?writerLabel) = "en")
            }}
        }}
    '''
    try:
        resp = requests.get(
            WIKIDATA_SPARQL,
            params={'query': query, 'format': 'json'},
            headers=WIKIDATA_HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        bindings = resp.json().get('results', {}).get('bindings', [])
        if not bindings:
            return
        row = bindings[0]
        composer = row.get('composers', {}).get('value') or None
        writer   = row.get('writers',   {}).get('value') or None
        if composer or writer:
            conn = sqlite3.connect(db_path)
            conn.execute(
                'UPDATE games SET composer=?, lead_writer=? WHERE id=?',
                (composer, writer, game_id)
            )
            conn.commit()
            conn.close()
            print(f'Wikidata game enrichment: composer={composer}, writer={writer}')
    except Exception as e:
        print(f'Wikidata game enrichment failed: {e}')


def add_game(title, status='completed', date_completed=None, db_path='tmdb_analytics.db'):
    """Search for a game, prompt user to pick, then fetch and store."""
    results = search_game(title)

    print('Search results:')
    for i, r in enumerate(results):
        year = '????'
        if 'first_release_date' in r:
            year = datetime.fromtimestamp(r['first_release_date'], tz=timezone.utc).strftime('%Y')
        print(f'  [{i + 1}] {r["name"]} ({year})')

    while True:
        choice = input(f'\nSelect a game [1-{len(results)}], or 0 / q to cancel: ').strip().lower()
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
    game_id = fetch_and_store_game(
        selected['id'], db_path, status=status, date_completed=date_completed
    )
    fetch_wikidata_game_enrichment(game_id, db_path)
    return game_id


def view_games(db_path='tmdb_analytics.db'):
    """Display all games in the DB."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        'SELECT name, first_release_date, status, date_completed FROM games ORDER BY datetime_added DESC'
    ).fetchall()
    conn.close()

    if not rows:
        print('No games stored yet.')
        return

    print(f'{"#":<5} {"Title":<40} {"Year":<6} {"Status":<15} {"Completed"}')
    print('-' * 80)
    for i, (name, release_date, status, date_completed) in enumerate(rows, 1):
        year      = release_date[:4] if release_date else '????'
        completed = date_completed[:10] if date_completed else '—'
        print(f'{i:<5} {name:<40} {year:<6} {status:<15} {completed}')
