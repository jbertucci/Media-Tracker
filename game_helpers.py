import csv
import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, 'credentials', 'igdb_credentials.txt')) as f:
    lines = f.read().strip().splitlines()
    IGDB_CLIENT_ID = lines[0].strip()
    IGDB_CLIENT_SECRET = lines[1].strip()

TOKEN_CACHE_PATH = os.path.join(BASE_DIR, 'credentials', 'igdb_token_cache.json')
WIKIDATA_SPARQL = 'https://query.wikidata.org/sparql'

_contact_path = os.path.join(BASE_DIR, 'credentials', 'contact_email.txt')
_contact = open(_contact_path).read().strip() if os.path.exists(_contact_path) else 'unknown'
WIKIDATA_HEADERS = {'User-Agent': f'MediaProjectAnalytics/1.0 ({_contact})'}


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
    for col, col_type in [
        ('completed_fully', 'INTEGER DEFAULT 0'),
        ('rank',            'INTEGER'),
        ('date_ranked',     'TEXT'),
        ('notes',           'TEXT'),
    ]:
        try:
            conn.execute(f'ALTER TABLE games ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()
    print('Games DB ready.')


def search_game(title):
    """Search IGDB by title, return top 5 results. If title is all digits, look up by IGDB ID."""
    if title.isdigit():
        resp = requests.post(
            'https://api.igdb.com/v4/games',
            headers=_igdb_headers(),
            data=f'fields name, first_release_date, cover.image_id, genres.name, summary; where id = {title};',
            timeout=15,
        )
        resp.raise_for_status()
        results = resp.json()
        if not results:
            raise ValueError(f'No game found with IGDB id {title}')
        return results

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


def import_games_from_csv(csv_path, db_path='tmdb_analytics.db'):
    """
    Import completed games from a CSV with 'Date completed', 'Title', and '100%' columns.
    Missing years are inferred from the surrounding rows.
    """
    raw_entries = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            title = row.get('Title', '').strip()
            if title:
                raw_entries.append({
                    'title':   title,
                    'raw_date': row.get('Date completed', '').strip(),
                    'fully':   row.get('100%', '').strip().lower() == 'yes',
                })

    entries, skipped = [], []
    prev_date = None
    for entry in raw_entries:
        raw = entry['raw_date']
        parsed = None

        for fmt in ('%m/%d/%y', '%m/%d/%Y'):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                pass

        if parsed is None:
            try:
                md = datetime.strptime(raw, '%m/%d')
                # Infer year: if this month is before the previous entry's month, roll to next year
                if prev_date is None:
                    year = datetime.now(timezone.utc).year
                elif md.month < prev_date.month:
                    year = prev_date.year + 1
                else:
                    year = prev_date.year
                parsed = md.replace(year=year)
            except ValueError:
                skipped.append((entry['title'], raw))
                continue

        prev_date = parsed
        entries.append({'title': entry['title'], 'date': parsed, 'fully': entry['fully']})

    print(f'Found {len(entries)} games to import.')
    if skipped:
        print(f'Skipping {len(skipped)} unparseable entries:')
        for t, d in skipped:
            print(f'  - "{t}": "{d}"')
    print()

    succeeded, failed = [], []
    for entry in entries:
        title    = entry['title']
        date_str = entry['date'].strftime('%Y-%m-%d')
        fully    = entry['fully']
        try:
            results = search_game(title)
            top     = results[0]
            year    = '????'
            if 'first_release_date' in top:
                year = datetime.fromtimestamp(top['first_release_date'], tz=timezone.utc).strftime('%Y')
            print(f'  "{title}" → {top["name"]} ({year}){" [100%]" if fully else ""}')
            game_id = fetch_and_store_game(
                top['id'], db_path,
                status='completed',
                date_completed=date_str,
                completed_fully=fully,
            )
            fetch_wikidata_game_enrichment(game_id, db_path)
            succeeded.append(title)
            time.sleep(0.3)
        except Exception as e:
            print(f'  ✗ "{title}" — {e}')
            failed.append((title, str(e)))

    print(f'\nImport complete: {len(succeeded)} succeeded, {len(failed)} failed.')
    if failed:
        print('Failed titles:')
        for title, err in failed:
            print(f'  - "{title}": {err}')


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
