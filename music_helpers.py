import os
import sqlite3
import time
from datetime import datetime, timezone

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MB_BASE  = 'https://musicbrainz.org/ws/2'
CAA_BASE = 'https://coverartarchive.org'

MB_HEADERS = {
    'User-Agent': 'MediaTracker/1.0 (personal-project)',
    'Accept':     'application/json',
}


def setup_music_db(db_path='tmdb_analytics.db'):
    """Create music tables. Safe to re-run."""
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS albums (
            id             TEXT PRIMARY KEY,
            title          TEXT,
            artist         TEXT,
            release_date   TEXT,
            release_type   TEXT,
            cover_url      TEXT,
            status         TEXT DEFAULT 'listened',
            listen_count   INTEGER DEFAULT 0,
            date_listened  TEXT,
            datetime_added TEXT,
            notes          TEXT
        );
        CREATE TABLE IF NOT EXISTS album_genres (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            album_id TEXT REFERENCES albums(id),
            name     TEXT
        );
    ''')
    conn.commit()
    conn.close()
    print('Music DB ready.')


def _parse_rg(rg):
    """Extract fields from a MusicBrainz release-group dict."""
    artist = ', '.join(
        ac.get('name') or ac.get('artist', {}).get('name', '')
        for ac in rg.get('artist-credit', [])
        if isinstance(ac, dict) and 'artist' in ac
    )
    genres = [g['name'] for g in rg.get('genres', [])]
    if not genres:
        tags = sorted(rg.get('tags', []), key=lambda t: t.get('count', 0), reverse=True)
        genres = [t['name'] for t in tags[:5]]
    return {
        'id':           rg['id'],
        'title':        rg.get('title', 'Unknown'),
        'artist':       artist,
        'release_date': rg.get('first-release-date'),
        'release_type': rg.get('primary-type'),
        'cover_url':    f"{CAA_BASE}/release-group/{rg['id']}/front-250",
        'genres':       genres,
    }


def search_album(query, release_type='album'):
    """Search MusicBrainz release-groups, return top 5 results."""
    resp = requests.get(
        f'{MB_BASE}/release-group',
        headers=MB_HEADERS,
        params={'query': query, 'fmt': 'json', 'limit': 5, 'type': release_type},
        timeout=15,
    )
    resp.raise_for_status()
    results = resp.json().get('release-groups', [])
    if not results:
        raise ValueError(f'No results found for "{query}"')
    return [_parse_rg(rg) for rg in results]


def fetch_and_store_album(mbid, db_path='tmdb_analytics.db', status='listened', date_listened=None):
    """Fetch full release-group details from MusicBrainz and store in DB."""
    resp = requests.get(
        f'{MB_BASE}/release-group/{mbid}',
        headers=MB_HEADERS,
        params={'fmt': 'json', 'inc': 'artists+genres+tags'},
        timeout=15,
    )
    resp.raise_for_status()
    a = _parse_rg(resp.json())

    now = datetime.now(timezone.utc).isoformat()
    date_listened_ts = date_listened or (now if status in ('listened', 'dropped') else None)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('INSERT OR IGNORE INTO albums (id, datetime_added) VALUES (?, ?)', (a['id'], now))
    cur.execute('''
        UPDATE albums SET title=?, artist=?, release_date=?, release_type=?, cover_url=?,
            status=?, date_listened=?, listen_count=listen_count+1
        WHERE id=?
    ''', (a['title'], a['artist'], a['release_date'], a['release_type'], a['cover_url'],
          status, date_listened_ts, a['id']))

    cur.execute('DELETE FROM album_genres WHERE album_id=?', (a['id'],))
    seen = set()
    for g in a['genres']:
        g = g.strip()
        if g and g not in seen:
            cur.execute('INSERT INTO album_genres (album_id, name) VALUES (?,?)', (a['id'], g))
            seen.add(g)

    conn.commit()
    conn.close()
    print(f"Stored: {a['title']} by {a['artist']}")
    return a['id']


def add_album(query, status='listened', date_listened=None, db_path='tmdb_analytics.db'):
    """Search for an album, prompt user to pick, then fetch and store."""
    results = search_album(query)

    print('Search results:')
    for i, r in enumerate(results):
        year = r['release_date'][:4] if r['release_date'] else '????'
        print(f'  [{i+1}] {r["title"]} by {r["artist"]} ({year})')

    while True:
        choice = input(f'\nSelect an album [1-{len(results)}], or 0 / q to cancel: ').strip().lower()
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
    return fetch_and_store_album(selected['id'], db_path, status=status, date_listened=date_listened)


def view_albums(db_path='tmdb_analytics.db'):
    """Display all albums in the DB."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        'SELECT title, artist, release_date, status, date_listened FROM albums ORDER BY datetime_added DESC'
    ).fetchall()
    conn.close()
    if not rows:
        print('No albums stored yet.')
        return
    print(f'{"#":<5} {"Title":<40} {"Artist":<25} {"Year":<6} {"Status":<14} {"Listened"}')
    print('-' * 100)
    for i, (title, artist, rel_date, status, date_listened) in enumerate(rows, 1):
        year     = rel_date[:4] if rel_date else '????'
        listened = date_listened[:10] if date_listened else '—'
        print(f'{i:<5} {title:<40} {artist:<25} {year:<6} {status:<14} {listened}')
