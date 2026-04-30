import csv as csv_module
import json
import re
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
import requests

with open('tmdbtoken.txt', 'r') as f:
    TMDB_TOKEN = f.read().strip()

TMDB_HEADERS = {'Authorization': f'Bearer {TMDB_TOKEN}', 'accept': 'application/json'}
WIKIDATA_HEADERS = {'User-Agent': 'MediaProjectAnalytics/1.0 (jbertucci131@gmail.com)'}

GENDER_MAP = {1: 'Female', 2: 'Male', 3: 'Non-Binary'}


def _parse_watched_date(date_str):
    """
    Parse a flexible date string into YYYY-MM-DDT00:00:00.
    Missing year defaults to the current year.
    Supported formats: YYYY-MM-DD, M/D/YY, M/D/YYYY, M/D (no year), Month D (no year).
    """
    current_year = datetime.now(timezone.utc).year

    for fmt in ('%Y-%m-%d', '%m/%d/%y', '%m/%d/%Y'):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime('%Y-%m-%dT00:00:00')
        except ValueError:
            pass

    for fmt in ('%m/%d', '%B %d', '%b %d'):
        try:
            return datetime.strptime(f'{date_str.strip()} {current_year}', f'{fmt} %Y').strftime('%Y-%m-%dT00:00:00')
        except ValueError:
            pass

    raise ValueError(f'Cannot parse date "{date_str}". Try formats like "2024-06-18", "6/18/24", or "6/18".')
WIKIDATA_SPARQL = 'https://query.wikidata.org/sparql'
WIKIDATA_BATCH_SIZE = 100


def setup_analytics_db(db_path='tmdb_analytics.db'):
    """Create normalized SQLite tables. Safe to re-run — migrates existing DBs."""
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS films (
            id                   INTEGER PRIMARY KEY,
            title                TEXT,
            original_title       TEXT,
            release_date         TEXT,
            runtime              INTEGER,
            budget               INTEGER,
            revenue              INTEGER,
            vote_average         REAL,
            vote_count           INTEGER,
            popularity           REAL,
            overview             TEXT,
            status               TEXT,
            datetime_added       TEXT,
            datetime_last_watched TEXT,
            watch_count          INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS genres (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            film_id INTEGER REFERENCES films(id),
            name    TEXT
        );
        CREATE TABLE IF NOT EXISTS cast_members (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            film_id       INTEGER REFERENCES films(id),
            person_id     INTEGER,
            name          TEXT,
            character     TEXT,
            billing_order INTEGER,
            gender        TEXT,
            ethnicity     TEXT,
            sexuality     TEXT
        );
        CREATE TABLE IF NOT EXISTS crew_members (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            film_id    INTEGER REFERENCES films(id),
            person_id  INTEGER,
            name       TEXT,
            job        TEXT,
            department TEXT,
            gender     TEXT,
            ethnicity  TEXT,
            sexuality  TEXT
        );
    ''')
    migrations = [
        ('films', 'datetime_added', 'TEXT'),
        ('films', 'datetime_last_watched', 'TEXT'),
        ('films', 'watch_count', 'INTEGER DEFAULT 0'),
        ('films', 'poster_path', 'TEXT'),
        ('films', 'rank', 'INTEGER'),
        ('films', 'date_ranked', 'TEXT'),
        ('films', 'notes', 'TEXT'),
        ('films', 'watch_status', "TEXT DEFAULT 'watched'"),
        ('cast_members', 'ethnicity', 'TEXT'),
        ('cast_members', 'sexuality', 'TEXT'),
        ('crew_members', 'ethnicity', 'TEXT'),
        ('crew_members', 'sexuality', 'TEXT'),
    ]
    for table, col, col_type in migrations:
        try:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()
    print('Analytics DB ready.')


def search_movie(title, year=None):
    """
    Search TMDB by title and return the top 5 results.
    A trailing 4-digit year in the title (e.g. 'aladdin 1992') is automatically
    extracted and used to filter results. An explicit year parameter takes precedence.
    """
    if year is None:
        match = re.search(r'\b((?:18|19|20)\d{2})\s*$', title.strip())
        if match:
            year = int(match.group(1))
            title = title[:match.start()].strip()

    params = {'query': title}
    if year:
        params['year'] = year

    resp = requests.get(
        'https://api.themoviedb.org/3/search/movie',
        headers=TMDB_HEADERS,
        params=params,
        timeout=15,
    )
    resp.raise_for_status()
    results = resp.json().get('results', [])
    if not results:
        raise ValueError(f'No results found for "{title}"' + (f' ({year})' if year else ''))
    return results[:5]


def fetch_and_store_movie(tmdb_id, db_path='tmdb_analytics.db',
                          first_watched=None, last_watched=None, watch_count=None):
    """Fetch full movie details + credits from TMDB and store in the analytics DB.

    first_watched / last_watched / watch_count are used by import_from_csv to inject
    historical dates. When omitted, normal add-movie behaviour applies (timestamp = now,
    watch_count increments).
    """
    resp = requests.get(
        f'https://api.themoviedb.org/3/movie/{tmdb_id}',
        headers=TMDB_HEADERS,
        params={'append_to_response': 'credits'},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    film_id = data['id']
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    now = datetime.now(timezone.utc).isoformat()
    first_ts = first_watched or now
    last_ts = last_watched or now

    # Seed the row only if it doesn't exist yet — preserves datetime_added on rewatches
    cur.execute(
        'INSERT OR IGNORE INTO films (id, datetime_added, watch_count) VALUES (?, ?, 0)',
        (film_id, first_ts)
    )

    # Refresh TMDB data and update watch tracking
    if watch_count is not None:
        # CSV import: set dates and count absolutely
        cur.execute('''
            UPDATE films SET
                title = ?, original_title = ?, release_date = ?, runtime = ?,
                budget = ?, revenue = ?, vote_average = ?, vote_count = ?,
                popularity = ?, overview = ?, status = ?, poster_path = ?,
                datetime_added = ?,
                datetime_last_watched = ?,
                watch_count = ?
            WHERE id = ?
        ''', (
            data.get('title'), data.get('original_title'), data.get('release_date'),
            data.get('runtime'), data.get('budget'), data.get('revenue'),
            data.get('vote_average'), data.get('vote_count'), data.get('popularity'),
            data.get('overview'), data.get('status'), data.get('poster_path'),
            first_ts, last_ts, watch_count,
            film_id,
        ))
    else:
        # Normal add: increment watch count
        cur.execute('''
            UPDATE films SET
                title = ?, original_title = ?, release_date = ?, runtime = ?,
                budget = ?, revenue = ?, vote_average = ?, vote_count = ?,
                popularity = ?, overview = ?, status = ?, poster_path = ?,
                datetime_last_watched = ?,
                watch_count = watch_count + 1
            WHERE id = ?
        ''', (
            data.get('title'), data.get('original_title'), data.get('release_date'),
            data.get('runtime'), data.get('budget'), data.get('revenue'),
            data.get('vote_average'), data.get('vote_count'), data.get('popularity'),
            data.get('overview'), data.get('status'), data.get('poster_path'),
            last_ts,
            film_id,
        ))

    cur.execute('DELETE FROM genres WHERE film_id = ?', (film_id,))
    for g in data.get('genres', []):
        cur.execute('INSERT INTO genres (film_id, name) VALUES (?,?)', (film_id, g['name']))

    credits = data.get('credits', {})

    cur.execute('DELETE FROM cast_members WHERE film_id = ?', (film_id,))
    for member in credits.get('cast', []):
        cur.execute('''
            INSERT INTO cast_members (film_id, person_id, name, character, billing_order, gender)
            VALUES (?,?,?,?,?,?)
        ''', (
            film_id,
            member.get('id'),
            member.get('name'),
            member.get('character'),
            member.get('order'),
            GENDER_MAP.get(member.get('gender'), 'Unknown'),
        ))

    cur.execute('DELETE FROM crew_members WHERE film_id = ?', (film_id,))
    for member in credits.get('crew', []):
        cur.execute('''
            INSERT INTO crew_members (film_id, person_id, name, job, department, gender)
            VALUES (?,?,?,?,?,?)
        ''', (
            film_id,
            member.get('id'),
            member.get('name'),
            member.get('job'),
            member.get('department'),
            GENDER_MAP.get(member.get('gender'), 'Unknown'),
        ))

    conn.commit()
    conn.close()
    print(f"Stored: {data.get('title')} ({data.get('release_date', '')[:4]})")
    return film_id


def fetch_wikidata_enrichment(film_id, db_path='tmdb_analytics.db'):
    """
    Look up ethnicity (P172) and sexuality (P91) for every cast and crew member
    via their TMDB person ID (P4985). One SPARQL round-trip per batch of 100 people.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cast_ids = [r[0] for r in cur.execute(
        'SELECT DISTINCT person_id FROM cast_members WHERE film_id = ? AND person_id IS NOT NULL',
        (film_id,)
    )]
    crew_ids = [r[0] for r in cur.execute(
        'SELECT DISTINCT person_id FROM crew_members WHERE film_id = ? AND person_id IS NOT NULL',
        (film_id,)
    )]
    all_ids = list(set(cast_ids + crew_ids))

    if not all_ids:
        conn.close()
        return

    enrichment_map = {}
    for i in range(0, len(all_ids), WIKIDATA_BATCH_SIZE):
        batch = all_ids[i:i + WIKIDATA_BATCH_SIZE]
        values_block = ' '.join(f'"{pid}"' for pid in batch)
        query = f'''
            SELECT ?tmdbId
                   (GROUP_CONCAT(DISTINCT ?ethnicityLabel; separator=", ") AS ?ethnicities)
                   (GROUP_CONCAT(DISTINCT ?sexualityLabel; separator=", ") AS ?sexualities)
            WHERE {{
                VALUES ?tmdbId {{ {values_block} }}
                ?person wdt:P4985 ?tmdbId .
                OPTIONAL {{
                    ?person wdt:P172 ?ethnicity .
                    ?ethnicity rdfs:label ?ethnicityLabel .
                    FILTER(LANG(?ethnicityLabel) = "en")
                }}
                OPTIONAL {{
                    ?person wdt:P91 ?sexuality .
                    ?sexuality rdfs:label ?sexualityLabel .
                    FILTER(LANG(?sexualityLabel) = "en")
                }}
            }}
            GROUP BY ?tmdbId
        '''
        resp = requests.get(
            WIKIDATA_SPARQL,
            params={'query': query, 'format': 'json'},
            headers=WIKIDATA_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()

        for binding in resp.json().get('results', {}).get('bindings', []):
            pid = int(binding['tmdbId']['value'])
            enrichment_map[pid] = {
                'ethnicity': binding.get('ethnicities', {}).get('value', '').strip() or None,
                'sexuality': binding.get('sexualities', {}).get('value', '').strip() or None,
            }

        if i + WIKIDATA_BATCH_SIZE < len(all_ids):
            time.sleep(1)

    for pid, fields in enrichment_map.items():
        cur.execute(
            'UPDATE cast_members SET ethnicity = ?, sexuality = ? WHERE film_id = ? AND person_id = ?',
            (fields['ethnicity'], fields['sexuality'], film_id, pid)
        )
        cur.execute(
            'UPDATE crew_members SET ethnicity = ?, sexuality = ? WHERE film_id = ? AND person_id = ?',
            (fields['ethnicity'], fields['sexuality'], film_id, pid)
        )

    matched = sum(1 for f in enrichment_map.values() if f['ethnicity'] or f['sexuality'])
    conn.commit()
    conn.close()
    print(f'Wikidata enrichment: {matched} of {len(all_ids)} people had ethnicity or sexuality data.')


def add_movie(title, date_watched=None, year=None, db_path='tmdb_analytics.db'):
    """
    Search for a movie by title, prompt the user to confirm from the top 5 results,
    then fetch full details and enrich with Wikidata data.

    title:         Movie title. A trailing release year (e.g. 'aladdin 1992') is parsed automatically.
    date_watched:  Date you watched the film. Accepts 'YYYY-MM-DD', 'M/D/YY', 'M/D/YYYY', or
                   'M/D' / 'Month D' (year defaults to current year). Time defaults to start of day.
                   Omit to use the current date and time.
    year:          Film release year for search disambiguation (rarely needed).
    """
    results = search_movie(title, year=year)

    print('Search results:')
    for i, r in enumerate(results):
        release_year = r.get('release_date', '')[:4] or '????'
        print(f'  [{i + 1}] {r["title"]} ({release_year})')

    while True:
        choice = input(f'\nSelect a film [1-{len(results)}], or 0 / q to cancel: ').strip().lower()
        if choice in ('0', 'q', 'n', ''):
            print('Cancelled.')
            return None
        try:
            idx = int(choice)
            if 1 <= idx <= len(results):
                break
        except ValueError:
            pass
        print(f'Please enter a number between 1 and {len(results)}, or 0 to cancel.')

    watch_ts = _parse_watched_date(date_watched) if date_watched else datetime.now(timezone.utc).isoformat()

    selected = results[idx - 1]
    print(f'\nAdding: {selected["title"]} ({selected.get("release_date", "")[:4]})')
    film_id = fetch_and_store_movie(selected['id'], db_path, first_watched=watch_ts, last_watched=watch_ts)
    fetch_wikidata_enrichment(film_id, db_path)
    return film_id


def import_from_csv(csv_path, db_path='tmdb_analytics.db'):
    """
    Import watch history from a CSV with 'Date Watched', 'Title', and 'Year' columns.
    Rows without a date are treated as want-to-watch entries and skipped.
    Each unique title gets one TMDB API call; watch counts and dates come from the CSV.
    """
    entries = []
    skipped = []

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv_module.DictReader(f)
        for row in reader:
            raw_date = row.get('Date Watched', '').strip()
            title = row.get('Title', '').strip()
            release_year = row.get('Year', '').strip()

            if not title or not raw_date:
                continue

            parsed_date = None
            for fmt in ('%m/%d/%y', '%m/%d/%Y'):
                try:
                    parsed_date = datetime.strptime(raw_date, fmt)
                    break
                except ValueError:
                    pass

            if parsed_date is None:
                skipped.append((title, raw_date))
                continue

            entries.append({
                'title': title,
                'date': parsed_date,
                'release_year': release_year if release_year.isdigit() else None,
            })

    if not entries:
        print('No valid entries found in CSV.')
        return

    entries.sort(key=lambda e: e['date'])

    title_groups = defaultdict(list)
    for entry in entries:
        title_groups[entry['title']].append(entry)

    print(f'Found {len(title_groups)} unique films across {len(entries)} watch entries.')
    if skipped:
        print(f'Skipping {len(skipped)} entries with unparseable dates:')
        for t, d in skipped:
            print(f'  - "{t}": "{d}"')
    print()

    succeeded, failed = [], []
    for title, watches in title_groups.items():
        first_watch = watches[0]['date'].strftime('%Y-%m-%d')
        last_watch = watches[-1]['date'].strftime('%Y-%m-%d')
        count = len(watches)
        release_year = next((w['release_year'] for w in watches if w['release_year']), None)

        try:
            results = search_movie(title, year=release_year)
            top = results[0]
            matched_title = top.get('title', top.get('original_title'))
            matched_year = top.get('release_date', '')[:4]
            print(f'  "{title}" → {matched_title} ({matched_year})  [x{count}]')

            film_id = fetch_and_store_movie(
                top['id'], db_path,
                first_watched=first_watch,
                last_watched=last_watch,
                watch_count=count,
            )
            fetch_wikidata_enrichment(film_id, db_path)
            succeeded.append(title)
            time.sleep(0.25)
        except Exception as e:
            print(f'  ✗ "{title}" — {e}')
            failed.append((title, str(e)))

    print(f'\nImport complete: {len(succeeded)} succeeded, {len(failed)} failed.')
    if failed:
        print('Failed titles:')
        for title, err in failed:
            print(f'  - "{title}": {err}')


def remove_movie(title, db_path='tmdb_analytics.db'):
    """
    Search the local DB for films matching the title, prompt the user to pick one,
    then delete it and all associated genres, cast, and crew records.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, title, release_date FROM films WHERE title LIKE ? ORDER BY release_date",
        (f'%{title}%',)
    ).fetchall()

    if not rows:
        print(f'No films matching "{title}" found in the database.')
        conn.close()
        return

    print('Matching films:')
    for i, (film_id, film_title, release_date) in enumerate(rows, 1):
        year = release_date[:4] if release_date else '????'
        print(f'  [{i}] {film_title} ({year})')

    while True:
        choice = input(f'\nSelect a film to remove [1-{len(rows)}], or 0 / q to cancel: ').strip().lower()
        if choice in ('0', 'q', 'n', ''):
            print('Cancelled.')
            conn.close()
            return
        try:
            idx = int(choice)
            if 1 <= idx <= len(rows):
                break
        except ValueError:
            pass
        print(f'Please enter a number between 1 and {len(rows)}, or 0 to cancel.')

    film_id, film_title, release_date = rows[idx - 1]
    year = release_date[:4] if release_date else '????'
    confirm = input(f'\nRemove "{film_title} ({year})"? [y/N]: ').strip().lower()
    if confirm != 'y':
        print('Cancelled.')
        conn.close()
        return

    cur.execute('DELETE FROM genres WHERE film_id = ?', (film_id,))
    cur.execute('DELETE FROM cast_members WHERE film_id = ?', (film_id,))
    cur.execute('DELETE FROM crew_members WHERE film_id = ?', (film_id,))
    cur.execute('DELETE FROM films WHERE id = ?', (film_id,))
    conn.commit()
    conn.close()
    print(f'Removed: {film_title} ({year})')


def view_movies(db_path='tmdb_analytics.db'):
    """Display all films currently stored in the analytics DB."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute(
        '''SELECT title, release_date, datetime_added, datetime_last_watched, watch_count
           FROM films ORDER BY datetime_added'''
    ).fetchall()
    conn.close()

    if not rows:
        print('No films stored yet.')
        return

    print(f'{"#":<5} {"Title":<45} {"Year":<6} {"Added (UTC)":<18} {"Last Watched (UTC)":<18} {"Watches"}')
    print('-' * 100)
    for i, (title, release_date, datetime_added, datetime_last_watched, watch_count) in enumerate(rows, 1):
        year = release_date[:4] if release_date else '????'
        added = datetime_added[:16].replace('T', ' ') if datetime_added else '—'
        last = datetime_last_watched[:16].replace('T', ' ') if datetime_last_watched else '—'
        print(f'{i:<5} {title:<45} {year:<6} {added:<18} {last:<18} {watch_count or 0}')
