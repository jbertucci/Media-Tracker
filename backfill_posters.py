import sqlite3
import time
import requests

with open('tmdbtoken.txt') as f:
    TMDB_TOKEN = f.read().strip()

HEADERS = {'Authorization': f'Bearer {TMDB_TOKEN}', 'accept': 'application/json'}
DB_PATH = 'tmdb_analytics.db'

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

rows = cur.execute('SELECT id, title FROM films WHERE poster_path IS NULL').fetchall()
print(f'{len(rows)} films need backfilling.\n')

ok, fail = 0, 0
for film_id, title in rows:
    try:
        resp = requests.get(
            f'https://api.themoviedb.org/3/movie/{film_id}',
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        poster = resp.json().get('poster_path')
        cur.execute('UPDATE films SET poster_path = ? WHERE id = ?', (poster, film_id))
        print(f'  ✓ {title}')
        ok += 1
    except Exception as e:
        print(f'  ✗ {title} — {e}')
        fail += 1
    time.sleep(0.25)

conn.commit()
conn.close()
print(f'\nDone: {ok} updated, {fail} failed.')
