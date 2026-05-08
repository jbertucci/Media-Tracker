import os
import random
import sqlite3
import threading
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify, request, send_from_directory

from tmdb_helpers import (
    _parse_watched_date,
    fetch_and_store_movie,
    fetch_wikidata_enrichment,
    search_movie,
)
from game_helpers import (
    fetch_and_store_game,
    fetch_wikidata_game_enrichment,
    search_game,
    setup_games_db,
)
from book_helpers import (
    fetch_and_store_book,
    search_book,
    setup_books_db,
)
from tv_helpers import (
    fetch_and_store_show,
    search_show,
    setup_tv_db,
)
from music_helpers import (
    fetch_and_store_album,
    search_album,
    setup_music_db,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'tmdb_analytics.db')

app = Flask(__name__)


def _load_api_key():
    key_file = os.path.join(BASE_DIR, 'flask_api_key.txt')
    if os.path.exists(key_file):
        with open(key_file) as f:
            return f.read().strip()
    key = os.environ.get('MEDIA_API_KEY', '').strip()
    if key:
        return key
    raise RuntimeError(
        'No API key found. Create api_key.txt with a secret string, '
        'or set the MEDIA_API_KEY environment variable.'
    )


API_KEY = _load_api_key()


def _ensure_battle_table():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS battle_state (
            id            INTEGER PRIMARY KEY DEFAULT 1,
            challenger_id INTEGER,
            opponent_id   INTEGER
        )
    ''')
    for col, col_type in [
        ('rank',        'INTEGER'),
        ('date_ranked', 'TEXT'),
        ('notes',       'TEXT'),
        ('watch_status', "TEXT DEFAULT 'watched'"),
    ]:
        try:
            conn.execute(f'ALTER TABLE films ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass
    conn.execute("UPDATE films SET watch_status = 'watched' WHERE watch_status IS NULL")
    conn.commit()
    conn.close()

_ensure_battle_table()
setup_games_db(DB_PATH)


def _ensure_game_battle_table():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS game_battle_state (
            id            INTEGER PRIMARY KEY DEFAULT 1,
            challenger_id INTEGER,
            opponent_id   INTEGER
        )
    ''')
    conn.commit()
    conn.close()

_ensure_game_battle_table()
setup_books_db(DB_PATH)
setup_tv_db(DB_PATH)
setup_music_db(DB_PATH)

# Migrate last_refreshed columns for existing DBs
_conn = sqlite3.connect(DB_PATH)
for _migration in [
    'ALTER TABLE tv_shows ADD COLUMN last_refreshed TEXT',
    'ALTER TABLE films ADD COLUMN last_refreshed TEXT',
    'ALTER TABLE albums ADD COLUMN release_type TEXT',
    'ALTER TABLE albums ADD COLUMN listen_count INTEGER DEFAULT 0',
]:
    try:
        _conn.execute(_migration)
        _conn.commit()
    except sqlite3.OperationalError:
        pass
_conn.close()

_TV_REFRESH_STATUSES = {'Returning Series', 'In Production', 'Planned', 'Pilot'}
_TV_REFRESH_DAYS = 7
_TV_CHECK_INTERVAL = 60 * 60  # re-check hourly


VALID_SEASON = "(episode_count IS NULL OR episode_count > 0) AND (air_date IS NULL OR air_date <= date('now'))"
_MOVIE_REFRESH_STATUSES = {'In Production', 'Post Production', 'Planned', 'Rumored'}


def _run_tv_refresh(force=False):
    import time
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_TV_REFRESH_DAYS)).isoformat()
    placeholders = ','.join('?' * len(_TV_REFRESH_STATUSES))
    conn = sqlite3.connect(DB_PATH)
    query = (
        f'SELECT t.id, t.name, t.watch_status,'
        f' (SELECT COUNT(*) FROM tv_seasons s WHERE s.show_id=t.id AND {VALID_SEASON}) as valid_seasons'
        f' FROM tv_shows t WHERE t.tmdb_status IN ({placeholders})'
    )
    params = list(_TV_REFRESH_STATUSES)
    if not force:
        query += ' AND (t.last_refreshed IS NULL OR t.last_refreshed < ?)'
        params.append(cutoff)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    count = 0
    for show_id, name, watch_status, old_seasons in rows:
        try:
            fetch_and_store_show(show_id, DB_PATH, refresh_only=True)
            if watch_status == 'off_season':
                conn2 = sqlite3.connect(DB_PATH)
                row = conn2.execute(
                    f'SELECT COUNT(*) FROM tv_seasons WHERE show_id=? AND {VALID_SEASON}',
                    (show_id,)
                ).fetchone()
                new_seasons = row[0] if row else old_seasons
                if new_seasons and new_seasons > (old_seasons or 0):
                    conn2.execute(
                        "UPDATE tv_shows SET watch_status='on_hold' WHERE id=?", (show_id,)
                    )
                    conn2.commit()
                    print(f'[TV refresh] {name}: new season detected, status → On Hold')
                conn2.close()
            print(f'[TV refresh] Updated: {name}')
            count += 1
        except Exception as e:
            print(f'[TV refresh] Failed for {name}: {e}')
        time.sleep(2)
    return count


def _run_movie_refresh(force=False):
    import time
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_TV_REFRESH_DAYS)).isoformat()
    placeholders = ','.join('?' * len(_MOVIE_REFRESH_STATUSES))
    conn = sqlite3.connect(DB_PATH)
    query = (
        f"SELECT id, title FROM films WHERE watch_status='want_to_watch'"
        f' AND status IN ({placeholders})'
    )
    params = list(_MOVIE_REFRESH_STATUSES)
    if not force:
        query += ' AND (last_refreshed IS NULL OR last_refreshed < ?)'
        params.append(cutoff)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    count = 0
    for film_id, title in rows:
        try:
            fetch_and_store_movie(film_id, DB_PATH, refresh_only=True)
            print(f'[Movie refresh] Updated: {title}')
            count += 1
        except Exception as e:
            print(f'[Movie refresh] Failed for {title}: {e}')
        time.sleep(2)
    return count


def _tv_refresh_worker():
    import time
    while True:
        time.sleep(_TV_CHECK_INTERVAL)
        try:
            _run_tv_refresh()
        except Exception as e:
            print(f'[TV refresh] Worker error: {e}')


def _movie_refresh_worker():
    import time
    while True:
        time.sleep(_TV_CHECK_INTERVAL)
        try:
            _run_movie_refresh()
        except Exception as e:
            print(f'[Movie refresh] Worker error: {e}')


threading.Thread(target=_tv_refresh_worker, daemon=True).start()
threading.Thread(target=_movie_refresh_worker, daemon=True).start()


def _auth():
    key = request.headers.get('X-API-Key') or request.args.get('api_key')
    return key == API_KEY


@app.route('/')
def index():
    return send_from_directory(BASE_DIR, 'index.html')


@app.route('/search')
def search():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    title = request.args.get('title', '').strip()
    if not title:
        return jsonify({'error': 'title is required'}), 400
    year = request.args.get('year')
    try:
        results = search_movie(title, year=int(year) if year else None)
        return jsonify([
            {
                'id': r['id'],
                'title': r['title'],
                'year': r.get('release_date', '')[:4] or None,
                'overview': r.get('overview', ''),
                'poster_path': r.get('poster_path'),
            }
            for r in results
        ])
    except ValueError as e:
        return jsonify({'error': str(e)}), 404


@app.route('/add', methods=['POST'])
def add():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    tmdb_id = data.get('tmdb_id')
    if not tmdb_id:
        return jsonify({'error': 'tmdb_id is required'}), 400

    watch_ts = None
    date_watched = data.get('date_watched')
    if date_watched:
        try:
            watch_ts = _parse_watched_date(date_watched)
        except ValueError as e:
            return jsonify({'error': str(e)}), 400

    status = (data.get('status') or 'watched').strip()
    try:
        film_id = fetch_and_store_movie(
            tmdb_id,
            db_path=DB_PATH,
            first_watched=watch_ts,
            last_watched=watch_ts,
        )
        if status == 'want_to_watch':
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "UPDATE films SET watch_status='want_to_watch', watch_count=0, datetime_last_watched=NULL WHERE id=?",
                (film_id,)
            )
            conn.commit()
            conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # Wikidata enrichment can take 10-30s — run it in the background
    threading.Thread(
        target=fetch_wikidata_enrichment,
        args=(film_id, DB_PATH),
        daemon=True,
    ).start()

    return jsonify({'ok': True, 'film_id': film_id})


@app.route('/movies')
def movies():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    status_filter = request.args.get('status', '').strip()
    conn = sqlite3.connect(DB_PATH)
    base = '''SELECT id, title, release_date, datetime_last_watched, watch_count, poster_path,
                     CASE WHEN notes IS NOT NULL AND notes != '' THEN 1 ELSE 0 END as has_notes,
                     COALESCE(watch_status, 'watched') as status,
                     status as tmdb_status
              FROM films'''
    if status_filter:
        rows = conn.execute(
            base + " WHERE COALESCE(watch_status,'watched')=? ORDER BY datetime_last_watched DESC",
            (status_filter,)
        ).fetchall()
    else:
        rows = conn.execute(base + ' ORDER BY datetime_last_watched DESC').fetchall()
    conn.close()
    return jsonify([
        {
            'id': r[0],
            'title': r[1],
            'year': r[2][:4] if r[2] else None,
            'last_watched': r[3],
            'watch_count': r[4] or 0,
            'poster_path': r[5],
            'has_notes':   bool(r[6]),
            'status':      r[7],
            'tmdb_status': r[8],
        }
        for r in rows
    ])


@app.route('/movie/<int:film_id>', methods=['DELETE'])
def remove_movie(film_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    row = cur.execute('SELECT title FROM films WHERE id = ?', (film_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Film not found'}), 404
    title = row[0]
    for table in ('genres', 'cast_members', 'crew_members'):
        cur.execute(f'DELETE FROM {table} WHERE film_id = ?', (film_id,))
    cur.execute('DELETE FROM films WHERE id = ?', (film_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'removed': title})


@app.route('/stats/films')
def stats_films():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401

    filter_type = request.args.get('type', '').strip()
    value = request.args.get('value', '').strip()
    if not filter_type or not value:
        return jsonify({'error': 'type and value are required'}), 400

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if filter_type == 'director':
        rows = cur.execute('''
            SELECT DISTINCT f.title, f.release_date, f.vote_average, f.poster_path FROM films f
            JOIN crew_members c ON f.id = c.film_id
            WHERE c.job = 'Director' AND c.name = ? ORDER BY f.release_date
        ''', (value,)).fetchall()
    elif filter_type == 'actor':
        rows = cur.execute('''
            SELECT DISTINCT f.title, f.release_date, f.vote_average, f.poster_path FROM films f
            JOIN cast_members c ON f.id = c.film_id
            WHERE c.billing_order <= 4 AND c.name = ? ORDER BY f.release_date
        ''', (value,)).fetchall()
    elif filter_type == 'genre':
        rows = cur.execute('''
            SELECT DISTINCT f.title, f.release_date, f.vote_average, f.poster_path FROM films f
            JOIN genres g ON f.id = g.film_id
            WHERE g.name = ? ORDER BY f.release_date
        ''', (value,)).fetchall()
    elif filter_type == 'year':
        rows = cur.execute('''
            SELECT title, release_date, vote_average, poster_path FROM films
            WHERE SUBSTR(release_date, 1, 4) = ? ORDER BY release_date
        ''', (value,)).fetchall()
    elif filter_type == 'decade':
        start = int(value)
        rows = cur.execute('''
            SELECT title, release_date, vote_average, poster_path FROM films
            WHERE CAST(SUBSTR(release_date, 1, 4) AS INTEGER) BETWEEN ? AND ?
            ORDER BY release_date
        ''', (start, start + 9)).fetchall()
    elif filter_type == 'gender':
        rows = cur.execute('''
            SELECT DISTINCT f.title, f.release_date, f.vote_average, f.poster_path FROM films f
            JOIN cast_members c ON f.id = c.film_id
            WHERE c.gender = ? ORDER BY f.release_date
        ''', (value,)).fetchall()
    elif filter_type == 'ethnicity':
        rows = cur.execute('''
            SELECT DISTINCT f.title, f.release_date, f.vote_average, f.poster_path FROM films f
            JOIN cast_members c ON f.id = c.film_id
            WHERE c.ethnicity = ? ORDER BY f.release_date
        ''', (value,)).fetchall()
    else:
        conn.close()
        return jsonify({'error': f'Unknown type: {filter_type}'}), 400

    conn.close()
    return jsonify([
        {
            'title': r['title'],
            'year': r['release_date'][:4] if r['release_date'] else None,
            'rating': r['vote_average'],
            'poster_path': r['poster_path'],
        }
        for r in rows
    ])


@app.route('/stats')
def stats():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    WW = "COALESCE(watch_status,'watched') != 'want_to_watch'"  # exclude want-to-watch

    summary = cur.execute(f'''
        SELECT
            COUNT(*) as total_films,
            COALESCE(SUM(watch_count), 0) as total_watches,
            COALESCE(SUM(CASE WHEN runtime IS NOT NULL THEN runtime * COALESCE(watch_count,1) ELSE 0 END), 0) as total_minutes,
            ROUND(AVG(CASE WHEN vote_average > 0 THEN vote_average END), 1) as avg_rating
        FROM films WHERE {WW}
    ''').fetchone()

    longest = cur.execute(
        f'SELECT title, runtime FROM films WHERE runtime IS NOT NULL AND {WW} ORDER BY runtime DESC LIMIT 1'
    ).fetchone()

    most_watched = cur.execute(
        f'SELECT title, watch_count FROM films WHERE watch_count > 1 AND {WW} ORDER BY watch_count DESC LIMIT 1'
    ).fetchone()

    top_rated = cur.execute(
        f'SELECT title, vote_average FROM films WHERE vote_average > 0 AND {WW} ORDER BY vote_average DESC LIMIT 1'
    ).fetchone()

    top_directors = cur.execute(f'''
        SELECT name, COUNT(DISTINCT film_id) as count FROM crew_members
        WHERE job = 'Director' AND name IS NOT NULL
          AND film_id IN (SELECT id FROM films WHERE {WW})
        GROUP BY name ORDER BY count DESC LIMIT 5
    ''').fetchall()

    top_actors = cur.execute(f'''
        SELECT name, COUNT(DISTINCT film_id) as count FROM cast_members
        WHERE billing_order <= 4 AND name IS NOT NULL
          AND film_id IN (SELECT id FROM films WHERE {WW})
        GROUP BY name ORDER BY count DESC LIMIT 5
    ''').fetchall()

    top_genres = cur.execute(f'''
        SELECT name, COUNT(DISTINCT film_id) as count FROM genres
        WHERE name IS NOT NULL
          AND film_id IN (SELECT id FROM films WHERE {WW})
        GROUP BY name ORDER BY count DESC LIMIT 8
    ''').fetchall()

    top_years = cur.execute(f'''
        SELECT SUBSTR(release_date,1,4) as label, COUNT(*) as count FROM films
        WHERE release_date IS NOT NULL AND LENGTH(release_date) >= 4 AND {WW}
        GROUP BY label ORDER BY count DESC LIMIT 8
    ''').fetchall()

    decades = cur.execute(f'''
        SELECT (CAST(SUBSTR(release_date,1,4) AS INTEGER)/10)*10 as decade, COUNT(*) as count
        FROM films WHERE release_date IS NOT NULL AND LENGTH(release_date) >= 4 AND {WW}
        GROUP BY decade ORDER BY decade
    ''').fetchall()

    cast_gender = cur.execute(f'''
        SELECT gender as label, COUNT(*) as count FROM cast_members
        WHERE gender IS NOT NULL AND gender != 'Unknown'
          AND film_id IN (SELECT id FROM films WHERE {WW})
        GROUP BY gender ORDER BY count DESC
    ''').fetchall()

    cast_ethnicity = cur.execute(f'''
        SELECT ethnicity as label, COUNT(*) as count FROM cast_members
        WHERE ethnicity IS NOT NULL AND ethnicity != ''
          AND film_id IN (SELECT id FROM films WHERE {WW})
        GROUP BY ethnicity ORDER BY count DESC LIMIT 7
    ''').fetchall()

    conn.close()

    return jsonify({
        'summary': {
            'total_films': summary['total_films'] or 0,
            'total_watches': summary['total_watches'] or 0,
            'total_hours': round((summary['total_minutes'] or 0) / 60, 1),
            'avg_rating': summary['avg_rating'],
        },
        'highlights': {
            'longest': dict(longest) if longest else None,
            'most_watched': dict(most_watched) if most_watched else None,
            'top_rated': dict(top_rated) if top_rated else None,
        },
        'top_directors': [dict(r) for r in top_directors],
        'top_actors': [dict(r) for r in top_actors],
        'top_genres': [dict(r) for r in top_genres],
        'top_years': [dict(r) for r in top_years],
        'decades': [{'label': f"{r['decade']}s", 'count': r['count']} for r in decades],
        'cast_gender': [dict(r) for r in cast_gender],
        'cast_ethnicity': [dict(r) for r in cast_ethnicity],
    })


# ── Game routes ───────────────────────────────────────────────────────────────

@app.route('/games/search')
def games_search():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    title = request.args.get('title', '').strip()
    if not title:
        return jsonify({'error': 'title is required'}), 400
    try:
        results = search_game(title)
        return jsonify([
            {
                'id':                 r['id'],
                'name':               r['name'],
                'year':               datetime.fromtimestamp(r['first_release_date'], tz=timezone.utc).strftime('%Y')
                                      if 'first_release_date' in r else None,
                'cover_image_id':     r.get('cover', {}).get('image_id'),
                'summary':            r.get('summary', ''),
            }
            for r in results
        ])
    except ValueError as e:
        return jsonify({'error': str(e)}), 404


@app.route('/games/add', methods=['POST'])
def games_add():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    igdb_id = data.get('igdb_id')
    if not igdb_id:
        return jsonify({'error': 'igdb_id is required'}), 400
    status           = data.get('status', 'completed')
    date_completed   = data.get('date_completed')
    completed_fully  = bool(data.get('completed_fully', False))
    try:
        game_id = fetch_and_store_game(igdb_id, DB_PATH, status=status, date_completed=date_completed, completed_fully=completed_fully)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    threading.Thread(
        target=fetch_wikidata_game_enrichment,
        args=(game_id, DB_PATH),
        daemon=True,
    ).start()
    return jsonify({'ok': True, 'game_id': game_id})


@app.route('/games')
def games_list():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    status_filter = request.args.get('status', '').strip()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if status_filter:
        rows = cur.execute(
            'SELECT id, name, first_release_date, status, date_completed, cover_image_id, completed_fully, '
            'CASE WHEN notes IS NOT NULL AND notes != \'\' THEN 1 ELSE 0 END as has_notes '
            'FROM games WHERE status=? ORDER BY CASE WHEN status="playing" THEN 0 ELSE 1 END, date_completed DESC, datetime_added DESC',
            (status_filter,)
        ).fetchall()
    else:
        rows = cur.execute(
            'SELECT id, name, first_release_date, status, date_completed, cover_image_id, completed_fully, '
            'CASE WHEN notes IS NOT NULL AND notes != \'\' THEN 1 ELSE 0 END as has_notes '
            'FROM games ORDER BY CASE WHEN status="playing" THEN 0 ELSE 1 END, date_completed DESC, datetime_added DESC'
        ).fetchall()
    result = []
    for r in rows:
        devs = [x['name'] for x in cur.execute(
            'SELECT name FROM game_developers WHERE game_id=?', (r['id'],)
        ).fetchall()]
        result.append({
            'id':             r['id'],
            'name':           r['name'],
            'year':           r['first_release_date'][:4] if r['first_release_date'] else None,
            'status':           r['status'],
            'date_completed':   r['date_completed'],
            'cover_image_id':   r['cover_image_id'],
            'completed_fully':  bool(r['completed_fully']),
            'has_notes':        bool(r['has_notes']),
            'developers':       devs,
        })
    conn.close()
    return jsonify(result)


@app.route('/games/stats')
def games_stats():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    WP = "status != 'want_to_play'"  # exclude want-to-play

    summary = cur.execute(f'''
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status="completed" THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN completed_fully=1  THEN 1 ELSE 0 END) as fully_completed,
            SUM(CASE WHEN status="playing"   THEN 1 ELSE 0 END) as playing,
            SUM(CASE WHEN status="dropped"   THEN 1 ELSE 0 END) as dropped
        FROM games WHERE {WP}
    ''').fetchone()

    top_devs = cur.execute(f'''
        SELECT name, COUNT(DISTINCT game_id) as count FROM game_developers
        WHERE name IS NOT NULL
          AND game_id IN (SELECT id FROM games WHERE {WP})
        GROUP BY name ORDER BY count DESC LIMIT 5
    ''').fetchall()

    top_pubs = cur.execute(f'''
        SELECT name, COUNT(DISTINCT game_id) as count FROM game_publishers
        WHERE name IS NOT NULL
          AND game_id IN (SELECT id FROM games WHERE {WP})
        GROUP BY name ORDER BY count DESC LIMIT 5
    ''').fetchall()

    top_genres = cur.execute(f'''
        SELECT name, COUNT(DISTINCT game_id) as count FROM game_genres
        WHERE name IS NOT NULL
          AND game_id IN (SELECT id FROM games WHERE {WP})
        GROUP BY name ORDER BY count DESC LIMIT 8
    ''').fetchall()

    perspectives = cur.execute(f'''
        SELECT name, COUNT(DISTINCT game_id) as count FROM game_perspectives
        WHERE name IS NOT NULL
          AND game_id IN (SELECT id FROM games WHERE {WP})
        GROUP BY name ORDER BY count DESC
    ''').fetchall()

    by_year = cur.execute('''
        SELECT SUBSTR(date_completed, 1, 4) as label, COUNT(*) as count
        FROM games WHERE date_completed IS NOT NULL AND status="completed"
        GROUP BY label ORDER BY label
    ''').fetchall()

    conn.close()

    completed   = summary['completed'] or 0
    fully       = summary['fully_completed'] or 0
    status_data = [
        {'label': 'Completed', 'count': completed},
        {'label': 'Playing',   'count': summary['playing'] or 0},
        {'label': 'Dropped',   'count': summary['dropped'] or 0},
    ]

    return jsonify({
        'summary': {
            'total':           (summary['total'] or 0),
            'completed':       completed,
            'fully_completed': fully,
            'completion_rate': round(fully / completed * 100) if completed else 0,
        },
        'top_developers': [dict(r) for r in top_devs],
        'top_publishers': [dict(r) for r in top_pubs],
        'top_genres':     [dict(r) for r in top_genres],
        'perspectives':   [dict(r) for r in perspectives],
        'by_year':        [dict(r) for r in by_year],
        'status_breakdown': [s for s in status_data if s['count'] > 0],
    })


@app.route('/games/stats/titles')
def games_stats_titles():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    filter_type = request.args.get('type', '').strip()
    value       = request.args.get('value', '').strip()
    if not filter_type or not value:
        return jsonify({'error': 'type and value are required'}), 400

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    base = 'SELECT DISTINCT g.name, g.first_release_date, g.cover_image_id, g.status, g.date_completed, g.completed_fully FROM games g'
    order = 'ORDER BY g.date_completed DESC'

    if filter_type == 'developer':
        rows = cur.execute(f'{base} JOIN game_developers d ON g.id=d.game_id WHERE d.name=? {order}', (value,)).fetchall()
    elif filter_type == 'publisher':
        rows = cur.execute(f'{base} JOIN game_publishers p ON g.id=p.game_id WHERE p.name=? {order}', (value,)).fetchall()
    elif filter_type == 'genre':
        rows = cur.execute(f'{base} JOIN game_genres gg ON g.id=gg.game_id WHERE gg.name=? {order}', (value,)).fetchall()
    elif filter_type == 'perspective':
        rows = cur.execute(f'{base} JOIN game_perspectives p ON g.id=p.game_id WHERE p.name=? {order}', (value,)).fetchall()
    elif filter_type == 'year':
        rows = cur.execute(
            'SELECT name, first_release_date, cover_image_id, status, date_completed, completed_fully '
            'FROM games WHERE SUBSTR(date_completed,1,4)=? ORDER BY date_completed DESC', (value,)
        ).fetchall()
    elif filter_type == 'status':
        rows = cur.execute(
            'SELECT name, first_release_date, cover_image_id, status, date_completed, completed_fully '
            'FROM games WHERE status=? ORDER BY date_completed DESC', (value,)
        ).fetchall()
    elif filter_type == 'fully_completed':
        rows = cur.execute(
            'SELECT name, first_release_date, cover_image_id, status, date_completed, completed_fully '
            'FROM games WHERE completed_fully=1 ORDER BY date_completed DESC'
        ).fetchall()
    else:
        conn.close()
        return jsonify({'error': f'Unknown type: {filter_type}'}), 400

    conn.close()
    return jsonify([{
        'name':           r['name'],
        'year':           r['first_release_date'][:4] if r['first_release_date'] else None,
        'cover_image_id': r['cover_image_id'],
        'status':         r['status'],
        'date_completed': r['date_completed'],
        'completed_fully': bool(r['completed_fully']),
    } for r in rows])


@app.route('/games/<int:game_id>/status', methods=['POST'])
def games_update_status(game_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    data             = request.get_json(silent=True) or {}
    status           = data.get('status', '').strip()
    date_completed   = data.get('date_completed')
    completed_fully  = bool(data.get('completed_fully', False))
    if status not in ('completed', 'playing', 'want_to_play', 'dropped'):
        return jsonify({'error': 'Invalid status'}), 400
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        'UPDATE games SET status=?, date_completed=?, completed_fully=? WHERE id=?',
        (status, date_completed or None, 1 if completed_fully else 0, game_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/games/<int:game_id>', methods=['DELETE'])
def games_remove(game_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    row = cur.execute('SELECT name FROM games WHERE id=?', (game_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Game not found'}), 404
    name = row[0]
    for table in ('game_genres', 'game_developers', 'game_publishers', 'game_perspectives'):
        cur.execute(f'DELETE FROM {table} WHERE game_id=?', (game_id,))
    cur.execute('DELETE FROM games WHERE id=?', (game_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'removed': name})


@app.route('/movies/<int:film_id>/status', methods=['POST'])
def movie_update_status(film_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    status = (request.get_json(silent=True) or {}).get('status', '').strip()
    if status not in ('watched', 'want_to_watch'):
        return jsonify({'error': 'Invalid status'}), 400
    conn = sqlite3.connect(DB_PATH)
    conn.execute('UPDATE films SET watch_status=? WHERE id=?', (status, film_id))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


# ── Book routes ───────────────────────────────────────────────────────────────

@app.route('/books/search')
def books_search():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({'error': 'q is required'}), 400
    try:
        return jsonify(search_book(query))
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        return jsonify({'error': f'Search failed: {e}'}), 500


@app.route('/books/add', methods=['POST'])
def books_add():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    data      = request.get_json(silent=True) or {}
    book_id   = data.get('book_id')
    if not book_id:
        return jsonify({'error': 'book_id is required'}), 400
    status    = data.get('status', 'read')
    date_read = data.get('date_read')
    try:
        fetch_and_store_book(book_id, DB_PATH, status=status, date_read=date_read)
        return jsonify({'ok': True, 'book_id': book_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/books')
def books_list():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    status_filter = request.args.get('status', '').strip()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    base = '''SELECT id, title, authors, published_date, page_count, cover_url, status, date_read,
                     CASE WHEN notes IS NOT NULL AND notes != '' THEN 1 ELSE 0 END as has_notes
              FROM books'''
    if status_filter:
        rows = cur.execute(
            base + ' WHERE status=? ORDER BY date_read DESC, datetime_added DESC', (status_filter,)
        ).fetchall()
    else:
        rows = cur.execute(
            base + ' ORDER BY CASE WHEN status="reading" THEN 0 ELSE 1 END, date_read DESC, datetime_added DESC'
        ).fetchall()
    result = []
    for r in rows:
        genres = [x['name'] for x in cur.execute(
            'SELECT name FROM book_genres WHERE book_id=?', (r['id'],)
        ).fetchall()]
        result.append({
            'id':           r['id'],
            'title':        r['title'],
            'authors':      r['authors'],
            'year':         r['published_date'][:4] if r['published_date'] else None,
            'page_count':   r['page_count'],
            'cover_url':    r['cover_url'],
            'status':       r['status'],
            'date_read':    r['date_read'],
            'has_notes':    bool(r['has_notes']),
            'genres':       genres,
        })
    conn.close()
    return jsonify(result)


@app.route('/books/stats')
def books_stats():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    WR = "status != 'want_to_read'"

    summary = cur.execute(f'''
        SELECT COUNT(*) as total,
               SUM(CASE WHEN status="read"     THEN 1 ELSE 0 END) as read_count,
               SUM(CASE WHEN status="reading"  THEN 1 ELSE 0 END) as reading,
               SUM(CASE WHEN status="dropped"  THEN 1 ELSE 0 END) as dropped,
               SUM(CASE WHEN status="read" AND page_count IS NOT NULL THEN page_count ELSE 0 END) as total_pages
        FROM books WHERE {WR}
    ''').fetchone()

    top_authors = cur.execute(f'''
        SELECT authors as name, COUNT(*) as count FROM books
        WHERE authors IS NOT NULL AND authors != '' AND {WR}
        GROUP BY authors ORDER BY count DESC LIMIT 5
    ''').fetchall()

    top_genres = cur.execute(f'''
        SELECT name, COUNT(DISTINCT book_id) as count FROM book_genres
        WHERE name IS NOT NULL AND name != 'General'
          AND book_id IN (SELECT id FROM books WHERE {WR})
        GROUP BY name ORDER BY count DESC LIMIT 8
    ''').fetchall()

    FICTION_IDS  = f"(SELECT DISTINCT book_id FROM book_genres WHERE name LIKE '%Fiction%')"
    NONFICT_IDS  = f"(SELECT DISTINCT b.id FROM books b WHERE b.id NOT IN {FICTION_IDS})"

    fiction_count = cur.execute(f'''
        SELECT COUNT(DISTINCT b.id) as n FROM books b
        WHERE b.id IN {FICTION_IDS} AND {WR}
    ''').fetchone()['n'] or 0

    nonfiction_count = cur.execute(f'''
        SELECT COUNT(DISTINCT b.id) as n FROM books b
        WHERE b.id IN {NONFICT_IDS} AND {WR}
    ''').fetchone()['n'] or 0

    fiction_genres = cur.execute(f'''
        SELECT g.name, COUNT(DISTINCT g.book_id) as count
        FROM book_genres g JOIN books b ON g.book_id = b.id
        WHERE b.id IN {FICTION_IDS} AND {WR}
          AND g.name NOT LIKE '%Fiction%' AND g.name != 'General'
        GROUP BY g.name ORDER BY count DESC LIMIT 8
    ''').fetchall()

    nonfiction_genres = cur.execute(f'''
        SELECT g.name, COUNT(DISTINCT g.book_id) as count
        FROM book_genres g JOIN books b ON g.book_id = b.id
        WHERE b.id IN {NONFICT_IDS} AND {WR}
          AND LOWER(g.name) NOT LIKE '%nonfiction%'
          AND LOWER(g.name) NOT LIKE '%non-fiction%'
          AND g.name != 'General'
        GROUP BY g.name ORDER BY count DESC LIMIT 8
    ''').fetchall()

    by_year = cur.execute('''
        SELECT SUBSTR(date_read, 1, 4) as label, COUNT(*) as count
        FROM books WHERE date_read IS NOT NULL AND status="read"
        GROUP BY label ORDER BY label
    ''').fetchall()

    pages_by_year = cur.execute('''
        SELECT SUBSTR(date_read, 1, 4) as label, SUM(page_count) as count
        FROM books WHERE date_read IS NOT NULL AND status="read" AND page_count IS NOT NULL
        GROUP BY label ORDER BY label
    ''').fetchall()

    status_data = [
        {'label': 'Read',         'count': summary['read_count'] or 0},
        {'label': 'Reading',      'count': summary['reading']    or 0},
        {'label': 'Dropped',      'count': summary['dropped']    or 0},
    ]
    conn.close()
    return jsonify({
        'summary': {
            'total':       summary['total'] or 0,
            'read':        summary['read_count'] or 0,
            'total_pages': summary['total_pages'] or 0,
        },
        'top_authors':      [dict(r) for r in top_authors],
        'top_genres':       [dict(r) for r in top_genres],
        'fiction_split':    [{'label': 'Fiction', 'count': fiction_count}, {'label': 'Non-Fiction', 'count': nonfiction_count}],
        'fiction_genres':   [dict(r) for r in fiction_genres],
        'nonfiction_genres':[dict(r) for r in nonfiction_genres],
        'by_year':          [dict(r) for r in by_year],
        'pages_by_year':    [dict(r) for r in pages_by_year],
        'status_breakdown': [s for s in status_data if s['count'] > 0],
    })


@app.route('/books/stats/titles')
def books_stats_titles():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    filter_type = request.args.get('type', '').strip()
    value       = request.args.get('value', '').strip()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if filter_type == 'author':
        rows = cur.execute(
            'SELECT id, title, authors, published_date, cover_url, status, date_read FROM books '
            'WHERE authors=? ORDER BY date_read DESC', (value,)
        ).fetchall()
    elif filter_type == 'genre':
        rows = cur.execute(
            'SELECT b.id, b.title, b.authors, b.published_date, b.cover_url, b.status, b.date_read FROM books b '
            'JOIN book_genres g ON b.id=g.book_id WHERE g.name=? ORDER BY b.date_read DESC', (value,)
        ).fetchall()
    elif filter_type == 'year':
        rows = cur.execute(
            'SELECT id, title, authors, published_date, cover_url, status, date_read FROM books '
            'WHERE SUBSTR(date_read,1,4)=? ORDER BY date_read DESC', (value,)
        ).fetchall()
    elif filter_type == 'status':
        rows = cur.execute(
            'SELECT id, title, authors, published_date, cover_url, status, date_read FROM books '
            'WHERE status=? ORDER BY date_read DESC', (value,)
        ).fetchall()
    elif filter_type == 'fiction':
        if value == 'true':
            rows = cur.execute(
                'SELECT DISTINCT b.id, b.title, b.authors, b.published_date, b.cover_url, b.status, b.date_read '
                'FROM books b JOIN book_genres g ON b.id=g.book_id '
                "WHERE g.name LIKE '%Fiction%' ORDER BY b.date_read DESC"
            ).fetchall()
        else:
            rows = cur.execute(
                'SELECT id, title, authors, published_date, cover_url, status, date_read FROM books '
                "WHERE id NOT IN (SELECT DISTINCT book_id FROM book_genres WHERE name LIKE '%Fiction%') "
                'ORDER BY date_read DESC'
            ).fetchall()
    else:
        conn.close()
        return jsonify({'error': f'Unknown type: {filter_type}'}), 400
    conn.close()
    return jsonify([{
        'title':     r['title'],
        'authors':   r['authors'],
        'year':      r['published_date'][:4] if r['published_date'] else None,
        'cover_url': r['cover_url'],
        'status':    r['status'],
        'date_read': r['date_read'],
    } for r in rows])


@app.route('/books/<book_id>/notes', methods=['GET', 'POST'])
def book_notes(book_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    if request.method == 'POST':
        notes = (request.get_json(silent=True) or {}).get('notes', '')
        conn.execute('UPDATE books SET notes=? WHERE id=?', (notes, book_id))
        conn.commit()
        conn.close()
        return jsonify({'ok': True})
    row = conn.execute('SELECT notes FROM books WHERE id=?', (book_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    return jsonify({'notes': row[0] or ''})


@app.route('/books/<book_id>/status', methods=['POST'])
def books_update_status(book_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    data      = request.get_json(silent=True) or {}
    status    = data.get('status', '').strip()
    date_read = data.get('date_read')
    if status not in ('read', 'reading', 'want_to_read', 'dropped'):
        return jsonify({'error': 'Invalid status'}), 400
    conn = sqlite3.connect(DB_PATH)
    conn.execute('UPDATE books SET status=?, date_read=? WHERE id=?',
                 (status, date_read or None, book_id))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/books/<book_id>', methods=['DELETE'])
def books_remove(book_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute('SELECT title FROM books WHERE id=?', (book_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Book not found'}), 404
    conn.execute('DELETE FROM book_genres WHERE book_id=?', (book_id,))
    conn.execute('DELETE FROM books WHERE id=?', (book_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'removed': row[0]})


# ── TV routes ─────────────────────────────────────────────────────────────────

@app.route('/shows/search')
def shows_search():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    title = request.args.get('title', '').strip()
    if not title:
        return jsonify({'error': 'title is required'}), 400
    try:
        results = search_show(title)
        return jsonify([{
            'id':             r['id'],
            'name':           r['name'],
            'year':           r.get('first_air_date', '')[:4] or None,
            'poster_path':    r.get('poster_path'),
            'overview':       r.get('overview', ''),
        } for r in results])
    except ValueError as e:
        return jsonify({'error': str(e)}), 404


@app.route('/shows/add', methods=['POST'])
def shows_add():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    data         = request.get_json(silent=True) or {}
    tmdb_id      = data.get('tmdb_id')
    watch_status = data.get('watch_status', 'watching')
    if not tmdb_id:
        return jsonify({'error': 'tmdb_id is required'}), 400
    try:
        fetch_and_store_show(tmdb_id, DB_PATH, watch_status=watch_status)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/shows')
def shows_list():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    status_filter = request.args.get('status', '').strip()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    base = '''
        SELECT t.id, t.name, t.first_air_date, t.poster_path, t.watch_status, t.tmdb_status,
               t.number_of_seasons, t.datetime_added,
               CASE WHEN t.notes IS NOT NULL AND t.notes != '' THEN 1 ELSE 0 END as has_notes,
               MAX(s.date_completed) as last_season_date
        FROM tv_shows t
        LEFT JOIN tv_seasons s ON s.show_id = t.id AND s.date_completed IS NOT NULL
    '''
    order = '''
        GROUP BY t.id
        ORDER BY
            CASE t.watch_status
                WHEN 'watching'      THEN 0
                WHEN 'on_hold'       THEN 1
                WHEN 'want_to_watch' THEN 2
                ELSE 3
            END,
            COALESCE(MAX(s.date_completed), t.datetime_added) DESC
    '''
    if status_filter:
        rows = cur.execute(f'{base} WHERE t.watch_status=? {order}', (status_filter,)).fetchall()
    else:
        rows = cur.execute(f'{base} {order}').fetchall()
    result = []
    for r in rows:
        VALID = "(episode_count IS NULL OR episode_count > 0) AND (air_date IS NULL OR air_date <= date('now'))"
        seasons_total = cur.execute(
            f'SELECT COUNT(*) as n FROM tv_seasons WHERE show_id=? AND {VALID}',
            (r['id'],)
        ).fetchone()['n']
        seasons_done = cur.execute(
            f'SELECT COUNT(*) as n FROM tv_seasons WHERE show_id=? AND {VALID} AND date_completed IS NOT NULL',
            (r['id'],)
        ).fetchone()['n']
        network = cur.execute(
            'SELECT name FROM tv_show_networks WHERE show_id=? LIMIT 1', (r['id'],)
        ).fetchone()
        result.append({
            'id':                r['id'],
            'name':              r['name'],
            'year':              r['first_air_date'][:4] if r['first_air_date'] else None,
            'poster_path':       r['poster_path'],
            'watch_status':      r['watch_status'],
            'tmdb_status':       r['tmdb_status'],
            'number_of_seasons': seasons_total,
            'seasons_done':      seasons_done,
            'network':           network['name'] if network else None,
            'has_notes':         bool(r['has_notes']),
        })
    conn.close()
    return jsonify(result)


@app.route('/shows/<int:show_id>/seasons')
def show_seasons(show_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        'SELECT season_number, season_name, episode_count, air_date, date_completed '
        "FROM tv_seasons WHERE show_id=? AND (episode_count IS NULL OR episode_count > 0)"
        " AND (air_date IS NULL OR air_date <= date('now'))"
        ' ORDER BY season_number',
        (show_id,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/shows/<int:show_id>/season/<int:season_number>', methods=['POST', 'DELETE'])
def show_season_complete(show_id, season_number):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    if request.method == 'DELETE':
        conn.execute(
            'UPDATE tv_seasons SET date_completed=NULL WHERE show_id=? AND season_number=?',
            (show_id, season_number)
        )
    else:
        date_completed = (request.get_json(silent=True) or {}).get('date_completed') or \
                         datetime.now(timezone.utc).isoformat()
        conn.execute(
            'UPDATE tv_seasons SET date_completed=? WHERE show_id=? AND season_number=?',
            (date_completed, show_id, season_number)
        )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/shows/<int:show_id>/status', methods=['POST'])
def shows_update_status(show_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    status = (request.get_json(silent=True) or {}).get('status', '').strip()
    if status not in ('watching', 'completed', 'want_to_watch', 'dropped', 'on_hold', 'off_season'):
        return jsonify({'error': 'Invalid status'}), 400
    conn = sqlite3.connect(DB_PATH)
    conn.execute('UPDATE tv_shows SET watch_status=? WHERE id=?', (status, show_id))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/shows/<int:show_id>/notes', methods=['GET', 'POST'])
def show_notes(show_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    if request.method == 'POST':
        notes = (request.get_json(silent=True) or {}).get('notes', '')
        conn.execute('UPDATE tv_shows SET notes=? WHERE id=?', (notes, show_id))
        conn.commit()
        conn.close()
        return jsonify({'ok': True})
    row = conn.execute('SELECT notes FROM tv_shows WHERE id=?', (show_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    return jsonify({'notes': row[0] or ''})


@app.route('/shows/<int:show_id>', methods=['DELETE'])
def shows_remove(show_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute('SELECT name FROM tv_shows WHERE id=?', (show_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Show not found'}), 404
    for table in ('tv_show_genres', 'tv_show_networks', 'tv_show_creators', 'tv_seasons'):
        conn.execute(f'DELETE FROM {table} WHERE show_id=?', (show_id,))
    conn.execute('DELETE FROM tv_shows WHERE id=?', (show_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'removed': row[0]})


@app.route('/shows/refresh', methods=['POST'])
def trigger_shows_refresh():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    force = (request.get_json(silent=True) or {}).get('force', False)
    threading.Thread(target=_run_tv_refresh, kwargs={'force': force}, daemon=True).start()
    return jsonify({'ok': True})


@app.route('/movies/refresh', methods=['POST'])
def trigger_movies_refresh():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    force = (request.get_json(silent=True) or {}).get('force', False)
    threading.Thread(target=_run_movie_refresh, kwargs={'force': force}, daemon=True).start()
    return jsonify({'ok': True})


@app.route('/shows/stats')
def shows_stats():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    WW = "watch_status != 'want_to_watch'"

    summary = cur.execute(f'''
        SELECT COUNT(*) as total,
               SUM(CASE WHEN watch_status="completed"  THEN 1 ELSE 0 END) as completed,
               SUM(CASE WHEN watch_status="watching"   THEN 1 ELSE 0 END) as watching,
               SUM(CASE WHEN watch_status="dropped"    THEN 1 ELSE 0 END) as dropped,
               SUM(CASE WHEN watch_status="on_hold"    THEN 1 ELSE 0 END) as on_hold,
               SUM(CASE WHEN watch_status="off_season" THEN 1 ELSE 0 END) as off_season
        FROM tv_shows WHERE {WW}
    ''').fetchone()

    seasons_done = cur.execute(
        'SELECT COUNT(*) as n FROM tv_seasons s '
        'JOIN tv_shows t ON s.show_id=t.id '
        f'WHERE s.date_completed IS NOT NULL AND t.{WW}'
    ).fetchone()['n']

    episodes_watched = cur.execute(
        'SELECT COALESCE(SUM(s.episode_count), 0) FROM tv_seasons s '
        'JOIN tv_shows t ON s.show_id=t.id '
        f'WHERE s.date_completed IS NOT NULL AND t.{WW}'
    ).fetchone()[0]

    shows_by_year = cur.execute(f'''
        SELECT SUBSTR(first_air_date, 1, 4) as label, COUNT(*) as count
        FROM tv_shows WHERE {WW} AND first_air_date IS NOT NULL AND first_air_date != ''
        GROUP BY label ORDER BY label
    ''').fetchall()

    top_genres = cur.execute(f'''
        SELECT g.name, COUNT(DISTINCT g.show_id) as count FROM tv_show_genres g
        JOIN tv_shows t ON g.show_id=t.id WHERE t.{WW}
        GROUP BY g.name ORDER BY count DESC LIMIT 8
    ''').fetchall()

    top_networks = cur.execute(f'''
        SELECT n.name, COUNT(DISTINCT n.show_id) as count FROM tv_show_networks n
        JOIN tv_shows t ON n.show_id=t.id WHERE t.{WW}
        GROUP BY n.name ORDER BY count DESC LIMIT 8
    ''').fetchall()

    top_creators = cur.execute(f'''
        SELECT c.name, COUNT(DISTINCT c.show_id) as count FROM tv_show_creators c
        JOIN tv_shows t ON c.show_id=t.id WHERE t.{WW}
        GROUP BY c.name ORDER BY count DESC LIMIT 5
    ''').fetchall()

    seasons_by_year = cur.execute(
        'SELECT SUBSTR(s.date_completed,1,4) as label, COUNT(*) as count FROM tv_seasons s '
        'JOIN tv_shows t ON s.show_id=t.id '
        f'WHERE s.date_completed IS NOT NULL AND t.{WW} '
        'GROUP BY label ORDER BY label'
    ).fetchall()

    conn.close()
    status_data = [
        {'label': 'Watching',   'count': summary['watching']   or 0},
        {'label': 'Completed',  'count': summary['completed']  or 0},
        {'label': 'Off Season', 'count': summary['off_season'] or 0},
        {'label': 'On Hold',    'count': summary['on_hold']    or 0},
        {'label': 'Dropped',    'count': summary['dropped']    or 0},
    ]
    total = summary['total'] or 0
    completed = summary['completed'] or 0
    completion_rate = round(completed / total * 100) if total else 0
    return jsonify({
        'summary':          {'total': total, 'completed': completed, 'seasons_done': seasons_done,
                             'episodes_watched': episodes_watched, 'completion_rate': completion_rate},
        'top_genres':       [dict(r) for r in top_genres],
        'top_networks':     [dict(r) for r in top_networks],
        'top_creators':     [dict(r) for r in top_creators],
        'seasons_by_year':  [dict(r) for r in seasons_by_year],
        'shows_by_year':    [dict(r) for r in shows_by_year],
        'status_breakdown': [s for s in status_data if s['count'] > 0],
    })


@app.route('/shows/stats/titles')
def shows_stats_titles():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    filter_type = request.args.get('type', '').strip()
    value       = request.args.get('value', '').strip()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    base = 'SELECT DISTINCT t.id, t.name, t.first_air_date, t.poster_path, t.watch_status, t.tmdb_status, t.number_of_seasons FROM tv_shows t'
    if filter_type == 'genre':
        rows = cur.execute(f'{base} JOIN tv_show_genres g ON t.id=g.show_id WHERE g.name=? ORDER BY t.name', (value,)).fetchall()
    elif filter_type == 'network':
        rows = cur.execute(f'{base} JOIN tv_show_networks n ON t.id=n.show_id WHERE n.name=? ORDER BY t.name', (value,)).fetchall()
    elif filter_type == 'creator':
        rows = cur.execute(f'{base} JOIN tv_show_creators c ON t.id=c.show_id WHERE c.name=? ORDER BY t.name', (value,)).fetchall()
    elif filter_type == 'year':
        rows = cur.execute(f'{base} WHERE SUBSTR(t.first_air_date,1,4)=? ORDER BY t.name', (value,)).fetchall()
    elif filter_type == 'status':
        rows = cur.execute(f'{base} WHERE t.watch_status=? ORDER BY t.name', (value,)).fetchall()
    else:
        conn.close()
        return jsonify({'error': f'Unknown type: {filter_type}'}), 400
    conn.close()
    return jsonify([{
        'name':             r['name'],
        'year':             r['first_air_date'][:4] if r['first_air_date'] else None,
        'poster_path':      r['poster_path'],
        'watch_status':     r['watch_status'],
        'tmdb_status':      r['tmdb_status'],
        'number_of_seasons': r['number_of_seasons'],
    } for r in rows])


# ── Music ─────────────────────────────────────────────────────────────────────

@app.route('/music/search')
def music_search():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    q            = request.args.get('q', '').strip()
    release_type = request.args.get('type', 'album').strip()
    if not q:
        return jsonify({'error': 'q is required'}), 400
    try:
        return jsonify(search_album(q, release_type=release_type))
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/music/add', methods=['POST'])
def music_add():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    data   = request.get_json(silent=True) or {}
    mbid   = data.get('album_id')
    status = data.get('status', 'listened')
    if not mbid:
        return jsonify({'error': 'album_id is required'}), 400
    try:
        fetch_and_store_album(mbid, DB_PATH, status=status, date_listened=data.get('date_listened'))
        if status == 'want_to_listen':
            conn = sqlite3.connect(DB_PATH)
            conn.execute("UPDATE albums SET listen_count=0, date_listened=NULL WHERE id=?", (mbid,))
            conn.commit()
            conn.close()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/music')
def music_list():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    status_filter = request.args.get('status', '').strip()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    base  = '''SELECT id, title, artist, release_date, release_type, cover_url, status,
                      listen_count, date_listened,
                      CASE WHEN notes IS NOT NULL AND notes != '' THEN 1 ELSE 0 END as has_notes
               FROM albums'''
    order = 'ORDER BY COALESCE(date_listened, datetime_added) DESC'
    rows  = cur.execute(f'{base} WHERE status=? {order}', (status_filter,)).fetchall() \
            if status_filter else cur.execute(f'{base} {order}').fetchall()
    conn.close()
    return jsonify([{
        'id':            r['id'],
        'title':         r['title'],
        'artist':        r['artist'],
        'year':          r['release_date'][:4] if r['release_date'] else None,
        'release_type':  r['release_type'],
        'cover_url':     r['cover_url'],
        'status':        r['status'],
        'listen_count':  r['listen_count'] or 0,
        'date_listened': r['date_listened'],
        'has_notes':     bool(r['has_notes']),
    } for r in rows])


@app.route('/music/<album_id>/status', methods=['POST'])
def music_update_status(album_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    data   = request.get_json(silent=True) or {}
    status = data.get('status', '').strip()
    if status not in ('listened', 'listening', 'want_to_listen', 'dropped'):
        return jsonify({'error': 'Invalid status'}), 400
    date_listened = data.get('date_listened') or \
                    (datetime.now(timezone.utc).isoformat() if status in ('listened', 'dropped') else None)
    conn = sqlite3.connect(DB_PATH)
    conn.execute('UPDATE albums SET status=?, date_listened=? WHERE id=?',
                 (status, date_listened, album_id))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/music/<album_id>/notes', methods=['GET', 'POST'])
def music_notes(album_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    if request.method == 'POST':
        notes = (request.get_json(silent=True) or {}).get('notes', '')
        conn.execute('UPDATE albums SET notes=? WHERE id=?', (notes, album_id))
        conn.commit()
        conn.close()
        return jsonify({'ok': True})
    row = conn.execute('SELECT notes FROM albums WHERE id=?', (album_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    return jsonify({'notes': row[0] or ''})


@app.route('/music/<album_id>', methods=['DELETE'])
def music_remove(album_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute('SELECT title FROM albums WHERE id=?', (album_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    conn.execute('DELETE FROM album_genres WHERE album_id=?', (album_id,))
    conn.execute('DELETE FROM albums WHERE id=?', (album_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'removed': row[0]})


@app.route('/music/stats')
def music_stats():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    WL = "status != 'want_to_listen'"

    summary = cur.execute(f'''
        SELECT COUNT(*) as total,
               SUM(CASE WHEN status='listened'  THEN 1 ELSE 0 END) as listened,
               SUM(CASE WHEN status='listening' THEN 1 ELSE 0 END) as listening,
               SUM(CASE WHEN status='dropped'   THEN 1 ELSE 0 END) as dropped
        FROM albums WHERE {WL}
    ''').fetchone()

    top_genres = cur.execute(f'''
        SELECT g.name, COUNT(DISTINCT g.album_id) as count FROM album_genres g
        JOIN albums a ON g.album_id=a.id WHERE a.{WL}
        GROUP BY g.name ORDER BY count DESC LIMIT 8
    ''').fetchall()

    top_artists = cur.execute(f'''
        SELECT artist as name, COUNT(*) as count FROM albums
        WHERE {WL} AND artist IS NOT NULL AND artist != ''
        GROUP BY artist ORDER BY count DESC LIMIT 8
    ''').fetchall()

    albums_by_year = cur.execute(f'''
        SELECT SUBSTR(release_date, 1, 4) as label, COUNT(*) as count
        FROM albums WHERE {WL} AND release_date IS NOT NULL AND release_date != ''
        GROUP BY label ORDER BY label
    ''').fetchall()

    listened_by_year = cur.execute('''
        SELECT SUBSTR(date_listened, 1, 4) as label, COUNT(*) as count
        FROM albums WHERE status='listened' AND date_listened IS NOT NULL
        GROUP BY label ORDER BY label
    ''').fetchall()

    conn.close()
    total    = summary['total'] or 0
    listened = summary['listened'] or 0
    status_data = [
        {'label': 'Listened',      'count': listened},
        {'label': 'Listening',     'count': summary['listening'] or 0},
        {'label': 'Dropped',       'count': summary['dropped']   or 0},
    ]
    return jsonify({
        'summary':          {'total': total, 'listened': listened},
        'top_genres':       [dict(r) for r in top_genres],
        'top_artists':      [dict(r) for r in top_artists],
        'albums_by_year':   [dict(r) for r in albums_by_year],
        'listened_by_year': [dict(r) for r in listened_by_year],
        'status_breakdown': [s for s in status_data if s['count'] > 0],
    })


@app.route('/music/stats/titles')
def music_stats_titles():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    filter_type = request.args.get('type', '').strip()
    value       = request.args.get('value', '').strip()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    base = 'SELECT DISTINCT a.id, a.title, a.artist, a.release_date, a.cover_url, a.status FROM albums a'
    if filter_type == 'genre':
        rows = cur.execute(f'{base} JOIN album_genres g ON a.id=g.album_id WHERE g.name=? ORDER BY a.title', (value,)).fetchall()
    elif filter_type == 'artist':
        rows = cur.execute(f'{base} WHERE a.artist=? ORDER BY a.release_date', (value,)).fetchall()
    elif filter_type == 'year':
        rows = cur.execute(f'{base} WHERE SUBSTR(a.release_date,1,4)=? ORDER BY a.release_date', (value,)).fetchall()
    else:
        conn.close()
        return jsonify({'error': f'Unknown type: {filter_type}'}), 400
    conn.close()
    return jsonify([{
        'title':        r['title'],
        'artist':       r['artist'],
        'year':         r['release_date'][:4] if r['release_date'] else None,
        'cover_url':    r['cover_url'],
        'status':       r['status'],
    } for r in rows])


# ── Notes ─────────────────────────────────────────────────────────────────────

@app.route('/movies/<int:film_id>/notes', methods=['GET', 'POST'])
def movie_notes(film_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    if request.method == 'POST':
        notes = (request.get_json(silent=True) or {}).get('notes', '')
        conn.execute('UPDATE films SET notes=? WHERE id=?', (notes, film_id))
        conn.commit()
        conn.close()
        return jsonify({'ok': True})
    row = conn.execute('SELECT notes FROM films WHERE id=?', (film_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    return jsonify({'notes': row[0] or ''})


@app.route('/games/<int:game_id>/notes', methods=['GET', 'POST'])
def game_notes(game_id):
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    if request.method == 'POST':
        notes = (request.get_json(silent=True) or {}).get('notes', '')
        conn.execute('UPDATE games SET notes=? WHERE id=?', (notes, game_id))
        conn.commit()
        conn.close()
        return jsonify({'ok': True})
    row = conn.execute('SELECT notes FROM games WHERE id=?', (game_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    return jsonify({'notes': row[0] or ''})


# ── Battle helpers ────────────────────────────────────────────────────────────

def _expire_rankings(cur):
    one_year_ago = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()
    cur.execute(
        'UPDATE films SET rank = NULL, date_ranked = NULL '
        'WHERE date_ranked IS NOT NULL AND date_ranked < ?',
        (one_year_ago,)
    )


def _film_card(cur, film_id):
    film = cur.execute(
        'SELECT id, title, release_date, poster_path, rank FROM films WHERE id = ?',
        (film_id,)
    ).fetchone()
    director = cur.execute(
        "SELECT name FROM crew_members WHERE film_id = ? AND job = 'Director' LIMIT 1",
        (film_id,)
    ).fetchone()
    cast = cur.execute(
        'SELECT name FROM cast_members WHERE film_id = ? AND billing_order <= 2 '
        'ORDER BY billing_order LIMIT 3',
        (film_id,)
    ).fetchall()
    composer = cur.execute(
        "SELECT name FROM crew_members WHERE film_id = ? "
        "AND job IN ('Original Music Composer', 'Music') "
        "ORDER BY CASE job WHEN 'Original Music Composer' THEN 0 ELSE 1 END LIMIT 1",
        (film_id,)
    ).fetchone()
    return {
        'id': film['id'],
        'title': film['title'],
        'year': film['release_date'][:4] if film['release_date'] else None,
        'poster_path': film['poster_path'],
        'rank': film['rank'],
        'director': director['name'] if director else None,
        'cast': [r['name'] for r in cast],
        'composer': composer['name'] if composer else None,
    }


# ── Battle routes ──────────────────────────────────────────────────────────────

@app.route('/battle/next')
def battle_next():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    _expire_rankings(cur)
    conn.commit()

    total = cur.execute('SELECT COUNT(*) as n FROM films').fetchone()['n']
    if total < 2:
        conn.close()
        return jsonify({'error': 'Need at least 2 films to battle'}), 400

    state = cur.execute(
        'SELECT challenger_id, opponent_id FROM battle_state WHERE id = 1'
    ).fetchone()

    if state and state['challenger_id'] and state['opponent_id']:
        challenger_id = state['challenger_id']
        opponent_id   = state['opponent_id']
    else:
        unranked = [r['id'] for r in cur.execute(
            'SELECT id FROM films WHERE rank IS NULL'
        ).fetchall()]
        ranked = cur.execute(
            'SELECT id, rank FROM films WHERE rank IS NOT NULL ORDER BY rank DESC'
        ).fetchall()

        if len(unranked) >= 2 and not ranked:
            # First ever battles — pick 2 random unranked
            challenger_id, opponent_id = random.sample(unranked, 2)
        elif unranked:
            # Normal case: random unranked vs bottom of ranked list
            challenger_id = random.choice(unranked)
            opponent_id   = ranked[0]['id']
        elif len(ranked) >= 2:
            # All ranked: temporarily unrank a random non-bottom film
            bottom_id     = ranked[0]['id']
            challenger_id = random.choice([r['id'] for r in ranked if r['id'] != bottom_id])
            cur.execute(
                'UPDATE films SET rank = NULL, date_ranked = NULL WHERE id = ?',
                (challenger_id,)
            )
            conn.commit()
            ranked = cur.execute(
                'SELECT id, rank FROM films WHERE rank IS NOT NULL ORDER BY rank DESC'
            ).fetchall()
            opponent_id = ranked[0]['id']
        else:
            conn.close()
            return jsonify({'error': 'Not enough films to battle'}), 400

        cur.execute(
            'INSERT OR REPLACE INTO battle_state (id, challenger_id, opponent_id) VALUES (1,?,?)',
            (challenger_id, opponent_id)
        )
        conn.commit()

    ranked_count = cur.execute(
        'SELECT COUNT(*) as n FROM films WHERE rank IS NOT NULL'
    ).fetchone()['n']

    result = jsonify({
        'challenger':   _film_card(cur, challenger_id),
        'opponent':     _film_card(cur, opponent_id),
        'ranked_count': ranked_count,
        'total_films':  total,
    })
    conn.close()
    return result


@app.route('/battle/result', methods=['POST'])
def battle_result():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json(silent=True) or {}
    winner_id = data.get('winner_id')
    if not winner_id:
        return jsonify({'error': 'winner_id required'}), 400

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    state = cur.execute(
        'SELECT challenger_id, opponent_id FROM battle_state WHERE id = 1'
    ).fetchone()
    if not state:
        conn.close()
        return jsonify({'error': 'No active battle'}), 400

    challenger_id = state['challenger_id']
    opponent_id   = state['opponent_id']
    now = datetime.now(timezone.utc).isoformat()

    opponent_rank = cur.execute(
        'SELECT rank FROM films WHERE id = ?', (opponent_id,)
    ).fetchone()['rank']

    if winner_id == challenger_id:
        if opponent_rank is None:
            # First battle: both unranked, challenger wins
            cur.execute('UPDATE films SET rank=1, date_ranked=? WHERE id=?', (now, challenger_id))
            cur.execute('UPDATE films SET rank=2, date_ranked=? WHERE id=?', (now, opponent_id))
            cur.execute('DELETE FROM battle_state WHERE id=1')
            conn.commit()
            conn.close()
            return jsonify({'status': 'placed', 'new_rank': 1})

        next_rank = opponent_rank - 1
        if next_rank < 1:
            # Beats everyone — becomes #1
            cur.execute('UPDATE films SET rank = rank + 1 WHERE rank IS NOT NULL')
            cur.execute('UPDATE films SET rank=1, date_ranked=? WHERE id=?', (now, challenger_id))
            cur.execute('DELETE FROM battle_state WHERE id=1')
            conn.commit()
            conn.close()
            return jsonify({'status': 'placed', 'new_rank': 1})

        next_opponent = cur.execute(
            'SELECT id FROM films WHERE rank = ?', (next_rank,)
        ).fetchone()
        if not next_opponent:
            # Gap in ranks (shouldn't happen) — place at next_rank + 1
            cur.execute('UPDATE films SET rank=?, date_ranked=? WHERE id=?',
                        (next_rank + 1, now, challenger_id))
            cur.execute('DELETE FROM battle_state WHERE id=1')
            conn.commit()
            conn.close()
            return jsonify({'status': 'placed', 'new_rank': next_rank + 1})

        cur.execute('UPDATE battle_state SET opponent_id=? WHERE id=1', (next_opponent['id'],))
        conn.commit()
        conn.close()
        return jsonify({'status': 'continue'})

    else:
        # Opponent wins — place challenger just below opponent
        if opponent_rank is None:
            # First battle: opponent wins
            cur.execute('UPDATE films SET rank=1, date_ranked=? WHERE id=?', (now, opponent_id))
            cur.execute('UPDATE films SET rank=2, date_ranked=? WHERE id=?', (now, challenger_id))
            cur.execute('DELETE FROM battle_state WHERE id=1')
            conn.commit()
            conn.close()
            return jsonify({'status': 'placed', 'new_rank': 2})

        place_at = opponent_rank + 1
        cur.execute('UPDATE films SET rank = rank + 1 WHERE rank >= ?', (place_at,))
        cur.execute('UPDATE films SET rank=?, date_ranked=? WHERE id=?',
                    (place_at, now, challenger_id))
        cur.execute('DELETE FROM battle_state WHERE id=1')
        conn.commit()
        conn.close()
        return jsonify({'status': 'placed', 'new_rank': place_at})


@app.route('/battle/rankings')
def battle_rankings():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        'SELECT id, title, release_date, poster_path, rank, date_ranked '
        'FROM films WHERE rank IS NOT NULL ORDER BY rank'
    ).fetchall()
    conn.close()
    return jsonify([{
        'id':          r['id'],
        'title':       r['title'],
        'year':        r['release_date'][:4] if r['release_date'] else None,
        'poster_path': r['poster_path'],
        'rank':        r['rank'],
        'date_ranked': r['date_ranked'],
    } for r in rows])


# ── Game battle helpers ────────────────────────────────────────────────────────

def _expire_game_rankings(cur):
    one_year_ago = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()
    cur.execute(
        'UPDATE games SET rank = NULL, date_ranked = NULL '
        'WHERE date_ranked IS NOT NULL AND date_ranked < ?',
        (one_year_ago,)
    )


def _game_card(cur, game_id):
    game = cur.execute(
        'SELECT id, name, first_release_date, cover_image_id, rank, lead_writer, composer FROM games WHERE id=?',
        (game_id,)
    ).fetchone()
    devs = cur.execute(
        'SELECT name FROM game_developers WHERE game_id=? LIMIT 2', (game_id,)
    ).fetchall()
    return {
        'id':             game['id'],
        'title':          game['name'],
        'year':           game['first_release_date'][:4] if game['first_release_date'] else None,
        'cover_image_id': game['cover_image_id'],
        'rank':           game['rank'],
        'developer':      ', '.join(r['name'] for r in devs) if devs else None,
        'lead_writer':    game['lead_writer'],
        'composer':       game['composer'],
    }


# ── Game battle routes ─────────────────────────────────────────────────────────

@app.route('/game_battle/next')
def game_battle_next():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    _expire_game_rankings(cur)
    conn.commit()

    total = cur.execute('SELECT COUNT(*) as n FROM games').fetchone()['n']
    if total < 2:
        conn.close()
        return jsonify({'error': 'Need at least 2 games to battle'}), 400

    state = cur.execute(
        'SELECT challenger_id, opponent_id FROM game_battle_state WHERE id = 1'
    ).fetchone()

    if state and state['challenger_id'] and state['opponent_id']:
        challenger_id = state['challenger_id']
        opponent_id   = state['opponent_id']
    else:
        unranked = [r['id'] for r in cur.execute('SELECT id FROM games WHERE rank IS NULL').fetchall()]
        ranked   = cur.execute('SELECT id, rank FROM games WHERE rank IS NOT NULL ORDER BY rank DESC').fetchall()

        if len(unranked) >= 2 and not ranked:
            challenger_id, opponent_id = random.sample(unranked, 2)
        elif unranked:
            challenger_id = random.choice(unranked)
            opponent_id   = ranked[0]['id']
        elif len(ranked) >= 2:
            bottom_id     = ranked[0]['id']
            challenger_id = random.choice([r['id'] for r in ranked if r['id'] != bottom_id])
            cur.execute('UPDATE games SET rank = NULL, date_ranked = NULL WHERE id = ?', (challenger_id,))
            conn.commit()
            ranked    = cur.execute('SELECT id, rank FROM games WHERE rank IS NOT NULL ORDER BY rank DESC').fetchall()
            opponent_id = ranked[0]['id']
        else:
            conn.close()
            return jsonify({'error': 'Not enough games to battle'}), 400

        cur.execute(
            'INSERT OR REPLACE INTO game_battle_state (id, challenger_id, opponent_id) VALUES (1,?,?)',
            (challenger_id, opponent_id)
        )
        conn.commit()

    ranked_count = cur.execute('SELECT COUNT(*) as n FROM games WHERE rank IS NOT NULL').fetchone()['n']
    result = jsonify({
        'challenger':   _game_card(cur, challenger_id),
        'opponent':     _game_card(cur, opponent_id),
        'ranked_count': ranked_count,
        'total_films':  total,
    })
    conn.close()
    return result


@app.route('/game_battle/result', methods=['POST'])
def game_battle_result():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401

    data      = request.get_json(silent=True) or {}
    winner_id = data.get('winner_id')
    if not winner_id:
        return jsonify({'error': 'winner_id required'}), 400

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()
    now  = datetime.now(timezone.utc).isoformat()

    state = cur.execute('SELECT challenger_id, opponent_id FROM game_battle_state WHERE id = 1').fetchone()
    if not state:
        conn.close()
        return jsonify({'error': 'No active battle'}), 400

    challenger_id = state['challenger_id']
    opponent_id   = state['opponent_id']
    opponent_rank = cur.execute('SELECT rank FROM games WHERE id=?', (opponent_id,)).fetchone()['rank']

    def place_challenger(rank):
        cur.execute('UPDATE games SET rank = rank + 1 WHERE rank >= ?', (rank,))
        cur.execute('UPDATE games SET rank=?, date_ranked=? WHERE id=?', (rank, now, challenger_id))
        cur.execute('DELETE FROM game_battle_state WHERE id=1')
        conn.commit()
        conn.close()

    if winner_id == challenger_id:
        if opponent_rank is None:
            cur.execute('UPDATE games SET rank=1, date_ranked=? WHERE id=?', (now, challenger_id))
            cur.execute('UPDATE games SET rank=2, date_ranked=? WHERE id=?', (now, opponent_id))
            cur.execute('DELETE FROM game_battle_state WHERE id=1')
            conn.commit()
            conn.close()
            return jsonify({'status': 'placed', 'new_rank': 1})

        next_rank = opponent_rank - 1
        if next_rank < 1:
            cur.execute('UPDATE games SET rank = rank + 1 WHERE rank IS NOT NULL')
            cur.execute('UPDATE games SET rank=1, date_ranked=? WHERE id=?', (now, challenger_id))
            cur.execute('DELETE FROM game_battle_state WHERE id=1')
            conn.commit()
            conn.close()
            return jsonify({'status': 'placed', 'new_rank': 1})

        next_opponent = cur.execute('SELECT id FROM games WHERE rank=?', (next_rank,)).fetchone()
        if not next_opponent:
            place_challenger(next_rank + 1)
            return jsonify({'status': 'placed', 'new_rank': next_rank + 1})

        cur.execute('UPDATE game_battle_state SET opponent_id=? WHERE id=1', (next_opponent['id'],))
        conn.commit()
        conn.close()
        return jsonify({'status': 'continue'})
    else:
        if opponent_rank is None:
            cur.execute('UPDATE games SET rank=1, date_ranked=? WHERE id=?', (now, opponent_id))
            cur.execute('UPDATE games SET rank=2, date_ranked=? WHERE id=?', (now, challenger_id))
            cur.execute('DELETE FROM game_battle_state WHERE id=1')
            conn.commit()
            conn.close()
            return jsonify({'status': 'placed', 'new_rank': 2})

        place_challenger(opponent_rank + 1)
        return jsonify({'status': 'placed', 'new_rank': opponent_rank + 1})


@app.route('/game_battle/rankings')
def game_battle_rankings():
    if not _auth():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()
    rows = cur.execute(
        'SELECT id, name, first_release_date, cover_image_id, rank, date_ranked FROM games WHERE rank IS NOT NULL ORDER BY rank'
    ).fetchall()
    conn.close()
    return jsonify([{
        'id':             r['id'],
        'title':          r['name'],
        'year':           r['first_release_date'][:4] if r['first_release_date'] else None,
        'cover_image_id': r['cover_image_id'],
        'rank':           r['rank'],
        'date_ranked':    r['date_ranked'],
    } for r in rows])


if __name__ == '__main__':
    print(f'Starting server — API key loaded from {"flask_api_key.txt" if os.path.exists(os.path.join(BASE_DIR, "flask_api_key.txt")) else "environment"}')
    app.run(host='0.0.0.0', port=5000, debug=False)
