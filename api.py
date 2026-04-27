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
    for col, col_type in [('rank', 'INTEGER'), ('date_ranked', 'TEXT')]:
        try:
            conn.execute(f'ALTER TABLE films ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()

_ensure_battle_table()


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

    try:
        film_id = fetch_and_store_movie(
            tmdb_id,
            db_path=DB_PATH,
            first_watched=watch_ts,
            last_watched=watch_ts,
        )
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
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        '''SELECT id, title, release_date, datetime_last_watched, watch_count, poster_path
           FROM films ORDER BY datetime_last_watched DESC'''
    ).fetchall()
    conn.close()
    return jsonify([
        {
            'id': r[0],
            'title': r[1],
            'year': r[2][:4] if r[2] else None,
            'last_watched': r[3],
            'watch_count': r[4] or 0,
            'poster_path': r[5],
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

    summary = cur.execute('''
        SELECT
            COUNT(*) as total_films,
            COALESCE(SUM(watch_count), 0) as total_watches,
            COALESCE(SUM(CASE WHEN runtime IS NOT NULL THEN runtime * COALESCE(watch_count,1) ELSE 0 END), 0) as total_minutes,
            ROUND(AVG(CASE WHEN vote_average > 0 THEN vote_average END), 1) as avg_rating
        FROM films
    ''').fetchone()

    longest = cur.execute(
        'SELECT title, runtime FROM films WHERE runtime IS NOT NULL ORDER BY runtime DESC LIMIT 1'
    ).fetchone()

    most_watched = cur.execute(
        'SELECT title, watch_count FROM films WHERE watch_count > 1 ORDER BY watch_count DESC LIMIT 1'
    ).fetchone()

    top_rated = cur.execute(
        'SELECT title, vote_average FROM films WHERE vote_average > 0 ORDER BY vote_average DESC LIMIT 1'
    ).fetchone()

    top_directors = cur.execute('''
        SELECT name, COUNT(DISTINCT film_id) as count FROM crew_members
        WHERE job = 'Director' AND name IS NOT NULL
        GROUP BY name ORDER BY count DESC LIMIT 5
    ''').fetchall()

    top_actors = cur.execute('''
        SELECT name, COUNT(DISTINCT film_id) as count FROM cast_members
        WHERE billing_order <= 4 AND name IS NOT NULL
        GROUP BY name ORDER BY count DESC LIMIT 5
    ''').fetchall()

    top_genres = cur.execute('''
        SELECT name, COUNT(DISTINCT film_id) as count FROM genres
        WHERE name IS NOT NULL
        GROUP BY name ORDER BY count DESC LIMIT 8
    ''').fetchall()

    top_years = cur.execute('''
        SELECT SUBSTR(release_date,1,4) as label, COUNT(*) as count FROM films
        WHERE release_date IS NOT NULL AND LENGTH(release_date) >= 4
        GROUP BY label ORDER BY count DESC LIMIT 8
    ''').fetchall()

    decades = cur.execute('''
        SELECT (CAST(SUBSTR(release_date,1,4) AS INTEGER)/10)*10 as decade, COUNT(*) as count
        FROM films WHERE release_date IS NOT NULL AND LENGTH(release_date) >= 4
        GROUP BY decade ORDER BY decade
    ''').fetchall()

    cast_gender = cur.execute('''
        SELECT gender as label, COUNT(*) as count FROM cast_members
        WHERE gender IS NOT NULL AND gender != 'Unknown'
        GROUP BY gender ORDER BY count DESC
    ''').fetchall()

    cast_ethnicity = cur.execute('''
        SELECT ethnicity as label, COUNT(*) as count FROM cast_members
        WHERE ethnicity IS NOT NULL AND ethnicity != ''
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


if __name__ == '__main__':
    print(f'Starting server — API key loaded from {"flask_api_key.txt" if os.path.exists(os.path.join(BASE_DIR, "flask_api_key.txt")) else "environment"}')
    app.run(host='0.0.0.0', port=5000, debug=False)
