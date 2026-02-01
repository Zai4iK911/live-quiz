"""
Live Quiz Backend (Flask + SocketIO + Postgres) — Render-ready

Features:
- Rooms + PIN
- Host + Player tokens (stable identity)
- Reconnect for host & players (state hydration from DB)
- Server-authoritative timer (QUESTION -> RESULTS)
- Background watchdog finalizes overdue questions (survives restarts / Render sleep)
- Cleanup rooms if host disconnected too long
- Rate limits + max players per room
- State guards

Env:
- SECRET_KEY (recommended)
- DATABASE_URL (Render Postgres URL)
- PORT (Render provides)
"""

import os
import time
import uuid
import random
import string
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import psycopg2
import psycopg2.extras
from flask import Flask, request, send_from_directory
from flask_socketio import SocketIO, emit, join_room

# ---------------------------
# Config
# ---------------------------

ROOM_TTL_SECONDS = 10 * 60  # host offline TTL to close room

MAX_PLAYERS_PER_ROOM = 50

JOIN_RATE_LIMIT = 5
JOIN_RATE_WINDOW = 10

ANSWER_RATE_LIMIT = 3
ANSWER_RATE_WINDOW = 3

CREATE_ROOM_LIMIT = 3
CREATE_ROOM_WINDOW = 60

WATCHDOG_INTERVAL_SECONDS = 2
CLEANUP_INTERVAL_SECONDS = 30

# ---------------------------
# Demo questions (replace later with DB)
# ---------------------------

QUESTIONS = [
    {
        "text": "Скільки буде 2 + 2 ?",
        "options": ["3", "4", "5", "6"],
        "correct_index": 1,
        "duration": 15,
    },
    {
        "text": "Столиця України?",
        "options": ["Львів", "Харків", "Київ", "Одеса"],
        "correct_index": 2,
        "duration": 15,
    },
]

# ---------------------------
# App + SocketIO
# ---------------------------

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret")

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="eventlet",
)

# ---------------------------
# In-memory (ephemeral) indices
# ---------------------------

# socket_index maps a socket sid to role+identity to handle disconnect quickly
# socket_index[sid] = {"pin": "123456", "role": "host|player", "token": "<uuid str>"}
socket_index: Dict[str, Dict[str, str]] = {}

# rate limit buckets: key -> list[timestamps]
rate_limits: Dict[str, list] = {}

# ---------------------------
# DB helpers
# ---------------------------

def _db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL env var is required")
    return url

def db_conn():
    # Render Postgres requires sslmode=require in most cases //disable
    return psycopg2.connect(_db_url(), sslmode="require")

def db_one(sql: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    with db_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None

def db_all(sql: str, params: Tuple[Any, ...] = ()) -> list[Dict[str, Any]]:
    with db_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]

def db_exec(sql: str, params: Tuple[Any, ...] = ()) -> None:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def ensure_tables():
    ddl = """
    CREATE TABLE IF NOT EXISTS rooms (
      id BIGSERIAL PRIMARY KEY,
      pin VARCHAR(6) UNIQUE NOT NULL,
      host_token UUID NOT NULL,

      state VARCHAR(16) NOT NULL DEFAULT 'LOBBY',
      current_question_index INT NOT NULL DEFAULT -1,
      question_started_at TIMESTAMPTZ,
      question_ends_at TIMESTAMPTZ,

      host_connected BOOLEAN NOT NULL DEFAULT TRUE,
      host_disconnected_at TIMESTAMPTZ,

      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      closed_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS players (
      id BIGSERIAL PRIMARY KEY,
      room_id BIGINT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      player_token UUID NOT NULL,
      name TEXT NOT NULL,
      score INT NOT NULL DEFAULT 0,
      connected BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(room_id, player_token)
    );

    CREATE TABLE IF NOT EXISTS answers (
      id BIGSERIAL PRIMARY KEY,
      room_id BIGINT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      player_token UUID NOT NULL,
      question_index INT NOT NULL,
      option_index INT NOT NULL,
      answered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(room_id, player_token, question_index)
    );

    CREATE INDEX IF NOT EXISTS idx_rooms_pin ON rooms(pin);
    CREATE INDEX IF NOT EXISTS idx_rooms_state ON rooms(state);
    CREATE INDEX IF NOT EXISTS idx_players_room ON players(room_id);
    CREATE INDEX IF NOT EXISTS idx_answers_room_q ON answers(room_id, question_index);
    """
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()

# ---------------------------
# Rate limiting
# ---------------------------

def is_rate_limited(key: str, limit: int, window: int) -> bool:
    now = time.time()
    bucket = rate_limits.setdefault(key, [])
    bucket[:] = [t for t in bucket if now - t < window]
    if len(bucket) >= limit:
        return True
    bucket.append(now)
    return False

# ---------------------------
# Helpers
# ---------------------------

def generate_pin() -> str:
    return "".join(random.choices(string.digits, k=6))

def generate_unique_pin(max_tries: int = 20) -> str:
    for _ in range(max_tries):
        pin = generate_pin()
        exists = db_one("SELECT 1 FROM rooms WHERE pin=%s AND closed_at IS NULL", (pin,))
        if not exists:
            return pin
    raise RuntimeError("Failed to generate unique PIN")

def require_room_state(room: Dict[str, Any], allowed_states: list[str]) -> bool:
    return (room.get("state") or "").upper() in [s.upper() for s in allowed_states]

def leaderboard_for_room_id(room_id: int) -> list[Dict[str, Any]]:
    rows = db_all(
        "SELECT name, score, connected FROM players WHERE room_id=%s ORDER BY score DESC, created_at ASC",
        (room_id,),
    )
    return [{"name": r["name"], "score": int(r["score"]), "connected": bool(r["connected"])} for r in rows]

def get_room_by_pin(pin: str) -> Optional[Dict[str, Any]]:
    return db_one("SELECT * FROM rooms WHERE pin=%s AND closed_at IS NULL", (pin,))

def touch_room_activity(room_id: int) -> None:
    db_exec("UPDATE rooms SET last_activity_at=now() WHERE id=%s", (room_id,))

def close_room(pin: str, reason: str = "host_timeout") -> None:
    room = get_room_by_pin(pin)
    if not room:
        return

    db_exec("UPDATE rooms SET closed_at=now(), state='FINISHED', last_activity_at=now() WHERE id=%s", (room["id"],))
    socketio.emit("room_closed", {"reason": reason}, room=pin)

# ---------------------------
# HTTP
# ---------------------------

@app.route("/")
def index():
    return "Live quiz backend is running"

# ---------------------------
# Socket core
# ---------------------------

@socketio.on("connect")
def on_connect():
    # no-op; identity assigned on join/create/reconnect
    pass

@socketio.on("disconnect")
def on_disconnect():
    sid = request.sid
    meta = socket_index.pop(sid, None)
    if not meta:
        return

    pin = meta.get("pin")
    role = meta.get("role")
    token = meta.get("token")

    room = get_room_by_pin(pin) if pin else None
    if not room:
        return

    if role == "host":
        db_exec(
            "UPDATE rooms SET host_connected=false, host_disconnected_at=now(), last_activity_at=now() WHERE id=%s",
            (room["id"],),
        )
    elif role == "player" and token:
        try:
            player_token = uuid.UUID(token)
        except Exception:
            return
        db_exec(
            "UPDATE players SET connected=false, last_seen_at=now() WHERE room_id=%s AND player_token=%s",
            (room["id"], str(player_token)),
        )

# ---------------------------
# Host: create room
# ---------------------------

@socketio.on("create_room")
def create_room():
    # anti-flood
    if is_rate_limited(f"create:{request.sid}", CREATE_ROOM_LIMIT, CREATE_ROOM_WINDOW):
        emit("error", {"message": "Too many rooms created. Slow down."})
        return

    pin = generate_unique_pin()
    host_token = uuid.uuid4()

    db_exec(
        """
        INSERT INTO rooms(pin, host_token, state, current_question_index, host_connected, host_disconnected_at)
        VALUES (%s, %s, 'LOBBY', -1, true, NULL)
        """,
        (pin, str(host_token)),
    )

    join_room(pin)
    socket_index[request.sid] = {"pin": pin, "role": "host", "token": str(host_token)}

    emit(
        "room_created",
        {
            "pin": pin,
            "host_token": str(host_token),
        },
    )

# ---------------------------
# Player: join room
# ---------------------------

@socketio.on("join_room")
def join_game(data):
    # anti-flood
    if is_rate_limited(f"join:{request.sid}", JOIN_RATE_LIMIT, JOIN_RATE_WINDOW):
        emit("join_error", {"message": "Too many join attempts"})
        return

    pin = (data or {}).get("pin")
    name = (data or {}).get("name") or ""

    name = name.strip()
    if not pin or len(str(pin)) != 6:
        emit("join_error", {"message": "Invalid PIN"})
        return

    if not name or len(name) > 40:
        emit("join_error", {"message": "Invalid name"})
        return

    room = get_room_by_pin(str(pin))
    if not room:
        emit("join_error", {"message": "Room not found"})
        return

    # cannot join if finished/closed
    if (room.get("state") or "").upper() == "FINISHED":
        emit("join_error", {"message": "Game finished"})
        return

    # max players
    count_row = db_one("SELECT COUNT(*) AS c FROM players WHERE room_id=%s", (room["id"],))
    if count_row and int(count_row["c"]) >= MAX_PLAYERS_PER_ROOM:
        emit("join_error", {"message": "Room is full"})
        return

    player_token = uuid.uuid4()

    db_exec(
        """
        INSERT INTO players(room_id, player_token, name, score, connected)
        VALUES (%s, %s, %s, 0, true)
        """,
        (room["id"], str(player_token), name),
    )

    touch_room_activity(room["id"])

    join_room(str(pin))
    socket_index[request.sid] = {"pin": str(pin), "role": "player", "token": str(player_token)}

    emit("joined", {"player_token": str(player_token)})

    # notify host (if host is currently connected, it will see it via socket room)
    socketio.emit("player_joined", {"name": name}, room=str(pin))

# ---------------------------
# Player: reconnect
# ---------------------------

@socketio.on("reconnect_player")
def reconnect_player(data):
    pin = (data or {}).get("pin")
    player_token_s = (data or {}).get("player_token")

    if not pin or not player_token_s:
        emit("reconnect_error", {"message": "Missing data"})
        return

    room = get_room_by_pin(str(pin))
    if not room:
        emit("reconnect_error", {"message": "Room not found"})
        return

    try:
        player_token = uuid.UUID(player_token_s)
    except Exception:
        emit("reconnect_error", {"message": "Invalid token"})
        return

    player = db_one(
        "SELECT * FROM players WHERE room_id=%s AND player_token=%s",
        (room["id"], str(player_token)),
    )
    if not player:
        emit("reconnect_error", {"message": "Player not found"})
        return

    db_exec(
        "UPDATE players SET connected=true, last_seen_at=now() WHERE id=%s",
        (player["id"],),
    )
    touch_room_activity(room["id"])

    join_room(str(pin))
    socket_index[request.sid] = {"pin": str(pin), "role": "player", "token": str(player_token)}

    payload: Dict[str, Any] = {
        "state": room["state"],
        "score": int(player["score"]),
        "current_question_index": int(room["current_question_index"]),
    }

    # hydrate QUESTION
    if (room["state"] or "").upper() == "QUESTION" and room.get("question_ends_at"):
        q_index = int(room["current_question_index"])
        if 0 <= q_index < len(QUESTIONS):
            q = QUESTIONS[q_index]
            payload["question"] = {
                "text": q["text"],
                "options": q["options"],
                "ends_at": room["question_ends_at"].timestamp(),
            }

    # hydrate RESULTS
    if (room["state"] or "").upper() == "RESULTS":
        q_index = int(room["current_question_index"])
        if 0 <= q_index < len(QUESTIONS):
            q = QUESTIONS[q_index]
            payload["results"] = {
                "correct_index": q["correct_index"],
                "leaderboard": leaderboard_for_room_id(room["id"]),
            }

    emit("reconnected", payload)

# ---------------------------
# Host: reconnect
# ---------------------------

@socketio.on("reconnect_host")
def reconnect_host(data):
    pin = (data or {}).get("pin")
    host_token_s = (data or {}).get("host_token")

    if not pin or not host_token_s:
        emit("reconnect_error", {"message": "Missing data"})
        return

    room = get_room_by_pin(str(pin))
    if not room:
        emit("reconnect_error", {"message": "Room not found"})
        return

    try:
        host_token = uuid.UUID(host_token_s)
    except Exception:
        emit("reconnect_error", {"message": "Invalid host token"})
        return

    if str(room["host_token"]) != str(host_token):
        emit("reconnect_error", {"message": "Invalid host token"})
        return

    db_exec(
        "UPDATE rooms SET host_connected=true, host_disconnected_at=NULL, last_activity_at=now() WHERE id=%s",
        (room["id"],),
    )

    join_room(str(pin))
    socket_index[request.sid] = {"pin": str(pin), "role": "host", "token": str(host_token)}

    payload: Dict[str, Any] = {
        "state": room["state"],
        "current_question_index": int(room["current_question_index"]),
        "players": leaderboard_for_room_id(room["id"]),
    }

    if (room["state"] or "").upper() == "QUESTION" and room.get("question_ends_at"):
        q_index = int(room["current_question_index"])
        if 0 <= q_index < len(QUESTIONS):
            q = QUESTIONS[q_index]
            payload["question"] = {
                "text": q["text"],
                "options": q["options"],
                "ends_at": room["question_ends_at"].timestamp(),
            }

    if (room["state"] or "").upper() == "RESULTS":
        q_index = int(room["current_question_index"])
        if 0 <= q_index < len(QUESTIONS):
            q = QUESTIONS[q_index]
            payload["results"] = {
                "correct_index": q["correct_index"],
                "leaderboard": leaderboard_for_room_id(room["id"]),
            }

    emit("host_reconnected", payload)

# ---------------------------
# Host actions (state-guarded + token-verified)
# ---------------------------

def verify_host_and_get_room(data) -> Optional[Dict[str, Any]]:
    pin = (data or {}).get("pin")
    host_token_s = (data or {}).get("host_token")
    if not pin or not host_token_s:
        emit("error", {"message": "Missing pin/host_token"})
        return None

    room = get_room_by_pin(str(pin))
    if not room:
        emit("error", {"message": "Room not found"})
        return None

    if str(room["host_token"]) != str(host_token_s):
        emit("error", {"message": "Invalid host token"})
        return None

    # also make sure caller is the current host socket (prevents token reuse from another socket without reconnect)
    meta = socket_index.get(request.sid)
    if not meta or meta.get("role") != "host" or meta.get("pin") != str(pin):
        emit("error", {"message": "Host socket not registered. Reconnect host."})
        return None

    return room

@socketio.on("start_game")
def start_game(data):
    room = verify_host_and_get_room(data)
    if not room:
        return

    if not require_room_state(room, ["LOBBY"]):
        emit("error", {"message": "start_game allowed only in LOBBY"})
        return

    # move to first question
    db_exec(
        "UPDATE rooms SET current_question_index=0, last_activity_at=now() WHERE id=%s",
        (room["id"],),
    )
    start_question(pin=str(room["pin"]))

@socketio.on("next_question")
def next_question(data):
    room = verify_host_and_get_room(data)
    if not room:
        return

    if not require_room_state(room, ["RESULTS"]):
        emit("error", {"message": "next_question allowed only in RESULTS"})
        return

    next_index = int(room["current_question_index"]) + 1

    if next_index >= len(QUESTIONS):
        db_exec(
            "UPDATE rooms SET state='FINISHED', closed_at=now(), last_activity_at=now() WHERE id=%s",
            (room["id"],),
        )
        socketio.emit(
            "game_finished",
            {"leaderboard": leaderboard_for_room_id(room["id"])},
            room=str(room["pin"]),
        )
        return

    db_exec(
        "UPDATE rooms SET current_question_index=%s, last_activity_at=now() WHERE id=%s",
        (next_index, room["id"]),
    )

    start_question(pin=str(room["pin"]))

# ---------------------------
# Answers (player) + scoring
# ---------------------------

@socketio.on("submit_answer")
def submit_answer(data):
    # anti-flood
    if is_rate_limited(f"answer:{request.sid}", ANSWER_RATE_LIMIT, ANSWER_RATE_WINDOW):
        return

    pin = (data or {}).get("pin")
    option = (data or {}).get("option")
    player_token_s = (data or {}).get("player_token")

    if pin is None or option is None or player_token_s is None:
        return

    room = get_room_by_pin(str(pin))
    if not room:
        return

    # state guard
    if not require_room_state(room, ["QUESTION"]):
        return

    # validate player identity to this socket
    meta = socket_index.get(request.sid)
    if not meta or meta.get("role") != "player" or meta.get("pin") != str(pin) or meta.get("token") != str(player_token_s):
        return

    try:
        player_token = uuid.UUID(str(player_token_s))
    except Exception:
        return

    # validate option
    try:
        option_index = int(option)
    except Exception:
        return

    q_index = int(room["current_question_index"])
    if not (0 <= q_index < len(QUESTIONS)):
        return
    q = QUESTIONS[q_index]
    if not (0 <= option_index < len(q["options"])):
        return

    # reject after time ends
    if not room.get("question_ends_at"):
        return

    if utc_now() > room["question_ends_at"]:
        return

    # store answer once (ON CONFLICT DO NOTHING)
    db_exec(
        """
        INSERT INTO answers(room_id, player_token, question_index, option_index)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (room_id, player_token, question_index) DO NOTHING
        """,
        (room["id"], str(player_token), q_index, option_index),
    )
    touch_room_activity(room["id"])

def start_question(pin: str):
    room = get_room_by_pin(pin)
    if not room:
        return

    # guard: can start question only from LOBBY or RESULTS
    if not require_room_state(room, ["LOBBY", "RESULTS"]):
        return

    q_index = int(room["current_question_index"])
    if not (0 <= q_index < len(QUESTIONS)):
        return

    question = QUESTIONS[q_index]
    duration = int(question["duration"])

    db_exec(
        """
        UPDATE rooms
        SET state='QUESTION',
            question_started_at=now(),
            question_ends_at=now() + (%s || ' seconds')::interval,
            last_activity_at=now()
        WHERE id=%s
        """,
        (duration, room["id"]),
    )

    # hydrate updated room
    room2 = get_room_by_pin(pin)
    if not room2 or not room2.get("question_ends_at"):
        return

    socketio.emit(
        "question_started",
        {
            "index": q_index,
            "text": question["text"],
            "options": question["options"],
            "ends_at": room2["question_ends_at"].timestamp(),
        },
        room=pin,
    )

def finish_question_if_due(pin: str):
    """
    Finalize QUESTION -> RESULTS if:
    - state is QUESTION
    - ends_at passed
    Uses atomic UPDATE ... WHERE state='QUESTION' to prevent double-finalize.
    """
    room = get_room_by_pin(pin)
    if not room:
        return

    if not require_room_state(room, ["QUESTION"]):
        return

    if not room.get("question_ends_at"):
        return

    if utc_now() <= room["question_ends_at"]:
        return

    # acquire finalize lock
    locked = db_one(
        """
        UPDATE rooms
        SET state='RESULTS', last_activity_at=now()
        WHERE id=%s AND state='QUESTION'
        RETURNING id, pin, current_question_index, question_started_at, question_ends_at
        """,
        (room["id"],),
    )
    if not locked:
        return

    room_id = int(locked["id"])
    q_index = int(locked["current_question_index"])
    if not (0 <= q_index < len(QUESTIONS)):
        return

    question = QUESTIONS[q_index]
    correct = int(question["correct_index"])
    duration = int(question["duration"])

    # fetch answers for this question
    answers = db_all(
        """
        SELECT player_token, option_index, answered_at
        FROM answers
        WHERE room_id=%s AND question_index=%s
        """,
        (room_id, q_index),
    )

    # compute points
    base_points = 1000
    ends_at: datetime = locked["question_ends_at"]

    # group by player (already unique by constraint)
    for a in answers:
        try:
            opt = int(a["option_index"])
        except Exception:
            continue
        if opt != correct:
            continue

        answered_at: datetime = a["answered_at"]
        # time_left in seconds
        time_left = (ends_at - answered_at).total_seconds()
        if time_left < 0:
            time_left = 0.0

        bonus = int((time_left / max(1, duration)) * 500)
        if bonus < 0:
            bonus = 0

        points = base_points + bonus

        db_exec(
            "UPDATE players SET score = score + %s, last_seen_at=now() WHERE room_id=%s AND player_token=%s",
            (points, room_id, str(a["player_token"])),
        )

    # emit results
    socketio.emit(
        "question_results",
        {
            "correct_index": correct,
            "leaderboard": leaderboard_for_room_id(room_id),
        },
        room=str(pin),
    )

# ---------------------------
# Background tasks
# ---------------------------

def watchdog_task():
    """
    Every few seconds:
    - finalize any rooms stuck in QUESTION past ends_at
    This makes the system survive restarts / Render sleep.
    """
    while True:
        socketio.sleep(WATCHDOG_INTERVAL_SECONDS)

        rooms_due = db_all(
            """
            SELECT pin
            FROM rooms
            WHERE closed_at IS NULL
              AND state='QUESTION'
              AND question_ends_at IS NOT NULL
              AND question_ends_at < now()
            """
        )
        for r in rooms_due:
            finish_question_if_due(str(r["pin"]))

def cleanup_task():
    """
    Close rooms if host disconnected too long.
    """
    while True:
        socketio.sleep(CLEANUP_INTERVAL_SECONDS)

        # find rooms whose host is disconnected for longer than TTL
        rows = db_all(
            """
            SELECT pin
            FROM rooms
            WHERE closed_at IS NULL
              AND host_connected=false
              AND host_disconnected_at IS NOT NULL
              AND host_disconnected_at < now() - (%s || ' seconds')::interval
            """,
            (ROOM_TTL_SECONDS,),
        )
        for r in rows:
            close_room(str(r["pin"]), reason="host_timeout")


@app.route("/host")
def host_page():
    return send_from_directory("static", "host.html")

@app.route("/player")
def player_page():
    return send_from_directory("static", "player.html")

# ---------------------------
# Startup
# ---------------------------

ensure_tables()

if __name__ == "__main__":
    socketio.start_background_task(watchdog_task)
    socketio.start_background_task(cleanup_task)

    socketio.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
    )
