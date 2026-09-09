"""
Delayed radio: plays an internet radio stream behind live, so radio commentary
can be lined up with a TV picture that arrives late.

One shared upstream connection, one ring buffer, N listeners.
Each listener picks its own delay; the buffer is sized for the maximum.

Config: STREAM_URL, DELAY_SECONDS (starting position), MAX_DELAY_SECONDS,
STATION_NAME.
"""

import os
import time
import threading
from collections import deque
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

# ---------------------------------------------------------------- config

# Radio Garden's public endpoint for Radio Kiss Kiss Napoli. It redirects to
# the station's real stream, which requests follows automatically. Replace it
# with the direct Fluidstream URL by setting STREAM_URL once you have it.
DEFAULT_STREAM_URL = "https://radio.garden/api/ara/content/listen/EtRKdLPn/channel.mp3"

STREAM_URL = os.environ.get("STREAM_URL", DEFAULT_STREAM_URL).strip()
STATION_NAME = os.environ.get("STATION_NAME", "Kiss Kiss Napoli")

# Where the slider starts. Streaming TV apps typically run 30-90s behind.
DEFAULT_DELAY_SECONDS = int(os.environ.get("DELAY_SECONDS", "60"))
# How far back the buffer can reach, i.e. the top of the slider.
MAX_DELAY_SECONDS = int(os.environ.get("MAX_DELAY_SECONDS", "180"))

# Extra history kept beyond the maximum delay, so a listener that drifts
# slightly behind still finds its next chunk in the buffer.
SLACK_SECONDS = 30
RETENTION_SECONDS = MAX_DELAY_SECONDS + SLACK_SECONDS

CHUNK_SIZE = 4096
DEFAULT_CONTENT_TYPE = "audio/mpeg"

# --- next fixture (optional) -------------------------------------------
# Free API key from football-data.org. Without it this whole feature stays
# switched off and the page looks exactly as it did before.
FOOTBALL_TOKEN = os.environ.get("FOOTBALL_DATA_TOKEN", "").strip()
TEAM_NAME = os.environ.get("TEAM_NAME", "Napoli")
# Serie A and Champions League: the two free-tier competitions Napoli plays.
COMPETITIONS = [c for c in os.environ.get("COMPETITIONS", "SA,CL").split(",") if c]
FIXTURE_REFRESH_SECONDS = 1800
FOOTBALL_API_BASE = os.environ.get("FOOTBALL_API_BASE", "https://api.football-data.org/v4")

# --- live match clock (optional) ---------------------------------------
# Separate free key from api-football.com: it is the one that carries live
# in-play data. Without it the page falls back to the manual stopwatch.
# Budget: the free plan allows 100 calls a day, so one match at 90s costs
# about 65. Raise LIVE_POLL_SECONDS if you follow more than one a day.
API_FOOTBALL_KEY = os.environ.get("API_FOOTBALL_KEY", "").strip()
API_FOOTBALL_BASE = os.environ.get("API_FOOTBALL_BASE", "https://v3.football.api-sports.io")
LIVE_POLL_SECONDS = int(os.environ.get("LIVE_POLL_SECONDS", "90"))
# When no fixture list is available we cannot tell when a match is on, so we
# look in rarely: 900s is 96 calls a day, just inside the free allowance.
LIVE_IDLE_POLL_SECONDS = int(os.environ.get("LIVE_IDLE_POLL_SECONDS", "900"))

# ---------------------------------------------------------------- buffer


class RingBuffer:
    """Chunks of audio tagged with the wall-clock time they arrived.

    Chunks carry a monotonically increasing sequence number so a listener can
    hold a cursor that survives eviction from the left of the deque.
    """

    def __init__(self, retention_seconds):
        self.retention = retention_seconds
        self._chunks = deque()  # (seq, ts, bytes)
        self._next_seq = 0
        self._lock = threading.Lock()
        self._new_data = threading.Condition(self._lock)

    def append(self, data):
        now = time.time()
        with self._new_data:
            self._chunks.append((self._next_seq, now, data))
            self._next_seq += 1
            cutoff = now - self.retention
            while self._chunks and self._chunks[0][1] < cutoff:
                self._chunks.popleft()
            self._new_data.notify_all()

    def span_seconds(self):
        """How far back the buffer currently reaches, in seconds."""
        with self._lock:
            if not self._chunks:
                return 0.0
            return time.time() - self._chunks[0][1]

    def first_seq_at_or_after(self, ts):
        """Sequence number of the oldest chunk not older than ts.

        Returns None if the buffer is empty. If every chunk is older than ts
        (upstream is stalled) the newest sequence + 1 is returned, so the
        listener waits for fresh data instead of replaying old audio.
        """
        with self._lock:
            if not self._chunks:
                return None
            for seq, chunk_ts, _ in self._chunks:
                if chunk_ts >= ts:
                    return seq
            return self._chunks[-1][0] + 1

    def get(self, seq, timeout=1.0):
        """Return (ts, data) for `seq`.

        ('evicted', oldest_seq) if `seq` has already fallen out of the buffer.
        None if `seq` has not arrived yet within `timeout`.
        """
        deadline = time.time() + timeout
        with self._new_data:
            while True:
                if self._chunks:
                    base = self._chunks[0][0]
                    if seq < base:
                        return ("evicted", base)
                    idx = seq - base
                    if idx < len(self._chunks):
                        _, ts, data = self._chunks[idx]
                        return (ts, data)
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                self._new_data.wait(remaining)


buffer = RingBuffer(RETENTION_SECONDS)

# ---------------------------------------------------------------- upstream


class Upstream:
    def __init__(self):
        self.connected = False
        self.content_type = DEFAULT_CONTENT_TYPE
        # How many times the upstream had to be re-opened since startup.
        # Each reconnect is a suspect for drift: Icecast normally replays a
        # few seconds of backlog on connect, and that backlog lands in the
        # buffer stamped "now", pushing every listener further behind.
        self.reconnects = 0
        self.started_at = time.time()


upstream = Upstream()


def upstream_loop():
    backoff = 1.0
    connections = 0
    while True:
        try:
            # No Icy-MetaData header: we want pure audio bytes, with no
            # metadata interleaved into the stream.
            response = requests.get(
                STREAM_URL,
                headers={"User-Agent": "delayed-radio/1.0"},
                stream=True,
                timeout=(10, 30),
            )
            response.raise_for_status()

            content_type = response.headers.get("Content-Type")
            if content_type:
                upstream.content_type = content_type.split(";")[0].strip()

            upstream.connected = True
            backoff = 1.0
            connections += 1
            upstream.reconnects = connections - 1

            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    buffer.append(chunk)

            raise RuntimeError("upstream closed the connection")

        except Exception as exc:  # noqa: BLE001 - the loop must never die
            upstream.connected = False
            app.logger.warning("upstream error (%s), retrying in %.0fs", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


# ---------------------------------------------------------------- fixtures


class Fixtures:
    """The next match, refreshed in the background.

    Deliberately inert: any failure leaves `next_match` as it was (or None)
    and never touches the audio path.
    """

    def __init__(self):
        self.next_match = None
        self.last_error = None
        self.checked_at = None


fixtures = Fixtures()


def _team_of(side):
    side = side or {}
    return side.get("shortName") or side.get("name") or ""


def fetch_next_match():
    now = datetime.now(timezone.utc)
    params = {
        "dateFrom": now.strftime("%Y-%m-%d"),
        "dateTo": (now + timedelta(days=60)).strftime("%Y-%m-%d"),
    }
    # A match already under way should stay on screen, so look slightly back.
    floor = (now - timedelta(hours=4)).isoformat().replace("+00:00", "Z")

    best = None
    for code in COMPETITIONS:
        response = requests.get(
            f"{FOOTBALL_API_BASE}/competitions/{code}/matches",
            headers={"X-Auth-Token": FOOTBALL_TOKEN},
            params=params,
            timeout=10,
        )
        response.raise_for_status()

        for match in response.json().get("matches", []):
            if match.get("status") == "FINISHED":
                continue
            home, away = _team_of(match.get("homeTeam")), _team_of(match.get("awayTeam"))
            if TEAM_NAME.lower() not in f"{home} {away}".lower():
                continue
            kickoff = match.get("utcDate")
            if not kickoff or kickoff < floor:
                continue
            if best is None or kickoff < best["kickoff_utc"]:
                best = {
                    "home": home,
                    "away": away,
                    "kickoff_utc": kickoff,
                    "competition": (match.get("competition") or {}).get("name", ""),
                }
    return best


def fixtures_loop():
    while True:
        try:
            fixtures.next_match = fetch_next_match()
            fixtures.last_error = None
        except Exception as exc:  # noqa: BLE001 - never disturb the radio
            fixtures.last_error = str(exc)
            app.logger.warning("fixtures lookup failed: %s", exc)
        fixtures.checked_at = time.time()
        time.sleep(FIXTURE_REFRESH_SECONDS)


# ---------------------------------------------------------------- live clock


# Play is stopped: the clock must freeze instead of running on.
PAUSED_STATUSES = {"HT", "BT", "SUSP", "INT", "PST"}
# No confirmation from the feed for this long and the clock is not trustworthy.
STALE_AFTER = 240
# The minute must advance about once a minute. If it does not, the feed is
# stuck and counting locally would quietly invent a time.
STUCK_AFTER = 180


class LiveMatch:
    """The match clock as read from the feed, plus the moment it was read.

    The feed only reports whole minutes, so we anchor on the instant the
    minute changes and let the clock run locally from there. That is what
    turns a coarse, occasional reading into a clock that ticks — but only
    while the feed keeps confirming it.
    """

    def __init__(self):
        self.anchored_at = None      # server time when this minute was first seen
        self.anchor_seconds = None   # match seconds at that instant
        self.last_seen = None        # last successful reading of OUR match
        self.status = None           # 1H, HT, 2H...
        self.home = None
        self.away = None
        self.last_error = None

    def clock_seconds(self):
        if self.anchored_at is None or self.last_seen is None:
            return None
        now = time.time()
        # The feed stopped confirming: better no clock than a made-up one.
        if now - self.last_seen > STALE_AFTER:
            return None
        if self.status in PAUSED_STATUSES:
            return self.anchor_seconds
        if now - self.anchored_at > STUCK_AFTER:
            return None
        return self.anchor_seconds + (now - self.anchored_at)

    def clear(self):
        self.anchored_at = None
        self.anchor_seconds = None
        self.last_seen = None
        self.status = None


live = LiveMatch()


def in_match_window():
    """True when a known fixture is under way (or about to be)."""
    match = fixtures.next_match
    if not match:
        return None  # unknown: no fixture list configured
    try:
        kickoff = datetime.fromisoformat(match["kickoff_utc"].replace("Z", "+00:00"))
    except (ValueError, KeyError, TypeError):
        return None
    now = datetime.now(timezone.utc)
    # Wide enough for extra time and a long half-time.
    return timedelta(minutes=-5) <= (now - kickoff) <= timedelta(minutes=190)


def _first_word(name):
    return (name or "").strip().lower().split(" ")[0]


def _same_fixture(home, away, kickoff_iso, expected):
    """Is this live fixture the one we are waiting for?

    Matching on "Napoli" alone is not enough: at any hour there are other
    live fixtures with that word in a team name (women's, youth, other
    countries). We also require the opponent, and the kickoff time when we
    have it, so we cannot latch onto the wrong match.
    """
    if not expected:
        return TEAM_NAME.lower() in f"{home} {away}".lower()

    names = {_first_word(home), _first_word(away)}
    wanted = {_first_word(expected.get("home")), _first_word(expected.get("away"))}
    if names != wanted:
        return False

    try:
        theirs = datetime.fromisoformat((kickoff_iso or "").replace("Z", "+00:00"))
        ours = datetime.fromisoformat(expected["kickoff_utc"].replace("Z", "+00:00"))
    except (ValueError, KeyError, TypeError):
        return True  # names already agree; no usable time to cross-check
    return abs((theirs - ours).total_seconds()) <= 900


def fetch_live():
    response = requests.get(
        f"{API_FOOTBALL_BASE}/fixtures",
        headers={"x-apisports-key": API_FOOTBALL_KEY},
        params={"live": "all"},
        timeout=10,
    )
    response.raise_for_status()

    expected = fixtures.next_match

    for item in response.json().get("response", []):
        teams = item.get("teams") or {}
        home = ((teams.get("home") or {}).get("name")) or ""
        away = ((teams.get("away") or {}).get("name")) or ""
        fixture = item.get("fixture") or {}

        if not _same_fixture(home, away, fixture.get("date"), expected):
            continue

        status = fixture.get("status") or {}
        elapsed = status.get("elapsed")
        if elapsed is None:
            continue

        extra = status.get("extra") or 0
        return {
            "seconds": (int(elapsed) + int(extra)) * 60,
            "status": status.get("short"),
            "home": home,
            "away": away,
        }
    return None


def live_loop():
    while True:
        window = in_match_window()
        if window is False:
            # A fixture is known and it is not now: no reason to spend a call.
            live.clear()
            time.sleep(60)
            continue

        try:
            found = fetch_live()
            live.last_error = None
            if found is None:
                live.clear()
            else:
                # Re-anchor only when the minute actually advances, so the
                # clock keeps running smoothly instead of stuttering.
                if found["seconds"] != live.anchor_seconds:
                    live.anchor_seconds = found["seconds"]
                    live.anchored_at = time.time()
                live.last_seen = time.time()
                live.status = found["status"]
                live.home, live.away = found["home"], found["away"]
        except Exception as exc:  # noqa: BLE001 - never disturb the radio
            live.last_error = str(exc)
            app.logger.warning("live lookup failed: %s", exc)

        time.sleep(LIVE_POLL_SECONDS if window else LIVE_IDLE_POLL_SECONDS)


# ---------------------------------------------------------------- app

# No static folder: index.html is served explicitly, so nothing else in the
# directory (app.py included) is reachable over HTTP.
app = Flask(__name__)
HERE = os.path.dirname(os.path.abspath(__file__))


def requested_delay():
    try:
        value = float(request.args.get("delay", DEFAULT_DELAY_SECONDS))
    except (TypeError, ValueError):
        value = DEFAULT_DELAY_SECONDS
    return max(0.0, min(value, float(MAX_DELAY_SECONDS)))


@app.route("/")
def index():
    return send_from_directory(HERE, "index.html")


def live_state():
    """Why there is (or is not) a live clock — so a problem can be read off
    /status from a phone instead of guessed at."""
    if not API_FOOTBALL_KEY:
        return "off"
    if live.anchored_at is None:
        return "no_match"
    if live.clock_seconds() is not None:
        return "paused" if live.status in PAUSED_STATUSES else "ok"
    return "stale"


@app.route("/status")
def status():
    span = buffer.span_seconds()
    delay = requested_delay()
    return jsonify(
        buffer_seconds=round(span, 1),
        ready=span >= delay,
        upstream_connected=upstream.connected,
        delay_seconds=round(delay),
        default_delay_seconds=DEFAULT_DELAY_SECONDS,
        max_delay_seconds=MAX_DELAY_SECONDS,
        station=STATION_NAME,
        upstream_reconnects=upstream.reconnects,
        uptime_seconds=round(time.time() - upstream.started_at),
        next_match=fixtures.next_match,
        live_match=(
            None
            if live.clock_seconds() is None
            else {
                "clock_seconds": round(live.clock_seconds(), 1),
                "status": live.status,
                "home": live.home,
                "away": live.away,
            }
        ),
        live_state=live_state(),
    )


def delayed_chunks(delay):
    # Wait until the buffer reaches back far enough to start serving.
    while buffer.span_seconds() < delay:
        time.sleep(0.5)

    seq = buffer.first_seq_at_or_after(time.time() - delay)
    if seq is None:
        return

    while True:
        item = buffer.get(seq, timeout=1.0)

        if item is None:
            # Nothing new yet: upstream stalled, or we caught up to the edge.
            continue

        if item[0] == "evicted":
            # We fell behind far enough that our next chunk is gone.
            # Rejoin at the oldest chunk still held rather than dropping out.
            seq = item[1]
            continue

        ts, data = item

        # Never serve audio younger than the delay.
        age = time.time() - ts
        if age < delay:
            time.sleep(min(delay - age, 0.5))
            continue

        yield data
        seq += 1


@app.route("/stream")
def stream():
    return Response(
        delayed_chunks(requested_delay()),
        mimetype=upstream.content_type,
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "close",
        },
    )


# ---------------------------------------------------------------- startup

threading.Thread(target=upstream_loop, name="upstream", daemon=True).start()

if FOOTBALL_TOKEN:
    threading.Thread(target=fixtures_loop, name="fixtures", daemon=True).start()

if API_FOOTBALL_KEY:
    threading.Thread(target=live_loop, name="live", daemon=True).start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), threaded=True)
