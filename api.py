#!/usr/bin/env python3
"""
Wiki Speedrun API
Forward port 47823 on your router to this machine.
Returns top 10 shortest paths.
"""

import sqlite3
import time
from flask import Flask, jsonify, request
from flask_cors import CORS

DB_FILE = r"C:\Users\Patrick Taylor\PycharmProjects\wikiSpeedrunner\wiki.db"
PORT = 47823
MAX_PATHS = 10

app = Flask(__name__)
CORS(app)

con = sqlite3.connect(DB_FILE, check_same_thread=False)
con.execute("PRAGMA cache_size = -4000000")
con.execute("PRAGMA mmap_size = 20000000000")
con.execute("PRAGMA journal_mode = WAL")


# ---------------------------------------------------------------------------
# db helpers
# ---------------------------------------------------------------------------

def find_page_id(query):
    cur = con.execute(
        "select page_id, page_title from page_ids where page_title = ? limit 1",
        (query,)
    )
    row = cur.fetchone()
    if not row:
        cur = con.execute(
            "select page_id, page_title from page_ids where page_title like ? limit 1",
            (f"%{query}%",)
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"no page found for '{query}'")
    return row[0], row[1]


def get_forward_neighbors(page_ids):
    placeholders = ",".join("?" * len(page_ids))
    cur = con.execute(
        f"""
        select pl.source_page_id, p.page_id
        from pagelinks pl
        join linktarget lt on pl.target_page_id = lt.lt_id
        join page_ids p on lt.lt_title = p.page_title
        where pl.source_page_id in ({placeholders})
        """,
        list(page_ids)
    )
    result = {pid: [] for pid in page_ids}
    for src, tgt in cur.fetchall():
        result[src].append(tgt)
    return result


def get_backward_neighbors(page_ids):
    placeholders = ",".join("?" * len(page_ids))
    cur = con.execute(
        f"""
        select p.page_id, pl.source_page_id
        from pagelinks pl
        join linktarget lt on pl.target_page_id = lt.lt_id
        join page_ids p on lt.lt_title = p.page_title
        where p.page_id in ({placeholders})
        """,
        list(page_ids)
    )
    result = {pid: [] for pid in page_ids}
    for tgt, src in cur.fetchall():
        result[tgt].append(src)
    return result


def id_to_title(page_ids):
    placeholders = ",".join("?" * len(page_ids))
    cur = con.execute(
        f"select page_id, page_title from page_ids where page_id in ({placeholders})",
        page_ids
    )
    lookup = {row[0]: row[1] for row in cur.fetchall()}
    return [lookup.get(pid, str(pid)) for pid in page_ids]


# ---------------------------------------------------------------------------
# multi-path bidirectional BFS
# ---------------------------------------------------------------------------

def bfs_all(start_id, end_id, max_paths=MAX_PATHS):
    """
    Bidirectional BFS that collects up to max_paths shortest paths.
    Once the shortest length is found, exhausts that depth level
    to find all meeting points, then reconstructs all paths from them.
    """
    if start_id == end_id:
        return [[start_id]]

    # fwd: node -> set of parents (multiple parents = multiple paths)
    fwd_parents = {start_id: set()}
    bwd_parents = {end_id: set()}
    fwd_frontier = {start_id}
    bwd_frontier = {end_id}

    meeting_nodes = set()
    found_depth = None

    for depth in range(1, 10):
        if len(fwd_frontier) <= len(bwd_frontier):
            direction = "fwd"
            frontier = fwd_frontier
            parents = fwd_parents
            other_parents = bwd_parents
            get_neighbors = get_forward_neighbors
        else:
            direction = "bwd"
            frontier = bwd_frontier
            parents = bwd_parents
            other_parents = fwd_parents
            get_neighbors = get_backward_neighbors

        neighbors_map = get_neighbors(frontier)
        next_frontier = set()

        for src, neighbors in neighbors_map.items():
            for nbr in neighbors:
                if nbr not in parents:
                    parents[nbr] = set()
                    next_frontier.add(nbr)
                # record ALL parents that lead here at this depth
                if nbr not in fwd_parents or nbr not in bwd_parents:
                    parents[nbr].add(src)

                if nbr in other_parents:
                    meeting_nodes.add(nbr)

        if direction == "fwd":
            fwd_frontier = next_frontier
        else:
            bwd_frontier = next_frontier

        # if we found meetings, finish this depth level then stop
        if meeting_nodes and found_depth is None:
            found_depth = depth
            # one more pass on the other side at same depth to catch more meetings
            # then break
            break

        if not next_frontier:
            break

    if not meeting_nodes:
        return []

    # reconstruct all paths through each meeting node
    def build_fwd_paths(node):
        if not fwd_parents.get(node):
            return [[node]]
        paths = []
        for parent in fwd_parents[node]:
            for p in build_fwd_paths(parent):
                paths.append(p + [node])
        return paths

    def build_bwd_paths(node):
        if not bwd_parents.get(node):
            return [[node]]
        paths = []
        for parent in bwd_parents[node]:
            for p in build_bwd_paths(parent):
                paths.append([node] + p)
        return paths

    all_paths = []
    for meeting in meeting_nodes:
        fwd_halves = build_fwd_paths(meeting)
        bwd_halves = build_bwd_paths(meeting)
        for fh in fwd_halves:
            for bh in bwd_halves:
                full = fh + bh[1:]  # don't double-count meeting node
                all_paths.append(full)
                if len(all_paths) >= max_paths:
                    return all_paths

    return all_paths[:max_paths]


# ---------------------------------------------------------------------------
# routes
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    cur = con.execute(
        "select page_title from page_ids where page_title like ? limit 10",
        (f"%{q}%",)
    )
    return jsonify([row[0] for row in cur.fetchall()])


@app.route("/path")
def path():
    src = request.args.get("from", "").strip()
    tgt = request.args.get("to", "").strip()

    if not src or not tgt:
        return jsonify({"error": "missing 'from' or 'to' param"}), 400

    try:
        t = time.time()
        start_id, start_title = find_page_id(src)
        end_id, end_title = find_page_id(tgt)
        raw_paths = bfs_all(start_id, end_id)
        elapsed = round(time.time() - t, 2)

        if not raw_paths:
            return jsonify({"error": "no path found"}), 404

        # resolve all unique page ids to titles in one query
        all_ids = list({pid for path in raw_paths for pid in path})
        placeholders = ",".join("?" * len(all_ids))
        cur = con.execute(
            f"select page_id, page_title from page_ids where page_id in ({placeholders})",
            all_ids
        )
        lookup = {row[0]: row[1] for row in cur.fetchall()}

        paths_titled = [
            [lookup.get(pid, str(pid)) for pid in path]
            for path in raw_paths
        ]

        return jsonify({
            "from": start_title,
            "to": end_title,
            "hops": len(raw_paths[0]) - 1,
            "paths_found": len(paths_titled),
            "paths": paths_titled,
            "elapsed_s": elapsed
        })

    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": f"internal error: {e}"}), 500


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"[wiki speedrun api] running on port {PORT}")
    print(f"[wiki speedrun api] test: http://localhost:{PORT}/path?from=Super_Mario&to=Superman")
    app.run(host="0.0.0.0", port=PORT)