#!/usr/bin/env python3
"""
Bidirectional BFS between two Wikipedia pages.
Both directions follow forward links only (A -> B -> C).
Forward BFS expands outgoing links from start.
Backward BFS expands incoming links to end (i.e. who links TO these pages).
They meet when a page is reachable from both sides.
"""

import sqlite3
import time

DB_FILE = "wiki.db"


def get_db():
    con = sqlite3.connect(DB_FILE)
    con.execute("PRAGMA cache_size = -2000000")
    con.execute("PRAGMA mmap_size = 10000000000")
    return con


def find_page_id(con, query):
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


def get_forward_neighbors(con, page_ids):
    """pages that these pages link TO"""
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


def get_backward_neighbors(con, page_ids):
    """pages that link TO these pages"""
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


def id_to_title(con, page_ids):
    placeholders = ",".join("?" * len(page_ids))
    cur = con.execute(
        f"select page_id, page_title from page_ids where page_id in ({placeholders})",
        page_ids
    )
    lookup = {row[0]: row[1] for row in cur.fetchall()}
    return [lookup.get(pid, str(pid)) for pid in page_ids]


def reconstruct_path(meeting, fwd_parent, bwd_parent):
    fwd_path = []
    node = meeting
    while node is not None:
        fwd_path.append(node)
        node = fwd_parent[node]
    fwd_path.reverse()

    bwd_path = []
    node = bwd_parent[meeting]
    while node is not None:
        bwd_path.append(node)
        node = bwd_parent[node]

    return fwd_path + bwd_path


def bfs(con, start_id, end_id):
    if start_id == end_id:
        return [start_id]

    fwd_visited = {start_id: None}
    bwd_visited = {end_id: None}
    fwd_frontier = {start_id}
    bwd_frontier = {end_id}

    for depth in range(1, 10):
        if len(fwd_frontier) <= len(bwd_frontier):
            direction = "fwd"
            frontier = fwd_frontier
            visited = fwd_visited
            other_visited = bwd_visited
            get_neighbors = get_forward_neighbors
        else:
            direction = "bwd"
            frontier = bwd_frontier
            visited = bwd_visited
            other_visited = fwd_visited
            get_neighbors = get_backward_neighbors

        print(f"  depth {depth} [{direction}] frontier={len(frontier):,} visited={len(visited):,}")

        neighbors_map = get_neighbors(con, frontier)
        next_frontier = set()

        for src, neighbors in neighbors_map.items():
            for nbr in neighbors:
                if nbr in visited:
                    continue
                visited[nbr] = src
                next_frontier.add(nbr)

                if nbr in other_visited:
                    print(f"  meeting point: {nbr}")
                    if direction == "fwd":
                        return reconstruct_path(nbr, fwd_visited, bwd_visited)
                    else:
                        return reconstruct_path(nbr, bwd_visited, fwd_visited)[::-1]

        if direction == "fwd":
            fwd_frontier = next_frontier
        else:
            bwd_frontier = next_frontier

        if not next_frontier:
            print(f"  frontier exhausted — no path found")
            return None

    print("max depth reached")
    return None


def main():
    con = get_db()

    source_query = "Discordian_calendar"
    target_query = "Robin_Williams"

    print(f"looking up '{source_query}'...")
    start_id, start_title = find_page_id(con, source_query)
    print(f"  -> [{start_id}] {start_title}")

    print(f"looking up '{target_query}'...")
    end_id, end_title = find_page_id(con, target_query)
    print(f"  -> [{end_id}] {end_title}")

    print(f"\nfinding path: {start_title} -> {end_title}\n")
    t = time.time()
    path_ids = bfs(con, start_id, end_id)

    if not path_ids:
        print("no path found")
        return

    titles = id_to_title(con, path_ids)
    elapsed = time.time() - t

    print(f"\npath ({len(titles)-1} hops) found in {elapsed:.2f}s:")
    for i, title in enumerate(titles):
        prefix = "START" if i == 0 else f"  {i}  "
        print(f"  {prefix} -> {title}")


if __name__ == "__main__":
    main()