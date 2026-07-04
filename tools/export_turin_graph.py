#!/usr/bin/env python3
"""Exports a Turin (city center) road graph to JSON for the WASM simulator.

Same purpose as OSMnx (drivable graph from OpenStreetMap), but without its
heavy dependencies: it uses only the stdlib and the Overpass API over HTTP.

Output: web/data/graph.json
    {
      "nodes": [[lat, lon], ...],     # index = node id
      "adj":   [[j, k, ...], ...]     # adj[i] = neighboring nodes (undirected graph)
    }

Nodes are the OSM points of drivable streets; edges connect consecutive
nodes along each way. The simulator walks from node to node following the
real shape of the streets and picks a direction at intersections.

Usage:
    python tools/export_turin_graph.py
"""
import json
import os
import urllib.request

# Bounding box of central Turin (south, west, north, east). Deliberately small:
# keeps the file light (spec: mind the size) and the tour within the center.
SOUTH, WEST, NORTH, EAST = 45.050, 7.660, 45.090, 7.705

# Road types drivable by car.
HIGHWAY = (
    "motorway|trunk|primary|secondary|tertiary|unclassified|residential|"
    "living_street|motorway_link|trunk_link|primary_link|secondary_link|"
    "tertiary_link"
)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

QUERY = f"""
[out:json][timeout:90];
way["highway"~"^({HIGHWAY})$"]({SOUTH},{WEST},{NORTH},{EAST});
(._;>;);
out body;
"""

OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "web", "data", "graph.json")


def fetch():
    print("Overpass query (central Turin)...")
    data = urllib.parse.urlencode({"data": QUERY}).encode()
    # Overpass rejects urllib's default User-Agent with 406: declare one.
    req = urllib.request.Request(
        OVERPASS_URL,
        data=data,
        headers={"User-Agent": "georuggine-exporter/1.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)


def build_graph(osm):
    coords = {}          # osm_id -> (lat, lon)
    ways = []            # list of lists of osm_id
    for el in osm["elements"]:
        if el["type"] == "node":
            coords[el["id"]] = (el["lat"], el["lon"])
        elif el["type"] == "way" and "nodes" in el:
            ways.append(el["nodes"])

    # Keep only the nodes actually used by the ways and give them a 0..N index.
    used = []
    seen = set()
    for w in ways:
        for nid in w:
            if nid in coords and nid not in seen:
                seen.add(nid)
                used.append(nid)
    index = {nid: i for i, nid in enumerate(used)}
    nodes = [[coords[nid][0], coords[nid][1]] for nid in used]

    # Adjacencies from consecutive pairs (undirected graph; for the demo
    # we ignore one-way streets).
    adj = [set() for _ in nodes]
    for w in ways:
        prev = None
        for nid in w:
            if nid not in index:
                prev = None
                continue
            cur = index[nid]
            if prev is not None and prev != cur:
                adj[prev].add(cur)
                adj[cur].add(prev)
            prev = cur

    return {"nodes": nodes, "adj": [sorted(s) for s in adj]}


def main():
    osm = fetch()
    graph = build_graph(osm)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(graph, f, separators=(",", ":"))
    size_kb = os.path.getsize(OUT_PATH) / 1024
    n_edges = sum(len(a) for a in graph["adj"]) // 2
    print(f"OK: {len(graph['nodes'])} nodes, {n_edges} edges -> {OUT_PATH} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
