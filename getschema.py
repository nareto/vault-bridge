#!/usr/bin/env python3
"""
Vault Bridge — Livesync Schema Probe
=====================================
Run this ONCE against the CouchDB instance to discover how Livesync stores
documents. Output goes to livesync_schema_probe.json, structured to match
Appendix C of the PRD.

Usage:
    export COUCH_URL="https://obsidianlivesync.example.com"
    export COUCH_USER="admin"
    export COUCH_PASS="password"
    python3 probe_livesync.py

The database name is auto-discovered — no need to specify it.
The script auto-redacts note content. Review the output before sharing —
check that no personal text leaked into metadata fields.
"""

import os
import sys
import json
import requests
import urllib.parse
from collections import defaultdict
from datetime import datetime

# --- Configuration from environment ---
COUCH_URL = os.environ.get("COUCH_URL", "").rstrip("/")
COUCH_USER = os.environ.get("COUCH_USER", "")
COUCH_PASS = os.environ.get("COUCH_PASS", "")

if not all([COUCH_URL, COUCH_USER, COUCH_PASS]):
    print("Error: Set COUCH_URL, COUCH_USER, COUCH_PASS env vars.")
    print("  COUCH_URL = full URL to CouchDB (e.g. https://obsidianlivesync.example.com)")
    print("  COUCH_USER = CouchDB admin username")
    print("  COUCH_PASS = CouchDB admin password")
    sys.exit(1)

AUTH = (COUCH_USER, COUCH_PASS)
OUTPUT_FILE = "livesync_schema_probe.json"

# --- Redaction ---

REDACT_FIELDS = {"data", "content", "text", "body", "markdown", "note"}
REDACT_PLACEHOLDER = "[REDACTED — {length} chars]"

def redact(obj, path=""):
    """Recursively redact fields that likely contain note content."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if k.lower() in REDACT_FIELDS and isinstance(v, str) and len(v) > 50:
                result[k] = REDACT_PLACEHOLDER.format(length=len(v))
            else:
                result[k] = redact(v, f"{path}.{k}")
        return result
    elif isinstance(obj, list):
        return [redact(item, f"{path}[]") for item in obj]
    elif isinstance(obj, str) and len(obj) > 200:
        # Long strings in unexpected fields — redact conservatively
        return REDACT_PLACEHOLDER.format(length=len(obj))
    return obj

# --- CouchDB helpers ---

def couch_get(path, params=None):
    """GET request to CouchDB. Returns parsed JSON or error dict."""
    url = f"{COUCH_URL}/{path}" if path else COUCH_URL
    try:
        r = requests.get(url, auth=AUTH, params=params, timeout=30, verify=True)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_probe_error": str(e)}

# --- Database discovery ---

def discover_database():
    """Auto-discover the Livesync database name from CouchDB."""
    print("[0/6] Discovering databases...")
    all_dbs = couch_get("_all_dbs")

    if isinstance(all_dbs, dict) and "_probe_error" in all_dbs:
        print(f"  ERROR: Could not connect to CouchDB: {all_dbs['_probe_error']}")
        sys.exit(1)

    # Filter out CouchDB system databases
    system_dbs = {"_replicator", "_users", "_global_changes"}
    user_dbs = [db for db in all_dbs if db not in system_dbs]

    print(f"  Found {len(all_dbs)} total databases, {len(user_dbs)} user databases:")
    for db in user_dbs:
        info = couch_get(db)
        doc_count = info.get("doc_count", "?")
        print(f"    - {db} ({doc_count} documents)")

    if len(user_dbs) == 0:
        print("  ERROR: No user databases found. Is Livesync configured?")
        sys.exit(1)
    elif len(user_dbs) == 1:
        selected = user_dbs[0]
        print(f"  Auto-selected: {selected}")
    else:
        print()
        for i, db in enumerate(user_dbs):
            print(f"  [{i}] {db}")
        choice = input("  Multiple databases found. Enter number to probe: ").strip()
        selected = user_dbs[int(choice)]
        print(f"  Selected: {selected}")

    return selected, all_dbs

# --- Probes ---

def probe_db_info(db):
    """Basic database info — doc count, update seq, etc."""
    print("[1/6] Fetching database info...")
    info = couch_get(db)
    return {
        "db_name": info.get("db_name"),
        "doc_count": info.get("doc_count"),
        "update_seq": str(info.get("update_seq", ""))[:80] + "...",
        "disk_size": info.get("sizes", {}).get("file"),
    }

def probe_doc_id_patterns(db):
    """Fetch all doc IDs and categorize them by pattern."""
    print("[2/6] Fetching document ID listing (this may take a moment)...")
    result = couch_get(f"{db}/_all_docs", params={"limit": 500})
    rows = result.get("rows", [])
    total = result.get("total_rows", len(rows))

    ids = [r["id"] for r in rows]

    # Categorize IDs by pattern
    categories = defaultdict(list)
    for doc_id in ids:
        if doc_id.startswith("_design/"):
            categories["design_docs"].append(doc_id)
        elif doc_id.startswith("h:"):
            categories["h_prefixed"].append(doc_id)
        elif "/" in doc_id or doc_id.endswith(".md"):
            categories["path_like"].append(doc_id)
        elif len(doc_id) == 32 or len(doc_id) == 64:
            categories["hash_like"].append(doc_id)
        else:
            categories["other"].append(doc_id)

    # Summary with examples (not full list)
    summary = {}
    for cat, cat_ids in categories.items():
        summary[cat] = {
            "count": len(cat_ids),
            "examples": cat_ids[:5],
        }

    return {
        "total_documents": total,
        "sampled": len(ids),
        "categories": summary,
        "note_to_developer": (
            "Examine the categories above to understand how Livesync maps "
            "vault paths to CouchDB document IDs. The largest category of "
            "non-design docs is likely how notes are stored."
        ),
    }

def probe_sample_documents(db, id_patterns):
    """Fetch raw documents for each detected category."""
    print("[3/6] Fetching sample documents from each category...")
    samples = {}

    for category, info in id_patterns["categories"].items():
        if category == "design_docs":
            continue  # skip design docs

        examples = info["examples"][:3]
        category_samples = []

        for doc_id in examples:
            safe_id = urllib.parse.quote(doc_id, safe="")
            doc = couch_get(f"{db}/{safe_id}", params={"revs_info": "true"})
            if "_probe_error" not in doc:
                # Record the raw field names and types (before redaction)
                field_map = {
                    k: type(v).__name__ + (f" (len={len(v)})" if isinstance(v, (str, list, dict)) else f" = {v}")
                    for k, v in doc.items()
                    if not k.startswith("_")
                }
                category_samples.append({
                    "doc_id": doc_id,
                    "field_types": field_map,
                    "raw_document": redact(doc),
                })
            else:
                category_samples.append({
                    "doc_id": doc_id,
                    "error": doc["_probe_error"],
                })

        samples[category] = category_samples

    return samples

def probe_changes_feed(db):
    """Capture recent _changes events."""
    print("[4/6] Fetching recent _changes feed events...")
    result = couch_get(f"{db}/_changes", params={
        "limit": "10",
        "include_docs": "true",
        "descending": "true",
    })

    changes = []
    for change in result.get("results", []):
        changes.append({
            "seq": str(change.get("seq", ""))[:60] + "...",
            "id": change.get("id"),
            "deleted": change.get("deleted", False),
            "doc_field_names": list(change.get("doc", {}).keys()) if change.get("doc") else None,
            "doc": redact(change.get("doc", {})),
        })

    return {
        "last_seq": str(result.get("last_seq", ""))[:60] + "...",
        "events": changes,
    }

def probe_size_variance(db):
    """Find the smallest and largest documents to identify chunking patterns."""
    print("[5/6] Identifying small and large documents for chunk analysis...")

    result = couch_get(f"{db}/_all_docs", params={
        "limit": 200,
        "include_docs": "true",
    })

    docs_by_size = []
    for row in result.get("rows", []):
        doc = row.get("doc", {})
        if row["id"].startswith("_design/"):
            continue
        # Estimate document size
        doc_str = json.dumps(doc)
        docs_by_size.append((len(doc_str), row["id"], doc))

    docs_by_size.sort(key=lambda x: x[0])

    smallest = docs_by_size[:3] if docs_by_size else []
    largest = docs_by_size[-3:] if docs_by_size else []

    def format_sample(size, doc_id, doc):
        return {
            "doc_id": doc_id,
            "serialized_size_bytes": size,
            "field_names": [k for k in doc.keys()],
            "document": redact(doc),
        }

    return {
        "note_to_developer": (
            "Compare the smallest and largest documents. If Livesync uses "
            "chunking, large notes may have a different field structure, or "
            "there may be separate chunk documents with IDs that reference "
            "the parent. Look for fields like 'children', 'chunks', 'type', "
            "'seq', or numeric suffixes in document IDs."
        ),
        "smallest_documents": [format_sample(*s) for s in smallest],
        "largest_documents": [format_sample(*s) for s in largest],
    }

def probe_deletion_marker(db):
    """Check if any recently deleted documents exist in the changes feed."""
    print("[6/6] Looking for deletion markers...")
    result = couch_get(f"{db}/_changes", params={
        "limit": 100,
        "include_docs": "true",
        "descending": "true",
    })

    deletions = [
        {
            "id": change["id"],
            "seq": str(change.get("seq", ""))[:60] + "...",
            "doc": redact(change.get("doc", {})) if change.get("doc") else None,
        }
        for change in result.get("results", [])
        if change.get("deleted", False)
    ]

    return {
        "found": len(deletions),
        "samples": deletions[:3],
        "note_to_developer": (
            "If no deletions found, create and then delete a test note in "
            "Obsidian, wait for Livesync to sync, then re-run this probe."
        ) if not deletions else "Deletion markers captured above.",
    }

# --- Main ---

def main():
    print(f"Vault Bridge — Livesync Schema Probe")
    print(f"Target: {COUCH_URL}")
    print(f"Time: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    # Step 0: discover the database
    db_name, all_dbs = discover_database()

    print("=" * 60)

    output = {
        "_probe_metadata": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "couchdb_url": COUCH_URL,
            "all_databases": all_dbs,
            "selected_database": db_name,
            "purpose": (
                "This file documents the CouchDB schema used by Obsidian "
                "Livesync. It serves as Appendix C of the Vault Bridge PRD "
                "and is the sole reference for implementing the "
                "livesync_decoder module. Content fields have been "
                "auto-redacted. REVIEW BEFORE SHARING — check that no "
                "personal text leaked into metadata fields."
            ),
        },
        "1_database_info": probe_db_info(db_name),
    }

    id_patterns = probe_doc_id_patterns(db_name)
    output["2_document_id_patterns"] = id_patterns
    output["3_sample_documents"] = probe_sample_documents(db_name, id_patterns)
    output["4_changes_feed"] = probe_changes_feed(db_name)
    output["5_size_variance_analysis"] = probe_size_variance(db_name)
    output["6_deletion_markers"] = probe_deletion_marker(db_name)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print("=" * 60)
    print(f"Done! Output written to: {OUTPUT_FILE}")
    print(f"Database probed: {db_name}")
    print()
    print("NEXT STEPS:")
    print("1. Open the file and review for any leaked personal content.")
    print("2. Add annotations explaining each field's purpose.")
    print("3. Paste the annotated result into Appendix C of the PRD.")
    print("4. Pay special attention to:")
    print("   - How document IDs map to vault paths")
    print("   - Whether large docs are chunked (compare section 5)")
    print("   - What fields carry note content vs metadata")
    print("   - How deletions are represented (section 6)")
    print()
    print(f"To use in the PRD config, set: database: \"{db_name}\"")

if __name__ == "__main__":
    main()
