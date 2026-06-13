"""
Splector HTTP Routes

Serves the dashboard, database viewer, and REST API endpoints.
"""

from flask import (
    Blueprint,
    jsonify,
    render_template,
    request,
    send_file,
    abort,
)

from core.config import load_config, save_config
from core.db import get_db_connection

main_bp = Blueprint("main", __name__)


# =========================================================
# PAGE ROUTES
# =========================================================

@main_bp.route("/")
def dashboard():
    return render_template("dashboard.html")


@main_bp.route("/database")
def database():
    return render_template("database.html")


@main_bp.route("/curation")
def curation():
    return render_template("curation.html")


# =========================================================
# API: CONFIGURATION
# =========================================================

@main_bp.route("/api/config", methods=["GET"])
def get_config():
    config = load_config()
    return jsonify(config.to_dict())


@main_bp.route("/api/config", methods=["POST"])
def update_config():
    updates = request.get_json(force=True)
    new_config = save_config(updates)
    return jsonify({"status": "ok", "config": new_config.to_dict()})


# =========================================================
# API: DATABASE STATS
# =========================================================

@main_bp.route("/api/stats")
def api_stats():
    config = load_config()
    stats = {
        "domains_loaded": 0,
        "urls_discovered": 0,
        "urls_filtered": 0,
        "final_docs": 0,
        "docs_extracted": 0,
    }

    try:
        conn = get_db_connection(config.abs_db_path)

        # Check which tables exist
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        if "stage2_discovered" in tables:
            stats["urls_discovered"] = conn.execute(
                "SELECT COUNT(*) FROM stage2_discovered"
            ).fetchone()[0]

        if "stage3_filtered" in tables:
            stats["urls_filtered"] = conn.execute(
                "SELECT COUNT(*) FROM stage3_filtered"
            ).fetchone()[0]

        if "stage4_final_docs" in tables:
            stats["final_docs"] = conn.execute(
                "SELECT COUNT(*) FROM stage4_final_docs"
            ).fetchone()[0]

        # domains_loaded from the latest pipeline run
        if "pipeline_runs" in tables:
            row = conn.execute(
                "SELECT domains_loaded FROM pipeline_runs "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                stats["domains_loaded"] = row[0] or 0

        # Phase 2: Documents successfully extracted
        if "document_refs" in tables:
            stats["docs_extracted"] = conn.execute(
                "SELECT COUNT(*) FROM document_refs "
                "WHERE processing_status = 'SUCCESS'"
            ).fetchone()[0]

        # System Stats (Telemetry)
        if "system_stats" in tables:
            rows = conn.execute("SELECT metric_key, metric_value FROM system_stats").fetchall()
            for k, v in rows:
                stats[k] = v

        conn.close()
    except Exception:
        pass  # DB might not exist yet

    return jsonify(stats)


# =========================================================
# API: DATATABLES SERVER-SIDE PROCESSING
# =========================================================

@main_bp.route("/api/data")
def api_data():
    """Server-side pagination for DataTables.js."""
    config = load_config()

    draw = request.args.get("draw", 1, type=int)
    start = request.args.get("start", 0, type=int)
    length = request.args.get("length", 25, type=int)
    search = request.args.get("search[value]", "").strip()
    table = request.args.get("table", "stage4_final_docs")

    # Whitelist allowed tables
    TABLE_SCHEMAS = {
        "stage4_final_docs": {
            "columns": [
                "id", "parent_index_page", "final_target_url",
                "anchor_text", "extracted_at",
            ],
            "searchable": [
                "parent_index_page", "final_target_url", "anchor_text",
            ],
        },
        "stage2_discovered": {
            "columns": [
                "id", "base_domain", "discovered_url",
                "anchor_text", "discovered_at",
            ],
            "searchable": [
                "base_domain", "discovered_url", "anchor_text",
            ],
        },
        "stage3_filtered": {
            "columns": [
                "id", "base_domain", "filtered_url",
                "anchor_text", "filtered_at",
            ],
            "searchable": [
                "base_domain", "filtered_url", "anchor_text",
            ],
        },
    }

    if table not in TABLE_SCHEMAS:
        return jsonify({
            "draw": draw,
            "recordsTotal": 0,
            "recordsFiltered": 0,
            "data": [],
        })

    schema = TABLE_SCHEMAS[table]
    cols = ", ".join(schema["columns"])

    try:
        conn = get_db_connection(config.abs_db_path)

        # Total records
        total = conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        if search:
            # Build WHERE clause for search
            conditions = " OR ".join(
                f"{col} LIKE ?" for col in schema["searchable"]
            )
            search_param = f"%{search}%"
            params = [search_param] * len(schema["searchable"])

            filtered = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {conditions}",
                params,
            ).fetchone()[0]

            rows = conn.execute(
                f"SELECT {cols} FROM {table} "
                f"WHERE {conditions} "
                f"ORDER BY id DESC LIMIT ? OFFSET ?",
                params + [length, start],
            ).fetchall()
        else:
            filtered = total
            rows = conn.execute(
                f"SELECT {cols} FROM {table} "
                f"ORDER BY id DESC LIMIT ? OFFSET ?",
                [length, start],
            ).fetchall()

        conn.close()

        return jsonify({
            "draw": draw,
            "recordsTotal": total,
            "recordsFiltered": filtered,
            "data": [list(row) for row in rows],
        })

    except Exception as e:
        return jsonify({
            "draw": draw,
            "recordsTotal": 0,
            "recordsFiltered": 0,
            "data": [],
            "error": str(e),
        })

# =========================================================
# API: CURATION
# =========================================================

@main_bp.route("/api/curation/documents", methods=["GET"])
def api_curation_documents():
    config = load_config()
    page = request.args.get('page', 1, type=int)
    limit = 100
    offset = (page - 1) * limit
    
    try:
        conn = get_db_connection(config.abs_db_path)
        
        # Get total unreviewed
        total_unreviewed = conn.execute(
            "SELECT COUNT(*) FROM document_refs WHERE workflow_state = 'UNREVIEWED'"
        ).fetchone()[0]
        
        # We need a left join because stage4_final_docs might not have a 1-1 match if it was seeded
        # Group by record_id to ensure exactly 100 unique cards
        query = """
            SELECT d.record_id, s.anchor_text, d.source_url, d.prepared_file_path, d.timestamp
            FROM document_refs d
            LEFT JOIN stage4_final_docs s ON d.source_url = s.final_target_url
            WHERE d.workflow_state = 'UNREVIEWED'
            GROUP BY d.record_id
            ORDER BY d.timestamp DESC
            LIMIT ? OFFSET ?
        """
        rows = conn.execute(query, (limit, offset)).fetchall()
        conn.close()
        
        docs = []
        for row in rows:
            docs.append({
                "record_id": row[0],
                "anchor_text": row[1] or "Unknown Source",
                "source_url": row[2],
                "prepared_file_path": row[3],
                "timestamp": row[4]
            })
            
        return jsonify({
            "total_unreviewed": total_unreviewed,
            "page": page,
            "limit": limit,
            "docs": docs
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@main_bp.route("/api/curation/action/<record_id>", methods=["POST"])
def api_curation_action(record_id):
    config = load_config()
    data = request.get_json(force=True)
    action = data.get("action")
    
    if action not in ['OBSOLETE', 'NOT_JOB_RELATED', 'DRAFT']:
        return jsonify({"error": "Invalid action"}), 400
        
    try:
        conn = get_db_connection(config.abs_db_path)
        conn.execute(
            "UPDATE document_refs SET workflow_state = ? WHERE record_id = ?",
            (action, record_id)
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@main_bp.route("/api/local_file/<record_id>", methods=["GET"])
def api_local_file(record_id):
    config = load_config()
    try:
        conn = get_db_connection(config.abs_db_path)
        row = conn.execute(
            "SELECT prepared_file_path FROM document_refs WHERE record_id = ?",
            (record_id,)
        ).fetchone()
        conn.close()
        
        if row and row[0]:
            file_path = row[0]
            import os
            # Use absolute path resolving relative to base_dir if it's relative
            if not os.path.isabs(file_path):
                file_path = os.path.join(config.base_dir, file_path)
            if os.path.exists(file_path):
                return send_file(file_path, mimetype='text/plain')
                
        return jsonify({"error": "File not found on disk: " + str(row[0] if row else 'No row')}), 404
    except Exception as e:
        import traceback
        return jsonify({"error": traceback.format_exc()}), 500

