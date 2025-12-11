# excel_query_openai_flask.py
"""
Single-file Flask app:
- Upload Excel
- Ask natural-language queries -> OpenAI (gpt-4o-mini) converts NL -> SQL
- Execute SQL against pandas DataFrame (table name `df`)
- Return result in XLSX, CSV, JSON, or PDF

Requirements:
pip install flask pandas pandasql openpyxl httpx reportlab python-dotenv
(Optional: python-multipart & aiofiles are not required for Flask)

Set environment:
export OPENAI_API_KEY="sk-..."
or edit OPENAI_API_KEY variable below.

Run:
python excel_query_openai_flask.py
"""
import os
import io
import uuid
import re
import json
from typing import Optional, Dict
from flask import Flask, request, jsonify, send_file, Response, render_template_string, abort
import pandas as pd
from pandasql import sqldf
import httpx
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib import colors
from dotenv import load_dotenv

load_dotenv()

# Config
# OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# You can set via environment or hardcode (not recommended)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TIMEOUT = 60  # seconds

if not OPENAI_API_KEY:
    # We'll raise helpful errors when trying to call the API.
    pass

app = Flask(__name__)

# in-memory storage (session_id -> dataframe)
DATA_STORE: Dict[str, pd.DataFrame] = {}
LAST_SQL: Dict[str, str] = {}
pysqldf = lambda q, env: sqldf(q, env)

# Minimal frontend HTML (same as original)
INDEX_HTML = """
<!-- modern dark ui inspired by freenotes -->
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Excel NL → SQL Assistant</title>

<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">

<style>
    body {
        font-family: 'Inter', sans-serif;
        background: #0d1117;
        color: #e6edf3;
        margin: 0;
        padding: 0;
    }

    /* Top Navbar */
    .navbar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 16px 30px;
        background: #161b22;
        border-bottom: 1px solid #30363d;
    }

    .navbar .title {
        font-size: 24px;
        font-weight: 600;
    }

    .btn {
        padding: 10px 18px;
        border-radius: 8px;
        border: none;
        cursor: pointer;
        font-weight: 600;
        transition: 0.2s;
    }

    .btn-gradient {
        background: linear-gradient(90deg, #ff4acd, #8a2be2);
        color: white;
    }

    .btn:hover {
        opacity: 0.85;
    }

    /* Main layout */
    .container {
        display: flex;
        gap: 20px;
        padding: 20px;
    }

    /* Left panel */
    .left-panel {
        width: 260px;
        background: #161b22;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 0 20px rgba(0,0,0,0.38);
        height: calc(100vh - 120px);
        overflow-y: auto;
    }

    .panel-title {
        font-size: 18px;
        font-weight: 600;
        margin-bottom: 12px;
    }

    .input-box {
        margin-bottom: 20px;
    }

    input, textarea, select {
        width: 100%;
        padding: 10px;
        background: #0d1117;
        border: 1px solid #30363d;
        border-radius: 6px;
        color: #e6edf3;
    }

    textarea { height: 100px; }

    /* Center panel */
    .center-panel {
        flex: 1;
        background: #161b22;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 0 20px rgba(0,0,0,0.38);
        overflow-y: auto;
    }

    /* Right panel (Preview) */
    .right-panel {
        width: 260px;
        background: #161b22;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 0 20px rgba(0,0,0,0.38);
        height: calc(100vh - 120px);
        overflow-y: auto;
    }

    .preview-box, .sql-box {
        background: #0d1117;
        border: 1px solid #30363d;
        padding: 12px;
        border-radius: 8px;
        margin-top: 10px;
        overflow-x: auto;
        max-height: 300px;
        font-size: 13px;
    }
</style>
</head>
<body>

<!-- Navbar -->
<div class="navbar">
    <div class="title">Excel → SQL Assistant</div>
    <button class="btn btn-gradient">Save</button>
</div>

<div class="container">

    <!-- LEFT PANEL -->
    <div class="left-panel">
        <div class="panel-title">Upload Excel</div>
        <input type="file" id="file" accept=".xls,.xlsx">
        <button onclick="upload()" class="btn btn-gradient" style="width:100%; margin-top:10px;">Upload</button>
        <div id="uploadResult" class="preview-box"></div>

        <div class="panel-title">Ask Query</div>
        <textarea id="nlquery" placeholder="e.g. list students age > 15"></textarea>

        <label>Output Format:</label>
        <select id="format">
            <option value="xlsx">Excel (.xlsx)</option>
            <option value="csv">CSV (.csv)</option>
            <option value="json">JSON (.json)</option>
            <option value="pdf">PDF (.pdf)</option>
        </select>

        <button onclick="runNLQuery()" class="btn btn-gradient" style="width:100%; margin-top:10px;">Run</button>
        <button onclick="showLastSql()" class="btn" style="width:100%; margin-top:10px; background:#21262d; color:white;">Show SQL</button>
    </div>

    <!-- CENTER PANEL -->
    <div class="center-panel">
        <div class="panel-title">Preview</div>
        <div id="preview" class="preview-box"></div>
    </div>

    <!-- RIGHT PANEL -->
    <div class="right-panel">
        <div class="panel-title">Generated SQL</div>
        <div id="lastSql" class="sql-box"></div>
    </div>

</div>

<script>
let session_id = null;

/* Same JS logic as your previous version */
async function upload() {
    const f = document.getElementById('file').files[0];
    if (!f) return alert("Choose file");

    const fd = new FormData();
    fd.append('file', f);

    const res = await fetch('/upload', { method:'POST', body:fd});
    const j = await res.json();

    if (res.ok) {
        session_id = j.session_id;
        document.getElementById('uploadResult').innerText =
            `Uploaded\nSession: ${session_id}\nRows: ${j.rows}\nColumns: ${j.columns.join(', ')}`;
    } else alert(JSON.stringify(j));
}

async function preview() {
    if(!session_id) return;

    const res = await fetch(`/preview/${session_id}`);
    const j = await res.json();
    document.getElementById('preview').innerText = JSON.stringify(j.preview_rows.slice(0,5), null, 2);
}

async function runNLQuery() {
    if (!session_id) return alert("Upload first!");
    const nl = document.getElementById('nlquery').value || "";

    const previewBox = document.getElementById('preview');
    previewBox.innerHTML = "<em>Loading preview...</em>";

    // 1) quick check: preview_simple (no OpenAI)
    try {
        const simpleRes = await fetch(`/preview_simple?session_id=${session_id}`);
        const simpleJson = await simpleRes.json();
        console.log("preview_simple response:", simpleRes.status, simpleJson);
        if (!simpleRes.ok) {
            previewBox.innerHTML = `<strong>preview_simple error:</strong> ${JSON.stringify(simpleJson)}`;
            return;
        } else {
            // show small sample so user knows session is valid
            previewBox.innerHTML = `<div style="margin-bottom:8px"><strong>Local sample rows (df.head):</strong></div><pre>${JSON.stringify(simpleJson.preview_rows, null, 2)}</pre>`;
        }
    } catch (err) {
        console.error("preview_simple fetch error", err);
        previewBox.innerHTML = "<strong>Network error calling preview_simple</strong>: " + err;
        return;
    }

    // 2) call preview_query (attempt LLM -> SQL -> run)
    try {
        const fd = new FormData();
        fd.append("session_id", session_id);
        fd.append("nl_query", nl);

        const res = await fetch("/preview_query", { method: "POST", body: fd });
        const j = await res.json();
        console.log("preview_query response:", res.status, j);

        // handle errors returned from server
        if (!res.ok) {
            previewBox.innerHTML += `<div style="color:tomato"><strong>Server error:</strong> ${j.error || JSON.stringify(j)}</div>`;
            if (j.preview_rows) previewBox.innerHTML += `<pre>${JSON.stringify(j.preview_rows, null, 2)}</pre>`;
            return;
        }

        // success: show SQL + preview table
        if (j.sql) document.getElementById('lastSql').innerText = j.sql;
        if (j.warning) previewBox.innerHTML += `<div style="color:goldenrod"><strong>Warning:</strong> ${j.warning}</div>`;

        if (j.preview_rows && j.preview_rows.length) {
            const rows = j.preview_rows;
            const cols = Object.keys(rows[0]);
            let html = '<table style="width:100%; border-collapse:collapse; margin-top:10px">';
            html += '<tr>';
            for (const c of cols) html += `<th style="text-align:left; padding:6px; border-bottom:1px solid #333">${c}</th>`;
            html += '</tr>';
            for (const r of rows) {
                html += '<tr>';
                for (const c of cols) html += `<td style="padding:6px; border-bottom:1px solid #222">${String(r[c] ?? "")}</td>`;
                html += '</tr>';
            }
            html += '</table>';
            previewBox.innerHTML += html;
        } else {
            previewBox.innerHTML += "<div>(no rows returned)</div>";
        }
    } catch (err) {
        console.error("preview_query fetch error", err);
        previewBox.innerHTML += "<div style='color:tomato'><strong>Client fetch error:</strong> " + String(err) + "</div>";
    }
}





async function showLastSql() {
    if (!session_id) return;
    const res = await fetch(`/last_sql/${session_id}`);
    const j = await res.json();
    document.getElementById('lastSql').innerText = j.sql || "(none)";
}
</script>

</body>
</html>

"""

# Utilities for prompt / SQL extraction / safety
def pandas_dtype_str(dtype) -> str:
    name = str(dtype)
    if "int" in name:
        return "int"
    if "float" in name:
        return "float"
    if "datetime" in name or "date" in name:
        return "datetime"
    return "text"

def build_prompt(nl_query: str, columns: Dict[str, str]) -> str:
    """
    Prompt for LLM to return a single SQL SELECT statement referencing table `df`.
    """
    col_lines = "\n".join([f"- `{c}` ({t})" for c, t in columns.items()])
    prompt = f"""Convert the user's natural language request into a single valid SQL SELECT statement.
Constraints:
- The table name must be exactly `df`.
- Output ONLY a single SQL SELECT statement (no commentary, no code fences).
- No DML (INSERT/UPDATE/DELETE/DROP/ALTER) and avoid SQL comments.
- Avoid trailing semicolons.
- Use simple SQL compatible with SQLite (pandasql).
Columns:
{col_lines}

User request:
\"\"\"{nl_query}\"\"\" 

Return exactly one SQL SELECT statement that answers the request."""
    return prompt

def extract_sql_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    # find first 'select' and take contiguous lines until blank line or end
    m = re.search(r"(?si)\bselect\b", text)
    if not m:
        return None
    substr = text[m.start():]
    substr = re.sub(r"```sql|```|```.*?$", "", substr, flags=re.MULTILINE)
    parts = re.split(r"\n\s*\n", substr)
    candidate = parts[0].strip()
    candidate = candidate.rstrip().rstrip(";")
    if candidate.lower().lstrip().startswith("select"):
        return candidate
    return None

def basic_sql_safety_check(sql: str) -> bool:
    banned = ["delete", "update", "drop", "insert", "alter", "attach", "detach", ";", "--"]
    lower = sql.lower()
    return not any(b in lower for b in banned)

# OpenAI call (synchronous using httpx)
def call_openai_chat(prompt_text: str, model: str = DEFAULT_MODEL) -> str:
    """
    Calls OpenAI Chat Completions endpoint and returns assistant message content.
    Uses OPENAI_API_KEY var.
    """
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set in environment. Set OPENAI_API_KEY before running.")

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": (
                "You are a helpful assistant that converts natural language to SQL. "
                "IMPORTANT: Output ONLY a single SQL SELECT statement (no explanation)."
            )},
            {"role": "user", "content": prompt_text},
        ],
        "temperature": 0.0,
        "max_tokens": 512,
        "n": 1,
    }

    try:
        resp = httpx.post(OPENAI_API_URL, headers=headers, json=payload, timeout=OPENAI_TIMEOUT)
    except Exception as e:
        raise RuntimeError(f"OpenAI request failed: {e}")

    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI API error {resp.status_code}: {resp.text}")

    data = resp.json()
    try:
        content = data["choices"][0]["message"]["content"]
        return content
    except Exception:
        return json.dumps(data)

# Dataframe -> formats
def df_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="result")
    out.seek(0)
    return out.read()

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def df_to_json_bytes(df: pd.DataFrame) -> bytes:
    return json.dumps(df.to_dict(orient="records"), default=str, indent=2).encode("utf-8")

def df_to_pdf_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4), leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18)
    data = [list(df.columns)]
    for _, row in df.iterrows():
        data.append([str(x) if x is not None else "" for x in row.tolist()])
    page_width = landscape(A4)[0] - doc.leftMargin - doc.rightMargin
    ncols = max(1, len(df.columns))
    col_width = page_width / ncols
    col_widths = [col_width] * ncols
    table = Table(data, colWidths=col_widths, repeatRows=1)
    style = TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#f0f0f0')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.black),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,0), 6),
        ('GRID', (0,0), (-1,-1), 0.25, colors.grey),
    ])
    table.setStyle(style)
    doc.build([table])
    buf.seek(0)
    return buf.read()

# Flask routes
@app.route("/", methods=["GET"])
def index():
    return render_template_string(INDEX_HTML)

@app.route("/upload", methods=["POST"])
def upload_excel():
    if "file" not in request.files:
        return jsonify({"detail": "missing file"}), 400
    file = request.files["file"]
    filename = file.filename or ""
    if not filename.lower().endswith((".xls", ".xlsx")):
        return jsonify({"detail": "Please upload an .xls or .xlsx file"}), 400
    content = file.read()
    try:
        xls = pd.read_excel(io.BytesIO(content), sheet_name=None)
        frames = []
        for sheet_name, df in xls.items():
            df = df.copy()
            df["_sheet_name"] = sheet_name
            frames.append(df)
        df_all = pd.concat(frames, ignore_index=True, sort=False)
    except Exception as e:
        return jsonify({"detail": f"Failed to read Excel: {e}"}), 400
    session_id = str(uuid.uuid4())
    DATA_STORE[session_id] = df_all
    return jsonify({"session_id": session_id, "rows": len(df_all), "columns": df_all.columns.tolist()})

@app.route("/preview/<session_id>", methods=["GET"])
def preview(session_id):
    if session_id not in DATA_STORE:
        return jsonify({"detail": "session_id not found"}), 404
    df = DATA_STORE[session_id]
    preview_rows = df.head(50).to_dict(orient="records")
    return jsonify({"columns": df.columns.tolist(), "preview_rows": preview_rows})

@app.route("/preview_simple", methods=["GET"])
def preview_simple():
    session_id = request.args.get("session_id")
    if not session_id or session_id not in DATA_STORE:
        return jsonify({"detail": "session_id not found"}), 404
    df = DATA_STORE[session_id]
    preview_rows = df.head(5).to_dict(orient="records")
    return jsonify({"columns": df.columns.tolist(), "preview_rows": preview_rows})

@app.route("/preview_query", methods=["POST"])
def preview_query():
    """
    Robust preview endpoint:
    - If OpenAI fails, it returns df.head(5) with a warning.
    - If SQL generation/execution succeeds, returns preview rows + generated SQL.
    Response JSON fields:
      { preview_rows: [...], sql: "...", warning: "...", error: "..." }
    """
    session_id = request.form.get("session_id")
    nl_query = request.form.get("nl_query", "").strip()

    if not session_id or session_id not in DATA_STORE:
        app.logger.debug("preview_query: missing/invalid session_id: %s", session_id)
        return jsonify({"error": "session_id not found"}), 404

    df = DATA_STORE[session_id]

    # if empty query -> return head
    if not nl_query:
        app.logger.debug("preview_query: empty nl_query, returning head")
        return jsonify({"preview_rows": df.head(5).to_dict(orient="records")})

    # Build prompt & try OpenAI -> SQL, but catch all errors
    try:
        columns = {str(c): pandas_dtype_str(dtype) for c, dtype in zip(df.columns, df.dtypes)}
        prompt = build_prompt(nl_query, columns)
        generated = call_openai_chat(prompt)  # may raise
        sql = extract_sql_from_text(generated) or generated.strip()
        app.logger.debug("preview_query: generated SQL: %s", sql)
        if not basic_sql_safety_check(sql):
            app.logger.warning("preview_query: SQL failed safety check: %s", sql)
            return jsonify({"warning": "Generated SQL failed safety checks. Returning fallback preview.",
                            "preview_rows": df.head(5).to_dict(orient="records")})
    except Exception as e:
        app.logger.error("preview_query: OpenAI/SQL-generation error: %s\n%s", e, traceback.format_exc())
        # return fallback head and helpful message
        return jsonify({"warning": "OpenAI or SQL generation failed. Returning df.head(5) fallback.",
                        "preview_rows": df.head(5).to_dict(orient="records"),
                        "openai_error": str(e)}), 200

    # Execute SQL safely
    try:
        result_df = pysqldf(sql, {"df": df})
        preview_rows = result_df.head(5).to_dict(orient="records")
        LAST_SQL[session_id] = sql
        return jsonify({"preview_rows": preview_rows, "sql": sql})
    except Exception as e:
        app.logger.error("preview_query: SQL execution error: %s\nSQL:%s\n%s", e, sql, traceback.format_exc())
        return jsonify({"error": f"SQL execution error: {e}", 
                        "preview_rows": df.head(5).to_dict(orient="records"),
                        "sql": sql}), 400











@app.route("/nl_query", methods=["POST"])
def nl_query():
    session_id = request.form.get("session_id")
    nl_query = request.form.get("nl_query", "")
    output_format = request.form.get("output_format", "xlsx")

    if not session_id or session_id not in DATA_STORE:
        return jsonify({"detail": "session_id not found"}), 404
    if not nl_query.strip():
        return jsonify({"detail": "nl_query is empty"}), 400
    if output_format not in {"xlsx", "csv", "json", "pdf"}:
        return jsonify({"detail": "Invalid output_format"}), 400

    df = DATA_STORE[session_id]

    columns = {str(c): pandas_dtype_str(dtype) for c, dtype in zip(df.columns, df.dtypes)}
    prompt = build_prompt(nl_query, columns)

    # call OpenAI
    try:
        generated = call_openai_chat(prompt)
    except Exception as e:
        return jsonify({"detail": f"OpenAI error: {e}"}), 500

    # extract SQL and store it
    sql = extract_sql_from_text(generated) or generated.strip()
    LAST_SQL[session_id] = sql

    if not basic_sql_safety_check(sql):
        return jsonify({"detail": "Generated SQL failed safety checks"}), 400

    # execute SQL
    try:
        result = pysqldf(sql, {"df": df})
    except Exception as e:
        return jsonify({"detail": f"Failed to run generated SQL: {e}\\nSQL: {sql}"}), 400

    # format output
    try:
        if output_format == "xlsx":
            data_bytes = df_to_xlsx_bytes(result)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ext = "xlsx"
        elif output_format == "csv":
            data_bytes = df_to_csv_bytes(result)
            media_type = "text/csv"
            ext = "csv"
        elif output_format == "json":
            data_bytes = df_to_json_bytes(result)
            media_type = "application/json"
            ext = "json"
        else:  # pdf
            data_bytes = df_to_pdf_bytes(result)
            media_type = "application/pdf"
            ext = "pdf"
    except Exception as e:
        return jsonify({"detail": f"Failed to serialize result: {e}"}), 500

    # send as attachment
    file_like = io.BytesIO(data_bytes)
    file_like.seek(0)
    headers = {
        "Content-Disposition": f'attachment; filename="nl_result_{session_id}.{ext}"'
    }
    return send_file(file_like, mimetype=media_type, as_attachment=True,
                     download_name=f"nl_result_{session_id}.{ext}")

@app.route("/last_sql/<session_id>", methods=["GET"])
def get_last_sql(session_id):
    if session_id not in DATA_STORE:
        return jsonify({"detail": "session_id not found"}), 404
    return jsonify({"sql": LAST_SQL.get(session_id, "")})

if __name__ == "__main__":
    # production: consider using waitress/gunicorn; this uses Flask dev server
    app.run(host="127.0.0.1", port=8000, debug=False)

