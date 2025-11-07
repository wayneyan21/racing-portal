# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import mysql.connector as mysql

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.config['JSON_SORT_KEYS'] = False
CORS(app)  # 上雲後可收緊 origins

import sqlite3

def get_conn():
    return sqlite3.connect("demo.db")




#def get_conn():
#    return mysql.connect(
#        host=os.getenv("DB_HOST", "127.0.0.1"),
#        port=int(os.getenv("DB_PORT", "3306")),
#        user=os.getenv("DB_USER", "root"),
#        password=os.getenv("DB_PASS", ""),
#        database=os.getenv("DB_NAME", "hkjc"),  # ← 如你用 hkjc_db 就改這裡
#        charset="utf8mb4"
#    )

@app.get("/api/health")
def health():
    return {"ok": True}

@app.get("/api/horses")
def list_horses():
    # ✅ 支援搜尋、篩選、分頁
    q          = (request.args.get("q") or "").strip()
    sex        = (request.args.get("sex") or "").strip()
    limit      = max(1, min(request.args.get("limit", type=int, default=200), 500))
    offset     = max(0, request.args.get("offset", type=int, default=0))

    sql = """
        SELECT
            horse_id,
            name          AS name_chi,
            horse_code,
            sex,
            colour,
            country,
            age,
            trainer       AS trainer_id,
            owner,
            DATE_FORMAT(updated_at, '%%Y-%%m-%%d %%H:%%i:%%S') AS updated_at
        FROM horse_profiles
        WHERE 1=1
    """
    params = []

    # 篩選條件
    if sex:
        sql += " AND sex = %s"
        params.append(sex)

    if q:
        like = f"%{q}%"
        sql += " AND (name LIKE %s OR horse_id LIKE %s OR horse_code LIKE %s OR owner LIKE %s)"
        params += [like, like, like, like]

    sql += " ORDER BY updated_at DESC, horse_id LIMIT %s OFFSET %s"
    params += [limit, offset]

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()


@app.get("/api/horses/<horse_id>")
def horse_detail(horse_id):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        # 以前係 FROM horses
        cur.execute("SELECT * FROM horse_profiles WHERE horse_id=%s", (horse_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "not_found"}), 404
        return jsonify(row)
    finally:
        conn.close()

def test_db_connection():
    try:
        conn = get_conn()
        cur = conn.cursor()
        # 以前係 SELECT COUNT(*) FROM horses; 會報不存在
        cur.execute("SELECT COUNT(*) FROM horse_profiles;")
        total = cur.fetchone()[0]
        print(f"✅ DB 連線 OK，horse_profiles = {total}")
        cur.close(); conn.close()
    except Exception as e:
        print("❌ DB 連線失敗：", e)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))

