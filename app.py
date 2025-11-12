# -*- coding: utf-8 -*-
"""
Flask API — HKJC (RDS 版)
- 讀取 .env 的 DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME
- mysql.connector 直連（亦可切換連線池）
- 統一查詢助手 execute_query()
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import mysql.connector as mysql
from mysql.connector import pooling
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO)

load_dotenv()  # 讓程式啟動時自動讀取 .env


# -----------------------------
# App & Config
# -----------------------------
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.config['JSON_SORT_KEYS'] = False
CORS(app)  # 上線後可改成 CORS(app, resources={r"/api/*": {"origins": ["https://你的網域"]}})

# -----------------------------
# DB 連線設定（環境變數）
# -----------------------------
DB_CFG = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "hkjc_db"),
    charset="utf8mb4",
)

USE_POOL = True  # 想簡單點可改 False 用「每次新連線」

pool = None
if USE_POOL:
    pool = pooling.MySQLConnectionPool(
        pool_name="hkjc_pool",
        pool_size=5,
        **DB_CFG
    )

def get_conn():
    """取得一個 MySQL 連線。"""
    if USE_POOL and pool is not None:
        return pool.get_connection()
    return mysql.connect(**DB_CFG)

def execute_query(sql, params=None, dict_cursor=True, many=False):
    """
    通用查詢助手：
    - dict_cursor=True 會回傳 dict 列
    - many=True 時，用 executemany
    - 自動關閉連線
    """
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=dict_cursor)
        if many:
            cur.executemany(sql, params or [])
        else:
            cur.execute(sql, params or ())
        if sql.strip().lower().startswith(("select", "show", "desc")):
            rows = cur.fetchall()
            return rows
        else:
            conn.commit()
            return {"affected": cur.rowcount, "lastrowid": getattr(cur, "lastrowid", None)}
    finally:
        cur.close()
        conn.close()

# -----------------------------
# Health Check
# -----------------------------
@app.get("/api/health")
def health():
    try:
        rows = execute_query("SELECT 1 AS ok;")
        return {"ok": True, "db": rows[0]["ok"] == 1}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

# -----------------------------
# Horses
# -----------------------------
@app.get("/api/horses")
def list_horses():
    try:
        q      = (request.args.get("q") or "").strip()
        sex    = (request.args.get("sex") or "").strip()
        limit  = max(1, min(request.args.get("limit", type=int, default=200), 500))
        offset = max(0, request.args.get("offset", type=int, default=0))

        sql = """
            SELECT
                horse_id,
                name                AS name_chi,
                horse_code,
                sex,
                colour,
                country,
                age,
                trainer             AS trainer_id,
                owner,
                DATE_FORMAT(updated_at, '%%Y-%%m-%%d %%H:%%i:%%s') AS updated_at
            FROM horse_profiles
            WHERE 1=1
        """
        params = []
        if sex:
            sql += " AND sex = %s"
            params.append(sex)
        if q:
            like = f"%{q}%"
            sql += " AND (name LIKE %s OR horse_id LIKE %s OR horse_code LIKE %s OR owner LIKE %s)"
            params += [like, like, like, like]
        sql += " ORDER BY updated_at DESC, horse_id LIMIT %s OFFSET %s"
        params += [limit, offset]

        rows = execute_query(sql, tuple(params), dict_cursor=True)
        return jsonify(rows)
    except Exception as e:
        app.logger.exception("list_horses failed")
        return jsonify({"error": "server", "detail": str(e)}), 500


@app.get("/api/horses/<horse_id>")
def horse_detail(horse_id):
    rows = execute_query("SELECT * FROM horse_profiles WHERE horse_id=%s", (horse_id,), dict_cursor=True)
    if not rows:
        return jsonify({"error": "not_found"}), 404
    return jsonify(rows[0])

# -----------------------------
# 啟動前自測
# -----------------------------
def test_db_connection():
    try:
        rows = execute_query("SELECT COUNT(*) AS c FROM horse_profiles;")
        total = rows[0]["c"]
        print(f"✅ DB 連線 OK，horse_profiles = {total}")
    except Exception as e:
        print("❌ DB 連線失敗：", e)

# -----------------------------
# 全域錯誤處理（可選）
# -----------------------------
@app.errorhandler(500)
def handle_500(err):
    return jsonify({"error": "internal_error", "detail": str(err)}), 500

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    test_db_connection()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
