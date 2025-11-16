# -*- coding: utf-8 -*-
"""
Flask API — HKJC (MySQL / AWS RDS)
- 讀取 .env 中的 DB_* 設定
- mysql.connector + 可選連線池
- 共用 execute_query()，自動關連線/提交
"""

from __future__ import annotations

import logging
import os
from typing import Any, Iterable, Optional

import mysql.connector as mysql
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from mysql.connector import Error as MySQLError, pooling

# -----------------------------
# Bootstrap & Config
# -----------------------------
load_dotenv()  # 開 app 時自動讀取 .env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

app = Flask(__name__)
app.config.update(JSON_AS_ASCII=False, JSON_SORT_KEYS=False)
CORS(app)  # 上線建議：限制 origins，例如 CORS(app, resources={r"/api/*": {"origins": ["https://your-domain"]}})

# -----------------------------
# DB 設定（環境變數）
# -----------------------------
DB_CFG = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "hkjc_db"),
    charset="utf8mb4",
)

USE_POOL = True  # 一鍵開關連線池

_POOL: Optional[pooling.MySQLConnectionPool] = None
if USE_POOL:
    _POOL = pooling.MySQLConnectionPool(
        pool_name="hkjc_pool",
        pool_size=5,
        **DB_CFG,
    )


def get_conn() -> mysql.MySQLConnection:
    """取得一個 MySQL 連線（來自連線池或即時建立）。"""
    if _POOL is not None:
        return _POOL.get_connection()
    return mysql.connect(**DB_CFG)


def execute_query(
    sql: str,
    params: Optional[Iterable[Any]] = None,
    *,
    dict_cursor: bool = True,
    many: bool = False,
) -> Any:
    """
    通用查詢助手：
      - SELECT/SHOW/DESC => 回傳 rows (list[dict] 或 list[tuple])
      - INSERT/UPDATE/DELETE => 回傳 {"affected": n, "lastrowid": x}
    自動關閉 cursor / connection。
    """
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=dict_cursor)
        if many:
            cur.executemany(sql, params or [])
        else:
            cur.execute(sql, params or ())

        if sql.lstrip().lower().startswith(("select", "show", "desc")):
            rows = cur.fetchall()
            return rows

        conn.commit()
        return {
            "affected": cur.rowcount,
            "lastrowid": getattr(cur, "lastrowid", None),
        }
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# -----------------------------
# Debug / Health
# -----------------------------
@app.get("/api/health")
def health():
    """簡單 health check + DB 有無反應。"""
    try:
        r = execute_query("SELECT 1 AS ok;")
        return {"ok": True, "db": bool(r and r[0].get("ok") == 1)}
    except Exception as e:
        app.logger.exception("health check failed")
        return {"ok": False, "error": str(e)}, 500


@app.get("/api/debug/pingdb")
def debug_pingdb():
    """回傳 MySQL 版本，確認 DB 真係接通。"""
    try:
        r = execute_query("SELECT @@version AS mysql_version;")
        return {"ok": True, "mysql_version": r[0]["mysql_version"]}
    except Exception as e:
        app.logger.exception("pingdb failed")
        return {"ok": False, "error": str(e)}, 500


@app.get("/api/debug/desc-horses")
def debug_desc_horses():
    """DESC horse_profiles 檢查欄位。"""
    try:
        rows = execute_query("DESC horse_profiles;")
        return jsonify(rows)
    except MySQLError as e:
        app.logger.exception("DESC horse_profiles failed")
        return {"ok": False, "mysql_error": str(e)}, 500


@app.get("/api/debug/sample-horses")
def debug_sample_horses():
    """試抓 5 筆馬匹資料。"""
    try:
        rows = execute_query(
            """
            SELECT  horse_id,
                    name AS name_chi,
                    horse_code,
                    sex, colour, country, age,
                    trainer AS trainer_id,
                    owner,
                    updated_at
            FROM    horse_profiles
            ORDER BY updated_at DESC
            LIMIT 5
        """
        )
        return jsonify(rows)
    except Exception as e:
        app.logger.exception("sample-horses failed")
        return {"ok": False, "error": str(e)}, 500


@app.get("/api/debug/where-db")
def debug_where_db():
    """辨識目前連線目標是否 RDS、實際 DB 端資訊（兼容 AWS RDS）"""
    try:
        env_host = DB_CFG["host"]

        info = execute_query(
            """SELECT @@hostname AS db_hostname,
                      @@port AS db_port,
                      @@version AS mysql_version,
                      DATABASE() AS current_schema;"""
        )[0]

        try:
            rds_vars = execute_query("SHOW VARIABLES LIKE 'rds%';")
        except Exception:
            rds_vars = []

        is_rds = (
            ".rds.amazonaws.com" in env_host.lower() or len(rds_vars) > 0
        )

        return {
            "env_host": env_host,
            "db_info": info,
            "rds_vars_count": len(rds_vars),
            "is_rds": is_rds,
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


# -----------------------------
# Horses
# -----------------------------
@app.get("/api/horses")
def list_horses():
    """前端用嘅 /api/horses 列表。"""
    try:
        q = (request.args.get("q") or "").strip()
        sex = (request.args.get("sex") or "").strip()
        limit = max(1, min(request.args.get("limit", type=int, default=200), 500))
        offset = max(0, request.args.get("offset", type=int, default=0))

        sql = """
            SELECT  horse_id,
                    name                AS name_chi,
                    horse_code,
                    sex, colour, country, age,
                    trainer             AS trainer_id,
                    owner,
                    updated_at          AS updated_at
            FROM    horse_profiles
            WHERE   1=1
        """
        params: list[Any] = []

        if sex:
            sql += " AND sex = %s"
            params.append(sex)

        if q:
            like = f"%{q}%"
            sql += (
                " AND (name LIKE %s OR horse_id LIKE %s "
                "OR horse_code LIKE %s OR owner LIKE %s)"
            )
            params += [like, like, like, like]

        sql += " ORDER BY updated_at DESC, horse_id LIMIT %s OFFSET %s"
        params += [limit, offset]

        rows = execute_query(sql, tuple(params), dict_cursor=True)
        return jsonify(rows)
    except Exception as e:
        app.logger.exception("list_horses failed")
        return {"error": "server", "detail": str(e)}, 500


@app.get("/api/horses/<horse_id>")
def horse_detail(horse_id: str):
    """單一馬匹詳情。"""
    try:
        rows = execute_query(
            "SELECT * FROM horse_profiles WHERE horse_id=%s",
            (horse_id,),
            dict_cursor=True,
        )
        if not rows:
            return {"error": "not_found"}, 404
        return jsonify(rows[0])
    except Exception as e:
        app.logger.exception("horse_detail failed")
        return {"error": "server", "detail": str(e)}, 500


# -----------------------------
# Pre-flight Test & Error hook
# -----------------------------
def test_db_connection() -> None:
    """啟動時先測一次 DB。"""
    try:
        r = execute_query("SELECT COUNT(*) AS c FROM horse_profiles;")
        app.logger.info("DB OK, horse_profiles=%s", r[0]["c"])
    except Exception as e:
        app.logger.error("DB 連線失敗: %s", e)


@app.errorhandler(500)
def handle_500(err):
    return jsonify({"error": "internal_error", "detail": str(err)}), 500


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    test_db_connection()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
