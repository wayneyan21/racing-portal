# -*- coding: utf-8 -*-
"""
根據 racecard_entries + horse_histories
計算「當日出賽馬」嘅馬匹統計並寫入 race_horse_stats

用法例子：
  python hkjc_race_horse_stats.py --date 2024-09-08
  python hkjc_race_horse_stats.py --date 2024-09-08 --venue ST --race-no 5
"""

import argparse
import re
import pymysql
from contextlib import contextmanager
from typing import Optional, List, Dict

# ========= 手動 DB 設定（⚠️ 只要改呢度就得） =========
DB_CFG = {
    "host": "hkjc-db.ccdsakuk6778.us-east-1.rds.amazonaws.com",     # 或 "localhost"
    "port": 3306,
    "user": "waynelam",          # << 你的 MySQL user
    "password": "9p3Xls7uapBp5JSzMvK6",# << 你的 MySQL 密碼
    "database": "hkjc_db",      # << racecard_entries 嗰個 DB 名
}

# ========= DB 連線 =========

@contextmanager
def mysql_conn(cfg: Dict = None):
    """
    用法：
        with mysql_conn() as conn:
            ...
    如有需要亦可以傳入其他 cfg 覆蓋 DB_CFG
    """
    if cfg is None:
        cfg = DB_CFG

    # debug 用，唔 print 密碼
    print("DB config =>", {
        "host": cfg["host"],
        "port": cfg["port"],
        "user": cfg["user"],
        "database": cfg["database"],
    })

    conn = pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ========= 工具 =========

def placing_to_rank(placing: Optional[str]) -> Optional[int]:
    """由 placing 字串抽出名次（只要數字部分，例如 '1', '1 ', '1=' 都當 1）"""
    if not placing:
        return None
    m = re.search(r"\d+", placing)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None

def compute_stats_for_horse(rows: List[Dict]) -> Dict:
    """
    rows: 由 horse_histories 撈返嚟嘅往績列表（已經只係賽日前）
    回傳：E,F,G,H,I 同埋各種 rate / score
    """
    total = win = p2 = p3 = p4 = 0

    for r in rows:
        rank = placing_to_rank(r.get("placing"))
        if not rank:
            continue
        total += 1
        if rank == 1:
            win += 1
        elif rank == 2:
            p2 += 1
        elif rank == 3:
            p3 += 1
        elif rank == 4:
            p4 += 1

    if total == 0:
        return {
            "total_runs": 0,
            "win_cnt": 0,
            "place2_cnt": 0,
            "place3_cnt": 0,
            "place4_cnt": 0,
            "win_rate": None,
            "q_rate": None,
            "plc_rate": None,
            "top4_rate": None,
            "base_score": 0.0
        }

    win_rate  = win / total
    q_rate    = (win + p2) / total
    plc_rate  = (win + p2 + p3) / total
    top4_rate = (win + p2 + p3 + p4) / total

    # 得分 = ((E*1.3)+(F*1.2)+(G*1.1)+(H*1))/(I*1.3)*100
    base_score = ((win * 1.3) + (p2 * 1.2) + (p3 * 1.1) + (p4 * 1.0)) / (total * 1.3) * 100

    return {
        "total_runs": total,
        "win_cnt": win,
        "place2_cnt": p2,
        "place3_cnt": p3,
        "place4_cnt": p4,
        "win_rate": win_rate,
        "q_rate": q_rate,
        "plc_rate": plc_rate,
        "top4_rate": top4_rate,
        "base_score": base_score
    }

def stats_from_counts(total: int, win: int, p2: int, p3: int, p4: int) -> Dict:
    """
    由「總場數 + 1~4 名次次數」計出各種 rate + base_score
    （同 compute_stats_for_horse 裏面條 formula 一樣）
    """
    if total <= 0:
        return {
            "total_runs": 0,
            "win_cnt": 0,
            "place2_cnt": 0,
            "place3_cnt": 0,
            "place4_cnt": 0,
            "win_rate": None,
            "q_rate": None,
            "plc_rate": None,
            "top4_rate": None,
            "base_score": 0.0,
        }

    win_rate  = win / total
    q_rate    = (win + p2) / total
    plc_rate  = (win + p2 + p3) / total
    top4_rate = (win + p2 + p3 + p4) / total
    base_score = ((win * 1.3) + (p2 * 1.2) + (p3 * 1.1) + (p4 * 1.0)) / (total * 1.3) * 100

    return {
        "total_runs": total,
        "win_cnt": win,
        "place2_cnt": p2,
        "place3_cnt": p3,
        "place4_cnt": p4,
        "win_rate": win_rate,
        "q_rate": q_rate,
        "plc_rate": plc_rate,
        "top4_rate": top4_rate,
        "base_score": base_score,
    }

def normalize_course_code(course: str, venue_code: str) -> str:
    """
    將 racecard_races.course 轉做內部用嘅 course_code：
      - 沙田 全天候 → 'STA'
      - 其他例如 '草地 / A' → 取最後一段 'A'
      - 如果已經係 'STA' / 'A' / 'B' 之類就原樣
    """
    if not course:
        return ''

    course = course.strip()

    # 沙田全天候：例如 '全天候 / AWT'、'全天候' 等
    if venue_code == 'ST' and ('全天候' in course or 'AWT' in course):
        return 'STA'

    # 草地：例如 '草地 / A'、'草地 / B'
    if '草地' in course and '/' in course:
        return course.split('/')[-1].strip()

    # 已經係簡碼（A / B / C / STA 等）
    return course


# ========= 主計算流程 =========

def fetch_races_for_date(conn, race_date: str,
                         venue: Optional[str] = None,
                         race_no: Optional[int] = None) -> List[Dict]:
    """
    由 racecard_races 搵出某日有哪些 (venue_code, race_no)
    """
    sql = """
    SELECT race_date, venue_code, race_no, race_name_zh
    FROM racecard_races
    WHERE race_date = %s
    """
    params = [race_date]
    if venue:
        sql += " AND venue_code = %s"
        params.append(venue)
    if race_no:
        sql += " AND race_no = %s"
        params.append(race_no)
    sql += " ORDER BY venue_code, race_no"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def fetch_entries_for_race(conn, race_date: str, race_no: int) -> List[Dict]:
    """
    只撈「當日出賽馬」：
      - 由 racecard_entries
      - 過濾 scratched = 0 or NULL
    """
    sql = """
    SELECT
      horse_no,
      horse_name_zh,
      horse_code,
      horse_id,
      draw
    FROM racecard_entries
    WHERE race_date = %s
      AND race_no   = %s
      AND (scratched IS NULL OR scratched = 0)
    ORDER BY horse_no
    """
    with conn.cursor() as cur:
        cur.execute(sql, (race_date, race_no))
        rows = cur.fetchall()
    return rows

def fetch_history_for_horse(conn, horse_id: str, race_date: str) -> List[Dict]:
    """
    從 horse_histories 撈「賽日前」嘅全部往績
    """
    sql = """
    SELECT race_date, race_no, placing
    FROM horse_histories
    WHERE horse_id = %s
      AND race_date < %s
    ORDER BY race_date DESC, race_no DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (horse_id, race_date))
        return cur.fetchall()

def fetch_draw_stats_for_race(conn, race_date: str, venue_code: str, race_no: int):
    """
    由 draw_stats 取「檔位統計」，唔再喺 race_results 計。

    步驟：
      1) 由 racecard_races 撈出呢場嘅 distance_m + course
      2) 從 course 抽出 course_code（例如「草地 / B」→「B」）
      3) 喺 draw_stats 之中，按
           - racecourse_code = 場地 (ST / HV)
           - course_code     = 上面抽出嘅 code (A/B/C/全天候…)
           - distance_m      = 呎程
           - going_code      = 'ALL'
         撈出所有 gate_no 行
      4) 對每個 gate_no 用 runs / win / 2,3,4 名次 計 rate + score
         （formula 同馬匹 score 一樣）
    回傳：{ gate_no(int) : {...統計...}, ... }
    """

    # 1) 撈返呢場嘅 meta（course + distance）
    sql_meta = """
        SELECT distance_m, course
        FROM racecard_races
        WHERE race_date  = %s
          AND venue_code = %s
          AND race_no    = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql_meta, (race_date, venue_code, race_no))
        meta = cur.fetchone()

    if not meta:
        print(f"    ⚠️ 找不到 {race_date} {venue_code} R{race_no} 的 racecard_races 資料，檔位統計略過")
        return {}

    distance_m = meta["distance_m"]
    raw_course = meta["course"]
    course_code = normalize_course_code(raw_course, venue_code)

    # 例如「草地 / B」→「B」，如果冇斜線就用全字（例如「全天候」）
    if "/" in raw_course:
        course_code = raw_course.split("/")[-1].strip()
    elif "／" in raw_course:  # 防萬一有全形斜線
        course_code = raw_course.split("／")[-1].strip()
    else:
        course_code = raw_course

    # 🔁 特別處理：沙田全天候 AWT 喺 draw_stats 用 STA
    #   - racecard_races.course 可能係「全天候 / AWT」或者「全天候」
    #   - draw_stats.racecourse_code = 'ST'，course_code = 'STA'
    if venue_code == "ST":
        if "全天候" in raw_course or course_code.upper() in ("AWT", "ALL WEATHER", "A.W.T"):
            course_code = "STA"

    # 2) 喺 draw_stats 撈晒同一場地 / 賽道 / 距離嘅所有檔位統計
    sql_ds = """
        SELECT
          gate_no,
          runs,
          win,
          second_place,
          third_place,
          fourth_place
        FROM draw_stats
        WHERE racecourse_code = %s
          AND course_code     = %s
          AND distance_m      = %s
          AND going_code      = 'ALL'
    """
    with conn.cursor() as cur:
        cur.execute(sql_ds, (venue_code, course_code, distance_m))
        rows = cur.fetchall()

    if not rows:
        print(f"    ℹ️ draw_stats 暫時冇 {venue_code} {course_code} {distance_m}m 資料，檔位統計全部 0")
        return {}

    # 3) 把每個 gate_no 的次數放入 map
    draw_map = {}
    for r in rows:
        gate_no = r.get("gate_no")
        if gate_no is None:
            continue
        try:
            gate_no = int(gate_no)
        except (TypeError, ValueError):
            continue

        runs = r.get("runs", 0) or 0
        win  = r.get("win", 0) or 0
        p2   = r.get("second_place", 0) or 0
        p3   = r.get("third_place", 0) or 0
        p4   = r.get("fourth_place", 0) or 0

        draw_map[gate_no] = {
            "draw_runs": runs,
            "draw_win": win,
            "draw_second_place": p2,
            "draw_third_place": p3,
            "draw_forth_place": p4,
        }

    # 4) 計 rate + raw score，同你馬匹個 formula 一樣
    max_raw = 0.0
    min_raw = None

    for gate_no, s in draw_map.items():
        runs = s["draw_runs"]
        if runs > 0:
            win = s["draw_win"]
            p2  = s["draw_second_place"]
            p3  = s["draw_third_place"]
            p4  = s["draw_forth_place"]

            s["draw_win_rate"]   = win / runs
            s["draw_q_rate"]     = (win + p2) / runs
            s["draw_place_rate"] = (win + p2 + p3) / runs
            s["draw_top4_rate"]  = (win + p2 + p3 + p4) / runs

            base = ((win * 1.3) + (p2 * 1.2) + (p3 * 1.1) + (p4 * 1.0)) / (runs * 1.3) * 100
        else:
            s["draw_win_rate"]   = None
            s["draw_q_rate"]     = None
            s["draw_place_rate"] = None
            s["draw_top4_rate"]  = None
            base = 0.0

        s["draw_score_raw"] = base

        if runs > 0:
            if base > max_raw:
                max_raw = base
            if min_raw is None or base < min_raw:
                min_raw = base

    if min_raw is None:
        # 理論上唔會，除非全部 runs=0
        return draw_map

    # 5) normalize → draw_score_norm / draw_score_final
    for gate_no, s in draw_map.items():
        runs = s["draw_runs"]
        base = s["draw_score_raw"]

        # 全部計：以最高 raw score 做 100
        if max_raw > 0:
            s["draw_score_norm"] = 100.0 * base / max_raw
        else:
            s["draw_score_norm"] = 0.0

        # 10綠＝50分：同馬匹一樣
        if max_raw > min_raw:
            score_50 = 100.0 * (base - min_raw) / (max_raw - min_raw)
        else:
            score_50 = 50.0

        # 出賽 < 10 而 <50 分 → 補到 50
        if runs < 10 and score_50 < 50.0:
            score_50 = 50.0

        s["draw_score_final"] = score_50

    return draw_map


def fetch_race_meta_single(conn, race_date: str, venue_code: str, race_no: int) -> Optional[Dict]:
    """
    由 racecard_races 撈返某一場嘅 meta（距離 + 跑道）
    """
    sql = """
    SELECT distance_m, course
    FROM racecard_races
    WHERE race_date = %s
      AND venue_code = %s
      AND race_no = %s
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (race_date, venue_code, race_no))
        return cur.fetchone()


def fetch_draw_counts(conn,
                      venue_code: str,
                      course: str,
                      distance_m: int,
                      gate_no: int) -> Dict:
    """
    由 draw_status 撈返某個檔位嘅統計次數：
      runs, win, second_place, third_place, fourth_place
    ⚠️ 如果你表名 / 欄名唔同，喺呢度改番就得。
    """
    sql = """
    SELECT runs, win, second_place, third_place, fourth_place
    FROM draw_status
    WHERE racecourse_code = %s
      AND course_code     = %s
      AND distance_m      = %s
      AND gate_no         = %s
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (venue_code, course, distance_m, gate_no))
        row = cur.fetchone()

    if not row:
        return {
            "runs": 0,
            "win": 0,
            "second_place": 0,
            "third_place": 0,
            "fourth_place": 0,
        }

    return {
        "runs":          row.get("runs", 0) or 0,
        "win":           row.get("win", 0) or 0,
        "second_place":  row.get("second_place", 0) or 0,
        "third_place":   row.get("third_place", 0) or 0,
        "fourth_place":  row.get("fourth_place", 0) or 0,
    }


def compute_stats_from_counts(total: int, win: int, p2: int, p3: int, p4: int) -> Dict:
    """
    用「次數」計一套 rate + base_score
    比 compute_stats_for_horse 相同輸出 format：
      total_runs, win_cnt, place2_cnt, place3_cnt, place4_cnt,
      win_rate, q_rate, plc_rate, top4_rate, base_score
    """
    if total <= 0:
        return {
            "total_runs": 0,
            "win_cnt": 0,
            "place2_cnt": 0,
            "place3_cnt": 0,
            "place4_cnt": 0,
            "win_rate": None,
            "q_rate": None,
            "plc_rate": None,
            "top4_rate": None,
            "base_score": 0.0,
        }

    win_rate  = win / total
    q_rate    = (win + p2) / total
    plc_rate  = (win + p2 + p3) / total
    top4_rate = (win + p2 + p3 + p4) / total

    base_score = ((win * 1.3) + (p2 * 1.2) + (p3 * 1.1) + (p4 * 1.0)) / (total * 1.3) * 100

    return {
        "total_runs": total,
        "win_cnt": win,
        "place2_cnt": p2,
        "place3_cnt": p3,
        "place4_cnt": p4,
        "win_rate": win_rate,
        "q_rate": q_rate,
        "plc_rate": plc_rate,
        "top4_rate": top4_rate,
        "base_score": base_score,
    }


def upsert_race_analysis_scores(conn,
                                race_date: str,
                                venue_code: str,
                                race_no: int,
                                rows: List[Dict]):
    """
    寫入 race_analysis_scores：
      - 馬匹統計 horse_*
      - 檔位統計 draw_*
    """
    sql = """
    INSERT INTO race_analysis_scores (
      race_date,
      race_no,
      venue_code,
      horse_id,

      horse_runs,
      win,
      second_place,
      third_place,
      forth_place,
      horse_win_rate,
      horse_q_rate,
      horse_place_rate,
      horse_top4_rate,
      horse_score_raw,
      horse_score_norm,
      horse_score_final,

      draw_runs,
      draw_win,
      draw_second_place,
      draw_third_place,
      draw_forth_place,
      draw_win_rate,
      draw_q_rate,
      draw_place_rate,
      draw_top4_rate,
      draw_score_raw,
      draw_score_norm,
      draw_score_final,

      total_score
    )
    VALUES (
      %(race_date)s,
      %(race_no)s,
      %(venue_code)s,
      %(horse_id)s,

      %(horse_runs)s,
      %(win)s,
      %(second_place)s,
      %(third_place)s,
      %(forth_place)s,
      %(horse_win_rate)s,
      %(horse_q_rate)s,
      %(horse_place_rate)s,
      %(horse_top4_rate)s,
      %(horse_score_raw)s,
      %(horse_score_norm)s,
      %(horse_score_final)s,

      %(draw_runs)s,
      %(draw_win)s,
      %(draw_second_place)s,
      %(draw_third_place)s,
      %(draw_forth_place)s,
      %(draw_win_rate)s,
      %(draw_q_rate)s,
      %(draw_place_rate)s,
      %(draw_top4_rate)s,
      %(draw_score_raw)s,
      %(draw_score_norm)s,
      %(draw_score_final)s,

      %(total_score)s
    )
    ON DUPLICATE KEY UPDATE
      horse_runs        = VALUES(horse_runs),
      win               = VALUES(win),
      second_place      = VALUES(second_place),
      third_place       = VALUES(third_place),
      forth_place       = VALUES(forth_place),
      horse_win_rate    = VALUES(horse_win_rate),
      horse_q_rate      = VALUES(horse_q_rate),
      horse_place_rate  = VALUES(horse_place_rate),
      horse_top4_rate   = VALUES(horse_top4_rate),
      horse_score_raw   = VALUES(horse_score_raw),
      horse_score_norm  = VALUES(horse_score_norm),
      horse_score_final = VALUES(horse_score_final),

      draw_runs         = VALUES(draw_runs),
      draw_win          = VALUES(draw_win),
      draw_second_place = VALUES(draw_second_place),
      draw_third_place  = VALUES(draw_third_place),
      draw_forth_place  = VALUES(draw_forth_place),
      draw_win_rate     = VALUES(draw_win_rate),
      draw_q_rate       = VALUES(draw_q_rate),
      draw_place_rate   = VALUES(draw_place_rate),
      draw_top4_rate    = VALUES(draw_top4_rate),
      draw_score_raw    = VALUES(draw_score_raw),
      draw_score_norm   = VALUES(draw_score_norm),
      draw_score_final  = VALUES(draw_score_final),

      total_score       = VALUES(total_score);
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)



def process_race(conn, race_date: str, venue_code: str, race_no: int):
    """
    處理某一場：
      1) 找出當日出賽馬
      2) 每匹馬由 horse_histories 撈賽日前往績（馬匹統計）
      3) 根據場地 / 途程 / 檔位撈 draw_status（檔位統計）
      4) 算出各自分數，寫入 race_analysis_scores
    """
    # 1) 出賽馬
    entries = fetch_entries_for_race(conn, race_date, race_no)
    if not entries:
        print(f"  ➜ {race_date} {venue_code} R{race_no}: 沒有出賽馬，跳過")
        return

    # ✅ 2) 檔位統計：一次過撈晒呢場所有 gate 的統計
    draw_map = fetch_draw_stats_for_race(conn, race_date, venue_code, race_no)

    # 3) 先計好每匹馬嘅 base_score + 檔位統計
    tmp_rows = []
    for e in entries:
        horse_no   = e["horse_no"]
        horse_name = e["horse_name_zh"]
        horse_id   = e.get("horse_id")
        gate_no    = e.get("draw")  # 檔位

        if not horse_id:
            print(f"    ⚠️ horse_no {horse_no} ({horse_name}) 沒有 horse_id，暫用空值")
            histories = []
        else:
            histories = fetch_history_for_horse(conn, horse_id, race_date)

        # 🐎 馬匹統計
        stat_horse = compute_stats_for_horse(histories)

        # 📊 檔位統計（如果 draw_map 冇，就全部當 0）
        draw_stat = draw_map.get(int(gate_no)) if gate_no is not None else None
        if not draw_stat:
            draw_stat = {
                "draw_runs": 0,
                "draw_win": 0,
                "draw_second_place": 0,
                "draw_third_place": 0,
                "draw_forth_place": 0,
                "draw_win_rate": None,
                "draw_q_rate": None,
                "draw_place_rate": None,
                "draw_top4_rate": None,
                "draw_score_raw": 0.0,
                "draw_score_norm": 0.0,
                "draw_score_final": 50.0,
            }

        tmp_rows.append({
            "horse_no":      horse_no,
            "horse_name_zh": horse_name,
            "horse_id":      horse_id,
            **stat_horse,
            **draw_stat,
        })

    # 4) 以「馬匹 base_score」做 max / min → all_pct / score_50
    base_vals = [r["base_score"] for r in tmp_rows if r["total_runs"] > 0]
    if base_vals:
        max_base = max(base_vals)
        min_base = min(base_vals)
    else:
        max_base = 0.0
        min_base = 0.0

    out_rows = []
    for r in tmp_rows:
        base = float(r["base_score"] or 0.0)
        total_runs = r["total_runs"]

        if max_base > 0:
            all_pct = 100.0 * base / max_base
        else:
            all_pct = 0.0

        if max_base > min_base:
            score_50 = 100.0 * (base - min_base) / (max_base - min_base)
        else:
            score_50 = 50.0

        if total_runs < 10 and score_50 < 50.0:
            score_50 = 50.0

        out_rows.append({
            "race_date": race_date,
            "race_no": race_no,
            "venue_code": venue_code,
            "horse_id": r["horse_id"],

            # 馬匹統計
            "horse_runs": r["total_runs"],
            "win": r["win_cnt"],
            "second_place": r["place2_cnt"],
            "third_place": r["place3_cnt"],
            "forth_place": r["place4_cnt"],
            "horse_win_rate": r["win_rate"],
            "horse_q_rate": r["q_rate"],
            "horse_place_rate": r["plc_rate"],
            "horse_top4_rate": r["top4_rate"],
            "horse_score_raw": round(base, 3),
            "horse_score_norm": round(all_pct, 3),
            "horse_score_final": round(score_50, 3),

            # 檔位統計（已經喺 tmp_rows merge 入去）
            "draw_runs": r["draw_runs"],
            "draw_win": r["draw_win"],
            "draw_second_place": r["draw_second_place"],
            "draw_third_place": r["draw_third_place"],
            "draw_forth_place": r["draw_forth_place"],
            "draw_win_rate": r["draw_win_rate"],
            "draw_q_rate": r["draw_q_rate"],
            "draw_place_rate": r["draw_place_rate"],
            "draw_top4_rate": r["draw_top4_rate"],
            "draw_score_raw": r["draw_score_raw"],
            "draw_score_norm": r["draw_score_norm"],
            "draw_score_final": r["draw_score_final"],

            # 暫時 total_score = 馬匹最終分數
            "total_score": round(score_50, 3),
        })

    upsert_race_analysis_scores(conn, race_date, venue_code, race_no, out_rows)
    print(f"  ➜ {race_date} {venue_code} R{race_no}: 已更新 {len(out_rows)} 匹馬統計（含檔位統計）")


def main():
    ap = argparse.ArgumentParser(description="計算「當日出賽馬」的馬匹統計（race_horse_stats）")
    ap.add_argument("--date", required=True, help="賽日 YYYY-MM-DD")
    ap.add_argument("--venue", help="ST / HV（可選，唔填就計晒當日所有場地）")
    ap.add_argument("--race-no", type=int, help="只計某一場（可選）")
    args = ap.parse_args()

    with mysql_conn() as conn:
        races = fetch_races_for_date(conn, args.date, args.venue, args.race_no)
        if not races:
            print(f"❌ {args.date} 沒有 racecard_races 記錄")
            return

        print(f"🔎 {args.date} 共有 {len(races)} 場需要計算")
        for r in races:
            # race_date 可能已經係 date object，保險起見轉返 YYYY-MM-DD 字串
            rd = str(r["race_date"])
            process_race(conn, rd, r["venue_code"], r["race_no"])

if __name__ == "__main__":
    main()
