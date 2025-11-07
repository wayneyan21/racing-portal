# -*- coding: utf-8 -*-
"""
HKJC 排位表 → 全日所有場次 + 後備馬（Python 版，對齊你的 v5 腦袋）
- 先暖場取 Cookie，帶 UA/Referer/Cookie 抓 RaceCard（中文），抓不到回退英文頁
- 自動挑最像出馬表的 <table>（f_fs12 / table_bd 等），表頭對不上用位置式後備
- 自動命名輸出檔案：R{no}_{讓賽名}.csv、R{no}_Reserves.csv
- 可選：寫入 MySQL（示例函式，關閉預設）

依賴：
  pip install requests mysql-connector-python  # 後者只有用到 MySQL 時才需要

用法：
  python hkjc_racecard_scraper.py --date 2025/10/01 --course ST --max-races 14 --out ./out
"""

import re
import os
import csv
import time
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import requests

# ------------------------- 設定（可在 CLI 覆蓋） -------------------------
DEFAULT_DATE = "2025/10/19"     # YYYY/MM/DD
DEFAULT_COURSE = "ST"           # ST=沙田 / HV=跑馬地
DEFAULT_MAX_RACES = 14
DEFAULT_TIMEOUT = 15            # 秒
DEFAULT_HISTORY_LIMIT = 10      # 這版不抓歷史，保留常數方便你後續擴充

# 出馬表欄位（中文優先；英文頁會映射到這些中文欄位）
HEADER = [
    '序號','6次近績','馬名','烙號','負磅','騎師','可能超磅','檔位','練馬師',
    '評分','評分+/-','排位體重','排位體重+/-','最佳時間','馬齡','性別',
    '今季獎金','優先參賽次序','上賽距今日數','配備','馬主','父系','母系','進口類別'
]
RES_HEADER = ['序號','馬名','排位體重','負磅','評分','馬齡','6次近績','練馬師','優先參賽次序','配備']

# ------------------------- HTTP：帶 Cookie 抓頁 -------------------------
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"


def warm_and_get_cookie(session: requests.Session) -> str:
    r = session.get("https://racing.hkjc.com/", headers={
        "User-Agent": UA,
        "Accept-Language": "zh-HK,zh;q=0.9,en;q=0.8",
        "Upgrade-Insecure-Requests": "1"
    }, timeout=DEFAULT_TIMEOUT)
    # 把 session.cookies 轉成 Cookie header
    cookies = session.cookies.get_dict()
    return "; ".join([f"{k}={v}" for k, v in cookies.items()])


def fetch_with_cookie(session: requests.Session, url: str) -> str:
    cookie_header = warm_and_get_cookie(session)
    r = session.get(url, headers={
        "User-Agent": UA,
        "Referer": "https://racing.hkjc.com/",
        "Accept-Language": "zh-HK,zh;q=0.9,en;q=0.8",
        "Cookie": cookie_header
    }, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
    return r.text if r.status_code == 200 else ""


def fetch_race_html(session: requests.Session, date_str: str, race_no: int, course: str, lang: str = "Chinese") -> str:
    base = f"https://racing.hkjc.com/racing/information/{lang}/Racing/RaceCard.aspx"
    url = f"{base}?RaceDate={requests.utils.quote(date_str)}&RaceNo={race_no}&Racecourse={course}"
    return fetch_with_cookie(session, url)

# ------------------------- 工具 -------------------------
def strip_html(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", s.replace("&nbsp;", " ").replace("&amp;", "&"))).strip()


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[\[\]\*\?\/\\:]", " ", name)
    name = re.sub(r"\s{2,}", " ", name).strip()
    return name[:120]


def guess_index(td_count: int, is_zh: bool) -> Dict[str, int]:
    def safe(i): return i if i < td_count else None
    order = [
        ('序號','No',0), ('6次近績','Last 6 Runs',1), ('馬名','Horse',2), ('烙號','Brand No.',3),
        ('負磅','Wt',4), ('騎師','Jockey',5), ('可能超磅','Over Wt.',6), ('檔位','Draw',7),
        ('練馬師','Trainer',8), ('評分','Rtg',9), ('評分+/-','Rtg+/-',10), ('排位體重','Horse Wt.',11),
        ('排位體重+/-','Wt+/-',12), ('最佳時間','Best Time',13), ('馬齡','Age',14), ('性別','Sex',15),
        ('今季獎金','Season Stakes',16), ('優先參賽次序','Priority',17), ('上賽距今日數','Days Since Last Run',18),
        ('配備','Gear',19), ('馬主','Owner',20), ('父系','Sire',21), ('母系','Dam',22), ('進口類別','Import',23)
    ]
    m = {}
    for zh, en, pos in order:
        key = zh if is_zh else en
        m[key] = safe(pos)
    return m


def index_by_header(ths: List[str]) -> Dict[str, int]:
    aliases = {
        '序號': ['馬號','No','Number'],
        '6次近績': ['近績','Last 6 Runs'],
        '馬名': ['馬匹','Horse'],
        '烙號': ['編號','Brand No.','Brand No','烙號/編號'],
        '負磅': ['負磅(磅)','Handicap','Wt','Weight'],
        '騎師': ['騎師(可能超磅)','Jockey'],
        '可能超磅': ['(可能超磅)','Over Wt.'],
        '檔位': ['檔','Draw'],
        '練馬師': ['練者','Trainer'],
        '評分': ['評分(Rtg)','Rtg','Rating'],
        '評分+/-': ['評分變動','Rtg+/-','+/-'],
        '排位體重': ['體重','宣告體重','Horse Wt.'],
        '排位體重+/-': ['體重增減','Wt+/-'],
        '最佳時間': ['Best Time','最佳'],
        '馬齡': ['Age'],
        '性別': ['Sex'],
        '今季獎金': ['季內獎金','Season Stakes'],
        '優先參賽次序': ['優先序','Priority'],
        '上賽距今日數': ['上次出賽日數','Days Since Last Run'],
        '配備': ['Gear'],
        '馬主': ['Owner'],
        '父系': ['Sire'],
        '母系': ['Dam'],
        '進口類別': ['來港類別','Import Cat.','Import','Import Category']
    }
    m: Dict[str, int] = {}
    for i, h in enumerate(ths):
        clean = re.sub(r"\s+", "", h).lower()
        for key, cands in aliases.items():
            for c in [key] + cands:
                cc = re.sub(r"\s+", "", c).lower()
                if (cc in clean) or (clean in cc):
                    if key not in m:
                        m[key] = i
    return m


def pick_starter_table(compact_html: str) -> str:
    tables = [m.group(0) for m in re.finditer(r"<table[^>]*>[\s\S]*?<\/table>", compact_html, re.I)]
    if not tables:
        return ""
    best, best_score = "", -1
    for t in tables:
        score = 0
        if re.search(r'class="[^"]*\bf_fs12\b', t): score += 40
        if re.search(r'class="[^"]*\btable_bd\b', t): score += 30
        if re.search(r"近績|馬名|排位體重|負磅", t): score += 25
        if re.search(r"Horse No\.|Last 6 Runs|Jockey|Horse Wt\.", t, re.I): score += 25
        trc = len(re.findall(r"<tr", t, flags=re.I))
        score += min(trc * 1.5, 40)
        first_tr = re.search(r"<tr[^>]*>[\s\S]*?<\/tr>", t, re.I)
        if first_tr:
            td_count = len(re.findall(r"<t[hd][^>]*>", first_tr.group(0), re.I))
            if td_count >= 10:
                score += 10
        if score > best_score:
            best_score, best = score, t
    return best


def has_starter_table(html: str) -> bool:
    return bool(re.search(r"近績|馬名|排位體重|負磅", html or ""))


def parse_race_meta(html: str) -> Dict[str, str]:
    m = re.search(r"<h1[^>]*>[\s\S]*?<\/h1>", html, re.I)
    title = strip_html(m.group(0)) if m else ""
    return {"title": title}


def parse_reserves_from_chinese(compact_html: str) -> List[List[str]]:
    m = re.search(r"後備馬匹[\s\S]*?<\/table>", compact_html)
    if not m:
        return []
    block = m.group(0)
    row_re = re.compile(r"<tr[^>]*>([\s\S]*?)<\/tr>", re.I)
    cell_re = re.compile(r"<t[dh][^>]*>([\s\S]*?)<\/t[dh]>", re.I)
    out: List[List[str]] = []
    first = True
    for rr in row_re.finditer(block):
        cells = [strip_html(x.group(1)) for x in cell_re.finditer(rr.group(1))]
        if first:
            first = False
            continue
        if not cells:
            continue
        # 取前 10 格（不足補空）
        row = (cells + [""] * 10)[:10]
        out.append(row)
    return out


def parse_table_generic(table_html: str, is_zh: bool) -> List[List[str]]:
    header_tr_m = re.search(r"<tr[^>]*>[\s\S]*?<\/tr>", table_html, re.I)
    header_tr = header_tr_m.group(0) if header_tr_m else ""
    ths = [strip_html(m.group(1)) for m in re.finditer(r"<t[hd][^>]*>([\s\S]*?)<\/t[hd]>", header_tr, re.I)]
    idx = index_by_header(ths)

    body_html = table_html.replace(header_tr, "")
    row_re = re.compile(r"<tr[^>]*>([\s\S]*?)<\/tr>", re.I)
    cell_re = re.compile(r"<t[dh][^>]*>([\s\S]*?)<\/t[dh]>", re.I)

    out: List[List[str]] = []
    if not idx:
        # 用位置式後備
        first_data_tr_m = re.search(r"<tr[^>]*>([\s\S]*?)<\/tr>", body_html, re.I)
        td_cnt = len(re.findall(r"<t[dh][^>]*>", first_data_tr_m.group(0), re.I)) if first_data_tr_m else 0
        idx = guess_index(td_cnt, is_zh)

    header_words = ['馬匹','馬名','近績','絲衣','負磅','騎師','檔位','練馬師','評分','體重','配備',
                    'Owner','Jockey','Horse','Draw','Rtg','Horse Wt.']

    for r in row_re.finditer(body_html):
        tr = r.group(1)
        if re.search(r"th>|合共|場序|Race\s*\d+", tr, re.I):
            continue
        cells = [strip_html(c.group(1)) for c in cell_re.finditer(tr)]
        if not cells:
            continue
        joined = "|".join(cells)
        hits = sum(1 for w in header_words if w in joined)
        if hits >= 3:
            continue

        def get(key_zh: str, key_en: str) -> str:
            k = key_zh if is_zh else key_en
            i = idx.get(k, None)
            if i is None:
                return ""
            return cells[i] if i < len(cells) else ""

        # 騎師 + 可能超磅 拆括號
        jockey_raw = get('騎師', 'Jockey')
        over_wt = ""
        m_over = re.search(r"\(([-+]?\d+)\)", jockey_raw)
        if m_over:
            over_wt = m_over.group(1)
            jockey = jockey_raw.replace(m_over.group(0), "").strip()
        else:
            jockey = jockey_raw
            over_wt = get('可能超磅', 'Over Wt.')

        row = [
            get('序號', 'No'),
            get('6次近績', 'Last 6 Runs') or get('近績', 'Last 6 Runs'),
            get('馬名', 'Horse'),
            get('烙號', 'Brand No.') or get('編號', 'Brand No.'),
            get('負磅', 'Wt'),
            jockey,
            over_wt,
            get('檔位', 'Draw') or get('檔', 'Draw'),
            get('練馬師', 'Trainer'),
            get('評分', 'Rtg'),
            get('評分+/-', 'Rtg+/-') or get('評分變動', 'Rtg+/-'),
            get('排位體重', 'Horse Wt.') or get('體重', 'Horse Wt.'),
            get('排位體重+/-', 'Wt+/-') or get('體重增減', 'Wt+/-'),
            get('最佳時間', 'Best Time'),
            get('馬齡', 'Age'),
            get('性別', 'Sex'),
            get('今季獎金', 'Season Stakes'),
            get('優先參賽次序', 'Priority'),
            get('上賽距今日數', 'Days Since Last Run'),
            get('配備', 'Gear'),
            get('馬主', 'Owner'),
            get('父系', 'Sire'),
            get('母系', 'Dam'),
            get('進口類別', 'Import')
        ]
        out.append(row)
    return out


def parse_chinese_starters(html: str) -> Tuple[List[List[str]], List[List[str]]]:
    compact = re.sub(r"\s{2,}", " ", re.sub(r"\r?\n+", " ", html))
    table_html = pick_starter_table(compact)
    if table_html:
        rows = parse_table_generic(table_html, is_zh=True)
        reserves = parse_reserves_from_chinese(compact)
        return rows, reserves

    # 回退英文頁
    m = re.search(r"RaceDate=([^&\"]+)[^>]*RaceNo=(\d+)[^>]*Racecourse=(ST|HV)", compact, re.I)
    if m:
        date_q, race_no, course = m.group(1), m.group(2), m.group(3)
        with requests.Session() as s:
            eng = fetch_with_cookie(s, f"https://racing.hkjc.com/racing/information/English/Racing/RaceCard.aspx?RaceDate={date_q}&RaceNo={race_no}&Racecourse={course}")
        eng_c = re.sub(r"\s{2,}", " ", re.sub(r"\r?\n+", " ", eng))
        eng_table = pick_starter_table(eng_c)
        if eng_table:
            rows = parse_table_generic(eng_table, is_zh=False)
            return rows, []
    return [], []


def parse_race_title_short(html: str) -> str:
    title = parse_race_meta(html).get("title", "")
    # e.g. "Race 1 - XXX  (Something)" → 取 "XXX"
    t = re.sub(r"^Race\s*\d+\s*-\s*", "", title, flags=re.I)
    t = t.split("  ")[0].strip() if t else ""
    return t or "Race"


def write_csv(path: Path, header: List[str], rows: List[List[str]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            # 保證欄數一致
            if len(r) < len(header):
                r = r + [""] * (len(header) - len(r))
            elif len(r) > len(header):
                r = r[:len(header)]
            w.writerow(r)


# ------------------------- 可選：寫入 MySQL（示例） -------------------------
def mysql_insert_example(rows: List[List[str]], race_no: int, race_date: str, racecourse: str,
                         mysql_conf: Dict[str, Any]):
    """
    將 rows 寫入你自訂的 table（示例）
    - 強烈建議你把欄位對應到你的正式 schema，再做 INSERT ... ON DUPLICATE KEY UPDATE
    - 這裡只給出範例：把所有欄位以 TEXT 存到一張 staging 表
    """
    import mysql.connector  # 延遲載入，避免沒裝套件也能跑 CSV 輸出

    ddl = """
    CREATE TABLE IF NOT EXISTS hkjc_racecard_staging (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      race_date VARCHAR(16),
      racecourse VARCHAR(4),
      race_no INT,
      seq_no VARCHAR(16),
      last6 VARCHAR(64),
      horse_name VARCHAR(128),
      brand_no VARCHAR(32),
      wt VARCHAR(32),
      jockey VARCHAR(128),
      over_wt VARCHAR(16),
      draw_no VARCHAR(16),
      trainer VARCHAR(128),
      rating VARCHAR(16),
      rating_diff VARCHAR(16),
      dec_wt VARCHAR(32),
      dec_wt_diff VARCHAR(16),
      best_time VARCHAR(64),
      age VARCHAR(8),
      sex VARCHAR(8),
      season_stakes VARCHAR(64),
      priority VARCHAR(32),
      days_since VARCHAR(16),
      gear VARCHAR(64),
      owner VARCHAR(128),
      sire VARCHAR(128),
      dam VARCHAR(128),
      import_cat VARCHAR(64),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    conn = mysql.connector.connect(**mysql_conf)
    cur = conn.cursor()
    cur.execute(ddl)

    sql = """
    INSERT INTO hkjc_racecard_staging
    (race_date, racecourse, race_no, seq_no, last6, horse_name, brand_no, wt, jockey, over_wt, draw_no, trainer,
     rating, rating_diff, dec_wt, dec_wt_diff, best_time, age, sex, season_stakes, priority, days_since, gear, owner,
     sire, dam, import_cat)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    for r in rows:
        data = (
            race_date, racecourse, race_no,
            (r[0] if len(r) > 0 else None),
            (r[1] if len(r) > 1 else None),
            (r[2] if len(r) > 2 else None),
            (r[3] if len(r) > 3 else None),
            (r[4] if len(r) > 4 else None),
            (r[5] if len(r) > 5 else None),
            (r[6] if len(r) > 6 else None),
            (r[7] if len(r) > 7 else None),
            (r[8] if len(r) > 8 else None),
            (r[9] if len(r) > 9 else None),
            (r[10] if len(r) > 10 else None),
            (r[11] if len(r) > 11 else None),
            (r[12] if len(r) > 12 else None),
            (r[13] if len(r) > 13 else None),
            (r[14] if len(r) > 14 else None),
            (r[15] if len(r) > 15 else None),
            (r[16] if len(r) > 16 else None),
            (r[17] if len(r) > 17 else None),
            (r[18] if len(r) > 18 else None),
            (r[19] if len(r) > 19 else None),
            (r[20] if len(r) > 20 else None),
            (r[21] if len(r) > 21 else None),
            (r[22] if len(r) > 22 else None),
            (r[23] if len(r) > 23 else None),
        )
        cur.execute(sql, data)
    conn.commit()
    cur.close()
    conn.close()


# ------------------------- 主流程 -------------------------
def run(date_str: str, course: str, max_races: int, out_dir: Path,
        mysql_conf: Optional[Dict[str, Any]] = None):
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = []

    with requests.Session() as session:
        for race_no in range(1, max_races + 1):
            html = fetch_race_html(session, date_str, race_no, course, lang="Chinese")
            if not html or not has_starter_table(html):
                # 第一場都抓不到 → 代表非賽日/未出排位 → 結束
                if race_no == 1:
                    print(f"找不到賽事：{date_str} {course}（可能未排位或日期/場地不符）")
                break

            compact = re.sub(r"\s{2,}", " ", re.sub(r"\r?\n+", " ", html))
            rows, reserves = parse_chinese_starters(html)

            # 命名：R{no}_{讓賽名}
            short = parse_race_title_short(html)
            pretty = sanitize_filename(f"R{race_no}_{short or f'Race{race_no}'}")
            if not rows:
                print(f"R{race_no}: 未能解析出馬表，略過")
                continue

            # 輸出 CSV
            csv_path = out_dir / f"{pretty}.csv"
            write_csv(csv_path, HEADER, rows)

            if reserves:
                res_path = out_dir / f"R{race_no}_Reserves.csv"
                write_csv(res_path, RES_HEADER, reserves)

            # 可選：寫入 MySQL
            if mysql_conf:
                try:
                    mysql_insert_example(rows, race_no, date_str, course, mysql_conf)
                except Exception as e:
                    print(f"[MySQL] R{race_no} 寫入失敗：{e}")

            summary.append((race_no, len(rows), len(reserves)))
            print(f"完成 R{race_no}: {len(rows)} 行，後備 {len(reserves)} 行 → {csv_path.name}")
            time.sleep(0.4)  # 禮貌性延遲，避免過快

    print("\n=== 總結 ===")
    for race_no, n, r in summary:
        print(f"R{race_no}: {n} + reserves {r}")
    if not summary:
        print("本次沒有可用場次。")


def main():
    ap = argparse.ArgumentParser(description="HKJC RaceCard 全日場次爬蟲（Python版）")
    ap.add_argument("--date", default=DEFAULT_DATE, help="賽日 YYYY/MM/DD（預設：%(default)s）")
    ap.add_argument("--course", default=DEFAULT_COURSE, choices=["ST", "HV"], help="場地 ST/HV（預設：%(default)s）")
    ap.add_argument("--max-races", type=int, default=DEFAULT_MAX_RACES, help="最大場次（預設：%(default)s）")
    ap.add_argument("--out", default="./out", help="輸出資料夾（預設：%(default)s）")
    # MySQL（可選）
    ap.add_argument("--mysql-host", help="MySQL host")
    ap.add_argument("--mysql-port", type=int, default=3306, help="MySQL port（預設：%(default)s）")
    ap.add_argument("--mysql-user", help="MySQL user")
    ap.add_argument("--mysql-pass", help="MySQL password")
    ap.add_argument("--mysql-db", help="MySQL database")
    args = ap.parse_args()

    mysql_conf = None
    if args.mysql_host and args.mysql_user and args.mysql_db is not None:
        mysql_conf = dict(
            host=args.mysql_host,
            port=args.mysql_port,
            user=args.mysql_user,
            password=args.mysql_pass or "",
            database=args.mysql_db
        )

    run(
        date_str=args.date,
        course=args.course,
        max_races=args.max_races,
        out_dir=Path(args.out),
        mysql_conf=mysql_conf
    )


if __name__ == "__main__":
    main()
