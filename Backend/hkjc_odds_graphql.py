# -*- coding: utf-8 -*-
"""
HKJC GraphQL — pmPools 賠率 (WIN / PLA) + 寫入 MySQL (最終版，配合 racecard_entries 結構)

- 取某日某場地某場 WIN / PLA 賠率
- oddsNodes -> {horse_no: {'WIN': x, 'PLA': y}}
- 更新 racecard_entries.win_odds / pla_odds / last_odds_update
  （依賴 UNIQUE KEY (race_date, race_no, horse_no)）
- 如賠率有變，插入 race_odds_snapshots

用法（手動測試）：
    cd Backend
    python3 hkjc_odds_graphql.py
"""

import os
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
import pymysql

HKJC_GRAPHQL_URL = "https://info.cld.hkjc.com/graphql/base/"

ODDS_QUERY = """
query racing($date: String, $venueCode: String, $oddsTypes: [OddsType], $raceNo: Int) {
  raceMeetings(date: $date, venueCode: $venueCode) {
    pmPools(oddsTypes: $oddsTypes, raceNo: $raceNo) {
      id
      status
      sellStatus
      oddsType
      lastUpdateTime
      guarantee
      minTicketCost
      name_en
      name_ch
      leg {
        number
        races
      }
      cWinSelections {
        composite
        name_ch
        name_en
        starters
      }
      oddsNodes {
        combString
        oddsValue
        hotFavourite
        oddsDropValue
        bankerOdds {
          combString
          oddsValue
        }
      }
    }
  }
}
""".strip()

# HKT 時區（之後如果要時間 stamp 用得著）
HKT = timezone(timedelta(hours=8))

# ---------- DB 連線設定（跟 server.js 一樣用 env） ----------

def get_db_cfg():
    """
    同 server.js 一樣，由環境變數攞 MySQL 設定：
      DB_HOST / DB_PORT / DB_USER / DB_PASS / DB_NAME
    """
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASS", ""),
        "database": os.getenv("DB_NAME", "hkjc_db"),
    }


def get_conn():
    cfg = get_db_cfg()

    # optional：debug 用，方便睇 Render log（唔會 print 密碼）
    print("DB config =>", {
        "host": cfg["host"],
        "port": cfg["port"],
        "user": cfg["user"],
        "database": cfg["database"],
    })

    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )



# ---------- GraphQL 取數據 ----------

def fetch_odds(date_str: str, venue_code: str, race_no: int,
               odds_types=None) -> dict:
    if odds_types is None:
        odds_types = ["WIN", "PLA"]

    payload = {
        "operationName": "racing",
        "variables": {
            "date": date_str,
            "venueCode": venue_code,
            "raceNo": race_no,
            "oddsTypes": odds_types,
        },
        "query": ODDS_QUERY,
    }

    headers = {"Content-Type": "application/json"}

    resp = requests.post(
        HKJC_GRAPHQL_URL,
        json=payload,
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def save_raw_json(data: dict, date_str: str, venue_code: str, race_no: int):
    out_dir = Path("../public/graphql_raw")
    out_dir.mkdir(exist_ok=True)
    fn = out_dir / f"odds_{date_str}_{venue_code}_R{race_no}.json"
    with fn.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 已儲存原始 odds JSON: {fn}")


# ---------- oddsNodes -> odds_map ----------

def build_odds_map(data: dict):
    """
    回傳:
      odds_map = { 1: {'WIN': 7.8, 'PLA': 3.0}, 2: {...}, ... }
      meeting  = data.data.raceMeetings[0] 或 None
    """
    meetings = data.get("data", {}).get("raceMeetings", [])
    if not meetings:
        return {}, None

    meeting = meetings[0]
    pools = meeting.get("pmPools") or []
    odds_map = {}

    for pool in pools:
        odds_type = pool.get("oddsType")  # 'WIN' / 'PLA'
        if odds_type not in ("WIN", "PLA"):
            continue

        for node in pool.get("oddsNodes") or []:
            comb = node.get("combString")  # e.g. '01'
            odds_val = node.get("oddsValue")
            if comb is None:
                continue

            comb_str = str(comb).lstrip("0") or "0"
            if not comb_str.isdigit():
                continue
            horse_no = int(comb_str)

            odds_map.setdefault(horse_no, {})[odds_type] = odds_val

    return odds_map, meeting


# ---------- 寫入 MySQL：最新 + 歷史 ----------

def update_mysql_odds(date_str: str, venue_code: str, race_no: int,
                      odds_map: dict):
    """
    odds_map: {horse_no: {'WIN': x, 'PLA': y}}

    - 更新 racecard_entries.win_odds / pla_odds / last_odds_update
      依賴 UNIQUE KEY (race_date, race_no, horse_no)
    - 如 odds 有變，插入一行到 race_odds_snapshots
    """
    if not odds_map:
        print("⚠️ odds_map 為空，無需寫入 DB")
        return

    now_ts = datetime.now(tz=HKT).replace(tzinfo=None)  # 存 DATETIME，不帶 tz
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            updated_latest = 0
            inserted_snapshots = 0

            for horse_no, odds_by_type in odds_map.items():
                win_odds = odds_by_type.get("WIN")
                pla_odds = odds_by_type.get("PLA")

                # 1) 最新賠率 -> racecard_entries
                sql_latest = """
                INSERT INTO racecard_entries
                  (race_date, race_no, horse_no,
                   win_odds, pla_odds, last_odds_update)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  win_odds         = VALUES(win_odds),
                  pla_odds         = VALUES(pla_odds),
                  last_odds_update = VALUES(last_odds_update)
                """
                cur.execute(sql_latest, (
                    date_str,
                    race_no,
                    horse_no,
                    win_odds,
                    pla_odds,
                    now_ts,
                ))
                updated_latest += 1

                # 2) 歷史 snapshot — WIN / PLA 各自檢查一次
                for odds_type, odds_val in odds_by_type.items():
                    if odds_val is None:
                        continue

                    sql_last = """
                    SELECT odds
                    FROM race_odds_snapshots
                    WHERE race_date = %s AND venue_code = %s
                      AND race_no = %s AND horse_no = %s
                      AND odds_type = %s
                    ORDER BY snapshot_ts DESC
                    LIMIT 1
                    """
                    cur.execute(sql_last, (
                        date_str,
                        venue_code,
                        race_no,
                        horse_no,
                        odds_type,
                    ))
                    row = cur.fetchone()
                    last_odds = float(row["odds"]) if row and row["odds"] is not None else None
                    this_odds = float(odds_val)

                    if last_odds is None or this_odds != last_odds:
                        sql_snap = """
                        INSERT INTO race_odds_snapshots
                          (race_date, venue_code, race_no,
                           horse_no, odds_type, odds, snapshot_ts)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        """
                        cur.execute(sql_snap, (
                            date_str,
                            venue_code,
                            race_no,
                            horse_no,
                            odds_type,
                            odds_val,
                            now_ts,
                        ))
                        inserted_snapshots += 1

        conn.commit()
        print(f"✅ 最新賠率已更新 {updated_latest} 匹馬，新增 snapshot {inserted_snapshots} 行")
    finally:
        conn.close()


# ---------- console 摘要（方便你睇） ----------

def print_odds_summary(data: dict, race_no: int):
    meetings = data.get("data", {}).get("raceMeetings", [])
    if not meetings:
        print("⚠️ 沒有 raceMeetings 資料（可能冇賽事 / 參數錯）")
        return

    meeting = meetings[0]
    pools = meeting.get("pmPools") or []

    date = meeting.get("date")
    venue = meeting.get("venueCode")
    print(f"\n=== 賠率摘要：{date} {venue} 第 {race_no} 場 ===\n")

    for pool in pools:
        odds_type = pool.get("oddsType")
        name_ch = pool.get("name_ch")
        name_en = pool.get("name_en")
        last_update = pool.get("lastUpdateTime")
        status = pool.get("status")
        sell_status = pool.get("sellStatus")

        print(f"【Pool】{odds_type}  {name_ch} / {name_en}")
        print(f"  狀態: {status}  售票: {sell_status}  最後更新: {last_update}")

        nodes = pool.get("oddsNodes") or []
        if not nodes:
            print("  （暫時未有 oddsNodes）\n")
            continue

        for node in nodes:
            comb = node.get("combString")
            value = node.get("oddsValue")
            drop = node.get("oddsDropValue")
            hot = node.get("hotFavourite")

            comb_str = str(comb).rjust(2, "0")
            hot_mark = "🔥" if hot else ""
            print(f"    {odds_type} {comb_str}: {value:<6}  變化: {drop}  {hot_mark}")
        print("")


# ---------- main（手動測試入口） ----------
def main():
    date_str = input("輸入日期 (YYYY-MM-DD): ").strip()
    venue_code = input("輸入場地代碼 (HV / ST): ").strip().upper()
    race_no = int(input("輸入場次 (整數): ").strip())

    print(f"\n🚀 正在向 HKJC GraphQL 取賠率: date={date_str}, venue={venue_code}, race={race_no}")
    try:
        data = fetch_odds(date_str, venue_code, race_no,
                          odds_types=["WIN", "PLA"])
    except Exception as e:
        print("❌ 請求失敗：", e)
        return

    save_raw_json(data, date_str, venue_code, race_no)
    print_odds_summary(data, race_no)

    odds_map, _ = build_odds_map(data)
    update_mysql_odds(date_str, venue_code, race_no, odds_map)


if __name__ == "__main__":
    main()
