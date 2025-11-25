# -*- coding: utf-8 -*-
"""
賠率 Scheduler
- 根據 racecard_races 裏面的 race_time（開跑時間）+ race_date
- 開跑前一日 13:00 開始關注
- 距離開跑 > 30 分鐘：每小時一次
- 距離開跑 -5 分鐘 至 +30 分鐘：每分鐘一次
- 開跑後 > 5 分鐘：唔再更新

依賴：
- hkjc_odds_graphql.get_conn
- hkjc_odds_graphql.fetch_odds
- hkjc_odds_graphql.build_odds_map
- hkjc_odds_graphql.update_mysql_odds
"""

from datetime import datetime, timedelta, time, timezone

from hkjc_odds_graphql import (
    get_conn,
    fetch_odds,
    build_odds_map,
    update_mysql_odds,
)

HKT = timezone(timedelta(hours=8))


def fetch_upcoming_races():
    """
    由 racecard_races 取出最近幾日賽事
    需要欄位：race_date, race_time, race_no, venue_code
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
            SELECT race_date, race_time, race_no, venue_code
            FROM racecard_races
            WHERE race_date >= CURDATE() - INTERVAL 1 DAY
              AND race_date <= CURDATE() + INTERVAL 1 DAY
            ORDER BY race_date, venue_code, race_no
            """
            cur.execute(sql)
            rows = cur.fetchall()
        return rows
    finally:
        conn.close()


def should_fetch_for_race(now_hkt: datetime, race_row: dict) -> bool:
    race_date = race_row["race_date"]       # DATE
    race_time_val = race_row["race_time"]   # TIME (PyMySQL → timedelta)
    race_no = race_row["race_no"]
    venue_code = race_row["venue_code"]

    # --- 將 race_date + race_time 變成 HKT datetime ---
    if isinstance(race_time_val, timedelta):
        # MySQL TIME 由 PyMySQL 變成 timedelta，轉返去 hour/minute
        total_sec = int(race_time_val.total_seconds())
        hh = (total_sec // 3600) % 24
        mm = (total_sec % 3600) // 60
        race_dt = datetime.combine(race_date, time(hour=hh, minute=mm), tzinfo=HKT)

    elif isinstance(race_time_val, time):
        race_dt = datetime.combine(race_date, race_time_val, tzinfo=HKT)

    else:
        # 若 DB 給 DATETIME / 字串 等其他型態
        race_dt = race_time_val
        if isinstance(race_dt, str):
            # 預期格式 'HH:MM:SS' 或 'HH:MM'
            try:
                hh, mm = race_dt.split(":")[:2]
                race_dt = datetime.combine(
                    race_date,
                    time(hour=int(hh), minute=int(mm)),
                    tzinfo=HKT,
                )
            except Exception:
                # 撞到奇怪格式就直接唔 fetch，避免爆錯
                return False
        else:
            if race_dt.tzinfo is None:
                race_dt = race_dt.replace(tzinfo=HKT)

    # --- 關注時間邏輯 ---
    # 開始關注時間：比賽日的前一日 13:00 (HKT)
    start_track = datetime.combine(
        race_date - timedelta(days=1),
        time(hour=13, minute=0),
        tzinfo=HKT,
    )

    if now_hkt < start_track:
        # 未到開始關注賠率的時間
        return False

    delta_sec = (race_dt - now_hkt).total_seconds()

    # 比賽結束：開跑後 5 分鐘就唔再更新
    if delta_sec < -5 * 60:
        return False

    # 距離開跑超過 30 分鐘：每小時一次（例如 minute == 0）
    if delta_sec > 30 * 60:
        return now_hkt.minute == 0

    # [-5 分, +30 分] 之間：每分鐘一次
    return True


def run_odds_scheduler():
    now_hkt = datetime.now(tz=HKT)
    print(f"⏱  Odds Scheduler at {now_hkt.isoformat()}")

    races = fetch_upcoming_races()
    if not races:
        print("⚠️ racecard_races 冇未來賽事")
        return

    for row in races:
        race_date = row["race_date"]
        venue_code = row["venue_code"]
        race_no = row["race_no"]

        if not should_fetch_for_race(now_hkt, row):
            continue

        race_date_str = race_date.strftime("%Y-%m-%d")
        print(f"🚀 更新賠率: {race_date_str} {venue_code} 第 {race_no} 場")

        try:
            data = fetch_odds(
                date_str=race_date_str,
                venue_code=venue_code,
                race_no=race_no,
                odds_types=["WIN", "PLA"],
            )
            odds_map, _ = build_odds_map(data)
            update_mysql_odds(race_date_str, venue_code, race_no, odds_map)
        except Exception as e:
            print(f"❌ 更新 {race_date_str} {venue_code} R{race_no} 失敗: {e}")


def main():
    run_odds_scheduler()


if __name__ == "__main__":
    main()
