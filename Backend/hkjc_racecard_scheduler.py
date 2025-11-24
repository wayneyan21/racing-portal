# -*- coding: utf-8 -*-
"""
排位表 Scheduler
- 依據 race_meetings.draw_date 中午 12:00（HKT）開始爬排位表
- 會 check racecard_races 有冇資料，避免重覆爬
- 俾 Render Cron Job / master_worker 用

建議 Schedule（Cron）：
  每 10 分鐘行一次都夠（例如：*/10 * * * *）
"""

from datetime import datetime, timedelta, time, timezone

from hkjc_odds_graphql import get_conn
from crawl_racecard_simple import fetch_and_store_racecard

HKT = timezone(timedelta(hours=8))


def fetch_upcoming_meetings():
    """
    由 race_meetings 取出最近幾日賽事
    只需欄位：race_date, draw_date, venue_code
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
            SELECT race_date, draw_date, venue_code
            FROM race_meetings
            WHERE race_date >= CURDATE() - INTERVAL 1 DAY
              AND race_date <= CURDATE() + INTERVAL 7 DAY
              AND draw_date IS NOT NULL
            ORDER BY race_date, venue_code
            """
            cur.execute(sql)
            rows = cur.fetchall()
        return rows
    finally:
        conn.close()


def meeting_already_has_racecard(race_date, venue_code) -> bool:
    """
    檢查 racecard_races 有冇已經入咗呢個 meeting 嘅排位表
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
            SELECT COUNT(*) AS cnt
            FROM racecard_races
            WHERE race_date = %s
              AND venue_code = %s
            """
            cur.execute(sql, (race_date, venue_code))
            row = cur.fetchone()
            return (row["cnt"] or 0) > 0
    finally:
        conn.close()


def should_fetch_for_meeting(now_hkt: datetime, race_date, draw_date) -> bool:
    """
    根據 draw_date 12:00 HKT 決定要唔要爬呢個 meeting
    """
    if draw_date is None:
        return False

    draw_dt = datetime.combine(draw_date, time(12, 0), tzinfo=HKT)

    # 未到 draw 日中午 12:00 → 唔爬
    if now_hkt < draw_dt:
        return False

    # 如果你想再保守，可以限制只係 draw_date 後 2 日內先爬：
    # if now_hkt > draw_dt + timedelta(days=2):
    #     return False

    return True


def run_racecard_scheduler():
    now_hkt = datetime.now(tz=HKT)
    print(f"⏱  Racecard Scheduler at {now_hkt.isoformat()}")

    meetings = fetch_upcoming_meetings()
    if not meetings:
        print("⚠️ race_meetings 冇未來賽事")
        return

    for row in meetings:
        race_date = row["race_date"]
        draw_date = row["draw_date"]
        venue_code = row["venue_code"]

        if not should_fetch_for_meeting(now_hkt, race_date, draw_date):
            continue

        if meeting_already_has_racecard(race_date, venue_code):
            # 已經有排位，唔洗再爬
            continue

        race_date_str = race_date.strftime("%Y-%m-%d")
        print(f"🚀 觸發排位爬蟲: race_date={race_date_str}, venue={venue_code}, draw_date={draw_date}")

        try:
            fetch_and_store_racecard(race_date_str, venue_code)
            print(f"✅ 排位表更新完成: {race_date_str} {venue_code}")
        except Exception as e:
            print(f"❌ 排位表更新失敗: {race_date_str} {venue_code} - {e}")


def main():
    run_racecard_scheduler()


if __name__ == "__main__":
    main()
