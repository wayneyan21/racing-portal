# master_scheduler.py
# -*- coding: utf-8 -*-

import subprocess
import sys
import datetime as dt
from zoneinfo import ZoneInfo

HKT = ZoneInfo("Asia/Hong_Kong")

def run_cmd(label, script_name):
    """幫手 call 其他 Python scheduler script"""
    now = dt.datetime.now(tz=HKT).isoformat()
    print(f"[{now}] ▶ {label} 開始 ({script_name})")

    try:
        # 用同一個 Python 去 run 其他檔案
        subprocess.run(
            [sys.executable, script_name],
            check=True,
        )
        print(f"[{label}] ✅ 完成")
    except subprocess.CalledProcessError as e:
        print(f"[{label}] ❌ 失敗，exit code = {e.returncode}")
    except Exception as e:
        print(f"[{label}] ❌ 例外：{e}")

def main():
    now = dt.datetime.now(tz=HKT)
    print(f"🕒 master_scheduler at {now.isoformat()} (HKT)")

    # 這裡唔駛再理時間，交俾各自 scheduler 自己決定做唔做嘢
    run_cmd("Racecard Scheduler", "hkjc_racecard_scheduler.py")
    run_cmd("Odds Scheduler", "hkjc_odds_scheduler.py")

    print("master_scheduler ✅ 任務檢查完畢，準備結束")

if __name__ == "__main__":
    main()
