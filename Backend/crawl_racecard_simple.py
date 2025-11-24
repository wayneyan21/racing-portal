# -*- coding: utf-8 -*-
"""
HKJC RaceCard 全日排位表（Selenium 擴充版）
- 自動偵測賽日/場地
- 勾選 父系/母系/進口類別 → 重新整理
- 解析「賽事層(meta)」＋「馬匹層(entries)」
- 產生開賽時間（Local/HKT/UTC）
- 支援：CSV 輸出、MySQL 入庫（racecard_races / racecard_entries）

用法：
  python crawl_racecard_simple.py --date 2025-10-22 --course HV --mysql \
    --mysql-host 127.0.0.1 --mysql-user root --mysql-pass Aa40404040 --mysql-db hkjc
"""
import re
import csv
import json
import time
import argparse
import datetime as _dt
import os

# 如果你本地用 .env，可以裝 python-dotenv：
#   pip install python-dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()  # 會自動讀專案根目錄的 .env（如果有）
except ImportError:
    # 沒有安裝 python-dotenv 都冇問題，在 Render 會直接用環境變數
    pass

from typing import List, Dict, Any, Tuple, Optional

from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo  # Python 3.9+
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------- 網址與路徑 ----------
BASE = "https://racing.hkjc.com"
DEFAULT_RC = f"{BASE}/racing/information/Chinese/racing/RaceCard.aspx"
ZH_PATHS = [
    "/racing/information/Chinese/Racing/RaceCard.aspx",
    "/racing/information/Chinese/racing/RaceCard.aspx",
]
EN_PATHS = [
    "/racing/information/English/Racing/RaceCard.aspx",
    "/racing/information/English/racing/RaceCard.aspx",
]

# 欄位次序（會 map 成具名 dict）
WANTED_COLUMNS = [
    '馬匹編號','6次近績','綵衣','馬名','烙號','負磅','騎師','檔位','練馬師',
    '評分','評分+/-','排位體重','排位體重+/-','馬齡','分齡讓磅','性別',
    '今季獎金','優先參賽次序','上賽距今日數','配備','馬主','父系','母系','進口類別'
]

TIME_RE = re.compile(r"(?<!\d)(\d{1,2}:\d{2})(?!\d)")
SURF_WORDS = ["草地","全天候","全天侯","AWT","泥地","All Weather","Turf"]
GOING_WORDS = ["好地","好至快","快地","黏地","軟地","濕軟",
               "Good","Good to Firm","Firm","Yielding","Soft","Good to Yielding","Sloppy"]

# ---------- 共用工具 ----------
def has_starter_table(html: str) -> bool:
    if not html: return False
    return bool(
        re.search(r"(馬號|馬名|排位體重|負磅|練馬師|騎師|出馬表)", html) or
        re.search(r"(Horse No\.|Last 6 Runs|Horse Wt\.|Trainer|Jockey|Draw|Rtg)", html, re.I)
    )

def strip_html(s: str) -> str:
    s = re.sub(r"(?i)<br\s*/?>", " / ", s or "")
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("&nbsp;"," ").replace("&amp;","&")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def compact_html(s: str) -> str:
    return re.sub(r"\s{2,}", " ", re.sub(r"\r?\n+", " ", s or ""))

def pick_starter_table(compact: str) -> str:
    tables = re.findall(r"(?is)<table[^>]*>[\s\S]*?</table>", compact)
    if not tables: return ""
    best, best_score = "", -1
    for t in tables:
        score = 0
        if re.search(r'class="[^"]*\bf_fs12\b', t, re.I): score += 40
        if re.search(r'class="[^"]*\btable_bd\b', t, re.I): score += 30
        if re.search(r'(近績|馬名|排位體重|負磅|練馬師|騎師)', t): score += 25
        if re.search(r'(Horse No\.|Last 6 Runs|Horse Wt\.|Trainer|Jockey|Draw|Rtg)', t, re.I): score += 25
        trc = len(re.findall(r"(?i)<tr", t)); score += min(trc * 1.1, 40)
        if score > best_score: best_score, best = score, t
    return best

def index_by_header(headers: List[str]) -> Dict[str, int]:
    idx = {}
    aliases = {
        '馬匹編號': ['序號','馬號','No','Number'],
        '6次近績': ['近績','Last 6 Runs','Form'],
        '綵衣': ['Silks','Colours','Colors','Jersey','絲衣','絲衫','絲褸'],
        '馬名': ['Horse','Horse Name','馬匹'],
        '烙號': ['Brand No.','Brand No','烙號/編號','編號'],
        '負磅': ['Handicap','Wt','Weight','負磅(磅)'],
        '騎師': ['Jockey','騎師(可能超磅)'],
        '檔位': ['Draw','Gate','Barrier','檔'],
        '練馬師': ['Trainer','Trainers','練者'],
        '評分': ['Rtg','Rating','評分(Rtg)'],
        '評分+/-': ['Rtg+/-','+/-','Rating+/-','評分變動'],
        '排位體重': ['Horse Wt.','Declared Wt.','體重','宣告體重'],
        '排位體重+/-': ['Wt+/-','體重增減'],
        '馬齡': ['Age'],
        '分齡讓磅': ['WFA','Weight For Age','Allow','Allowance'],
        '性別': ['Sex','G'],
        '今季獎金': ['Season Stakes','季內獎金'],
        '優先參賽次序': ['Priority','優先序'],
        '上賽距今日數': ['Days Since Last Run','DSLR','上次出賽日數'],
        '配備': ['Gear','Equip'],
        '馬主': ['Owner'],
        '父系': ['Sire'],
        '母系': ['Dam'],
        '進口類別': ['Import Cat.','Import','Import Category','來港類別'],
    }
    for i, h in enumerate(headers):
        clean = re.sub(r"\s+","",h).lower()
        for key, arr in aliases.items():
            for cand in [key] + arr:
                cc = re.sub(r"\s+","",cand).lower()
                if cc in clean or clean in cc:
                    idx.setdefault(key, i); break
    return idx

def _first_img_src(html_cell: str) -> str:
    m = re.search(r'<img[^>]+(?:data-src|src)="([^"]+)"', html_cell, re.I)
    if m:
        src = m.group(1)
        if src.startswith("http"): return src
        return BASE + ("" if src.startswith("/") else "/") + src
    m = re.search(r'<img[^>]+alt="([^"]+)"', html_cell, re.I)
    return strip_html(m.group(1)) if m else strip_html(html_cell)

def parse_table_generic(table_html: str) -> List[List[str]]:
    """強化版出馬表解析（容錯表頭、濾工具列/小表頭、補馬名/體重解析）"""
    if not table_html:
        return []
    trs = re.findall(r"(?is)<tr[^>]*>([\s\S]*?)</tr>", table_html)
    if not trs:
        return []

    # 1) 找最佳表頭行
    header_keywords = ['馬名','近績','騎師','練馬師','檔','檔位','Draw','Rtg','Horse Wt.']
    best_i, best_score = 0, -1
    for i, tr in enumerate(trs[:8]):
        th_count = len(re.findall(r"(?i)<th\b", tr))
        raw = strip_html(tr)
        hit = sum(1 for kw in header_keywords if kw in raw)
        score = hit * 10 + th_count
        if score > best_score:
            best_score, best_i = score, i

    def extract_headers(tr_html: str):
        return [strip_html(x) for x in re.findall(r"(?is)<t[hd][^>]*>([\s\S]*?)</t[hd]>", tr_html)]

    header_tr = trs[best_i]
    headers = extract_headers(header_tr)

    # 分組表頭 → 往下一行尋找具體葉子欄位
    leaf_needles = {'馬名','檔位','排位體重','評分','騎師','練馬師',
                    'Horse','Draw','Horse Wt.','Jockey','Trainer','Rtg'}
    if not any(h for h in headers if any(n in h for n in leaf_needles)):
        for j in range(best_i + 1, min(best_i + 4, len(trs))):
            cand = extract_headers(trs[j])
            if any(h for h in cand if any(n in h for n in leaf_needles)):
                header_tr = trs[j]
                headers = cand
                best_i = j
                break

    idx = index_by_header(headers)
    have_header = bool(idx)

    # 2) 用第一條數據行補猜欄位（檔位/練馬師/騎師）
    for i, tr in enumerate(trs):
        if i == best_i:
            continue
        cells_html = re.findall(r"(?is)<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr)
        cells_txt = [strip_html(x) for x in cells_html]
        if not cells_txt:
            continue
        joined = "|".join(cells_txt)
        # 小表頭/工具列
        if sum(w in joined for w in ['馬名','近績','騎師','練馬師','Draw','Horse','Jockey','Trainer','Rtg','Horse Wt.']) >= 3:
            continue

        used = set(idx.values())
        for j, cell in enumerate(cells_txt):
            if j in used:
                continue
            if '檔位' not in idx and re.fullmatch(r"\d{1,2}", cell or "") and 1 <= int(cell) <= 20:
                idx.setdefault('檔位', j); used.add(j); continue
            if '練馬師' not in idx and (('師' in cell) or (re.search(r"[一-龥]{2,}", cell) and not re.search(r"\d$", cell))):
                idx.setdefault('練馬師', j); used.add(j); continue
            if '騎師' not in idx and (re.search(r"[一-龥]{2,}", cell) or re.search(r"^[A-Z]\.[A-Z][a-z]+", cell)):
                idx.setdefault('騎師', j); used.add(j); continue
        break

    use_guess = not have_header and not idx
    out: List[List[str]] = []

    # 3) 逐行產出
    for i, tr in enumerate(trs):
        if i == best_i:
            continue
        cells_html = re.findall(r"(?is)<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr)
        if not cells_html:
            continue
        cells_txt = [strip_html(x) for x in cells_html]

        raw_tr_text = strip_html(tr)
        # 過濾非資料列
        if ("我的排位表" in raw_tr_text) or ("設定我的排位表" in raw_tr_text) \
           or re.search(r'(?i)<input[^>]+type="checkbox"', tr):
            continue
        if any(w in raw_tr_text for w in ("下載排位資料", "統計資料", "晨操片段", "即時賠率", "貼士指數", "天氣及跑道狀況")):
            continue
        non_data = sum(1 for c in cells_txt if (c or "").strip() == "")
        if non_data >= max(2, len(cells_txt) - 2):
            continue
        if any("我的排位表" in (c or "") for c in cells_txt[:2]):
            continue
        joined = "|".join(cells_txt)
        if sum(w in joined for w in ['馬名','近績','騎師','練馬師','Draw','Horse','Jockey','Trainer','Rtg','Horse Wt.']) >= 3:
            continue

        if use_guess:
            guess = {
                '馬匹編號': 0, '6次近績': 1, '綵衣': 2, '馬名': 3, '烙號': 4, '負磅': 5, '騎師': 6,
                '檔位': 7, '練馬師': 8, '評分': 9, '評分+/-': 10, '排位體重': 11, '排位體重+/-': 12,
                '馬齡': 13, '分齡讓磅': 14, '性別': 15, '今季獎金': 16, '優先參賽次序': 17,
                '上賽距今日數': 18, '配備': 19, '馬主': 20, '父系': 21, '母系': 22, '進口類別': 23
            }
            td_count = len(cells_html)
            idx_guess = {k: (v if v < td_count else -1) for k, v in guess.items()}

            def get_guess(key: str) -> str:
                j = idx_guess.get(key, -1)
                if j < 0 or j >= len(cells_html):
                    return ""
                return _first_img_src(cells_html[j]) if key == '綵衣' else cells_txt[j]

            out.append([get_guess(k) for k in WANTED_COLUMNS])
            continue

        def get_by_header(key: str) -> str:
            j = idx.get(key, -1)
            if j < 0 or j >= len(cells_html):
                return ""
            cell_html = cells_html[j]
            cell_txt = cells_txt[j]
            if key == '綵衣':
                return _first_img_src(cell_html)
            if key == '騎師':
                return re.sub(r"\((?:[-+]?\d+)\)", "", cell_txt).strip()
            if key == '馬名':
                m = re.search(r'<a[^>]+href="[^"]*Horse[^"]*"[^>]*>([\s\S]*?)</a>', cell_html, re.I)
                if m:
                    name = strip_html(m.group(1))
                    if name:
                        return name
                if (re.fullmatch(r"\d+", cell_txt or "") or len(cell_txt) <= 2):
                    mm = re.search(r'<a[^>]+href="[^"]*Horse[^"]*"[^>]*>([\s\S]*?)</a>', tr, re.I)
                    if mm:
                        alt = strip_html(mm.group(1))
                        if alt:
                            return alt
                return cell_txt
            if key in ('排位體重', '排位體重+/-'):
                m = re.search(r'(\d{2,4})\s*(?:\(\s*([+-]?\d+)\s*\))?', strip_html(cell_html)) or \
                    re.search(r'(\d{2,4})\s*(?:\(\s*([+-]?\d+)\s*\))?', cell_txt)
                if m:
                    wt = m.group(1) or ''
                    dlt = m.group(2) or ''
                    return wt if key == '排位體重' else dlt
                return cell_txt
            return cell_txt

        out.append([get_by_header(k) for k in WANTED_COLUMNS])

    return out

def parse_reserves_from_chinese(compact_html: str) -> List[List[str]]:
    blk = re.search(r"後備馬匹[\s\S]*?</table>", compact_html)
    if not blk: return []
    row_re = re.compile(r"(?is)<tr[^>]*>([\s\S]*?)</tr>")
    cell_re = re.compile(r"(?is)<t[dh][^>]*>([\s\S]*?)</t[dh]>")
    out=[]; first=True
    for m in row_re.finditer(blk.group(0)):
        cells = [strip_html(c) for c in cell_re.findall(m.group(1))]
        if first: first=False; continue
        if not cells: continue
        row = [(cells[i] if i < len(cells) else "") for i in range(10)]
        out.append(row)
    return out

# ---------- 開賽時間 + 賽事層 ----------
def extract_off_time_local(html: str) -> str:
    if not html:
        return ""
    for tag in ("h1", "h2"):
        m = re.search(fr"<{tag}[^>]*>([\s\S]*?)</{tag}>", html, re.I)
        if m:
            t = strip_html(m.group(1))
            m2 = TIME_RE.search(t)
            if m2:
                return m2.group(1)
    cut = re.split(r"設定我的排位表|My Race Card", html, flags=re.I)[0]
    t = strip_html(cut)
    m3 = TIME_RE.search(t)
    return m3.group(1) if m3 else ""

def compose_off_times(meeting_date_iso: str, hhmm: str) -> dict:
    if not (meeting_date_iso and hhmm):
        return {'off_time_local': '', 'off_time_hkt': '', 'off_time_utc': ''}
    dt_hkt = _dt.datetime.strptime(f"{meeting_date_iso} {hhmm}", "%Y-%m-%d %H:%M").replace(
        tzinfo=ZoneInfo("Asia/Hong_Kong")
    )
    return {
        'off_time_local': hhmm,
        'off_time_hkt': dt_hkt.isoformat(timespec="seconds"),
        'off_time_utc': dt_hkt.astimezone(ZoneInfo("UTC")).isoformat(timespec="seconds").replace("+00:00","Z")
    }

def parse_race_meta(html: str) -> Dict[str, Any]:
    m = re.search(r"<h1[^>]*>([\s\S]*?)</h1>", html, re.I)
    title = strip_html(m.group(1)) if m else ""
    return {"title": title}

def extract_race_details(html_zh: str, html_en: str, meeting_date_iso: str, venue_code: str) -> Dict[str, Any]:
    soup_zh = BeautifulSoup(html_zh, "lxml")
    text_zh = soup_zh.get_text(" ", strip=True)

    # race name zh：h1 的「第 n 場 - 名稱」右邊部份
    h1 = soup_zh.find("h1")
    race_name_zh = ""
    if h1:
        t = strip_html(str(h1))
        race_name_zh = re.sub(r"^第\s*\d+\s*場\s*[-–—]\s*", "", strip_html(t))

    # race name en（英頁 h1）
    race_name_en = ""
    if html_en:
        soup_en = BeautifulSoup(html_en, "lxml")
        h1e = soup_en.find("h1")
        if h1e:
            tt = strip_html(str(h1e))
            race_name_en = re.sub(r"^Race\s*\d+\s*[-–—]\s*", "", strip_html(tt))

    # surface / course line / distance
    surface = ""
    for w in SURF_WORDS:
        if w in text_zh:
            surface = "AWT" if ("AWT" in w or "全天" in w) else "草地"
            break

    m_line = re.search(r"[\"“]([ABC](?:\+\d)?)[\"”]\s*賽道", text_zh)
    course_line = m_line.group(1) if m_line else ""

    m_dist = re.search(r"(\d{3,4})\s*米", text_zh)
    distance_m = int(m_dist.group(1)) if m_dist else None

    # going（有時在其他區塊）
    going = ""
    for w in GOING_WORDS:
        if w in text_zh:
            going = w
            break

    # class / handicap
    class_text = ""
    m_cls = re.search(r"(第[一二三四五六七八九十]+班|Class\s*\d+|Group\s*\d+)", text_zh, re.I)
    if m_cls:
        class_text = m_cls.group(1)
    handicap = "讓賽" if ("讓賽" in race_name_zh or "Handicap" in race_name_en) else ""

    # 開賽時間（已在外面抽 local，再合成）
    off_local = extract_off_time_local(html_zh)
    off = compose_off_times(meeting_date_iso, off_local)

    return {
        "race_name_zh": race_name_zh,
        "race_name_en": race_name_en,
        "race_time_local": off["off_time_local"],
        "race_time_hkt": off["off_time_hkt"],
        "race_time_utc": off["off_time_utc"],
        "distance_m": distance_m,
        "surface": surface,           # 草地 / AWT
        "course_line": course_line,   # A / B / C / C+3 ...
        "going": going,
        "class_text": class_text,
        "handicap": handicap,
        "venue_code": venue_code      # ST / HV
    }

# ---------- 把單行出馬資料轉 dict（方便入 DB） ----------
def row_to_entry(row: List[str]) -> Dict[str, Any]:
    m = { WANTED_COLUMNS[i]: (row[i] if i < len(row) else "") for i in range(len(WANTED_COLUMNS)) }
    def to_int(s):
        try:
            return int(re.sub(r"[^\d-]+","", s))
        except:
            return None

    draw_val = m.get("檔位") or ""
    trainer_val = m.get("練馬師") or ""
    name_val = m.get("馬名") or ""

    # 名稱 sanity：只過濾「我的排位表」或純數字；兩個字的正常馬名保留
    norm = (name_val or "").replace(" ", "")
    if ("我的排位表" in norm) or re.fullmatch(r"\d+", norm or ""):
        name_val = ""

    # trainer 是 1~20 的純數字而 draw 空 → 視為 draw
    if (not draw_val) and re.fullmatch(r"\d{1,2}", trainer_val or "") and 1 <= int(trainer_val) <= 20:
        draw_val, trainer_val = trainer_val, ""
    # draw 有中文字/英文字樣而 trainer 空 → 互換
    if (not trainer_val) and re.search(r"[A-Za-z一-龥]", draw_val or "") and not re.fullmatch(r"\d{1,2}", draw_val or ""):
        draw_val, trainer_val = "", draw_val

    return {
        "horse_no": to_int(m.get("馬匹編號") or ""),
        "last6": m.get("6次近績") or "",
        "silks": m.get("綵衣") or "",
        "horse_name_zh": name_val,
        "brand": m.get("烙號") or "",
        "weight_lb": to_int(m.get("負磅") or ""),
        "jockey_zh": m.get("騎師") or "",
        "draw": to_int(draw_val or ""),
        "trainer_zh": trainer_val,
        "rating": to_int(m.get("評分") or ""),
        "rating_pm": m.get("評分+/-") or "",
        "declared_wt": to_int(m.get("排位體重") or ""),
        "declared_wt_pm": m.get("排位體重+/-") or "",
        "age": to_int(m.get("馬齡") or ""),
        "wfa": m.get("分齡讓磅") or "",
        "sex": m.get("性別") or "",
        "season_stakes": m.get("今季獎金") or "",
        "priority": m.get("優先參賽次序") or "",
        "days_since": m.get("上賽距今日數") or "",
        "gear": m.get("配備") or "",
        "owner": m.get("馬主") or "",
        "sire": m.get("父系") or "",
        "dam": m.get("母系") or "",
        "import_cat": m.get("進口類別") or "",
    }

# ---------- 自訂欄位（勾選） ----------
DESIRED_LABELS = ["父系", "母系", "進口類別"]

def _find_label_checkbox(driver, label_text):
    try:
        lab = driver.find_element(By.XPATH, f"//label[contains(normalize-space(.), '{label_text}')]")
        try:
            cid = lab.get_attribute("for")
            cb = driver.find_element(By.ID, cid) if cid else lab.find_element(By.XPATH, ".//input[@type='checkbox']")
            return cb
        except Exception:
            pass
    except Exception:
        pass
    try:
        cb = driver.find_element(
            By.XPATH,
            f"//td[.//text()[contains(., '{label_text}')]]//input[@type='checkbox']"
        )
        return cb
    except Exception:
        return None

def ensure_racecard_columns(driver, labels=DESIRED_LABELS, timeout=12):
    try:
        anchor = driver.find_element(By.XPATH, "//*[contains(normalize-space(.), '設定我的排位表')]")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", anchor)
    except Exception:
        pass
    try:
        for txt in ["按此關閉", "按此開啟", "按此關閉 ", "按此開啟 "]:
            elems = driver.find_elements(By.XPATH, f"//a[normalize-space()='{txt}']")
            if elems:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elems[0])
                elems[0].click()
                break
    except Exception:
        pass
    for name in labels:
        cb = _find_label_checkbox(driver, name)
        if not cb:
            continue
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", cb)
        if not cb.is_selected():
            try:
                cb.click()
            except Exception:
                driver.execute_script("arguments[0].checked = true;", cb)
    try:
        btns = driver.find_elements(By.XPATH, "//*[contains(normalize-space(.), '設定我的排位表')]/following::a[normalize-space()='重新整理'][1]")
        if not btns:
            btns = driver.find_elements(By.XPATH, "//a[normalize-space()='重新整理']")
        if btns:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btns[0])
            btns[0].click()
        else:
            b2 = driver.find_element(By.XPATH, "//button[normalize-space()='重新整理']")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b2)
            b2.click()
    except Exception:
        pass
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//table//th[contains(., '父系')] | //table//th[contains(., '母系')] | //table//th[contains(., '進口類別')]"
            ))
        )
    except Exception:
        WebDriverWait(driver, 3).until(
            lambda d: "父系" in d.page_source or "母系" in d.page_source or "進口類別" in d.page_source
        )

# ---------- Autodetect 賽日/場地 ----------
def autodetect_meeting(driver) -> Tuple[str, str]:
    driver.get(DEFAULT_RC)
    WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.5)
    html = driver.page_source

    for m in re.finditer(r'href="[^"]*RaceCard\.aspx\?([^"]+)"', html, re.I):
        qs = m.group(1)
        mdate = re.search(r'(?:RaceDate|RDate|racedate)=([^&"]+)', qs, re.I)
        mcourse = re.search(r'Racecourse=(ST|HV)', qs, re.I)
        if mdate and mcourse:
            date_raw = mdate.group(1)
            course = mcourse.group(1).upper()
            return date_raw.replace("-", "/"), course

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    date_str = None
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
    if m:
        y, mo, d = m.groups()
        date_str = f"{int(y):04d}/{int(mo):02d}/{int(d):02d}"
    course = None
    if "跑馬地" in text: course = "HV"
    if "沙田" in text:   course = course or "ST"
    if not date_str or not course:
        raise RuntimeError("無法自動偵測 RaceDate / Racecourse。")
    return date_str, course

# ---------- 單場抓取 ----------
def fetch_one_race_html(driver, date_str: str, race_no: int, course: str, wait_sec=15) -> str:
    def open_and_prepare(url: str) -> str:
        driver.get(url)
        try:
            WebDriverWait(driver, wait_sec).until(lambda d: 'table' in d.page_source.lower())
        except Exception:
            pass
        try:
            ensure_racecard_columns(driver, labels=DESIRED_LABELS)
        except Exception:
            pass
        return driver.page_source

    c = "HV" if str(course).upper()=="HV" else "ST"
    last_html = ""
    for p in ZH_PATHS:
        for k in ["RaceDate","RDate","racedate"]:
            url = f"{BASE}{p}?{k}={date_str}&RaceNo={race_no}&Racecourse={c}"
            html = open_and_prepare(url)
            last_html = html or last_html
            if has_starter_table(html):
                return html
    return last_html

def fetch_one_race_html_en(driver, date_str: str, race_no: int, course: str, wait_sec=8) -> str:
    c = "HV" if str(course).upper()=="HV" else "ST"
    for p in EN_PATHS:
        for k in ["RaceDate","RDate","racedate"]:
            url = f"{BASE}{p}?{k}={date_str}&RaceNo={race_no}&Racecourse={c}"
            driver.get(url)
            try:
                WebDriverWait(driver, wait_sec).until(lambda d: 'table' in d.page_source.lower())
            except Exception:
                pass
            return driver.page_source
    return ""

# ---------- 會期 ----------
def crawl_meeting(auto_date: Optional[str],
                  auto_course: Optional[str],
                  max_races: Optional[int],
                  headful: bool=False,
                  delay_between=0.35) -> Dict[str, Any]:
    opts = Options()
    if not headful:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--lang=zh-HK")
    opts.add_argument("--window-size=1280,2200")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

    with webdriver.Chrome(options=opts) as driver:
        driver.get(f"{BASE}/")
        WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(0.3)

        # 自動偵測（如無傳入）
        if not auto_date or not auto_course:
            date_str, course = autodetect_meeting(driver)
        else:
            date_str = auto_date.replace("-", "/")
            course = auto_course.upper()

        meeting = {
            "date": date_str.replace("/", "-"),
            "course": "沙田" if course == "ST" else ("跑馬地" if course == "HV" else ""),
            "venue_code": course,
            "races": []
        }

        # 場數自動：如沒指定 max_races，就嘗試最多 20 場；連續 2 場搵唔到就收手
        hard_cap = max_races if (max_races and max_races > 0) else 20
        consecutive_miss = 0
        last_ok_html = ""
        any_found = False

        for rn in range(1, hard_cap + 1):
            html = fetch_one_race_html(driver, date_str, rn, course)
            if not (html and has_starter_table(html)):
                consecutive_miss += 1
                if consecutive_miss >= 2:
                    break
                else:
                    continue

            # 有表：清零 miss 計數
            consecutive_miss = 0
            any_found = True
            last_ok_html = html

            # 英文頁（補英文賽名）
            html_en = fetch_one_race_html_en(driver, date_str, rn, course)

            compact = compact_html(html)
            meta_title = parse_race_meta(html)
            rows_raw = parse_table_generic(pick_starter_table(compact))
            reserves_raw = parse_reserves_from_chinese(compact)

            # 賽事層
            race_meta = extract_race_details(html, html_en, meeting["date"], course)

            # 逐行轉 dict，無馬名的跳過；亦會過濾「我的排位表」假行
            entries = []
            for r in rows_raw:
                e = row_to_entry(r)
                name = (e.get("horse_name_zh") or "").strip()
                if not re.search(r"[A-Za-z一-龥]", name):
                    continue
                if "我的排位表" in name:
                    continue
                entries.append(e)

            reserves = []
            for r in reserves_raw or []:
                e = row_to_entry(r)
                name = (e.get("horse_name_zh") or "").strip()
                if not re.search(r"[A-Za-z一-龥]", name):
                    continue
                reserves.append(e)

            # 如果呢場連一匹有效馬都無，就當作空場，唔加入
            if not entries and not reserves:
                continue

            meeting["races"].append({
                "race_no": rn,
                "title": meta_title.get("title", ""),
                "meta": race_meta,
                "entries": entries,
                "reserves": reserves
            })

            time.sleep(delay_between)

        # 用最後成功頁修正日期/場地中文
        if any_found and last_ok_html:
            soup = BeautifulSoup(last_ok_html, "lxml")
            text = soup.get_text(" ", strip=True)
            m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
            if m:
                y, mo, d = m.groups()
                meeting["date"] = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            if "跑馬地" in text:
                meeting["course"] = "跑馬地"
            if "沙田" in text:
                meeting["course"] = meeting["course"] or "沙田"

        return meeting

# ---------- CSV ----------
def write_csv(meeting: Dict[str,Any], races_csv="races.csv", entries_csv="entries.csv"):
    # races
    with open(races_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow([
            "date","venue_code","course","race_no",
            "race_name_zh","race_name_en","race_time_local","race_time_hkt","race_time_utc",
            "distance_m","surface","course_line","going","class_text","handicap","title"
        ])
        for r in meeting.get("races",[]):
            m = r.get("meta", {})
            w.writerow([
                meeting.get("date",""),
                meeting.get("venue_code",""),
                meeting.get("course",""),
                r.get("race_no",""),
                m.get("race_name_zh",""), m.get("race_name_en",""),
                m.get("race_time_local",""), m.get("race_time_hkt",""), m.get("race_time_utc",""),
                m.get("distance_m",""), m.get("surface",""), m.get("course_line",""), m.get("going",""),
                m.get("class_text",""), m.get("handicap",""), r.get("title","")
            ])
    # entries
    with open(entries_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        cols = [
            "date","venue_code","race_no","is_reserve",
            "horse_no","horse_name_zh","brand","draw","jockey_zh","trainer_zh",
            "rating","rating_pm","weight_lb","declared_wt","declared_wt_pm",
            "age","sex","wfa","gear","last6","owner","sire","dam","import_cat"
        ]
        w.writerow(cols)
        for r in meeting.get("races",[]):
            for e in r.get("entries",[]):
                w.writerow([
                    meeting.get("date",""), meeting.get("venue_code",""), r["race_no"], 0,
                    e.get("horse_no",""), e.get("horse_name_zh",""), e.get("brand",""),
                    e.get("draw",""), e.get("jockey_zh",""), e.get("trainer_zh",""),
                    e.get("rating",""), e.get("rating_pm",""), e.get("weight_lb",""),
                    e.get("declared_wt",""), e.get("declared_wt_pm",""),
                    e.get("age",""), e.get("sex",""), e.get("wfa",""),
                    e.get("gear",""), e.get("last6",""), e.get("owner",""),
                    e.get("sire",""), e.get("dam",""), e.get("import_cat","")
                ])
            for e in r.get("reserves",[]):
                w.writerow([
                    meeting.get("date",""), meeting.get("venue_code",""), r["race_no"], 1,
                    e.get("horse_no",""), e.get("horse_name_zh",""), e.get("brand",""),
                    e.get("draw",""), e.get("jockey_zh",""), e.get("trainer_zh",""),
                    e.get("rating",""), e.get("rating_pm",""), e.get("weight_lb",""),
                    e.get("declared_wt",""), e.get("declared_wt_pm",""),
                    e.get("age",""), e.get("sex",""), e.get("wfa",""),
                    e.get("gear",""), e.get("last6",""), e.get("owner",""),
                    e.get("sire",""), e.get("dam",""), e.get("import_cat","")
                ])

# ---------- MySQL 保存 ----------
import pymysql
from contextlib import contextmanager

@contextmanager
def _mysql_conn(cfg):
    conn = pymysql.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        user=cfg["user"],
        password=cfg.get("password", ""),
        database=cfg["db"],
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

def _safe_join(parts, sep=" / "):
    vals = [str(p).strip() for p in parts if p]
    return sep.join(vals) if vals else None

def load_mysql_cfg_from_env() -> dict:
    """
    由環境變數讀 MySQL 設定：
      DB_HOST / DB_PORT / DB_USER / DB_PASS / DB_NAME
    - 本地：用 .env + OS 環境變數
    - Render：用 Render Dashboard 設嘅環境變數
    """
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASS", ""),
        "db": os.getenv("DB_NAME", "hkjc_db"),
    }


def save_to_mysql(meeting: dict, mysql_cfg: dict):
    """
    將 meeting 結構寫入：
      - racecard_races (一場一行)
      - racecard_entries (每匹一行)
    以 (race_date, race_no) 及 (race_date, race_no, horse_no) 作唯一鍵 UPSERT。
    """
    race_sql = """
    INSERT INTO racecard_races (
        race_date, race_no, race_time, race_name_zh, race_name_en,
        distance_m, course, going, class_text, handicap,
        draw_date, venue_code
    ) VALUES (
        %(race_date)s, %(race_no)s, %(race_time)s, %(race_name_zh)s, %(race_name_en)s,
        %(distance_m)s, %(course)s, %(going)s, %(class_text)s, %(handicap)s,
        %(draw_date)s, %(venue_code)s
    )
    ON DUPLICATE KEY UPDATE
        race_time=VALUES(race_time),
        race_name_zh=VALUES(race_name_zh),
        race_name_en=VALUES(race_name_en),
        distance_m=VALUES(distance_m),
        course=VALUES(course),
        going=VALUES(going),
        class_text=VALUES(class_text),
        handicap=VALUES(handicap),
        draw_date=VALUES(draw_date),
        venue_code=VALUES(venue_code);
    """

    entry_sql = """
    INSERT INTO racecard_entries (
        race_date, race_no, horse_no,
        horse_name_zh, horse_name_en,
        horse_code, draw,
        jockey_zh, trainer_zh,
        rating, rating_pm,
        weight_lb, declared_wt, declared_wt_pm,
        age, sex, wfa,
        season_stakes, priority, days_since,
        owner, sire, dam, import_cat,
        silks, brand, gear, last6,
        scratched
    ) VALUES (
        %(race_date)s, %(race_no)s, %(horse_no)s,
        %(horse_name_zh)s, %(horse_name_en)s,
        %(horse_code)s, %(draw)s,
        %(jockey_zh)s, %(trainer_zh)s,
        %(rating)s, %(rating_pm)s,
        %(weight_lb)s, %(declared_wt)s, %(declared_wt_pm)s,
        %(age)s, %(sex)s, %(wfa)s,
        %(season_stakes)s, %(priority)s, %(days_since)s,
        %(owner)s, %(sire)s, %(dam)s, %(import_cat)s,
        %(silks)s, %(brand)s, %(gear)s, %(last6)s,
        %(scratched)s
    )
    ON DUPLICATE KEY UPDATE
        horse_name_zh  = VALUES(horse_name_zh),
        horse_name_en  = VALUES(horse_name_en),
        horse_code     = VALUES(horse_code),
        draw           = VALUES(draw),
        jockey_zh      = VALUES(jockey_zh),
        trainer_zh     = VALUES(trainer_zh),
        rating         = VALUES(rating),
        rating_pm      = VALUES(rating_pm),
        weight_lb      = VALUES(weight_lb),
        declared_wt    = VALUES(declared_wt),
        declared_wt_pm = VALUES(declared_wt_pm),
        age            = VALUES(age),
        sex            = VALUES(sex),
        wfa            = VALUES(wfa),
        season_stakes  = VALUES(season_stakes),
        priority       = VALUES(priority),
        days_since     = VALUES(days_since),
        owner          = VALUES(owner),
        sire           = VALUES(sire),
        dam            = VALUES(dam),
        import_cat     = VALUES(import_cat),
        silks          = VALUES(silks),
        brand          = VALUES(brand),
        gear           = VALUES(gear),
        last6          = VALUES(last6),
        scratched      = VALUES(scratched);
    """

    race_date = meeting.get("date")
    venue_code = meeting.get("venue_code")

    race_rows = []
    entry_rows = []

    for r in meeting.get("races", []):
        meta = r.get("meta", {}) or {}
        race_rows.append({
            "race_date": race_date,
            "race_no": r.get("race_no"),
            "race_time": (meta.get("race_time_local") or None),  # 'HH:MM' → TIME
            "race_name_zh": meta.get("race_name_zh") or "",
            "race_name_en": meta.get("race_name_en") or "",
            "distance_m": meta.get("distance_m"),
            "course": _safe_join([meta.get("surface"), meta.get("course_line")]),  # 例如「草地 / B」
            "going": meta.get("going") or "",
            "class_text": meta.get("class_text") or "",
            "handicap": meta.get("handicap") or "",
            "draw_date": None,
            "venue_code": venue_code or None,
        })

        for e in (r.get("entries") or []):
            if not e.get("horse_no"):
                continue

            entry_rows.append({
                "race_date": race_date,
                "race_no": r.get("race_no"),
                "horse_no": e.get("horse_no"),

                "horse_name_zh": e.get("horse_name_zh") or "",
                "horse_name_en": "",  # 暫時冇英文名

                # 你依家 JSON 入面 horse_code 冇真 code，只得 brand no，所以先沿用 brand 做 horse_code
                "horse_code": e.get("brand") or None,

                "draw": e.get("draw"),
                "jockey_zh": e.get("jockey_zh") or "",
                "trainer_zh": e.get("trainer_zh") or "",

                "rating": e.get("rating"),
                "rating_pm": e.get("rating_pm") or "",

                "weight_lb": e.get("weight_lb"),
                "declared_wt": e.get("declared_wt"),
                "declared_wt_pm": e.get("declared_wt_pm") or "",

                "age": e.get("age"),
                "sex": e.get("sex") or "",
                "wfa": e.get("wfa") or "",

                "season_stakes": e.get("season_stakes") or "",
                "priority": e.get("priority") or "",
                "days_since": e.get("days_since") or "",

                "owner": e.get("owner") or "",
                "sire": e.get("sire") or "",
                "dam": e.get("dam") or "",
                "import_cat": e.get("import_cat") or "",

                "silks": e.get("silks") or "",
                "brand": e.get("brand") or "",
                "gear": e.get("gear") or "",
                "last6": e.get("last6") or "",

                "scratched": 0,
            })


    if not race_rows and not entry_rows:
        return 0, 0

    with _mysql_conn(mysql_cfg) as conn:
        with conn.cursor() as cur:
            if race_rows:
                cur.executemany(race_sql, race_rows)
            if entry_rows:
                cur.executemany(entry_sql, entry_rows)

    return len(race_rows), len(entry_rows)


# ---------- 給 scheduler 用的封裝函式 ----------
def fetch_and_store_racecard(
    race_date: str,
    venue_code: str,
    draw_date: Optional[str] = None,
    mysql_cfg: Optional[dict] = None,
):
    """
    比 hkjc_racecard_scheduler.py 用：
      race_date  : 'YYYY-MM-DD'
      venue_code : 'ST' / 'HV'
      draw_date  : 'YYYY-MM-DD'（排位日，不填就用 race_date）
    DB:
      如 mysql_cfg 為 None → 自動用環境變數（.env / Render）
    """
    if mysql_cfg is None:
        mysql_cfg = load_mysql_cfg_from_env()

    # 爬全日排位
    meeting = crawl_meeting(race_date, venue_code, max_races=None, headful=False)

    # 補 draw_date / venue_code 資訊（可選）
    for r in meeting.get("races", []):
        meta = r.get("meta") or {}
        meta.setdefault("draw_date", draw_date or race_date)
        meta.setdefault("venue_code", venue_code)
        r["meta"] = meta

    races_cnt, entries_cnt = save_to_mysql(meeting, mysql_cfg)
    return races_cnt, entries_cnt


# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="HKJC RaceCard (Selenium) — 資料擴充版")
    ap.add_argument("--date", help="YYYY-MM-DD；不填則自動偵測")
    ap.add_argument("--course", choices=["ST","HV"], help="ST/HV；不填則自動偵測")
    ap.add_argument("--max-races", type=int, default=0, help="最多試幾多場；0=自動(最多20並連續2場無就停)")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--csv-out", action="store_true")

    # MySQL
    ap.add_argument("--mysql", action="store_true", help="寫入 MySQL")
    ap.add_argument("--mysql-host", default="127.0.0.1")
    ap.add_argument("--mysql-port", type=int, default=3306)
    ap.add_argument("--mysql-user", default="root")
    ap.add_argument("--mysql-pass", default="")
    ap.add_argument("--mysql-db",   default="hkjc")

    args = ap.parse_args()

    meeting = crawl_meeting(args.date, args.course, args.max_races or None, headful=args.headful)
    print("✅ 爬取完成")
    print(json.dumps(meeting, ensure_ascii=False, indent=2))

    if args.csv_out:
        write_csv(meeting)
        print("📄 已輸出 races.csv / entries.csv")

    if args.mysql:
        # 一律用環境變數（.env / Render Environment）
        cfg = load_mysql_cfg_from_env()
        races_cnt, entries_cnt = save_to_mysql(meeting, cfg)
        print(f"✅ 成功寫入 MySQL → 賽事 {races_cnt} 場，匹馬 {entries_cnt} 行")


if __name__ == "__main__":
    main()
