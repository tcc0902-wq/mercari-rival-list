#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Mercari プロフィールを順番にクロール → 各スプレッドシートへ同期（送信順の挙動優先）
- 1本目のロジック/待機(90s)をベース
- プロファイル指定/ログイン情報は使わない（--user-data-dir等なし）
- 4本目の「複数プロフィールを1つに集約」も対応（PROFILE_URLS を指定）

使い方:
  $ python mercari-rival-list.py                 # ブラウザ表示
  $ HEADLESS=1 python mercari-rival-list.py      # ヘッドレスON（CI推奨）
"""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

# ===================== 設定（送信順に並べる） =====================
TARGETS: List[Dict[str, Any]] = [
    {
        "MERCARI_PROFILE_URL": "https://jp.mercari.com/user/profile/679772118",
        "SPREADSHEET_URL": "https://docs.google.com/spreadsheets/d/1Nzl_g2EUqiZ9Y0FzvkcRCcv3V153ngeAG-po-DPg6rY/edit#gid=0",
    },
    {
        "MERCARI_PROFILE_URL": "https://jp.mercari.com/user/profile/327400503",
        "SPREADSHEET_URL": "https://docs.google.com/spreadsheets/d/1e-m6gLOqBqm-xsBUypgyZd84WvRvCh5OzGDSojK88P4/edit?gid=0#gid=0",
    },
    {
        "MERCARI_PROFILE_URL": "https://jp.mercari.com/user/profile/418988491",
        "SPREADSHEET_URL": "https://docs.google.com/spreadsheets/d/1uka8MCb4Ia0yhJde4kNHrn_o3Ap9Ex2HZpzDjEjFGOU/edit?gid=1073804106#gid=1073804106",
    },
    # 4本目：複数プロフィール → 1スプレッドシート
    {
        "PROFILE_URLS": [
            "https://jp.mercari.com/user/profile/853549493",
            "https://jp.mercari.com/user/profile/650928367",
            "https://jp.mercari.com/user/profile/500665966",
        ],
        "SPREADSHEET_URL": "https://docs.google.com/spreadsheets/d/1KMAxajmhIVNppUX9T4XWgDVUhExTU1xUekPYKGZSK3Y/edit?gid=1073804106",
    },
]

# 共有認証：CIでは env CREDENTIALS_PATH を使う（未設定なら /github/workspace/creds.json）
CREDENTIALS_PATH = os.environ.get(
    "CREDENTIALS_PATH",
    "/github/workspace/creds.json"
)

# ヘッダー定義（1本目準拠）
TODAY_HEADERS = ["商品名", "価格", "URL"]
LIST_SHEET = "出品リスト"
SOLD_SHEET = "販売済み"
LIST_HEADERS = ["商品名", "価格", "URL", "出品日"]
SOLD_HEADERS = ["商品名", "価格", "URL", "出品日", "販売日"]

# JSTの今日/昨日（1本目準拠）
JST = timezone(timedelta(hours=9))
today_sheet_name = datetime.now(JST).strftime("%Y%m%d")
yesterday_str = (datetime.now(JST) - timedelta(days=1)).strftime("%Y/%m/%d")

# ===================== 共通ユーティリティ =====================
def build_driver(headless: bool = False) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--renderer-process-limit=2")
    opts.add_argument("--window-size=1200,800" if headless else "--window-size=1920,1080")
    return webdriver.Chrome(options=opts)  # Selenium Manager が自動解決

def safe_click(driver, by, value, retries=3, timeout=90):
    for i in range(retries):
        try:
            el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, value)))
            el.click()
            return
        except StaleElementReferenceException:
            print(f"⚠️ StaleElement (retry {i+1}/{retries})")
            time.sleep(1)
    raise Exception("❌ 要素が安定せずクリックできませんでした")

def rows_to_dict_by_url(rows, url_idx: int) -> dict:
    out = {}
    for r in rows:
        if url_idx < len(r):
            u = (r[url_idx] or "").strip()
            if u and u not in out:
                out[u] = r
    return out

def get_or_create_worksheet(ss, title: str, headers: List[str]):
    """ワークシート取得。無ければ作成しヘッダーを書く。既存なら不足ヘッダーを末尾に追加。"""
    try:
        ws = ss.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows="1000", cols="20")
        ws.update("A1", [headers])
        return ws
    current = ws.row_values(1)
    updated = current[:]
    changed = False
    for h in headers:
        if h not in current:
            updated.append(h)
            changed = True
    if changed:
        ws.update("A1", [updated])
    return ws

def header_index_map(ws):
    head = ws.row_values(1)
    return {name: i for i, name in enumerate(head)}, head

def update_or_create_today_sheet(ss, sheet_name, header, rows):
    """本日シートをクリアして書き込み（なければ作成）"""
    try:
        ws = ss.worksheet(sheet_name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=sheet_name, rows="1000", cols="10")
    ws.update("A1", [header] + rows)
    return ws

def click_more_until_done(driver, wait: WebDriverWait, max_clicks: int = 300):
    """『もっと見る』を安全弁付きで連打"""
    more_xpath = '//button[text()="もっと見る"]'
    clicks = 0
    while clicks < max_clicks:
        try:
            more = wait.until(EC.element_to_be_clickable((By.XPATH, more_xpath)))
            more.click()
            driver.execute_script("window.scrollBy(0, 300);")
            time.sleep(1)
            clicks += 1
        except Exception:
            break

def slow_scroll_to_load_all(driver, pause: float = 2.0):
    """ゆっくりスクロールで全件表示"""
    last_h = 0
    retries = 5
    while retries > 0:
        driver.execute_script("window.scrollBy(0, 500);")
        time.sleep(pause)
        new_h = driver.execute_script("return document.body.scrollHeight")
        if new_h == last_h:
            retries -= 1
        else:
            last_h = new_h
            retries = 5

def collect_items_current_page(driver):
    """現在ページから商品カード抽出"""
    items = []
    seen = set()
    elements = driver.find_elements(By.XPATH, '//a[contains(@href, "/item/")]')
    for el in elements:
        try:
            url = el.get_attribute("href") or ""
            if (not url) or (url in seen):
                continue
            seen.add(url)
            name_elem = el.find_element(By.XPATH, './/span[@data-testid="thumbnail-item-name"]')
            price_elem = el.find_element(By.XPATH, './/span[contains(@class,"number__")]')
            name = name_elem.text.strip()
            price = price_elem.text.strip()
            items.append([name, price, url])
        except Exception:
            continue
    return items

# ===================== 主処理（ターゲット1件分） =====================
def run_once(target: Dict[str, Any], headless: bool) -> None:
    driver = build_driver(headless=headless)
    wait = WebDriverWait(driver, 90)
    try:
        print("\n==== Start target ====")
        # ---- 収集 ----
        all_items: List[List[str]] = []

        if "MERCARI_PROFILE_URL" in target:
            profile_url = target["MERCARI_PROFILE_URL"]
            print(f"▶ 単一プロフィール: {profile_url}")
            driver.get(profile_url)
            try:
                safe_click(driver, By.XPATH, '//*[@id="main"]/div[3]/div/label/input', timeout=90)
                time.sleep(2)
            except Exception as e:
                print(f"⚠️ 入力欄クリックをスキップ: {e}")

            click_more_until_done(driver, wait, max_clicks=300)
            slow_scroll_to_load_all(driver, pause=2.0)
            items = collect_items_current_page(driver)
            print(f"📥 取得件数: {len(items)}")
            all_items.extend(items)

        elif "PROFILE_URLS" in target:
            profile_urls: List[str] = target["PROFILE_URLS"]
            print(f"▶ 複数プロフィール集約: {len(profile_urls)} 件")
            for purl in profile_urls:
                print(f"  … {purl}")
                driver.get(purl)
                try:
                    safe_click(driver, By.XPATH, '//*[@id="main"]/div[3]/div/label/input', timeout=90)
                    time.sleep(2)
                except Exception:
                    pass
                click_more_until_done(driver, wait, max_clicks=300)
                slow_scroll_to_load_all(driver, pause=2.0)
                items = collect_items_current_page(driver)
                print(f"    取得: {len(items)}")
                all_items.extend(items)
            print(f"📦 集約合計: {len(all_items)}")

        else:
            raise SystemExit("❌ ターゲット設定に URL がありません（MERCARI_PROFILE_URL または PROFILE_URLS が必要）")

        # ---- gspread 認証 & スプレッドシート操作 ----
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        credentials = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
        client = gspread.authorize(credentials)
        ss = client.open_by_url(target["SPREADSHEET_URL"])

        # 1) 本日シート
        ws_today = update_or_create_today_sheet(ss, today_sheet_name, TODAY_HEADERS, all_items)

        # 2) 差分同期（出品リスト / 販売済み）
        today_header = ws_today.row_values(1)
        today_rows = ws_today.get_all_values()[1:] if today_header else []
        try:
            t_idx_name = today_header.index("商品名")
            t_idx_price = today_header.index("価格")
            t_idx_url = today_header.index("URL")
        except ValueError:
            raise SystemExit(f"❌ 本日シートのヘッダー不足。必要: {TODAY_HEADERS} / 実: {today_header}")

        today_by_url = rows_to_dict_by_url(today_rows, t_idx_url)
        today_urls = set(today_by_url.keys())

        # 出品リスト
        ws_list = get_or_create_worksheet(ss, LIST_SHEET, LIST_HEADERS)
        list_map, list_header = header_index_map(ws_list)
        list_rows_all = ws_list.get_all_values()
        list_rows = list_rows_all[1:] if len(list_rows_all) > 1 else []
        l_idx_name = list_map.get("商品名")
        l_idx_price = list_map.get("価格")
        l_idx_url = list_map.get("URL")
        l_idx_outdate = list_map.get("出品日")
        list_by_url = rows_to_dict_by_url(list_rows, l_idx_url if l_idx_url is not None else -1)
        list_urls = set(list_by_url.keys())

        # 販売済み
        ws_sold = get_or_create_worksheet(ss, SOLD_SHEET, SOLD_HEADERS)
        sold_map, sold_header = header_index_map(ws_sold)
        s_idx_name = sold_map.get("商品名")
        s_idx_price = sold_map.get("価格")
        s_idx_url = sold_map.get("URL")
        s_idx_outdate = sold_map.get("出品日")
        s_idx_sold = sold_map.get("販売日")

        # 差分
        to_add_urls = sorted(today_urls - list_urls)
        to_sold_urls = sorted(list_urls - today_urls)

        # 出品リストへ追記
        rows_to_append_to_list = []
        for u in to_add_urls:
            r = today_by_url[u]
            name = r[t_idx_name] if t_idx_name < len(r) else ""
            price = r[t_idx_price] if t_idx_price < len(r) else ""
            new_row = [""] * len(list_header)
            if l_idx_name is not None and l_idx_name < len(new_row): new_row[l_idx_name] = name
            if l_idx_price is not None and l_idx_price < len(new_row): new_row[l_idx_price] = price
            if l_idx_url is not None and l_idx_url < len(new_row): new_row[l_idx_url] = u
            if l_idx_outdate is not None and l_idx_outdate < len(new_row): new_row[l_idx_outdate] = yesterday_str
            rows_to_append_to_list.append(new_row)

        # 販売済みへ移動
        rows_to_append_to_sold = []
        for u in to_sold_urls:
            r = list_by_url[u]
            name = r[l_idx_name] if (l_idx_name is not None and l_idx_name < len(r)) else ""
            price = r[l_idx_price] if (l_idx_price is not None and l_idx_price < len(r)) else ""
            outdate = r[l_idx_outdate] if (l_idx_outdate is not None and l_idx_outdate < len(r)) else ""
            sold_row = [""] * len(sold_header)
            if s_idx_name is not None and s_idx_name < len(sold_row): sold_row[s_idx_name] = name
            if s_idx_price is not None and s_idx_price < len(sold_row): sold_row[s_idx_price] = price
            if s_idx_url is not None and s_idx_url < len(sold_row): sold_row[s_idx_url] = u
            if s_idx_outdate is not None and s_idx_outdate < len(sold_row): sold_row[s_idx_outdate] = outdate
            if s_idx_sold is not None and s_idx_sold < len(sold_row): sold_row[s_idx_sold] = yesterday_str
            rows_to_append_to_sold.append(sold_row)

        # 出品リストの残す行
        remaining_rows = []
        for r in list_rows:
            u = r[l_idx_url] if (l_idx_url is not None and l_idx_url < len(r)) else ""
            if u and u in to_sold_urls:
                continue
            remaining_rows.append(r)

        # 書き込み
        if rows_to_append_to_sold:
            ws_sold.append_rows(rows_to_append_to_sold, value_input_option="USER_ENTERED")

        ws_list.clear()
        ws_list.update("A1", [list_header])
        if remaining_rows:
            ws_list.update(f"A2:{rowcol_to_a1(len(remaining_rows)+1, len(list_header))}", remaining_rows)

        if rows_to_append_to_list:
            ws_list.append_rows(rows_to_append_to_list, value_input_option="USER_ENTERED")

        # 3) 本日シート削除
        try:
            ws_today = ss.worksheet(today_sheet_name)
            ss.del_worksheet(ws_today)
            print(f"🗑️ 本日シート {today_sheet_name} を削除しました（履歴を残さない運用）")
        except Exception as e:
            print(f"⚠️ 本日シート削除に失敗: {e}")

        print(f"✅ 取得件数: {len(all_items)} 件")
        print(f"🧮 差分同期: 追加 {len(rows_to_append_to_list)} 件（出品日={yesterday_str}） / 販売済みへ移動 {len(rows_to_append_to_sold)} 件（販売日={yesterday_str}）")

    finally:
        driver.quit()
        print("==== Done target ====")

# ===================== エントリーポイント =====================
if __name__ == "__main__":
    headless = os.environ.get("HEADLESS", "0") == "1"
    for t in TARGETS:
        run_once(t, headless=headless)
    print("🎉 すべてのターゲットの同期が完了しました。")
