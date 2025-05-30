import os
import re
import time
import json
import random
import logging
import argparse
import urllib.parse
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import traceback

# ✅ キャッシュファイル
CACHE_FILE = "bing_cache_v6_final_full.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1200,800")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    return driver

# ✅ ドメインスコア設定
DOMAIN_PRIORITY = [
    ".co.jp", ".go.jp", ".or.jp",
    "prtimes.jp", "news.yahoo.co.jp", "nikkei.com",
    "businessinsider.jp", "itmedia.co.jp",
    "impress.co.jp", "reuters.com", "asahi.com",
    "mainichi.jp", "yomiuri.co.jp",
    "sankei.com", "jiji.com", "nhk.or.jp"
]

LOW_QUALITY_DOMAINS = [
    "genspark.ai", "reflet-office.com", "note.com", "qiita.com", "zenn.dev",
    "office-tsuda.net", "advisors-freee.jp", "freee.co.jp",
    "houmukyoku.moj.go.jp", "bing.com/ck/a",
    "ai-con.lawyer", "shiodome.co.jp",
    "zeiri4.com", "bizocean.jp", "corporate.ai-con.lawyer", "kaonavi.jp"
]

def domain_score(url):
    url = url or ""
    for domain in LOW_QUALITY_DOMAINS:
        if domain in url:
            return -100
    for i, domain in enumerate(DOMAIN_PRIORITY):
        if domain in url:
            return len(DOMAIN_PRIORITY) - i
    return 0

# ✅ フィルター
def is_low_quality(snippet, url):
    low_keywords = [
        "商号変更とは", "社名変更とは", "会社名が変更になる場合は"
    ]
    snippet = snippet or ""
    url = url or ""
    for kw in low_keywords:
        if kw in snippet or kw in url:
            return True
    return False

# ✅ キーワード部分一致
def company_in_text(company, text):
    keyword = company.replace("株式会社", "").strip()
    return re.search(re.escape(keyword), text, re.IGNORECASE) is not None

# ✅ スコア計算
def result_score(company, title, snippet, url):
    score = domain_score(url)
    if company_in_text(company, title + snippet):
        score += 5
    return score

# ✅ Bing検索
def search_bing(driver, company):
    query = f"{company} 社名変更 OR 商号変更 OR 新社名"
    url = f"https://www.bing.com/search?q={urllib.parse.quote(query)}"
    driver.get(url)
    time.sleep(random.uniform(1.5, 4.5))
    results = []
    for elem in driver.find_elements(By.CSS_SELECTOR, "li.b_algo")[:10]:
        try:
            title = elem.find_element(By.TAG_NAME, "h2").text
            snippet = elem.find_element(By.CLASS_NAME, "b_caption").text
            link = elem.find_element(By.TAG_NAME, "a").get_attribute("href")
            results.append((title + "\n" + snippet, snippet, link))
        except Exception as e:
            logging.debug(f"検索結果解析エラー: {e}")
            continue
    return results

# 🚫 除外ワード
EXCLUDE_NAME_PATTERNS = [
    r"正式には", r"通称", r"呼ばれ", r"一般的に", r"略称", r"通名", r"会社名とは", r"社名とは"
]

# 🚫 BAD_NAMES 強化
BAD_NAMES = [
    "当ページを参考", "こちら", "不明", "参考", "社名は", "といいます", "正式には", "商号", "社名変更とは"
]

# ✅ extract_info 改良版
def extract_info(text, old_name):
    if any(re.search(pat, text) for pat in EXCLUDE_NAME_PATTERNS):
        return None, None, None

    name_match = re.search(
        r'社名(?:を)?「?([^\s「」]{2,})」?に変更|'
        r'新社名は「?([^\s「」]{2,})」?|'
        r'「?([^\s「」]{2,})株式会社」?に変更|'
        r'([^\s「」]{2,})株式会社に変更', 
        text
    )

    date_match = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日|\d{4}年\d{1,2}月|\d{4}年)", text)
    reason_match = re.search(r"(理由は[^。]{3,15})。", text)

    new_name = None
    if name_match:
        for g in name_match.groups():
            if g and g not in BAD_NAMES and not g.startswith("は"):
                new_name = g
                break
    if not new_name:
        return None, None, None

    if old_name.replace("株式会社", "").strip() in new_name:
        return None, None, None

    date = date_match.group(1) if date_match else "変更日不明"
    reason = reason_match.group(1).replace("理由は", "") if reason_match else "不明"

    return new_name, date, reason

# ✅ キャッシュ操作
def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

# ✅ 会社1社ずつ処理
def analyze_company(company):
    cache = load_cache()
    key = company.strip().lower()

    if key in cache:
        logging.info(f"【RESUME】スキップ: {company}")
        result = cache[key]
        if result[4] == "スキップ":
            result[4] = "変更なし"
        return result

    driver = None
    try:
        logging.info(f"検索開始: {company}")
        driver = get_driver()
        results = search_bing(driver, company)

        results_sorted = sorted(
            [r for r in results if not is_low_quality(r[1], r[2])],
            key=lambda x: result_score(company, x[0], x[1], x[2]),
            reverse=True
        )

        for full_text, snippet, url in results_sorted:
            new_name, date, reason = extract_info(full_text, company)
            if new_name:
                result = [company, new_name, date, reason, "変更あり", snippet or "なし", url or ""]
                cache[key] = result
                save_cache(cache)
                return result

        if results_sorted:
            snippet = results_sorted[0][1] or "なし"
            url = results_sorted[0][2] or ""
        else:
            snippet = "なし"
            url = ""

        result = [company, "変更なし", "変更日不明", "不明", "変更なし", snippet, url]
        cache[key] = result
        save_cache(cache)
        return result

    except Exception as e:
        logging.error(f"エラー: {company} - {e}")
        logging.error(traceback.format_exc())
        return [company, "エラー", "不明", "不明", "処理失敗", str(e), ""]
    finally:
        if driver:
            driver.quit()

# ✅ 並列処理
def process_all(companies):
    max_workers = 6
    logging.info(f"スレッド数: {max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(lambda c: analyze_company(c), companies), total=len(companies)))
    return results

# ✅ メイン
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="会社名CSVファイル")
    parser.add_argument("output", help="出力CSVファイル")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    companies = df["会社名"].dropna().tolist()
    logging.info(f"対象社数: {len(companies)}社")

    results_dict = {}
    all_results = []

    for result in process_all(companies):
        key = result[0].strip().lower()
        results_dict[key] = result
        all_results.append(result)

    df_out_rows = []
    for company in companies:
        key = company.strip().lower()
        result = results_dict.get(key)
        if result is None:
            result = analyze_company(company)
        df_out_rows.append(result)

    df_out = pd.DataFrame(df_out_rows, columns=[
        "会社名", "新社名", "変更日", "変更理由", "変更状況", "検出文", "URL"
    ])
    df_out.to_csv(args.output, index=False, encoding="utf-8-sig")
    logging.info(f"出力完了: {args.output}")

if __name__ == "__main__":
    main()
