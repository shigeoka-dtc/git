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
CACHE_FILE = "bing_cache_v6_final_stable.json"

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
    "genspark.ai", "reflet-office.com", "note.com", "qiita.com", "zenn.dev"
]

def domain_score(url):
    for domain in LOW_QUALITY_DOMAINS:
        if domain in url:
            return -10
    for i, domain in enumerate(DOMAIN_PRIORITY):
        if domain in url:
            return len(DOMAIN_PRIORITY) - i
    return 0

# ✅ フィルター（緩め）
def is_low_quality(snippet, url):
    low_keywords = [
        "商号変更とは", "社名変更とは", "会社名が変更になる場合は"
    ]
    for kw in low_keywords:
        if kw in snippet or kw in url:
            return True
    return False

# ✅ キーワード部分一致 (会社名が本文にあるか)
def company_in_text(company, text):
    keyword = company.replace("株式会社", "").strip()
    return re.search(re.escape(keyword), text, re.IGNORECASE) is not None

# ✅ スコア計算：ドメイン＋会社名マッチなら加点
def result_score(company, title, snippet, url):
    score = domain_score(url)
    if company_in_text(company, title + snippet):
        score += 5  # 会社名が本文にあれば加点
    return score

# ✅ Bing検索
def search_bing(driver, company):
    query = f"{company} 社名変更 OR 商号変更 OR 新社名"
    url = f"https://www.bing.com/search?q={urllib.parse.quote(query)}"
    driver.get(url)
    time.sleep(random.uniform(2.0, 4.0))
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

# ✅ extract_info V4ベース
def extract_info(text, old_name):
    name_match = re.search(r"へ([^\s「」]{3,})に社名変更|([^\s「」]{3,})に商号変更", text)
    date_match = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日|\d{4}年\d{1,2}月|\d{4}年)", text)
    reason_match = re.search(r"(理由は[^。]{3,15})。", text)

    new_name = name_match.group(1) or name_match.group(2) if name_match else "不明"
    date = date_match.group(1) if date_match else "変更日不明"
    reason = reason_match.group(1).replace("理由は", "") if reason_match else "不明"

    if new_name.startswith("は"):
        return None, None, None

    if new_name != "不明" and old_name not in new_name:
        return new_name, date, reason

    return None, None, None

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
        result[4] = "スキップ"
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
                result = [company, new_name, date, reason, "変更あり", snippet, url]
                cache[key] = result
                save_cache(cache)
                return result

        result = [company, "変更なし", "変更日不明", "不明", "変更なし", "", ""]
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
        return list(tqdm(executor.map(lambda c: analyze_company(c), companies), total=len(companies)))

# ✅ メイン
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="会社名CSVファイル")
    parser.add_argument("output", help="出力CSVファイル")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    companies = df["会社名"].dropna().drop_duplicates().tolist()
    logging.info(f"対象社数: {len(companies)}社")

    results = process_all(companies)

    df_out = pd.DataFrame(results, columns=[
        "会社名", "新社名", "変更日", "変更理由", "変更状況", "検出文", "URL"
    ])
    df_out.to_csv(args.output, index=False, encoding="utf-8-sig")
    logging.info(f"出力完了: {args.output}")

if __name__ == "__main__":
    main()
