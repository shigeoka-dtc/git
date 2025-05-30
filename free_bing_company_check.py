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
import traceback

# キャッシュファイル
CACHE_FILE = "bing_cache_simple.json"

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Driver は Selenium が自動管理
def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1200,800")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    return driver

# 優先ドメインスコア
DOMAIN_PRIORITY = [
    ".co.jp", ".go.jp", ".or.jp",
    "prtimes.jp", "news.yahoo.co.jp", "nikkei.com",
    "businessinsider.jp", "itmedia.co.jp"
]

def domain_score(url):
    for i, domain in enumerate(DOMAIN_PRIORITY):
        if domain in url:
            return len(DOMAIN_PRIORITY) - i  # 高スコアほど上位
    return 0

# Bing検索
def search_bing(driver, company):
    query = f"{company} 社名変更 OR 商号変更 OR 新社名"
    url = f"https://www.bing.com/search?q={urllib.parse.quote(query)}"
    driver.get(url)
    time.sleep(random.uniform(1.5, 3.0))
    results = []
    for elem in driver.find_elements(By.CSS_SELECTOR, "li.b_algo")[:10]:  # 10件見る
        try:
            title = elem.find_element(By.TAG_NAME, "h2").text
            snippet = elem.find_element(By.CLASS_NAME, "b_caption").text
            link = elem.find_element(By.TAG_NAME, "a").get_attribute("href")
            results.append((title + "\n" + snippet, snippet, link))
        except Exception as e:
            logging.debug(f"検索結果の解析エラー: {e}")
            continue
    return results

# 強化パターン
def extract_info(text, old_name):
    name_pattern = re.compile(
        r"(?:社名変更|商号変更|社名を変更|改称)[^\n]*?(?:へ|に|として|とする|することに)?([^\s「」（）『』【】]{2,})"
    )
    date_pattern = re.compile(
        r"(\d{4}年\d{1,2}月\d{1,2}日付?|\d{4}年\d{1,2}月付?|\d{4}年付?)"
    )
    reason_pattern = re.compile(
        r"(?:理由は|ため|ことから|背景には)([^。]{3,15})。"
    )

    name_match = name_pattern.search(text)
    date_match = date_pattern.search(text)
    reason_match = reason_pattern.search(text)

    new_name = name_match.group(1) if name_match else "不明"
    date = date_match.group(1) if date_match else "変更日不明"
    reason = reason_match.group(1) if reason_match else "不明"

    if new_name != "不明" and old_name not in new_name:
        return new_name, date, reason

    return None, None, None

# キャッシュロード
def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

# キャッシュ保存
def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

# 1社分析
def analyze_company(company):
    cache = load_cache()
    key = company.strip().lower()

    if key in cache:
        logging.info(f"【RESUME】スキップ: {company}")
        return cache[key]

    driver = None
    try:
        logging.info(f"検索開始: {company}")
        driver = get_driver()
        results = search_bing(driver, company)

        # スコア順にソート
        results_sorted = sorted(results, key=lambda x: domain_score(x[2]), reverse=True)

        for full_text, snippet, url in results_sorted:
            new_name, date, reason = extract_info(full_text, company)
            if new_name:
                result = [company, new_name, date, reason, "変更あり", snippet, url]
                cache[key] = result
                save_cache(cache)
                return result

        # 変更なし
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

# 全社並列処理
def process_all(companies):
    max_workers = 12  # ★ 1400社なら12本推奨
    logging.info(f"スレッド数: {max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(lambda c: analyze_company(c), companies))

# メイン
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="会社名CSVファイル")
    parser.add_argument("output", help="出力CSVファイル")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    companies = df["会社名"].dropna().unique().tolist()
    logging.info(f"対象社数: {len(companies)}社")

    results = process_all(companies)

    df_out = pd.DataFrame(results, columns=[
        "会社名", "新社名", "変更日", "変更理由", "変更状況", "検出文", "URL"
    ])
    df_out.to_csv(args.output, index=False, encoding="utf-8-sig")
    logging.info(f"出力完了: {args.output}")

if __name__ == "__main__":
    main()
