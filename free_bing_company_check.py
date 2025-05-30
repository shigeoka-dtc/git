import os
import re
import json
import logging
import asyncio
import pandas as pd
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright

CACHE_FILE = "bing_cache_simple.json"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def search_bing(company, page):
    query = f"{company} 社名変更 OR 商号変更 OR 新社名"
    url = f"https://www.bing.com/search?q={query}"
    await page.goto(url)
    await page.wait_for_timeout(1000)
    results = []
    elements = await page.query_selector_all("li.b_algo")
    for elem in elements[:10]:
        try:
            title_elem = await elem.query_selector("h2")
            title_text = await title_elem.inner_text() if title_elem else ""
            snippet_elem = await elem.query_selector(".b_caption")
            snippet_text = await snippet_elem.inner_text() if snippet_elem else ""
            link_elem = await elem.query_selector("a")
            link = await link_elem.get_attribute("href") if link_elem else ""
            results.append((title_text + "\n" + snippet_text, snippet_text, link))
        except Exception as e:
            logging.debug(f"検索結果の解析エラー: {e}")
            continue
    return results

def extract_info(text, old_name):
    name_pattern1 = re.compile(
        r"[「『【]([^\n「」『』【】]{2,})[」』】]\s*(?:に変更|へ変更|とする|と決定)"
    )
    name_pattern2 = re.compile(
        r"(?:社名変更|商号変更|社名を変更|改称)[^\n]{0,50}?([^\s「」（）『』【】]{2,}(株式会社|有限会社|合同会社|Inc\.|LLC))"
    )
    name_pattern3 = re.compile(
        r"(?:「)?([A-Za-z0-9一-龥ぁ-んァ-ンー＆’\'\-\.\s]{2,})(?:」)?(?:に変更|へ変更|へ名称を変更|とする|と決定)"
    )

    date_pattern = re.compile(
        r"(\d{4}年\d{1,2}月\d{1,2}日付?|\d{4}年\d{1,2}月付?|\d{4}年付?)"
    )

    reason_pattern = re.compile(
        r"(?:理由は|ため|ことから|背景には)([^。]{3,15})。"
    )

    name_match1 = name_pattern1.search(text)
    name_match2 = name_pattern2.search(text) if not name_match1 else None
    name_match3 = name_pattern3.search(text) if not name_match1 and not name_match2 else None

    date_match = date_pattern.search(text)
    reason_match = reason_pattern.search(text)

    if name_match1:
        new_name = name_match1.group(1)
    elif name_match2:
        new_name = name_match2.group(1)
    elif name_match3:
        new_name = name_match3.group(1)
    else:
        new_name = "不明"

    date = date_match.group(1) if date_match else "変更日不明"
    reason = reason_match.group(1) if reason_match else "不明"

    if new_name != "不明" and old_name not in new_name:
        return new_name, date, reason

    return None, None, None

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

async def analyze_company(browser, company):
    cache = load_cache()
    key = company.strip().lower()

    if key in cache:
        logging.info(f"【RESUME】スキップ: {company}")
        result = cache[key]
        result[4] = "スキップ"
        return result

    context = await browser.new_context()
    page = await context.new_page()

    try:
        logging.info(f"検索開始: {company}")
        results = await search_bing(company, page)

        results_sorted = sorted(
            [r for r in results if not is_low_quality(r[1], r[2])],
            key=lambda x: domain_score(x[2]),
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

    finally:
        await context.close()

DOMAIN_PRIORITY = [
    ".co.jp", ".go.jp", ".or.jp",
    "prtimes.jp", "news.yahoo.co.jp", "nikkei.com",
    "businessinsider.jp", "itmedia.co.jp"
]

def domain_score(url):
    for i, domain in enumerate(DOMAIN_PRIORITY):
        if domain in url:
            return len(DOMAIN_PRIORITY) - i
    return 0

def is_low_quality(snippet, url):
    low_keywords = ["登記", "手続き", "ガイド", "説明", "法律事務所", "司法書士", "届出"]
    for kw in low_keywords:
        if kw in snippet or kw in url:
            return True
    return False

async def main():
    df = pd.read_csv("companies.csv")
    companies = df["会社名"].dropna().drop_duplicates().tolist()
    logging.info(f"対象社数: {len(companies)}社")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        results = []

        for company in tqdm(companies):
            result = await analyze_company(browser, company)
            results.append(result)

        await browser.close()

    df_out = pd.DataFrame(results, columns=[
        "会社名", "新社名", "変更日", "変更理由", "変更状況", "検出文", "URL"
    ])
    df_out.to_csv("output_playwright.csv", index=False, encoding="utf-8-sig")
    logging.info("出力完了: output_playwright.csv")

if __name__ == "__main__":
    asyncio.run(main())
