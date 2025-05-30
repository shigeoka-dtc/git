import asyncio
from playwright.async_api import async_playwright
import json
import re
import pandas as pd
import urllib.parse
import os
import random
from tqdm.asyncio import tqdm_asyncio
import argparse

# ✅ キャッシュファイル
CACHE_FILE = "bing_cache_playwright.json"

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
    "zeiri4.com", "bizocean.jp", "corporate.ai-con.lawyer", "kaonavi.jp",
    "legal-script.com", "houmu-news.com", "bengo4.com", "kaisha.tech"
]

EXCLUDE_NAME_PATTERNS = [
    r"正式には", r"通称", r"呼ばれ", r"一般的に", r"略称", r"通名", r"会社名とは", r"社名とは"
]

BAD_NAMES = [
    "当ページを参考", "こちら", "不明", "参考", "社名は", "といいます", "正式には", "商号", "社名変更とは"
]

# ✅ 関数群
def normalize_company(name):
    return name.replace("株式会社", "").replace(" ", "").replace("　", "").lower()

def domain_score(url):
    url = url or ""
    for domain in LOW_QUALITY_DOMAINS:
        if domain in url:
            return -100
    for i, domain in enumerate(DOMAIN_PRIORITY):
        if domain in url:
            return len(DOMAIN_PRIORITY) - i
    return 0

def is_low_quality(snippet, url):
    low_keywords = [
        "商号変更とは", "社名変更とは", "会社名が変更になる場合は",
        "法人登記", "やり方", "手続き", "無料相談", "注意点", "解説",
        "法律事務所", "弁護士", "登記変更", "申請方法", "料金"
    ]
    snippet = snippet or ""
    url = url or ""
    if "bing.com/ck/a" in url:
        return True
    if any(domain in url for domain in LOW_QUALITY_DOMAINS):
        return True
    for kw in low_keywords:
        if kw in snippet or kw in url:
            return True
    return False

def clean_bing_redirect(url):
    from urllib.parse import unquote, urlparse, parse_qs
    if "bing.com/ck/a" in url:
        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            real_url = query.get('u', [None])[0]
            if real_url:
                return unquote(real_url)
        except Exception:
            pass
    return url

def result_score(company, title, snippet, url):
    score = domain_score(url)
    # AI風補助: 強いワードがあれば優遇
    strong_keywords = ["新社名", "商号変更", "新商号", "変更予定", "決定", "発表", "ニュースリリース"]
    if normalize_company(company) in (title + snippet):
        score += 10
    if any(kw in (title + snippet) for kw in strong_keywords):
        score += 8
    if "pdf" in url.lower():
        score -= 3
    return score

def extract_info(text, old_name):
    text = text.replace("\n", "").replace("\r", "").strip()
    if any(re.search(pat, text) for pat in EXCLUDE_NAME_PATTERNS):
        return None, None, None

    name_patterns = [
        r'社名(?:を)?「?([^\s「」]{2,50})」?に変更',
        r'新社名[は:]?「?([^\s「」]{2,50})」?',
        r'「?([^\s「」]{2,50})株式会社」?に変更',
        r'([^\s「」]{2,50})株式会社に変更',
        r'新商号[は:]?\s*([^\s「」]{2,50})株式会社',
        r'商号変更.*?([^\s「」]{2,50})株式会社',
    ]

    new_name = None
    for pat in name_patterns:
        m = re.search(pat, text)
        if m:
            g = m.group(1)
            if g and g not in BAD_NAMES and not g.startswith("は"):
                new_name = g
                break

    if not new_name:
        return None, None, None

    if normalize_company(old_name) in normalize_company(new_name):
        return None, None, None

    date_match = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日|\d{4}年\d{1,2}月|\d{4}年)", text)
    date = date_match.group(1) if date_match else "変更日不明"

    reason_match = re.search(r"(?:理由は|変更理由は)([^。]{3,30})。", text)
    reason = reason_match.group(1) if reason_match else "不明"

    return new_name, date, reason

# ✅ キャッシュ
def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

# ✅ Bing検索
async def search_bing(playwright, company):
    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()
    query = f"{company} 社名変更 OR 商号変更 OR 新社名"
    url = f"https://www.bing.com/search?q={urllib.parse.quote(query)}"

    await page.goto(url)
    await page.wait_for_timeout(random.randint(1500, 4000))

    elements = await page.query_selector_all("li.b_algo")
    results = []
    for elem in elements[:10]:
        try:
            title = await elem.query_selector("h2")
            snippet_elem = await elem.query_selector(".b_caption")
            link_elem = await elem.query_selector("a")

            title_text = await title.inner_text() if title else ""
            snippet_text = await snippet_elem.inner_text() if snippet_elem else ""
            link_url = await link_elem.get_attribute("href") if link_elem else ""

            results.append((title_text + "\n" + snippet_text, snippet_text, link_url))
        except Exception:
            continue

    await browser.close()
    return results

# ✅ 1社ずつ処理
async def analyze_company(playwright, company):
    cache = load_cache()
    key = normalize_company(company)

    if key in cache:
        print(f"[CACHE HIT] {company}")
        return cache[key]

    try:
        print(f"[SEARCH] {company}")
        results = await search_bing(playwright, company)

        results_sorted = sorted(
            [r for r in results if not is_low_quality(r[1], r[2])],
            key=lambda x: result_score(company, x[0], x[1], x[2]),
            reverse=True
        )

        for full_text, snippet, url in results_sorted:
            cleaned_url = clean_bing_redirect(url)
            new_name, date, reason = extract_info(full_text, company)
            if new_name:
                result = [company, new_name, date, reason, "変更あり", snippet or "なし", cleaned_url or ""]
                cache[key] = result
                save_cache(cache)
                return result

        if results_sorted:
            snippet = results_sorted[0][1] or "なし"
            cleaned_url = clean_bing_redirect(results_sorted[0][2]) or ""
        else:
            snippet = "なし"
            cleaned_url = ""

        result = [company, "変更なし", "変更日不明", "不明", "変更なし", snippet, cleaned_url]
        cache[key] = result
        save_cache(cache)
        return result

    except Exception as e:
        print(f"[ERROR] {company}: {e}")
        return [company, "エラー", "不明", "不明", "処理失敗", str(e), ""]

# ✅ メイン
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", help="入力 CSVファイル（会社名列が必要）")
    parser.add_argument("output_csv", help="出力 CSVファイル")
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv)
    companies = df["会社名"].dropna().tolist()
    print(f"Total companies: {len(companies)}")

    results_dict = {}
    all_results = []

    async with async_playwright() as playwright:
        tasks = [analyze_company(playwright, company) for company in companies]
        for result in tqdm_asyncio.as_completed(tasks, total=len(companies)):
            r = await result
            key = normalize_company(r[0])
            results_dict[key] = r
            all_results.append(r)

    df_out_rows = []
    for company in companies:
        key = normalize_company(company)
        result = results_dict.get(key)
        df_out_rows.append(result)

    df_out = pd.DataFrame(df_out_rows, columns=[
        "会社名", "新社名", "変更日", "変更理由", "変更状況", "検出文", "URL"
    ])
    df_out.to_csv(args.output_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Output saved: {args.output_csv}")

# ✅ エントリーポイント
if __name__ == "__main__":
    asyncio.run(main())
