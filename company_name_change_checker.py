import asyncio
from playwright.async_api import async_playwright
import json
import re
import pandas as pd
import urllib.parse
import os
import sys
import random
from tqdm.asyncio import tqdm_asyncio
import argparse
import logging
import datetime
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

# --- 1. ロギング設定 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("company_name_checker.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# --- 2. グローバル変数・定数定義 ---
CACHE_FILE = "bing_cache_playwright.json"

DOMAIN_PRIORITY = [
    ".co.jp", ".go.jp", ".or.jp",
    "prtimes.jp", "news.yahoo.co.jp", "nikkei.com",
    "businessinsider.jp", "itmedia.co.jp", "impress.co.jp",
    "reuters.com", "asahi.com", "mainichi.jp", "yomiuri.co.jp",
    "sankei.com", "jiji.com", "nhk.or.jp",
    "irweb.jp", "release.tdnet.info",
    "hatena.ne.jp", "note.com"
]

LOW_QUALITY_DOMAINS = [
    "genspark.ai", "office-tsuda.net", "advisors-freee.jp", "freee.co.jp",
    "bing.com/ck/a", "ai-con.lawyer", "shiodome.co.jp", "zeiri4.com", "bizocean.jp",
    "corporate.ai-con.lawyer", "kaonavi.jp", "legal-script.com",
    "hourei.net", "gyosei-shoshi.or.jp", "zeirishi-soudan.jp",
    "biz.moneyforward.com", "corp.moneyforward.com",
    "youtube.com", "twitter.com", "facebook.com", "instagram.com", "linkedin.com",
    "ja.wikipedia.org", "dic.nicovideo.jp", "encyclopedia-biz.jp",
    "smbiz.asahi.com", "biz.chosakai.or.jp",
    "jobtag.j-platpat.inpit.go.jp", "tatekae.jp", "sumabase.jp", "ciel-law.jp"
]

EXCLUDE_NAME_PATTERNS = [
    r"正式には", r"通称", r"呼ばれ", r"一般的に", r"略称", r"通名", r"会社名とは", r"社名とは",
    r"変更方法", r"手続き方法", r"よくある質問", r"株式会社の変更", r"合同会社の変更",
    r"会社名変更に伴う", r"商号変更に伴う", r"変更手続き", r"定款変更", r"登記申請",
    r"参考資料", r"当ページ", r"こちら", r"以下", r"について解説", r"〇〇とは",
    r"新会社法の施行", r"法改正", r"登記について", r"M&Aとは",
    r"一覧\s*|リスト\s*|データベース\s*|企業情報"
]

BAD_NAMES = [
    "当ページを参考", "こちら", "不明", "参考", "社名は", "といいます", "正式には", "商号", "社名変更とは",
    "変更後の社名", "新名称", "変更後の名称", "変更後の会社名", "会社名", "代表", "役員",
    "変更される", "変更後の", "現時点での", "変更予定", "変更済", "発表", "決定", "変更",
    "に関する", "について", "の概要", "の変更", "お知らせ", "ニュースリリース", "報道発表",
    "の目的", "のため", "についてのご案内", "株主総会", "定時株主総会", "臨時株主総会",
    "会社概要", "事業内容", "所在地", "代表者", "連絡先", "資本金", "設立年月日",
    "概要", "沿革", "歴史", "変遷", "沿革と社名変更", "組織再編", "事業統合", "吸収合併",
    "新設分割", "吸収分割", "解散", "清算", "承継", "承継会社", "承継元", "承継先",
    "M&A", "子会社化", "グループ会社", "関連会社", "連結子会社", "非連結子会社",
    "事業譲渡", "事業譲受", "業務提携", "資本提携", "提携", "契約", "合弁", "ジョイントベンチャー",
    "システム変更", "システム統合", "システム刷新", "リニューアル", "移転", "新設", "設立",
    "株式会社", "有限会社", "合同会社", "合資会社", "合名会社",
    "社名", "新会社", "旧会社", "既存会社", "設立", "合併"
]

strong_keywords = [
    "新社名", "商号変更", "新商号", "社名変更に関するお知らせ", "正式決定", "発表", "ニュースリリース",
    "IR資料", "会社名変更", "効力発生日", "合併", "分割", "吸収合併", "経営統合", "事業再編",
    "ブランド統合", "組織再編", "新生", "商号変更のお知らせ", "社名変更のお知らせ", "社名変更に関するお知らせ",
    "社名変更決議", "社名変更決議のお知らせ"
]

LEGAL_ENTITIES_RE = r'(?:株式会社|有限会社|合同会社|合資会社|合名会社|相互会社|特定非営利活動法人|NPO法人|一般社団法人|公益社団法人|一般財団法人|公益財団法人|学校法人|医療法人|社会福祉法人|国立大学法人|独立行政法人|地方独立行政法人|特殊法人|認可法人|国立研究開発法人|国立大学法人|国立高等専門学校機構|国立病院機構|地域医療機能推進機構|日本年金機構|日本郵政株式会社|日本放送協会|日本銀行|日本私立学校振興・共済事業団)'

# --- normalize_company ---
def normalize_company(name):
    """会社名を正規化する（空白、法人格などの表記を削除し、小文字化）"""
    if not isinstance(name, str):
        return ""
    name = name.replace("　", "").replace(" ", "").replace("\t", "").strip()
    name = re.sub(r'(?:株式会社|有限会社|合同会社|合資会社|合名会社)\s*[\(\)（）\-\.・]*$', '', name)
    name = re.sub(r'^(?:株式会社|有限会社|合同会社|合資会社|合名会社)\s*[\(\)（）\-\.・]*', '', name)
    name = name.replace("コーポレーション", "").replace("グループ", "").replace("ホールディングス", "")
    name = name.replace("インク", "").replace("カンパニー", "").replace("ジャパン", "")
    name = name.replace("・", "").replace("（", "").replace("）", "")
    return name.lower()

BAD_NAMES_NORMALIZED = [normalize_company(name) for name in BAD_NAMES]

# --- domain_score ---
def domain_score(url):
    """URLに基づいてドメインスコアを計算する"""
    url = url or ""
    if any(domain in url for domain in LOW_QUALITY_DOMAINS):
        return -100
    for i, domain in enumerate(DOMAIN_PRIORITY):
        if domain in url:
            return len(DOMAIN_PRIORITY) - i
    return 0

# --- is_low_quality ---
def is_low_quality(snippet, url):
    """スニペットとURLから低品質な情報を判定する"""
    low_keywords = [
        "商号変更とは", "社名変更とは", "会社名が変更になる場合は",
        "法人登記", "やり方", "手続き", "無料相談", "注意点", "解説",
        "法律事務所", "弁護士", "登記変更", "申請方法", "料金",
        "プレスリリース配信", "PR記事", "広告記事", "用語解説", "契約書", "事務所",
        "サンプル", "テンプレート", "ひな形", "書式", "フォーム", "雛形",
        "登録方法", "設立", "合併手続き", "分割手続き", "会社法", "商業登記法",
        "税理士", "会計士", "行政書士", "司法書士",
        "コンサルティング", "ソリューション", "クラウドサービス", "AIサービス",
        "ブログ", "コラム", "まとめ", "Q&A", "よくある質問",
        "旧商号", "旧社名", "旧法人名",
        "上場企業", "商号変更会社一覧", "日本取引所グループ"
    ]

    snippet = snippet or ""
    url = url or ""

    if "bing.com/ck/a" in url:
        return True

    if any(domain in url for domain in LOW_QUALITY_DOMAINS):
        return True

    for kw in low_keywords:
        if kw.lower() in snippet.lower() or kw.lower() in url.lower():
            return True

    return False

# --- clean_bing_redirect ---
def clean_bing_redirect(url):
    """BingのリダイレクトURLをクリーンアップする"""
    if "bing.com/ck/a" in url:
        try:
            parsed = urllib.parse.urlparse(url)
            query = urllib.parse.parse_qs(parsed.query)
            real_url = query.get('u', [None])[0]
            if real_url:
                return urllib.parse.unquote(real_url)
        except Exception as e:
            logging.warning(f"Failed to clean Bing redirect URL '{url}': {e}")
    return url

# --- result_score ---
def result_score(company, title, snippet, url):
    """検索結果にスコアを付ける"""
    score = domain_score(url)
    combined_text = (title or "") + " " + (snippet or "")

    normalized_company = normalize_company(company)
    if normalized_company and normalized_company in normalize_company(combined_text):
        score += 10

    if any(kw.lower() in combined_text.lower() for kw in strong_keywords):
        score += 8

    if ".pdf" in url.lower():
        if any(d in url.lower() for d in [".co.jp", ".go.jp", ".or.jp"]) or \
           (normalized_company and normalized_company.split('株式会社')[0].lower() in url.lower()):
            score += 5
        else:
            score -= 5

    if any(path_part in url.lower() for path_part in ["/news/", "/ir/", "/press/", "/release/", "/company/", "/profile/", "/history/", "/about/"]):
        score += 5

    if "wikipedia.org" in url.lower() or "twitter.com" in url.lower() or "linkedin.com" in url.lower():
        score -= 2

    return score

# --- extract_info ---
def extract_info(text, old_name):
    """テキストから新社名、変更日、変更理由を抽出する"""
    text = text.replace("\n", " ").replace("\r", " ").strip()
    text = re.sub(r'\s+', ' ', text)

    if any(re.search(pat, text, re.IGNORECASE) for pat in EXCLUDE_NAME_PATTERNS):
        return None, None, None

    new_name = None
    company_name_base = r'(?:[^\s、。「」（）()\-]{2,80}?)'

    name_patterns = [
        r'(?:新社名|新商号|新名称)\s*[:：は、]?\s*「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?',
        r'(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*に(?:社名|商号|名称)変更(?:しました|しましたことをお知らせします|いたします|いたしました|を発表|することが決定)',
        r'(?:旧社名|旧商号)\s*[:：は]\s*[^、。]+?(?:、|。)?\s*(?:新社名|新商号)\s*[:：は]\s*「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?',
        r'(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*（(?:旧社名|旧商号)[\s:]*[^）]+?）',
        r'(?:合併|統合)(?:により|して)?\s*「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?(?:となります|になる|に変更|することを決定)',
        r'(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*として新たにスタート',
        r'(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*へ商号変更',
        r'「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?\s*への社名変更(?:のお知らせ|が決定|を承認)',
        r'社名を\s*「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?\s*に変更',
    ]

    for pat in name_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            candidate_name = m.group(1).strip()
            norm_candidate = normalize_company(candidate_name)
            norm_old_name = normalize_company(old_name)

            if len(candidate_name) < 2 or \
                any(bad_name in norm_candidate for bad_name in BAD_NAMES_NORMALIZED) or \
                norm_candidate == norm_old_name or \
                norm_old_name in norm_candidate:
                continue

            if not re.search(LEGAL_ENTITIES_RE, candidate_name):
                candidate_name += "株式会社"

            candidate_name = re.sub(r'[「」『』（）()]', '', candidate_name).strip()
            candidate_name = re.sub(r'^\s*[、。・\-\/\\]', '', candidate_name).strip()

            final_norm_candidate = normalize_company(candidate_name)
            if candidate_name and len(final_norm_candidate) >= 2 and \
                final_norm_candidate != norm_old_name and \
                norm_old_name not in final_norm_candidate:
                new_name = candidate_name
                break

    date = "変更日不明"
    date_patterns = [
        r"(\d{4}年\d{1,2}月\d{1,2}日(?:付)?)(?:より|から)?(?:変更|実施|施行|開始)",
        r"(?:変更日|実施日|効力発生日)[:：は、]?\s*(\d{4}年\d{1,2}月\d{1,2}日)",
        r"(\d{4}年\d{1,2}月\d{1,2}日)",
        r"(\d{4}年\d{1,2}月(?:から|より)?)",
    ]

    for pat in date_patterns:
        date_match = re.search(pat, text)
        if date_match:
            date = date_match.group(1).strip()
            date = date.replace("付", "").replace("より", "").replace("から", "").strip()
            break

    reason = "不明"
    reason_patterns = [
        r'(?:変更理由|理由は|背景は|目的は|経緯は)[^。]{3,200}。',
        r'(?:変更理由は|背景は)\s*「?([^」。「]{10,100}?)」?(?:です|となります|ことをお知らせします)',
        r'(?:ブランド統一|グローバル展開|企業価値向上|事業再編|経営統合|M&A|吸収合併|事業譲渡|効率化|多様化|グループ連携強化|成長戦略|企業イメージ刷新|創立\d+周年記念|事業体制再編)'
    ]

    for pat in reason_patterns:
        reason_match = re.search(pat, text, re.IGNORECASE)
        if reason_match:
            extracted_reason = reason_match.group(0).strip()
            if extracted_reason.endswith("。"):
                reason = extracted_reason
            else:
                reason = extracted_reason + "のため"
            break

    if reason == "不明":
        if re.search(r'(ブランド統一|グローバル展開|企業価値向上|事業再編|経営統合|M&A|吸収合併|事業譲渡|効率化|多様化|グループ連携強化|成長戦略|企業イメージ刷新|ホールディングス体制|持株会社体制)', text, re.IGNORECASE):
            reason = "事業戦略・組織再編のため"
        elif re.search(r'(周年|記念|節目)', text, re.IGNORECASE):
            reason = "創立記念・節目を機に"
        elif re.search(r'(本社移転|拠点移転|移転に伴い)', text, re.IGNORECASE):
            reason = "拠点移転に伴い"

    return new_name, date, reason

# --- load_cache ---
def load_cache():
    """キャッシュファイルを読み込む"""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logging.warning(f"キャッシュファイル '{CACHE_FILE}' の読み込みエラー: {e}。新しいキャッシュを作成します。")
            return {}
        except Exception as e:
            logging.warning(f"キャッシュファイルの読み込み中に予期せぬエラーが発生しました: {e}。新しいキャッシュを作成します。")
            return {}
    return {}

# --- save_cache ---
def save_cache(data):
    """キャッシュファイルを保存する"""
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logging.error(f"キャッシュファイル '{CACHE_FILE}' の保存中にエラーが発生しました: {e}", exc_info=True)

async def search_bing(page, query):
    results = []
    try:
        encoded_query = urllib.parse.quote(query)
        search_url = f"https://www.bing.com/search?q={encoded_query}"
        await page.goto(search_url, timeout=90000)

        # メインの wait_for_selector
        try:
            await page.wait_for_selector("ol#b_results", state="visible", timeout=30000)
        except Exception as e:
            logging.warning(f"main selector wait failed, trying fallback body ready check...: {e}")
            await page.wait_for_selector("body", timeout=30000)

        # Bing対策のランダムウェイト
        await asyncio.sleep(random.uniform(5, 10))

        # 検索結果セレクタ
        selectors = [
            "main ol#b_results li.b_data_row h2 a",
            "main ol#b_results li.b_algo h2 a",
            "main ol#b_results li div.b_title h2 a",
            "main li a",
            "main article a",
            "main section a",
            "main div.card a",
        ]

        search_results_elements = []
        for selector in selectors:
            try:
                if page.is_closed():
                    logging.warning("Page is already closed. Skipping remaining selectors.")
                    break

                elements = await page.locator(selector).all()
                if elements:
                    search_results_elements.extend(elements)
                if len(search_results_elements) >= 10:
                    break
            except Exception as e:
                logging.warning(f"[WARN] Selector '{selector}' 取得時にエラー発生: {e}")
                continue

        logging.info(f"[DEBUG] Found {len(search_results_elements)} potential search result links for '{query}'")

        for i, element in enumerate(search_results_elements):
            if i >= 10:
                break
            try:
                title = await element.text_content()
                url = await element.get_attribute("href")

                # スニペット取得
                snippet = "なし"
                try:
                    parent_element = await element.locator(
                        "xpath=ancestor::li | xpath=ancestor::article | xpath=ancestor::section | xpath=ancestor::div"
                    ).first
                    if parent_element:
                        snippet_element_p = parent_element.locator("p").first
                        if await snippet_element_p.count() > 0:
                            snippet = await snippet_element_p.text_content()
                        else:
                            snippet_element_span = parent_element.locator(
                                "span.b_lineclamp3, div.b_text, div.b_richcard_snippet"
                            ).first
                            if await snippet_element_span.count() > 0:
                                snippet = await snippet_element_span.text_content()
                except Exception as e:
                    logging.debug(f"スニペット取得エラー: {e}")
                    snippet = "スニペット取得失敗"

                if url:
                    url = clean_bing_redirect(url)

                logging.debug(f"[{i+1}] Title: {title}")
                logging.debug(f"[{i+1}] Snippet: {snippet}")
                logging.debug(f"[{i+1}] URL: {url}")

                if not title or not url:
                    continue

                results.append({"title": title, "snippet": snippet, "url": url})

            except Exception as e:
                logging.warning(f"[{i+1}] Error parsing search result (title/url/snippet): {e}")

    except Exception as e:
        logging.error(f"Bing検索中にエラーが発生しました（クエリ: {query}）: {e}", exc_info=True)

    return results

async def analyze_company(browser, company_name, processed_companies_tracker, semaphore):
    original_company_name = company_name
    norm_company_name = normalize_company(company_name)
    cache = load_cache()

    # --- Initialize all variables that might be accessed later ---
    context = None
    page = None
    detail_page = None # Initialize detail_page here too

    best_new_name = None
    best_change_date = "変更日不明"
    best_change_reason = "不明"
    best_url = "なし"
    best_snippet = "なし"
    best_score = -float('inf')
    potential_changes_found = False
    # --- End initialization ---

    # --- キャッシュヒット ---
    if norm_company_name in cache:
        logging.info(f"キャッシュヒット: {original_company_name}")
        processed_companies_tracker.add(norm_company_name)
        return cache[norm_company_name]

    try:
        async with semaphore:
            # --- Browser接続チェック ---
            if not browser.is_connected():
                logging.error(f"Browser connection lost before processing company: {original_company_name}. Skipping.")
                result = [original_company_name, "処理失敗", "不明", "不明", "処理失敗", "なし", ""]
                cache[norm_company_name] = result
                save_cache(cache)
                processed_companies_tracker.add(norm_company_name)
                return result

            # --- Browser context & page 作成 ---
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
            page = await context.new_page()

            # --- Bing検索クエリ ---
            query = f'"{original_company_name}" 社名変更 OR 商号変更 OR 新社名 OR 新商号 OR ブランド変更 OR プレスリリース OR 公式サイト OR 沿革 OR IR情報'
            logging.info(f"検索開始: {original_company_name} (クエリ: {query})")

            search_results = await search_bing(page, query)

            # --- スコア順に並べて分析 ---
            scored_results = []
            for result_item in search_results: # Renamed 'result' to 'result_item' to avoid conflict with outer 'result' variable
                title = result_item.get("title")
                snippet = result_item.get("snippet")
                url = result_item.get("url")

                score = result_score(original_company_name, title, snippet, url)

                if is_low_quality(snippet, url):
                    logging.debug(f"低品質判定によりスキップ: URL={url}, Snippet='{snippet}'")
                    continue

                scored_results.append((score, result_item))

            scored_results.sort(key=lambda x: x[0], reverse=True)

            for score, result_item in scored_results:
                title = result_item.get("title")
                snippet = result_item.get("snippet")
                url = result_item.get("url")

                combined_text = (title or "") + " " + (snippet or "")
                new_name_extracted, date_extracted, reason_extracted = extract_info(combined_text, original_company_name)

                # --- 新社名判定 ---
                if new_name_extracted and normalize_company(new_name_extracted) != normalize_company(original_company_name):
                    if not any(bad_name in normalize_company(new_name_extracted) for bad_name in BAD_NAMES_NORMALIZED) and \
                       normalize_company(original_company_name) not in normalize_company(new_name_extracted):
                        potential_changes_found = True
                        if score > best_score:
                            best_score = score
                            best_new_name = new_name_extracted
                            best_change_date = date_extracted
                            best_change_reason = reason_extracted
                            best_url = url
                            best_snippet = snippet
                            break # Found a strong candidate, no need to check weaker ones
                elif new_name_extracted is None and any(kw.lower() in combined_text.lower() for kw in strong_keywords):
                    potential_changes_found = True
                    # If we haven't found a specific new name yet, but found strong keywords,
                    # keep track of the best URL/snippet in case it leads to info later
                    if best_new_name is None and score > best_score:
                        best_score = score
                        best_url = url
                        best_snippet = snippet

            # --- 本文クロール（必要なら） ---
            if best_url != "なし":
                try:
                    logging.info(f"本文クロール開始: {best_url}")
                    detail_page = await browser.new_page() # Create a new page for detailed crawl
                    await detail_page.goto(best_url, timeout=90000, wait_until="domcontentloaded")
                    await detail_page.wait_for_load_state('networkidle', timeout=90000)

                    full_text = await detail_page.evaluate("() => document.body.innerText")
                    # No need to close detail_page here, it's handled in the main finally

                    full_new_name, full_date, full_reason = extract_info(full_text, original_company_name)

                    if full_new_name and normalize_company(full_new_name) != normalize_company(original_company_name) and \
                       not any(bad_name in normalize_company(full_new_name) for bad_name in BAD_NAMES_NORMALIZED) and \
                       normalize_company(original_company_name) not in normalize_company(full_new_name):
                        logging.info(f"本文クロールで新社名発見！ {original_company_name} → {full_new_name}")
                        best_new_name = full_new_name
                        if full_date and full_date != "変更日不明": best_change_date = full_date
                        if full_reason and full_reason != "不明": best_change_reason = full_reason
                    else:
                        logging.info(f"本文クロールで新社名を発見できませんでした: {original_company_name}")

                except Exception as e:
                    logging.error(f"本文クロール中にエラー発生（URL: {best_url}）。詳細: {e}", exc_info=True)
                    # If an error occurs during detail page crawl, ensure detail_page is marked for cleanup
                    if detail_page and not detail_page.is_closed():
                        try:
                            await detail_page.close() # Attempt to close immediately if error occurs
                        except Exception as close_e:
                            logging.warning(f"Error closing detail page after crawl error: {close_e}")
                        finally:
                            detail_page = None # Mark as closed/unavailable

            # --- 最終判定 ---
            status = "変更なし"

            if best_new_name and best_change_date != "変更日不明" and best_change_reason != "不明":
                status = "変更あり"
                logging.info(f"明確な変更検出: {original_company_name} -> {best_new_name}")
            elif potential_changes_found and best_new_name:
                status = "要確認（新社名候補あり）"
                logging.info(f"要確認（新社名候補あり）: {original_company_name} -> 新社名候補: {best_new_name}")
            elif potential_changes_found and best_url != "なし" and best_snippet != "なし" and any(kw.lower() in (best_snippet.lower() + best_url.lower()) for kw in strong_keywords):
                status = "要確認（関連情報検出）"
                logging.info(f"要確認（関連情報検出）: {original_company_name} - 強力キーワード検出のみ")
            else:
                status = "変更なし"
                logging.info(f"変更なしと判断: {original_company_name}")

            result = [
                original_company_name,
                best_new_name if best_new_name else "変更なし",
                best_change_date,
                best_change_reason,
                status,
                best_snippet,
                best_url
            ]

            cache[norm_company_name] = result
            save_cache(cache)
            processed_companies_tracker.add(norm_company_name)
            return result

    except Exception as e:
        logging.error(f"会社名 '{original_company_name}' の処理中に致命的なエラーが発生しました: {e}", exc_info=True)
        # Ensure result is always defined here before being returned
        result = [original_company_name, "処理失敗", "不明", "不明", "処理失敗", "なし", ""]
        cache[norm_company_name] = result
        save_cache(cache)
        processed_companies_tracker.add(norm_company_name)
        return result

    finally:
        # --- Ensure all Playwright resources are closed here ---
        if detail_page and not detail_page.is_closed():
            try:
                await detail_page.close()
            except Exception as e:
                logging.warning(f"Error closing detail page in outer finally: {e}")
            finally:
                detail_page = None # Nullify even if close fails

        if page and not page.is_closed():
            try:
                await page.close()
            except Exception as e:
                logging.warning(f"Error closing main page in outer finally: {e}")
            finally:
                page = None # Nullify even if close fails

        if context: # Context should be closed after its pages
            try:
                await context.close()
            except Exception as e:
                logging.warning(f"Error closing context in outer finally: {e}")
            finally:
                context = None # Nullify even if close fails
                
# --- 4. メイン関数 ---
async def main():
    parser = argparse.ArgumentParser(
        description="""
        会社名リストから社名変更情報を検索し、CSVに出力します。
        重複する会社名はスキップされ、キャッシュも活用されます。
        """,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("input_csv", help="入力 CSVファイル（'会社名'列が必須）")
    parser.add_argument("output_csv", help="出力 CSVファイル")
    parser.add_argument("--max_concurrent_searches", type=int, default=3,
                        help="同時に実行するBing検索の最大数 (デフォルト: 1)")
    parser.add_argument("--headless", action="store_true", help="ヘッドレスモードで実行（ブラウザ画面を表示しない）")
    args = parser.parse_args()

    logging.info("--- 社名変更情報チェッカーを開始します ---")
    logging.info(f"入力ファイル: {args.input_csv}")
    logging.info(f"出力ファイル: {args.output_csv}")
    logging.info(f"キャッシュファイル: {CACHE_FILE}")
    logging.info(f"同時実行検索数: {args.max_concurrent_searches}")

    # --- ヘッドレスモード判定 ---
    headless_mode = args.headless
    logging.info(f"Playwright ヘッドレスモード: {headless_mode}")

    # --- CSV読み込み ---
    df = None
    try:
        df = pd.read_csv(args.input_csv, encoding='utf-8')
        logging.info("CSVファイルを 'utf-8' エンコーディングで読み込みました。")
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(args.input_csv, encoding='utf-8-sig')
            logging.info("CSVファイルを 'utf-8-sig' エンコーディングで読み込みました。")
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(args.input_csv, encoding='cp932')
                logging.info("CSVファイルを 'cp932' (Shift_JIS) エンコーディングで読み込みました。")
            except Exception as e:
                logging.error(f"エラー: 入力 CSVファイルの読み込み中に問題が発生しました: {e}", exc_info=True)
                return

    if df is None:
        logging.error("エラー: CSVファイルの読み込みに失敗しました。")
        return

    if "会社名" not in df.columns:
        logging.error("エラー: 入力 CSVファイルには '会社名' という列が必要です。")
        return

    companies_raw = df["会社名"].dropna().tolist()
    logging.info(f"入力された会社名総数: {len(companies_raw)}社")

    processed_companies_tracker = set()
    tasks = []
    original_company_names_in_order = []
    results_map = {}

    # 統計情報の初期化
    total_companies = len(companies_raw)
    changed_companies = 0
    no_change_companies = 0
    pending_review_companies = 0
    duplicate_companies = 0
    failed_companies = 0
    cache_hits = 0

    # セマフォ
    semaphore = asyncio.Semaphore(args.max_concurrent_searches)

    async with async_playwright() as playwright_instance:
# main関数内
        browser = await playwright_instance.chromium.launch(
    # headless=headless_mode, # <-- This uses the argparse value
        headless=True, # <-- Try forcing headless to True
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-gpu', # Keep this, it helps in many environments
            '--disable-dev-shm-usage',
        # '--single-process', # ENSURE THIS IS REMOVED
            '--no-zygote',
            '--disable-web-security',
            '--ignore-certificate-errors',
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-site-isolation-trials'
        ],
        timeout=120000
    )

        for company_name in companies_raw:
            norm_company_name = normalize_company(company_name)
            current_cache = load_cache()

            if norm_company_name in processed_companies_tracker:
                duplicate_companies += 1
                tasks.append(asyncio.create_task(
                    asyncio.sleep(0, result=[company_name, "スキップ", "スキップ", "スキップ", "重複会社名", "", ""])
                ))
            elif norm_company_name in current_cache:
                cache_hits += 1
                tasks.append(asyncio.create_task(
                    asyncio.sleep(0, result=current_cache[norm_company_name])
                ))
            else:
                tasks.append(asyncio.create_task(analyze_company(browser, company_name, processed_companies_tracker, semaphore)))

            original_company_names_in_order.append(company_name)

        # --- タスク実行 ---
        for future in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="会社名調査中"):
            try:
                result = await future
                norm_name_for_map = normalize_company(result[0])
                results_map[norm_name_for_map] = result

                status = result[4]
                if status == "変更あり":
                    changed_companies += 1
                elif status == "変更なし":
                    no_change_companies += 1
                elif status in ["要確認（新社名候補あり）", "要確認（関連情報検出）"]:
                    pending_review_companies += 1
                elif status == "重複会社名":
                    pass
                elif status == "処理失敗":
                    failed_companies += 1

            except Exception as e:
                logging.error(f"タスク処理中に予期せぬエラーが発生しました: {e}", exc_info=True)
                pass

        await browser.close()

    # --- 結果出力 ---
    df_out_rows = []
    for original_company_name in original_company_names_in_order:
        norm_name = normalize_company(original_company_name)
        result_row = results_map.get(norm_name)

        if result_row:
            if result_row[4] in ["重複会社名", "スキップ"]:
                result_row[0] = original_company_name
            df_out_rows.append(result_row)
        else:
            logging.warning(f"結果が見つかりませんでした: {original_company_name}。 '未処理'として出力します。")
            df_out_rows.append([original_company_name, "未処理", "不明", "不明", "未処理", "なし", ""])

    df_out = pd.DataFrame(df_out_rows, columns=[
        "会社名", "新社名", "変更日", "変更理由", "変更状況", "関連スニペット", "URL"
    ])

    output_excel_file = args.output_csv.replace(".csv", ".xlsx")

    try:
        df_out.to_excel(output_excel_file, index=False)
        logging.info(f"結果をExcelファイル '{output_excel_file}' に出力しました。")
    except Exception as e:
        logging.error(f"Excelファイルへの書き込み中にエラーが発生しました: {e}", exc_info=True)

    logging.info("--- 社名変更情報チェッカーを終了します ---")

# --- エントリーポイント ---
if __name__ == "__main__":
    asyncio.run(main())
