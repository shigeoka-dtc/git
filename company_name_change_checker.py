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
import logging
import datetime
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import AutoFilter

# --- 1. ロギング設定 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("company_name_checker.log", encoding="utf-8"),
        logging.StreamHandler() # コンソールにも出力する場合
    ]
)

# --- 2. グローバル変数・定数定義 ---
CACHE_FILE = "bing_cache_playwright.json"

DOMAIN_PRIORITY = [
    ".co.jp", ".go.jp", ".or.jp", # 日本国内の公的機関、法人
    "prtimes.jp", "news.yahoo.co.jp", "nikkei.com", # プレスリリース、大手ニュース
    "businessinsider.jp", "itmedia.co.jp", "impress.co.jp", # IT系ニュース
    "reuters.com", "asahi.com", "mainichi.jp", "yomiuri.co.jp", # 大手報道機関
    "sankei.com", "jiji.com", "nhk.or.jp", # 大手報道機関
    "irweb.jp", "release.tdnet.info" # IR情報関連
]

LOW_QUALITY_DOMAINS = [
    "genspark.ai", "reflet-office.com", "note.com", "qiita.com", "zenn.dev", # 個人ブログ、AI生成系
    "office-tsuda.net", "advisors-freee.jp", "freee.co.jp", # 事務所、会計系ツールの紹介
    "houmukyoku.moj.go.jp", # 法務局のトップページ（直接のニュースではない）
    "bing.com/ck/a", # BingリダイレクトURL（クリーンアップ後も念のため）
    "ai-con.lawyer", "shiodome.co.jp", "zeiri4.com", "bizocean.jp", # 法律・税務・ビジネス文書テンプレート
    "corporate.ai-con.lawyer", "kaonavi.jp", "legal-script.com", # 企業情報、サービス紹介
    "houmu-news.com", "bengo4.com", "kaisha.tech", "yagi-jimusho.com", # 法務系ニュース、事務所
    "hourei.net", # 法令集サイト
    "gyosei-shoshi.or.jp", "zeirishi-soudan.jp", "pro-kensetsu.com", "g-tax.jp", # 士業関連
    "biz.moneyforward.com", "corp.moneyforward.com", # 会計ソフト系のブログ/情報
    "youtube.com", "twitter.com", "facebook.com", "instagram.com", "linkedin.com", # SNS
    "ja.wikipedia.org", "dic.nicovideo.jp", "encyclopedia-biz.jp", # 百科事典・用語集
    "smbiz.asahi.com", "biz.chosakai.or.jp", # 中小企業支援系
    "jobtag.j-platpat.inpit.go.jp", "tatekae.jp", "sumabase.jp", "ciel-law.jp", # その他の情報サイト
]

EXCLUDE_NAME_PATTERNS = [
    r"正式には", r"通称", r"呼ばれ", r"一般的に", r"略称", r"通名", r"会社名とは", r"社名とは",
    r"変更方法", r"手続き方法", r"よくある質問", r"株式会社の変更", r"合同会社の変更", # 手続きに関するキーワード
    r"会社名変更に伴う", r"商号変更に伴う", r"変更手続き", r"定款変更", r"登記申請",
    r"参考資料", r"当ページ", r"こちら", r"以下", r"について解説", r"〇〇とは" # 参考情報への誘導、説明記事
]

BAD_NAMES = [
    "当ページを参考", "こちら", "不明", "参考", "社名は", "といいます", "正式には", "商号", "社名変更とは",
    "変更後の社名", "新名称", "変更後の名称", "変更後の会社名", "会社名", "代表", "役員", # 汎用的なワード
    "変更される", "変更後の", "現時点での", "変更予定", "変更済", "発表", "決定", "変更",
    "に関する", "について", "の概要", "の変更", "お知らせ", "ニュースリリース", "報道発表",
    "の目的", "のため", "についてのご案内", "株主総会", "定時株主総会", "臨時株主総会",
    "会社概要", "事業内容", "所在地", "代表者", "連絡先", "資本金", "設立年月日",
    "概要", "沿革", "歴史", "変遷", "沿革と社名変更", "組織再編", "事業統合", "吸収合併",
    "新設分割", "吸収分割", "解散", "清算", "承継", "承継会社", "承継元", "承継先",
    "M&A", "子会社化", "グループ会社", "関連会社", "連結子会社", "非連結子会社",
    "事業譲渡", "事業譲受", "業務提携", "資本提携", "提携", "契約", "合弁", "ジョイントベンチャー",
    "システム変更", "システム統合", "システム刷新", "リニューアル", "移転", "新設", "設立",
    "株式会社", "有限会社", "合同会社", "合資会社", "合名会社", # 法人格単独
    "社名" # 社名単独
]

strong_keywords = [
    "新社名", "商号変更", "新商号", "変更予定", "決定", "発表",
    "ニュースリリース", "正式決定", "株主総会", "IR資料", "会社名変更", "移管",
    "登記完了", "効力発生日", "代表取締役", "本店移転", "合併", "分割"
]


# --- 3. 関数定義 ---

def normalize_company(name):
    """会社名を正規化する（空白、法人格などの表記を削除し、小文字化）"""
    if not isinstance(name, str):
        return ""
    name = name.replace("　", "").replace(" ", "").replace("\t", "").strip()
    name = re.sub(r'(?:株式会社|有限会社|合同会社|合資会社|合名会社)$', '', name)
    name = re.sub(r'^(?:株式会社|有限会社|合同会社|合資会社|合名会社)', '', name)
    name = name.replace("コーポレーション", "").replace("グループ", "").replace("ホールディングス", "")
    name = name.replace("インク", "").replace("カンパニー", "").replace("ジャパン", "")
    return name.lower()

BAD_NAMES_NORMALIZED = [normalize_company(name) for name in BAD_NAMES]


def domain_score(url):
    """URLに基づいてドメインスコアを計算する"""
    url = url or ""
    for domain in LOW_QUALITY_DOMAINS:
        if domain in url:
            return -100 # 低品質ドメインは大きく減点
    for i, domain in enumerate(DOMAIN_PRIORITY):
        if domain in url:
            return len(DOMAIN_PRIORITY) - i # 優先度が高いほど高得点
    return 0

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
        "旧商号", "旧社名", "旧法人名" # 旧社名に関する説明ページは低品質とみなす
    ]

    snippet = snippet or ""
    url = url or ""

    if "bing.com/ck/a" in url: # BingのリダイレクトURL自体は低品質とみなす
        return True

    # URLパターンとドメインチェックの結合
    if any(domain in url for domain in LOW_QUALITY_DOMAINS):
        return True

    for kw in low_keywords:
        if kw.lower() in snippet.lower() or kw.lower() in url.lower():
            return True

    return False

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
            pass
    return url

def result_score(company, title, snippet, url):
    """検索結果にスコアを付ける"""
    score = domain_score(url)
    combined_text = (title or "") + " " + (snippet or "")

    normalized_company = normalize_company(company)
    # 会社名がタイトルやスニペットに高頻度で出現するか
    if normalized_company and normalized_company in normalize_company(combined_text):
        score += 10 # 強力なキーワードより高スコア

    # 強力なキーワードが含まれるか
    if any(kw.lower() in combined_text.lower() for kw in strong_keywords):
        score += 8
    
    # PDFは公式IR資料の可能性もあるため、URLが公式ドメインなら加点、それ以外は減点
    if ".pdf" in url.lower():
        if any(d in url.lower() for d in [".co.jp", ".go.jp", ".or.jp"]) or \
           (normalized_company and normalized_company.split('株式会社')[0].lower() in url.lower()):
            score += 5 # 公式PDFは優先
        else:
            score -= 5 # それ以外のPDFは注意

    # サイトのパスに「news」「ir」などが含まれる場合
    if "/news/" in url.lower() or "/ir/" in url.lower() or "/press/" in url.lower() or "/release/" in url.lower():
        score += 3
    
    return score

def extract_info(text, old_name):
    """テキストから新社名、変更日、変更理由を抽出する"""
    text = text.replace("\n", " ").replace("\r", " ").strip()
    text = re.sub(r'\s+', ' ', text) # 複数のスペースを単一スペースに

    # 除外パターンにマッチしたら即座にNoneを返す
    if any(re.search(pat, text) for pat in EXCLUDE_NAME_PATTERNS):
        return None, None, None

    new_name = None
    # 新社名抽出パターンを強化 (より厳密に、かつ多様な表現に対応)
    legal_entities = '(?:株式会社|有限会社|合同会社|合資会社|合名会社|相互会社|特定非営利活動法人|NPO法人|一般社団法人|公益社団法人|一般財団法人|公益財団法人|学校法人|医療法人|社会福祉法人|国立大学法人|独立行政法人|地方独立行政法人|特殊法人|認可法人|国立研究開発法人|国立大学法人|国立高等専門学校機構|国立病院機構|地域医療機能推進機構|日本年金機構|日本郵政株式会社|日本放送協会|日本銀行|日本私立学校振興・共済事業団)'
    company_name_base = r'[^「」\s\(\)（）\-,]{2,80}' # 2文字以上80文字以下の非スペース・非括弧・非記号

    name_patterns = [
        r'(?:社名|商号)(?:を)?「?(' + company_name_base + legal_entities + r'?)」に(?:変更|なる|移行|改称|切り替える|決定)',
        r'(?:新社名|新商号)[は:]?\s*「?(' + company_name_base + legal_entities + r'?)」?(?:となる|に決定|と発表|が正式決定|に移行|に決まりました)',
        r'(' + company_name_base + legal_entities + r'?)へ(?:と)?\s*(?:社名|商号)変更(?:(?:を)?実施|(?:が)?完了)?',
        r'(?:旧社名|旧商号)\s*[:：]\s*[^、。]+?(?:、|。)?\s*(?:新社名|新商号)\s*[:：]\s*(' + company_name_base + legal_entities + r'?)',
        r'(' + company_name_base + legal_entities + r'?)に\s*(?:商号|社名)変更',
        r'(?:社名(?:を)?|商号(?:を)?)?「?(?:株式会社|有限会社|合同会社|合資会社|合名会社)?(' + company_name_base + r'?)」に(?:変更|なる|移行)',
        r'(?:新たな社名が|変更後の社名が)\s*「?(' + company_name_base + legal_entities + r'?)」?(?:です|となりました|に決定した)',
        r'(' + company_name_base + legal_entities + r'?)（(?:旧社名|旧商号)\s*[^）]+?）',
        r'(?:社名|商号)は、?「?(' + company_name_base + legal_entities + r'?)」となります' # 「社名は、〇〇株式会社となります」のようなパターン
    ]

    for pat in name_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            candidate_name = m.group(1).strip()
            # 法人格が末尾にない場合に付加（ただし、日本企業に多い「株式会社」に限定）
            if not re.search(r'(株式会社|有限会社|合同会社|合資会社|合名会社|法人)$', candidate_name) and len(candidate_name) > 1:
                if not any(re.fullmatch(r, candidate_name.lower()) for r in ['ir', 'pr', 'news', 'corp', 'inc', 'co', 'jp', 'com', 'solution', 'group', 'japan']):
                    candidate_name += "株式会社" # デフォルトで株式会社を追加

            candidate_name = re.sub(r'[「」『』（）()]', '', candidate_name).strip()
            candidate_name = re.sub(r'^\s*[、。・\-\/\\]', '', candidate_name).strip() # 先頭の句読点・記号除去

            norm_candidate = normalize_company(candidate_name)
            norm_old_name = normalize_company(old_name)

            if candidate_name and \
               norm_candidate not in BAD_NAMES_NORMALIZED and \
               not norm_candidate.startswith("は") and \
               norm_candidate != norm_old_name and \
               norm_old_name not in norm_candidate and \
               len(candidate_name) > 3 and \
               not re.search(r'変更|手続き|について|解説|情報|社名|商号', candidate_name): # 新社名自体が変更関連の単語でないか
                new_name = candidate_name
                break

    if not new_name:
        return None, None, None

    date = "変更日不明"
    date_patterns = [
        r"(\d{4}年\d{1,2}月\d{1,2}日(?:付)?)(?:をもって|より|から|以降|に)?(?:変更|実施|施行|開始|適用)?",
        r"(\d{4}年\d{1,2}月\d{1,2}日(?:付)?)",
        r"(\d{4}年\d{1,2}月(?:中旬|下旬|上旬)?(?:頃|予定)?)(?:から|より|に)?(?:変更)?",
        r"(\d{4}年\d{1,2}月(?:に|から)?(?:より)?(?:実施)?(?:変更)?)",
        r"(\d{4}年(?:度|期)?)",
        r"(\d{4}年\d{1,2}月)",
        r"(?:令和|平成|昭和|大正|明治)\d{1,2}年\d{1,2}月\d{1,2}日" # 和暦対応
    ]
    for pat in date_patterns:
        date_match = re.search(pat, text)
        if date_match:
            extracted_date = date_match.group(1).strip()
            try:
                # 和暦を西暦に変換する簡易的な処理 (完全ではない)
                if '令和' in extracted_date:
                    year_match = re.search(r'令和(\d{1,2})年', extracted_date)
                    if year_match:
                        rewa_year = int(year_match.group(1))
                        extracted_date = extracted_date.replace(f'令和{rewa_year}年', f'{2018 + rewa_year}年')
                elif '平成' in extracted_date:
                    year_match = re.search(r'平成(\d{1,2})年', extracted_date)
                    if year_match:
                        heisei_year = int(year_match.group(1))
                        extracted_date = extracted_date.replace(f'平成{heisei_year}年', f'{1988 + heisei_year}年')
                
                # 月日だけの形式
                if re.match(r'\d{4}年\d{1,2}月\d{1,2}日', extracted_date):
                    dt_obj = datetime.datetime.strptime(extracted_date, '%Y年%m月%d日')
                    date = dt_obj.strftime('%Y年%m月%d日')
                elif re.match(r'\d{4}年\d{1,2}月', extracted_date):
                    dt_obj = datetime.datetime.strptime(extracted_date, '%Y年%m月')
                    date = dt_obj.strftime('%Y年%m月')
                else:
                    date = extracted_date # パースできない場合はそのまま
            except ValueError:
                date = extracted_date # パースエラーでもそのままの文字列を保持
            break

    reason = "不明"
    reason_patterns = [
        r'(?:変更理由[は:]?|理由は|背景は|目的は|社名変更の背景[は:]?|商号変更の理由[は:]?)([^。、「」\s]{3,200}?)。',
        r'目的[は:]?([^。、「」\s]{3,200}?)。',
        r'に伴い([^。、「」\s]{3,200}?)。',
        r'(?:組織再編|事業再編|経営統合|グループ再編|合併|分割|事業譲渡|持株会社化)\s*(?:による|のため|に伴い)\s*([^。、「」\s]{3,200}?)。',
        r'(?:新たなブランド戦略|グローバル展開|企業価値向上|ガバナンス強化|事業拡大|企業イメージの刷新|経営戦略の強化|成長戦略の推進|企業体制の強化|事業再編に伴う|合併による)([^。、「」\s]{3,200}?)。',
        r'(?:経営体制の強化|ブランドイメージの刷新|事業の多角化|事業領域の拡大|企業価値の最大化|経営の効率化)\s*(?:を目的として|を図るため|のため|により|に伴い)\s*([^。、「」\s]{3,200}?)。'
    ]
    # 理由として不適切な表現を除外するキーワードリスト
    bad_reason_keywords = [
        "詳細はこちら", "詳しくは", "参考資料", "参照元", "当社の事業", "当社グループ",
        "プレスリリース", "開示資料", "以下参照", "以下に記載", "上記に記載", "本書をご確認", "当ページ",
        "ご案内", "お知らせ", "ニュース", "全文", "掲載", "PDF", "添付"
    ]

    for pat in reason_patterns:
        reason_match = re.search(pat, text)
        if reason_match:
            candidate_reason = reason_match.group(1).strip()
            if not any(bad_r.lower() in candidate_reason.lower() for bad_r in bad_reason_keywords):
                reason = candidate_reason
                break
            else:
                reason = "不明"
    
    if reason == "不明":
        if re.search(r'(経営統合|合併|吸収合併|会社分割|事業譲渡|持株会社化|組織再編|グループ再編)', text):
            reason = "経営統合・事業再編のため"
        elif re.search(r'(ブランド統一|グローバル展開|企業価値向上|事業拡大|企業イメージ)', text):
            reason = "事業戦略・ブランド戦略のため"

    return new_name, date, reason

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.error(f"Cache file '{CACHE_FILE}' is corrupted. Recreating.")
            return {}
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

async def search_bing(browser, company):
    page = None
    try:
        page = await browser.new_page() # 共有されたブラウザインスタンスから新しいページを作成
        query = f"{company} 社名変更 OR 商号変更 OR 新社名"
        url = f"https://www.bing.com/search?q={urllib.parse.quote(query)}"

        await page.goto(url, wait_until='domcontentloaded')
        
        try:
            await page.wait_for_selector("li.b_algo", timeout=15000)
        except Exception:
            logging.warning(f"[{company}] No search results selector found within timeout or page not loaded.")
            pass

        await page.wait_for_load_state('networkidle', timeout=20000)
        await page.wait_for_timeout(random.randint(1000, 3000)) # ランダムな待機時間

        elements = await page.query_selector_all("li.b_algo")
        results = []
        for elem in elements[:15]: # 上位15件を処理
            try:
                title = await elem.query_selector("h2")
                snippet_elem = await elem.query_selector(".b_caption")
                link_elem = await elem.query_selector("a")

                title_text = await title.inner_text() if title else ""
                snippet_text = await snippet_elem.inner_text() if snippet_elem else ""
                link_url = await link_elem.get_attribute("href") if link_elem else ""

                results.append((title_text + "\n" + snippet_text, snippet_text, link_url))
            except Exception as e:
                logging.debug(f"[{company}] Failed to extract result element: {e}")
                continue

        return results
    except Exception as e:
        logging.error(f"[{company}] Error during Bing search: {e}", exc_info=True)
        return []
    finally:
        if page:
            await page.close() # 各タスクの実行後にページを閉じる

async def analyze_company(browser, company, processed_companies_tracker): # 引数をbrowserに変更
    """
    一社ずつ会社名を分析し、社名変更情報を抽出する。
    重複する会社名はスキップし、その旨を結果に含める。
    """
    cache = load_cache()
    norm_company_name = normalize_company(company)

    if norm_company_name in processed_companies_tracker:
        logging.info(f"[SKIP - DUPLICATE] {company}")
        return [company, "スキップ", "スキップ", "スキップ", "重複会社名", "", ""] # スニペットとURLも空
    
    if norm_company_name in cache:
        logging.info(f"[CACHE HIT] {company}")
        processed_companies_tracker.add(norm_company_name) # キャッシュヒットも処理済みに追加
        return cache[norm_company_name]
    
    # 新規処理の会社としてマーク
    processed_companies_tracker.add(norm_company_name)

    try:
        logging.info(f"[SEARCH] {company}")
        results = await search_bing(browser, company) # browserを渡す

        results_sorted = sorted(
            [r for r in results if not is_low_quality(r[1], r[2])],
            key=lambda x: result_score(company, x[0], x[1], x[2]),
            reverse=True
        )

        found_info = False
        for full_text, snippet, url in results_sorted:
            cleaned_url = clean_bing_redirect(url)
            new_name, date, reason = extract_info(full_text, company)
            
            if new_name:
                result = [company, new_name, date, reason, "変更あり", snippet or "なし", cleaned_url or ""]
                cache[norm_company_name] = result
                save_cache(cache)
                found_info = True
                return result
        
        # 変更情報が見つからなかった場合
        if not found_info:
            snippet_to_save = "なし"
            cleaned_url_to_save = ""
            status = "変更なし" 
            
            if results_sorted:
                top_snippet = results_sorted[0][1] or ""
                snippet_to_save = top_snippet
                cleaned_url_to_save = clean_bing_redirect(results_sorted[0][2]) or ""
                
                # 強いキーワードがあるが情報抽出に失敗した場合
                if any(kw.lower() in top_snippet.lower() for kw in strong_keywords):
                    status = "要確認（情報不足）"
                    logging.warning(f"[{company}] Detected strong keywords but failed to extract info. Snippet: {top_snippet[:100]}...")
                else:
                    status = "変更なし"

            result = [company, "変更なし", "変更日不明", "不明", status, snippet_to_save, cleaned_url_to_save]
            cache[norm_company_name] = result
            save_cache(cache)
            return result

    except Exception as e:
        logging.error(f"[ERROR] {company}: {e}", exc_info=True)
        error_result = [company, "エラー", "不明", "不明", "処理失敗", str(e), ""]
        cache[norm_company_name] = error_result
        save_cache(cache)
        return error_result

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
    args = parser.parse_args()

    logging.info("--- 社名変更情報チェッカーを開始します ---")
    logging.info(f"入力ファイル: {args.input_csv}")
    logging.info(f"出力ファイル: {args.output_csv}")
    logging.info(f"キャッシュファイル: {CACHE_FILE}")

    try:
        df = pd.read_csv(args.input_csv)
    except FileNotFoundError:
        logging.error(f"エラー: 入力 CSVファイルが見つかりません。パスを確認してください: {args.input_csv}")
        return
    except Exception as e:
        logging.error(f"エラー: 入力 CSVファイルの読み込み中に問題が発生しました: {e}", exc_info=True)
        return

    if "会社名" not in df.columns:
        logging.error("エラー: 入力 CSVファイルには '会社名' という列が必要です。")
        return

    companies_raw = df["会社名"].dropna().tolist()
    logging.info(f"入力された会社名総数: {len(companies_raw)}社")

    processed_companies_tracker = set()
    tasks = []
    original_company_names_in_order = [] 
    results_map = {} # main関数内でresults_mapを定義

    # 統計情報の初期化
    total_companies = len(companies_raw)
    changed_companies = 0
    no_change_companies = 0
    pending_review_companies = 0
    duplicate_companies = 0
    failed_companies = 0
    cache_hits = 0

    async with async_playwright() as playwright_instance:
        browser = await playwright_instance.chromium.launch(headless=True) # ブラウザインスタンスを一度だけ起動
        
        for company_name in companies_raw:
            norm_company_name = normalize_company(company_name)
            if norm_company_name in processed_companies_tracker:
                duplicate_companies += 1
                tasks.append(asyncio.create_task(
                    asyncio.sleep(0, result=[company_name, "スキップ", "スキップ", "スキップ", "重複会社名", "", ""]) # ダミータスク
                ))
            elif norm_company_name in load_cache(): # キャッシュヒットもここでカウント
                cache_hits += 1
                tasks.append(asyncio.create_task(
                    asyncio.sleep(0, result=load_cache()[norm_company_name]) # ダミータスク
                ))
            else:
                tasks.append(asyncio.create_task(analyze_company(browser, company_name, processed_companies_tracker)))
            
            original_company_names_in_order.append(company_name)


        # 全てのタスクを並列で実行し、完了したものから結果を収集
        for future in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="会社名調査中"):
            try:
                result = await future
                norm_name_for_map = normalize_company(result[0])
                results_map[norm_name_for_map] = result

                # 統計情報の更新
                status = result[4] # "変更状況"
                if status == "変更あり":
                    changed_companies += 1
                elif status == "変更なし":
                    no_change_companies += 1
                elif status == "要確認（情報不足）":
                    pending_review_companies += 1
                elif status == "処理失敗":
                    failed_companies += 1

            except Exception as e:
                logging.error(f"タスク処理中に予期せぬエラーが発生しました: {e}", exc_info=True)
                pass

        await browser.close() # 全てのタスク完了後にブラウザを閉じる

    df_out_rows = []
    for original_company_name in original_company_names_in_order:
        norm_name = normalize_company(original_company_name)
        
        result_row = results_map.get(norm_name)
        
        if result_row:
            df_out_rows.append(result_row)
        else:
            # results_mapに結果が見つからない（何らかの理由でタスクが結果を返さなかった）場合のフォールバック
            logging.warning(f"結果が見つかりませんでした: {original_company_name}。 '未処理'として出力します。")
            df_out_rows.append([original_company_name, "未処理", "不明", "不明", "未処理", "なし", ""])

    df_out = pd.DataFrame(df_out_rows, columns=[
        "会社名", "新社名", "変更日", "変更理由", "変更状況", "関連スニペット", "URL"
    ])
    
    # Excel出力
    output_excel_file = args.output_csv.replace(".csv", ".xlsx") # .csvを.xlsxに置換
    try:
        wb = Workbook()
        ws_main = wb.active
        ws_main.title = "会社名変更詳細"

        # ヘッダーの書き込み
        headers = ["会社名", "新社名", "変更日", "変更理由", "変更状況", "関連スニペット", "URL"]
        ws_main.append(headers)

        # ヘッダーのスタイル設定
        header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid") # 薄い青
        header_font = Font(bold=True)
        for col_idx, cell in enumerate(ws_main[1]):
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(wrap_text=True, vertical='top')

        # データの書き込みと書式設定
        row_idx = 2
        for row_data in df_out.itertuples(index=False):
            # URLがハイパーリンクとして認識されるように
            row_list = list(row_data)
            if row_list[6] and row_list[6] != "なし":
                row_list[6] = f'=HYPERLINK("{row_list[6]}", "リンク")' # Excelでハイパーリンク化

            ws_main.append(row_list)
            
            # 条件付き書式設定
            status = row_list[4] # "変更状況"列
            if status == "変更あり":
                fill_color = "E0F7FA" # 薄い水色
            elif status == "要確認（情報不足）":
                fill_color = "FFFDE7" # 薄い黄色
            elif status == "重複会社名":
                fill_color = "E0E0E0" # 薄いグレー
            elif status == "処理失敗":
                fill_color = "FFEBEE" # 薄い赤色
            else:
                fill_color = None # 変更なしは色なし

            if fill_color:
                for col_cell in ws_main[row_idx]:
                    col_cell.fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
            
            # 日付列の書式設定（文字列として保持）
            date_cell = ws_main.cell(row=row_idx, column=3) # '変更日' は3列目
            date_cell.number_format = '@' # テキストとして強制

            # セルの折り返し
            for col_cell in ws_main[row_idx]:
                col_cell.alignment = Alignment(wrap_text=True, vertical='top')
            
            row_idx += 1

        # 列幅の自動調整（ある程度の最大幅を設定し、無限に広がらないようにする）
        for col_idx, column in enumerate(ws_main.columns):
            max_length = 0
            column_letter = get_column_letter(col_idx + 1)
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            # 特定の列は広めに設定
            if column_letter == 'F': # 関連スニペット
                adjusted_width = min(adjusted_width, 100) # 最大幅を100に制限
            elif column_letter == 'G': # URL
                adjusted_width = min(adjusted_width, 50) # 最大幅を50に制限
            else:
                adjusted_width = min(adjusted_width, 30) # それ以外の列の最大幅
            ws_main.column_dimensions[column_letter].width = adjusted_width

        # オートフィルタの設定
        ws_main.auto_filter.ref = ws_main.dimensions

        # サマリーシートの作成
        ws_summary = wb.create_sheet("サマリー")
        ws_summary.append(["統計項目", "数値"])
        ws_summary.append(["総会社数", total_companies])
        ws_summary.append(["社名変更あり", changed_companies])
        ws_summary.append(["社名変更なし", no_change_companies])
        ws_summary.append(["要確認（情報不足）", pending_review_companies])
        ws_summary.append(["重複会社名（スキップ）", duplicate_companies])
        ws_summary.append(["処理失敗", failed_companies])
        ws_summary.append(["キャッシュヒット数", cache_hits])
        ws_summary.append(["調査実行数（キャッシュ除く）", total_companies - duplicate_companies - cache_hits])

        # サマリーシートの書式設定
        for col_idx, column in enumerate(ws_summary.columns):
            max_length = 0
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            ws_summary.column_dimensions[get_column_letter(col_idx + 1)].width = max(max_length + 2, 20) # 最小幅を20に

        # サマリーヘッダーのスタイル
        for col_idx, cell in enumerate(ws_summary[1]):
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(wrap_text=True, vertical='top')

        wb.save(output_excel_file)
        logging.info(f"✅ 全ての処理が完了し、結果が '{output_excel_file}' に保存されました。")
    except Exception as e:
        logging.error(f"エラー: 結果Excelファイル '{output_excel_file}' の保存中に問題が発生しました: {e}", exc_info=True)

    logging.info("--- 社名変更情報チェッカーを終了します ---")

# --- 5. エントリーポイント ---
if __name__ == "__main__":
    asyncio.run(main())