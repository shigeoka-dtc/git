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
    level=logging.INFO, # デバッグ時は logging.DEBUG に変更
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
    r"一覧\s*|リスト\s*|データベース\s*|企業情報",
    r"商号変更とは", r"社名変更とは", r"法人登記", r"登記変更", r"申請方法", r"費用", r"行政書士", r"司法書士",
    r"弁護士", r"顧問契約", r"コンサル", r"フォーム", r"ひな形", r"テンプレート", r"書式", r"サンプル",
    r"ブログ", r"コラム", r"まとめ", r"ニュース記事", r"報道", r"プレスリリース", # 一般的な記事も排除
    r"Q\&A", r"FAQ", r"よくある質問", r"無料相談", r"税理士", r"会計士"
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
    "社名", "新会社", "旧会社", "既存会社", "設立", "合併", "変更日", "効力発生日", "変更期日",
    "登記完了日", "変更登記", "年月日", "〇〇年", "〇〇月", "〇〇日", "〇〇日より" # 日付関連も追加
]

strong_keywords = [
    "新社名", "商号変更", "新商号", "社名変更に関するお知らせ", "正式決定", "発表", "ニュースリリース",
    "IR資料", "会社名変更", "効力発生日", "合併", "分割", "吸収合併", "経営統合", "事業再編",
    "ブランド統合", "組織再編", "新生", "商号変更のお知らせ", "社名変更のお知らせ", "社名変更に関するお知らせ",
    "社名変更決議", "社名変更決議のお知らせ", "旧社名", "旧商号", "社名変更の目的", "変更理由", "変更の経緯",
    "新体制", "新組織"
]

LEGAL_ENTITIES_RE = r'(?:株式会社|有限会社|合同会社|合資会社|合名会社|相互会社|特定非営利活動法人|NPO法人|一般社団法人|公益社団法人|一般財団法人|公益財団法人|学校法人|医療法人|社会福祉法人|国立大学法人|独立行政法人|地方独立行政法人|特殊法人|認可法人|国立研究開発法人|国立大学法人|国立高等専門学校機構|国立病院機構|地域医療機能推進機構|日本年金機構|日本郵政株式会社|日本放送協会|日本銀行|日本私立学校振興・共済事業団)'
LEGAL_ENTITIES_OPT_RE = r'(?:株式会社|有限会社|合同会社|合資会社|合名会社|相互会社|特定非営利活動法人|NPO法人|一般社団法人|公益社団法人|一般財団法人|公益財団法人|学校法人|医療法人|社会福祉法人|国立大学法人|独立行政法人|地方独立行政法人|特殊法人|認可法人|国立研究開発法人|国立大学法人|国立高等専門学校機構|国立病院機構|地域医療機能推進機構|日本年金機構|日本郵政株式会社|日本放送協会|日本銀行|日本私立学校振興・共済事業団)?'

# --- normalize_company ---
def normalize_company(name):
    """会社名を正規化する（空白、法人格などの表記を削除し、小文字化）"""
    if not isinstance(name, str):
        return ""
    name = name.replace("　", "").replace(" ", "").replace("\t", "").strip()
    name = re.sub(r'(?:株|有|合|合同|合資|合名)\s*会社$', '', name) # (株)などを削除
    name = re.sub(r'^(株|有|合|合同|合資|合名)\s*会社', '', name) # (株)などを削除
    name = re.sub(LEGAL_ENTITIES_RE + r'[\(\)（）\-\.・]*$', '', name)
    name = re.sub(r'^[\(\)（）\-\.・]*' + LEGAL_ENTITIES_RE, '', name)
    name = name.replace("コーポレーション", "").replace("グループ", "").replace("ホールディングス", "")
    name = name.replace("インク", "").replace("カンパニー", "").replace("ジャパン", "")
    name = name.replace("・", "").replace("（", "").replace("）", "").replace("/", "") # スラッシュも除去
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
        "上場企業", "商号変更会社一覧", "日本取引所グループ",
        "変更手続き", "変更点", "ポイント", "影響", "メリット", "デメリット",
        "登記申請書", "登記の", "届出", "変更登記", "変更届"
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
    
    # 会社名がタイトルやスニペットに含まれているか（正規化後）
    if normalized_company and normalized_company in normalize_company(combined_text):
        score += 10
    
    # URLに会社名が含まれているか（正規化後、法人格除去）
    # 例: panasonic.co.jp のように、法人格なしの会社名でドメインに含まれる場合
    if normalized_company and normalized_company.replace("株式会社", "").replace("有限会社", "").replace("合同会社", "") in url.lower():
        score += 7

    # 強力なキーワードが含まれているか
    if any(kw.lower() in combined_text.lower() for kw in strong_keywords):
        score += 8
    
    # PDFファイルの場合、公式サイト系であれば高評価、それ以外は減点
    if ".pdf" in url.lower():
        if any(d in url.lower() for d in [".co.jp", ".go.jp", ".or.jp"]) or \
           (normalized_company and normalized_company.split('株式会社')[0].lower() in url.lower()) or \
           ("ir.pdf" in url.lower() or "release.pdf" in url.lower()): # IRやリリース系のPDFは高評価
            score += 5
        else:
            score -= 5 # 一般のPDFは減点

    # URLパスに公式発表系のキーワードが含まれているか
    if any(path_part in url.lower() for path_part in ["/news/", "/ir/", "/press/", "/release/", "/company/", "/profile/", "/history/", "/about/", "/info/", "/information/"]):
        score += 5

    # SNSやWikiは低評価
    if "wikipedia.org" in url.lower() or "twitter.com" in url.lower() or "linkedin.com" in url.lower() or "facebook.com" in url.lower():
        score -= 10 # より減点

    # 公式サイトらしきURLであるか（重要度を上げる）
    parsed_url = urllib.parse.urlparse(url)
    domain_parts = parsed_url.netloc.split('.')
    # 例: company.co.jp -> company を抽出
    main_domain = domain_parts[-2] if len(domain_parts) >= 2 else domain_parts[0]
    if normalize_company(company).replace("株式会社", "") in main_domain.lower():
        score += 15 # 公式サイト可能性が高いものはさらに高評価
        if parsed_url.path == "/" or parsed_url.path == "/index.html": # トップページに近いURL
            score += 5

    return score

# --- extract_info ---
def extract_info(text, old_name):
    """テキストから新社名、変更日、変更理由を抽出する"""
    text = text.replace("\n", " ").replace("\r", " ").strip()
    text = re.sub(r'\s+', ' ', text)

    # 最初に除外パターンをチェック
    if any(re.search(pat, text, re.IGNORECASE) for pat in EXCLUDE_NAME_PATTERNS):
        logging.debug(f"EXCLUDE_NAME_PATTERNSにより除外: {text[:50]}...")
        return None, None, None

    new_name = None
    # 会社名のベースパターンをより柔軟に (2文字以上、記号や空白を許可)
    company_name_base = r'(?:[^\s、。「」（）()\-]{2,80}?)'

    # 強力な新社名抽出パターン (順序が重要)
    name_patterns = [
        # 例: 新社名は「株式会社〇〇」となります
        r'(?:新社名|新商号|新名称)\s*[:：は、]?\s*「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?\s*(?:となります|に変更|することを決定|を発表|といたします|に決定)',
        # 例: 株式会社〇〇に社名変更
        r'(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*に(?:社名|商号|名称)変更(?:しました|いたします|いたしました|を発表|することが決定|することを決定|といたします)',
        # 例: (旧：XXX) 新：YYY のようなパターン (ただし、正規表現が複雑になるため、ここでは新社名候補に絞る)
        r'(?:新：|新社名：|新商号：)\s*「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?',
        # 例: 旧社名：XXX、新社名：YYY
        r'(?:旧社名|旧商号)\s*[:：は、]\s*(?:[^、。]+?)(?:、|。)?\s*(?:新社名|新商号)\s*[:：は、]?\s*「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?',
        # 例: 株式会社〇〇（旧：XXX）
        r'(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*（(?:旧社名|旧商号|旧名称)[\s:]*[^）]+?）',
        # 例: 株式会社Aと株式会社Bが合併し、株式会社Cに
        r'(?:合併|統合|事業再編)(?:により|して)?\s*「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?(?:となります|になる|に変更|することを決定|といたします)',
        # 例: 〇〇株式会社として新たにスタート
        r'(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*として新たにスタート(?:します|いたしました)?',
        # 例: 〇〇株式会社へ商号変更
        r'(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*へ商号変更(?:しました|いたします)?',
        # 例: 社名を「株式会社〇〇」に変更
        r'社名を\s*「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?\s*に変更(?:しました|いたしました)?',
        # 例: 「株式会社〇〇」に社名変更 (冒頭近く)
        r'「?(' + company_name_base + LEGAL_ENTITIES_RE + r'?)」?\s*に社名変更',
        # 例: 株式会社Aが株式会社Bに商号を変更
        re.escape(old_name.replace('株式会社', LEGAL_ENTITIES_OPT_RE)) + r'\s*が\s*(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*に商号を変更',
        # 例: パナソニック株式会社 (旧松下電器産業株式会社)
        r'(' + company_name_base + LEGAL_ENTITIES_RE + r'?)\s*\(旧' + re.escape(normalize_company(old_name)) + r'\)'
    ]

    found_potential_name = False
    for i, pat in enumerate(name_patterns):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            candidate_name = m.group(1).strip()
            norm_candidate = normalize_company(candidate_name)
            norm_old_name = normalize_company(old_name)

            logging.debug(f"Pattern {i+1} matched. Candidate: '{candidate_name}', Norm Candidate: '{norm_candidate}', Norm Old: '{norm_old_name}'")

            # フィルタリングの強化
            if len(norm_candidate) < 2:
                logging.debug(f"  -> Candidate too short. Skipping.")
                continue
            if norm_candidate == norm_old_name:
                logging.debug(f"  -> Candidate is same as old name. Skipping.")
                continue
            # 元の名前を含むが、明らかに新しい名前でない場合 (例: "ABC" -> "ABCホールディングス" はOK, "ABC" -> "ABC" はNG)
            if norm_old_name in norm_candidate and len(norm_candidate) <= len(norm_old_name) + 5 and \
               not (norm_candidate.startswith(norm_old_name) and (norm_candidate.endswith("ホールディングス") or norm_candidate.endswith("グループ"))):
                logging.debug(f"  -> Old name contained in candidate, but not sufficiently different or just legal form change. Skipping.")
                continue
            
            # 抽出された候補がBAD_NAMESに含まれるかチェック
            if any(bad_name in norm_candidate for bad_name in BAD_NAMES_NORMALIZED):
                # ただし、正規の法人格が付与されていれば、BAD_NAMESに引っかかっても採用するケースも考慮
                if not re.search(LEGAL_ENTITIES_RE, candidate_name):
                     logging.debug(f"  -> Candidate '{candidate_name}' matched BAD_NAMES_NORMALIZED and no legal entity. Skipping.")
                     continue
                
            # 法人格が付いていなければ「株式会社」を付与（ただし、元のテキストに法人格がない場合はそのままにする）
            if not re.search(LEGAL_ENTITIES_RE, candidate_name) and not re.search(LEGAL_ENTITIES_RE, old_name):
                # 元の社名も法人格がない場合（例：Apple -> Apple Japan）は無理に付けない
                pass
            elif not re.search(LEGAL_ENTITIES_RE, candidate_name) and len(candidate_name) > 2: # 2文字以上で法人格がなければ付与を検討
                candidate_name += "株式会社" # 一旦付与して再評価

            # 不要な記号や空白を除去
            candidate_name = re.sub(r'[「」『』（）()]', '', candidate_name).strip()
            candidate_name = re.sub(r'^\s*[、。・\-\/\\]', '', candidate_name).strip()

            final_norm_candidate = normalize_company(candidate_name)
            
            # 再度チェック (より厳密に)
            if candidate_name and len(final_norm_candidate) >= 2 and \
               final_norm_candidate != norm_old_name and \
               not (norm_old_name in final_norm_candidate and len(final_norm_candidate) <= len(norm_old_name) + 5): # 厳密な包含チェック
                
                # 新社名が旧社名と完全に一致する場合の除外（正規化後の比較）
                if final_norm_candidate == normalize_company(old_name):
                    logging.debug(f"  -> Final candidate '{candidate_name}' is still same as normalized old name. Skipping.")
                    continue

                new_name = candidate_name
                logging.debug(f"  -> New name found: '{new_name}'")
                found_potential_name = True
                break # 最も適切なパターンが見つかったら終了

    date = "変更日不明"
    date_patterns = [
        r"(\d{4}年\d{1,2}月\d{1,2}日(?:付)?)(?:より|から)?(?:変更|実施|施行|開始|決定|発効|いたします|いたしました|をもちまして)",
        r"(?:変更日|実施日|効力発生日)[:：は、]?\s*(\d{4}年\d{1,2}月\d{1,2}日)",
        r"(\d{4}年\d{1,2}月\d{1,2}日)", # 例: 2023年4月1日
        r"(\d{4}年\d{1,2}月(?:から|より)?)", # 例: 2023年4月から
        r"(\d{4}年\d{1,2}月)", # 例: 2023年4月
        r"(\d{4}年)", # 例: 2023年
        r"(?:(?:令和|平成|昭和)?\d{1,2}年\d{1,2}月\d{1,2}日)", # 和暦対応
        r"(?:効力発生日を|変更日を|実施日を|決議日を)\s*(\d{4}年\d{1,2}月\d{1,2}日)",
    ]

    for pat in date_patterns:
        date_match = re.search(pat, text)
        if date_match:
            date = date_match.group(1).strip()
            date = date.replace("付", "").replace("より", "").replace("から", "").replace("をもちまして", "").strip()
            logging.debug(f"Date found: {date}")
            break

    reason = "不明"
    reason_patterns = [
        r'(?:変更理由|理由は|背景は|目的は|経緯は)[^。]{3,200}。', # 理由の文全体を捉える
        r'(?:変更理由は|背景は)\s*「?([^」。「]{10,100}?)」?(?:です|となります|ことをお知らせします|ため|と発表した)',
        r'(?:ブランド統一|グローバル展開|企業価値向上|事業再編|経営統合|M&A|吸収合併|事業譲渡|効率化|多様化|グループ連携強化|成長戦略|企業イメージ刷新|創立\d+周年記念|事業体制再編|再編|リブランディング|商号変更の目的|企業グループの再編|持株会社体制への移行|新たな成長のため|多様な事業展開に対応|ブランドイメージの統一|創業\d+周年記念)',
        r'(?:グループ組織再編の一環|経営統合の一環|グループ経営の強化のため|新たな企業文化の創造|より機動的な経営体制の構築)'
    ]

    for pat in reason_patterns:
        reason_match = re.search(pat, text, re.IGNORECASE)
        if reason_match:
            extracted_reason = reason_match.group(0).strip()
            if extracted_reason.endswith("。") or extracted_reason.endswith("ため"):
                reason = extracted_reason
            else:
                reason = extracted_reason + "のため" # 不完全な理由を補完
            logging.debug(f"Reason found: {reason}")
            break

    if reason == "不明": # より広い範囲で理由を推定 (キーワードベース)
        if re.search(r'(ブランド統一|グローバル展開|企業価値向上|事業再編|経営統合|M&A|吸収合併|事業譲渡|効率化|多様化|グループ連携強化|成長戦略|企業イメージ刷新|ホールディングス体制|持株会社体制|再編|組織再編|事業統合|業務提携|資本提携|経営戦略|成長戦略)', text, re.IGNORECASE):
            reason = "事業戦略・組織再編のため"
        elif re.search(r'(周年|記念|節目|創業|設立)', text, re.IGNORECASE):
            reason = "創立記念・節目を機に"
        elif re.search(r'(本社移転|拠点移転|移転に伴い|新拠点)', text, re.IGNORECASE):
            reason = "拠点移転に伴い"
        elif re.search(r'(上場|新規公開|上場準備)', text, re.IGNORECASE):
            reason = "上場に伴う変更"
        
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
        logging.info(f"Bing検索ページへ移動: {search_url}")
        
        await page.goto(search_url, timeout=90000, wait_until="domcontentloaded")

        try:
            await page.wait_for_selector("ol#b_results", state="visible", timeout=30000)
            logging.debug("ol#b_results appeared.")
        except Exception as e:
            logging.warning(f"ol#b_results selector wait failed, trying fallback body ready check...: {e}")
            await page.wait_for_load_state('networkidle', timeout=30000)
            logging.warning("Network idle state reached, but main results container might not be visible.")

        await asyncio.sleep(random.uniform(5, 10)) # Bing対策のランダムウェイト

        # 検索結果セレクタ (より多くのパターンを試す)
        search_result_selectors = [
            "li.b_data_row h2 a", # Primary results
            "li.b_algo h2 a",     # Common results
            "div.b_title h2 a",   # Another common title selector
            "a[href^='http']",    # Any link with an http href within main content (fallback)
            "div.b_attribution a", # Sometimes link is inside attribution
        ]
        
        snippet_selectors = [
            "p.b_lineclamp3",
            "div.b_snippet",
            "div.b_text",
            "div.b_richcard_snippet",
            "div.b_attribution + p", # Sometimes snippet is sibling to attribution
            "div.b_caption p",       # Another common pattern for snippets
            "span.ac_algo_bodyline", # Sometimes seen in search result snippets
            "div[class*='snippet'], div[class*='desc']", # Generic snippet classes
            "div[id*='snippet'], div[id*='desc']", # Generic snippet ids
            "div.l_eccl div.b_vlist", # For some list-style results
        ]

        search_results_elements = []
        for selector in search_result_selectors:
            try:
                if page.is_closed():
                    logging.warning("Page is already closed. Skipping remaining selectors.")
                    break
                
                elements = await page.locator(selector).all()
                if elements:
                    search_results_elements.extend(elements)
                if len(search_results_elements) >= 20: # より多くの結果を取得し、後でスコアで選別
                    break
            except Exception as e:
                logging.debug(f"[DEBUG] Selector '{selector}' 取得時にエラー発生: {e}")
                continue
        
        unique_elements = {}
        for el in search_results_elements:
            href = await el.get_attribute("href")
            # 重複判定の前に、BingのリダイレクトURLをクリーンアップ
            if href:
                cleaned_href = clean_bing_redirect(href)
                unique_elements[cleaned_href] = el # クリーンアップされたURLをキーに
        search_results_elements = list(unique_elements.values())
        
        logging.info(f"[DEBUG] Found {len(search_results_elements)} unique potential search result links for '{query}'")

        for i, element in enumerate(search_results_elements):
            if i >= 20: # 実際に処理するのは上位20件まで
                break
            try:
                title = await element.text_content()
                url = await element.get_attribute("href")
                url = clean_bing_redirect(url) # ここでもクリーンアップ

                snippet = "なし"
                parent_element = None
                try:
                    # タイトル要素の親要素を探す（より多くのパターン）
                    parent_element = await element.locator(
                        "xpath=ancestor::li[@class*='b_algo'] | xpath=ancestor::li[@class*='b_data_row'] | xpath=ancestor::article | xpath=ancestor::section | xpath=ancestor::div[@class*='b_rs'] | xpath=ancestor::div[contains(@class, 'c_container')] | xpath=ancestor::div[contains(@class, 'serp-item')] | xpath=ancestor::div[contains(@class, 'search-result')] | xpath=ancestor::div[contains(@class, 'news-card')] | xpath=ancestor::div[contains(@class, 'algo-entry')] | xpath=ancestor::div[contains(@class, 'results-item')] | xpath=ancestor::div[contains(@class, 'card-component')]"
                    ).first
                    
                    if not parent_element: # より広い範囲で親要素を探すフォールバック
                        parent_element = await element.locator("xpath=ancestor::div").first
                        
                    if parent_element:
                        for snip_sel in snippet_selectors:
                            try:
                                snippet_el = parent_element.locator(snip_sel).first
                                if await snippet_el.count() > 0:
                                    snippet_text = await snippet_el.text_content()
                                    if snippet_text.strip(): # 空白でないことを確認
                                        snippet = snippet_text
                                        logging.debug(f"スニペット取得成功（セレクタ: {snip_sel}）: {snippet[:50]}...")
                                        break
                            except Exception as snip_e:
                                logging.debug(f"スニペットセレクタ '{snip_sel}' でエラー: {snip_e}")
                        
                        if snippet == "なし": # 最終手段として、親要素のinnerText全体をスニペットとして考慮
                            try:
                                full_parent_text = await parent_element.text_content()
                                # タイトルを除去し、適切な長さにトリミング
                                if title in full_parent_text:
                                    temp_snippet = full_parent_text.replace(title, "").strip()
                                else:
                                    temp_snippet = full_parent_text.strip()

                                if len(temp_snippet) > 50: # 短すぎる場合は採用しない
                                    snippet = temp_snippet[:500] + ("..." if len(temp_snippet) > 500 else "")
                                    logging.debug(f"スニペット取得（親要素のinnerText）: {snippet[:50]}...")
                                else:
                                    snippet = "スニペット取得失敗" # 短すぎる場合は失敗と見なす
                            except Exception as full_text_e:
                                logging.debug(f"親要素innerTextからのスニペット取得失敗: {full_text_e}")
                                snippet = "スニペット取得失敗"
                    else:
                        logging.debug("親要素が見つからずスニペット取得をスキップ。")
                        snippet = "スニペット取得失敗"

                except Exception as e:
                    logging.debug(f"スニペット取得エラー（全体）: {e}")
                    snippet = "スニペット取得失敗"

                if not title or not url:
                    logging.debug(f"タイトルまたはURLが空のためスキップ: Title='{title}', URL='{url}'")
                    continue

                results.append({"title": title, "snippet": snippet, "url": url})
                logging.debug(f"[{i+1}] Title: {title}")
                logging.debug(f"[{i+1}] Snippet: {snippet[:100]}...")
                logging.debug(f"[{i+1}] URL: {url}")

            except Exception as e:
                logging.warning(f"[{i+1}] Error parsing search result (title/url/snippet): {e}", exc_info=True)

    except Exception as e:
        logging.error(f"Bing検索中にエラーが発生しました（クエリ: {query}）: {e}", exc_info=True)

    return results

async def analyze_company(browser, company_name, processed_companies_tracker, semaphore):
    original_company_name = company_name
    norm_company_name = normalize_company(company_name)
    cache = load_cache()

    context = None
    page = None
    detail_page = None 

    best_new_name = None
    best_change_date = "変更日不明"
    best_change_reason = "不明"
    best_url = "なし"
    best_snippet = "なし"
    best_score = -float('inf')
    potential_changes_found = False
    
    # --- キャッシュヒット ---
    if norm_company_name in cache:
        logging.info(f"キャッシュヒット: {original_company_name}")
        processed_companies_tracker.add(norm_company_name)
        return cache[norm_company_name]

    try:
        async with semaphore:
            if not browser.is_connected():
                logging.error(f"Browser connection lost before processing company: {original_company_name}. Skipping.")
                result = [original_company_name, "処理失敗", "不明", "不明", "処理失敗", "なし", ""]
                cache[norm_company_name] = result
                save_cache(cache)
                processed_companies_tracker.add(norm_company_name)
                return result

            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
            page = await context.new_page()

            query = f'"{original_company_name}" 社名変更 OR 商号変更 OR 新社名 OR 新商号 OR ブランド変更 OR プレスリリース OR 公式サイト OR 沿革 OR IR情報 OR "旧社名" OR "旧商号"'
            logging.info(f"検索開始: {original_company_name} (クエリ: {query})")

            search_results = await search_bing(page, query)

            # --- スコア順に並べて分析 ---
            scored_results = []
            for result_item in search_results:
                title = result_item.get("title")
                snippet = result_item.get("snippet")
                url = result_item.get("url")

                score = result_score(original_company_name, title, snippet, url)

                if is_low_quality(snippet, url):
                    logging.debug(f"低品質判定によりスキップ: URL={url}, Snippet='{snippet[:50]}'")
                    continue

                scored_results.append((score, result_item))

            scored_results.sort(key=lambda x: x[0], reverse=True)
            logging.debug(f"スコア順の検索結果 (上位5件): {[f'{s:.2f}:{r.get("title")}' for s, r in scored_results[:5]]}")

            # --- 公式サイトらしきURLを優先的に本文クロール対象とする ---
            target_url_for_crawl = None
            norm_original_name_for_url = normalize_company(original_company_name).replace("株式会社", "").replace("有限会社", "").replace("合同会社", "")
            
            # まずはスコアの高い順にチェック
            for score, result_item in scored_results:
                url = result_item.get("url")
                parsed_url = urllib.parse.urlparse(url)
                domain = parsed_url.netloc

                # ドメインが会社名と一致するか、または非常に類似しているか
                # 例: panasonic.co.jp, skymap.co.jp
                if norm_original_name_for_url in normalize_company(domain):
                    # 特にトップページや会社概要ページに近いパスを優先
                    if parsed_url.path in ["/", "/index.html", "/company/", "/about/", "/profile/", "/news/", "/ir/"]:
                        target_url_for_crawl = url
                        logging.info(f"公式サイト候補（高優先）を検出: {target_url_for_crawl}")
                        break
                    elif target_url_for_crawl is None: # 最初のマッチを保持 (より明確なものがなければ)
                        target_url_for_crawl = url
                        logging.info(f"公式サイト候補（中優先）を検出: {target_url_for_crawl}")

            # 公式サイトらしきURLが見つからなければ、最もスコアの高いURLを使用
            if target_url_for_crawl is None and scored_results:
                target_url_for_crawl = scored_results[0][1].get("url")
                logging.info(f"公式サイト候補なし。最もスコアの高いURLをクロール対象に設定: {target_url_for_crawl}")
            elif not scored_results:
                logging.info("検索結果がありませんでした。")


            # --- 本文クロール（最も有力なURLに対して実行） ---
            if target_url_for_crawl:
                try:
                    logging.info(f"本文クロール開始: {target_url_for_crawl}")
                    detail_page = await browser.new_page()
                    await detail_page.goto(target_url_for_crawl, timeout=90000, wait_until="domcontentloaded")
                    await detail_page.wait_for_load_state('networkidle', timeout=90000)
                    
                    full_text = await detail_page.evaluate("() => document.body.innerText")
                    logging.debug(f"本文クロールテキスト（冒頭）: {full_text[:500]}...")

                    full_new_name, full_date, full_reason = extract_info(full_text, original_company_name)

                    if full_new_name:
                        norm_full_new_name = normalize_company(full_new_name)
                        norm_original_company_name = normalize_company(original_company_name)
                        
                        # 本文から抽出された新社名が適切か再チェック
                        if len(norm_full_new_name) >= 2 and \
                           not any(bad_name in norm_full_new_name for bad_name in BAD_NAMES_NORMALIZED) and \
                           norm_full_new_name != norm_original_company_name and \
                           not (norm_original_company_name in norm_full_new_name and len(norm_full_new_name) <= len(norm_original_company_name) + 5):
                            
                            logging.info(f"本文クロールで新社名発見！ {original_company_name} -> {full_new_name}")
                            best_new_name = full_new_name
                            best_url = target_url_for_crawl # 本文クロールしたURLを最良URLとする
                            best_snippet = "本文から情報取得" # スニペットは本文から取得した旨を明示
                            if full_date and full_date != "変更日不明": best_change_date = full_date
                            if full_reason and full_reason != "不明": best_change_reason = full_reason
                            potential_changes_found = True # 本文から見つかったので変更可能性ありとする
                        else:
                            logging.info(f"本文クロールで抽出された新社名候補 '{full_new_name}' は不適切。")
                    else:
                        logging.info(f"本文クロールで新社名を発見できませんでした: {original_company_name}")
                        # 本文から日時や理由だけ抽出できた場合は更新
                        if full_date and full_date != "変更日不明": best_change_date = full_date
                        if full_reason and full_reason != "不明": best_change_reason = full_reason

                except Exception as e:
                    logging.error(f"本文クロール中にエラー発生（URL: {target_url_for_crawl}）。詳細: {e}", exc_info=True)
                finally:
                    if detail_page and not detail_page.is_closed():
                        try:
                            await detail_page.close()
                        except Exception as close_e:
                            logging.warning(f"Error closing detail page after crawl error: {close_e}")
                        finally:
                            detail_page = None

            # --- 本文クロールで新社名が見つからなかった場合、検索結果スニペットから再度判定 ---
            if best_new_name is None:
                for score, result_item in scored_results:
                    title = result_item.get("title")
                    snippet = result_item.get("snippet")
                    url = result_item.get("url")

                    combined_text = (title or "") + " " + (snippet or "")
                    new_name_extracted, date_extracted, reason_extracted = extract_info(combined_text, original_company_name)

                    if new_name_extracted:
                        norm_new_name_extracted = normalize_company(new_name_extracted)
                        norm_original_company_name = normalize_company(original_company_name)
                        
                        if len(norm_new_name_extracted) < 2 or \
                           any(bad_name in norm_new_name_extracted for bad_name in BAD_NAMES_NORMALIZED) or \
                           (norm_new_name_extracted == norm_original_company_name) or \
                           (norm_original_company_name in norm_new_name_extracted and len(norm_new_name_extracted) < len(norm_original_company_name) + 5):
                            logging.debug(f"抽出されたスニペット新社名候補 '{new_name_extracted}' が不適切と判断されスキップ。")
                            new_name_extracted = None
                            
                    if new_name_extracted:
                        potential_changes_found = True
                        if score > best_score: # スニペットからの情報がより良いスコアであれば更新
                            best_score = score
                            best_new_name = new_name_extracted
                            best_change_date = date_extracted if date_extracted != "変更日不明" else best_change_date
                            best_change_reason = reason_extracted if reason_extracted != "不明" else best_change_reason
                            best_url = url
                            best_snippet = snippet
                            logging.debug(f"最適なスニペットからの新社名候補を更新: {best_new_name} (スコア: {score:.2f})")
                            break # スニペットから見つかったら、これ以上スニペットを処理する必要はない

                    elif any(kw.lower() in combined_text.lower() for kw in strong_keywords):
                        potential_changes_found = True
                        if score > best_score and best_new_name is None:
                            best_score = score
                            best_url = url
                            best_snippet = snippet
                            logging.debug(f"スニペットで強力キーワード検出。最適なURL/スニペット候補を更新 (スコア: {score:.2f})")

            # --- 最終判定 ---
            status = "変更なし"

            if best_new_name and best_change_date != "変更日不明" and best_change_reason != "不明":
                status = "変更あり"
                logging.info(f"明確な変更検出: {original_company_name} -> {best_new_name}, 日付: {best_change_date}, 理由: {best_change_reason}")
            elif best_new_name:
                status = "要確認（新社名候補あり）"
                logging.info(f"要確認（新社名候補あり）: {original_company_name} -> 新社名候補: {best_new_name}, 日付: {best_change_date}, 理由: {best_change_reason}")
            elif potential_changes_found and best_url != "なし": # スニペット取得失敗でも関連情報があれば要確認
                status = "要確認（関連情報検出）"
                logging.info(f"要確認（関連情報検出）: {original_company_name} - 強力キーワード検出のみ, URL: {best_url}")
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
        result = [original_company_name, "処理失敗", "不明", "不明", "処理失敗", "なし", ""]
        cache[norm_company_name] = result
        save_cache(cache)
        processed_companies_tracker.add(norm_company_name)
        return result

    finally:
        if detail_page and not detail_page.is_closed():
            try:
                await detail_page.close()
            except Exception as e:
                logging.warning(f"Error closing detail page in outer finally: {e}")
            finally:
                detail_page = None

        if page and not page.is_closed():
            try:
                await page.close()
            except Exception as e:
                logging.warning(f"Error closing main page in outer finally: {e}")
            finally:
                page = None

        if context:
            try:
                await context.close()
            except Exception as e:
                logging.warning(f"Error closing context in outer finally: {e}")
            finally:
                context = None
                
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
                         help="同時に実行するBing検索の最大数 (デフォルト: 3)")
    parser.add_argument("--headless", action="store_true", help="ヘッドレスモードで実行（ブラウザ画面を表示しない）")
    args = parser.parse_args()

    logging.info("--- 社名変更情報チェッカーを開始します ---")
    logging.info(f"入力ファイル: {args.input_csv}")
    logging.info(f"出力ファイル: {args.output_csv}")
    logging.info(f"キャッシュファイル: {CACHE_FILE}")
    logging.info(f"同時実行検索数: {args.max_concurrent_searches}")

    headless_mode = args.headless
    logging.info(f"Playwright ヘッドレスモード: {headless_mode}")

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

    total_companies = len(companies_raw)
    changed_companies = 0
    no_change_companies = 0
    pending_review_companies = 0
    duplicate_companies = 0
    failed_companies = 0
    cache_hits = 0

    semaphore = asyncio.Semaphore(args.max_concurrent_searches)

    async with async_playwright() as playwright_instance:
        browser = await playwright_instance.chromium.launch(
            headless=headless_mode,
            args=[
                '--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu',
                '--disable-dev-shm-usage', '--no-zygote', '--disable-web-security',
                '--ignore-certificate-errors', '--no-first-run',
                '--no-default-browser-check', '--disable-site-isolation-trials'
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

    df_out_rows = []
    for original_company_name in original_company_names_in_order:
        norm_name = normalize_company(original_company_name)
        result_row = results_map.get(norm_name)

        if result_row:
            if result_row[4] == "重複会社名":
                df_out_rows.append([original_company_name, "スキップ", "N/A", "N/A", "重複会社名", "N/A", "N/A"])
            else:
                # オリジナルの会社名で出力し、その他の情報も最新のものを使う
                final_row = list(result_row)
                final_row[0] = original_company_name # 会社名列を元の名前に戻す
                df_out_rows.append(final_row)
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