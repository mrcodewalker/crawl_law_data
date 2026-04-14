import os
import sys
import json
import time
import random
import logging
import re
import threading
import base64
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.errors import ElementNotFoundError

try:
    import ddddocr
    _ocr = ddddocr.DdddOcr(show_ad=False)
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    log_tmp = logging.getLogger(__name__)
    log_tmp.warning("ddddocr chưa cài — captcha sẽ cần xử lý thủ công. Cài: pip install ddddocr")

# ── Config ──────────────────────────────────────────────────────────────────
SEARCH_BASE_URL = (
    "https://thuvienphapluat.vn/page/tim-van-ban.aspx"
    "?keyword=&area=0&match=True&type=0&status=0&signer=0"
    "&edate=13/04/2026&sort=1&lan=1&scan=0&org=1&fields="
)

OUTPUT_DIR     = "output"
VISITED_FILE   = "visited.json"
DATA_FILE      = "data.jsonl"
HTML_FILE      = "html.jsonl"
STATE_FILE     = "state.json"   # lưu trang hiện tại của mỗi worker để resume
SAVE_HTML_FILE = True
INCLUDE_HREF   = False

DELAY_MIN       = 2
DELAY_MAX       = 5
BATCH_SIZE      = 3    # số bài rồi nghỉ dài
BATCH_PAUSE_MIN = 6
BATCH_PAUSE_MAX = 14

# Worker 1: trang 1→100   → output/0-100/
# Worker 2: trang 101→200 → output/101-200/
WORKERS = [
    {"id": 1, "page_start": 1,   "page_end": 100, "out_folder": "0-100"},
    {"id": 2, "page_start": 101, "page_end": 200, "out_folder": "101-200"},
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Shared state ─────────────────────────────────────────────────────────────
_visited_lock = threading.Lock()
_write_lock   = threading.Lock()
_visited: set = set()
_done_event   = threading.Event()


# ── Helpers ──────────────────────────────────────────────────────────────────
def load_visited() -> set:
    if os.path.exists(VISITED_FILE):
        with open(VISITED_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_visited():
    with open(VISITED_FILE, "w", encoding="utf-8") as f:
        json.dump(list(_visited), f, ensure_ascii=False, indent=2)


# ── State (resume) ────────────────────────────────────────────────────────────
def load_state() -> dict:
    """Trả về dict {worker_id: current_page}"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(worker_id: int, page: int):
    with _write_lock:
        state = load_state()
        state[str(worker_id)] = page
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)


def append_data(record: dict, out_folder: str):
    d = os.path.join(OUTPUT_DIR, out_folder)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, DATA_FILE), "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def append_html_record(record: dict, out_folder: str):
    d = os.path.join(OUTPUT_DIR, out_folder)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, HTML_FILE), "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def make_driver() -> ChromiumPage:
    opts = ChromiumOptions()
    opts.auto_port()
    opts.set_argument("--window-size=1280,900")
    opts.set_argument("--no-sandbox")
    opts.set_argument("--disable-dev-shm-usage")
    return ChromiumPage(addr_or_opts=opts)


def search_url(page: int) -> str:
    return f"{SEARCH_BASE_URL}&page={page}"


# ── Cloudflare wait ───────────────────────────────────────────────────────────
def wait_cloudflare(driver: ChromiumPage, label: str = ""):
    cf_warned = False
    for _ in range(60):
        t = driver.title
        if "Just a moment" in t or "Checking your browser" in t:
            if not cf_warned:
                log.warning("  ⚠ Cloudflare%s — vui lòng click checkbox...",
                             f" [{label}]" if label else "")
                cf_warned = True
            time.sleep(2)
        else:
            if cf_warned:
                log.info("  ✓ Đã qua Cloudflare")
            break


def solve_captcha_image(driver: ChromiumPage) -> str:
    """Lấy ảnh captcha từ /RegistImage.aspx và OCR bằng ddddocr."""
    try:
        img_b64: str = driver.run_js("""
            var img = document.querySelector("img[src*='RegistImage']");
            if (!img) return '';
            var canvas = document.createElement('canvas');
            canvas.width = img.naturalWidth || img.width;
            canvas.height = img.naturalHeight || img.height;
            canvas.getContext('2d').drawImage(img, 0, 0);
            return canvas.toDataURL('image/png').split(',')[1];
        """)
        if not img_b64:
            return ""
        img_bytes = base64.b64decode(img_b64)
        if OCR_AVAILABLE:
            result = _ocr.classification(img_bytes)
            log.info("  OCR captcha: '%s'", result)
            return result.strip().upper()
    except Exception as e:
        log.debug("solve_captcha_image lỗi: %s", e)
    return ""


def handle_captcha(driver: ChromiumPage) -> bool:
    """
    Kiểm tra và xử lý captcha Robot của thuvienphapluat.
    Trả về True nếu pass (hoặc không có captcha).
    """
    try:
        robot_el = driver.ele("css:td[colspan='3']", timeout=2)
        if not robot_el or "Robot" not in (robot_el.text or ""):
            return True
    except Exception:
        return True

    log.warning("  ⚠ Phát hiện captcha Robot — đang OCR...")
    for attempt in range(3):
        code = solve_captcha_image(driver)
        if not code:
            log.warning("  OCR thất bại lần %d, chờ 3s...", attempt + 1)
            time.sleep(3)
            driver.refresh()
            time.sleep(2)
            continue
        try:
            inp = driver.ele("#ctl00_Content_txtSecCode", timeout=3)
            btn = driver.ele("#ctl00_Content_CheckButton", timeout=3)
            if inp and btn:
                inp.clear()
                inp.input(code)
                btn.click()
                time.sleep(2)
                robot_check = driver.ele("css:td[colspan='3']", timeout=2)
                if not robot_check or "Robot" not in (robot_check.text or ""):
                    log.info("  ✓ Captcha giải thành công")
                    return True
                log.warning("  Captcha sai lần %d, thử lại...", attempt + 1)
                time.sleep(2)
        except Exception as e:
            log.debug("handle_captcha submit lỗi: %s", e)

    log.warning("  ✗ Không giải được captcha sau 3 lần, chờ 30s...")
    time.sleep(30)
    return False


def clean_content_html(tab: ChromiumPage) -> str:
    try:
        tab.run_js("""
            ['.__mucluc','.NoiDungChiaSe','#hdsdcondau','script'].forEach(sel => {
                document.querySelectorAll('#tab1 ' + sel).forEach(el => el.remove());
            });
        """)
        raw: str = tab.run_js(
            "var e=document.getElementById('tab1'); return e ? e.outerHTML : '';"
        )
        if not raw:
            return ""
        raw = re.sub(r'<!--.*?-->', '', raw, flags=re.DOTALL)
        raw = re.sub(r'\s+on\w+="[^"]*"', '', raw)
        raw = re.sub(r"\s+on\w+='[^']*'", '', raw)
        raw = re.sub(
            r'\s*background-image\s*:\s*url\((?:&quot;|\'|")?[^)]*(?:&quot;|\'|")?\)\s*;?',
            '', raw)
        if not INCLUDE_HREF:
            raw = re.sub(r'\s+href="[^"]*"', '', raw)
            raw = re.sub(r"\s+href='[^']*'", '', raw)
        raw = re.sub(r'\n{3,}', '\n\n', raw)
        return raw.strip()
    except Exception as e:
        log.debug("clean_content_html: %s", e)
        return ""


def build_full_html(title: str, content_html: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
  body {{ font-family: Arial, sans-serif; background: #f5f5f5; margin: 0; padding: 20px; }}
  #tab1 {{ width:100% !important; max-width:900px !important; margin:0 auto !important;
           background:#fff; padding:30px !important; box-sizing:border-box; }}
  #divContentDoc, .cldivContentDocVn {{ width:100% !important; float:none !important; margin:0 !important; }}
  #ctl00_Content_ThongTinVB_pnlDocContent {{ width:100% !important; }}
  #divContentDoc > div > div > table:first-of-type,
  .content1 > div > div > table:first-of-type {{ width:100% !important; }}
  #divContentDoc > div > div > table:first-of-type td,
  .content1 > div > div > table:first-of-type td {{ width:50% !important; }}
</style>
</head>
<body>
{content_html}
</body>
</html>"""


def save_html_file(title: str, content_html: str, out_folder: str):
    d = os.path.join(OUTPUT_DIR, out_folder)
    os.makedirs(d, exist_ok=True)
    safe = re.sub(r'[\\/*?:"<>|]', '', title).strip()[:120] or "van_ban"
    with open(os.path.join(d, safe + ".html"), "w", encoding="utf-8") as f:
        f.write(build_full_html(title, content_html))
    log.info("    → Lưu: %s", safe[:60])


def scrape_metadata(tab: ChromiumPage) -> dict:
    meta, label_map = {}, {
        "Số hiệu": "so_hieu", "Loại văn bản": "loai_van_ban",
        "Nơi ban hành": "noi_ban_hanh", "Người ký": "nguoi_ky",
        "Ngày ban hành": "ngay_ban_hanh", "Ngày hiệu lực": "ngay_hieu_luc",
        "Ngày hết hiệu lực": "ngay_het_hieu_luc", "Tình trạng": "tinh_trang",
        "Cập nhật": "cap_nhat", "Lĩnh vực": "linh_vuc",
    }
    try:
        rows = tab.eles("css:.right-col tr, .box-thuoc-tinh tr, .thuoc-tinh tr, #tab2 tr")
        for row in rows:
            try:
                cells = row.eles("tag:td")
                if len(cells) >= 2:
                    key = label_map.get(cells[0].text.strip().rstrip(":"))
                    if key:
                        meta[key] = cells[1].text.strip()
            except Exception:
                continue
    except Exception as e:
        log.debug("scrape_metadata: %s", e)
    return meta


# ── Scrape 1 văn bản trong tab mới ───────────────────────────────────────────
def scrape_doc_in_new_tab(driver: ChromiumPage, url: str, out_folder: str) -> bool:
    """Mở tab mới, scrape, đóng tab. Tab gốc (search) không bị ảnh hưởng."""
    tab = None
    try:
        tab = driver.new_tab(url)
        tab.wait.doc_loaded(timeout=30)
        wait_cloudflare(tab, url.split("/")[-1][:40])

        if not tab.ele("#tab1", timeout=30):
            log.warning("  Không tìm thấy #tab1: %s", url)
            return False

        title = ""
        el = tab.ele("css:h1.title-vb, h1", timeout=3)
        if el:
            title = el.text.strip()

        meta       = scrape_metadata(tab)
        content_el = tab.ele("css:#tab1", timeout=3)
        raw_html   = clean_content_html(tab)
        doc_title  = title or url.split("/")[-1]

        data_record = {
            "url": url, "title": title,
            "content": content_el.text.strip() if content_el else "",
            "meta": meta,
        }
        html_record = {
            "url": url, "title": title,
            "content_html": build_full_html(doc_title, raw_html),
        }
        html_record.update(meta)

        with _write_lock:
            append_data(data_record, out_folder)
            append_html_record(html_record, out_folder)
            save_visited()

        if SAVE_HTML_FILE and raw_html:
            save_html_file(doc_title, raw_html, out_folder)

        return True

    except Exception as e:
        log.warning("  scrape_doc lỗi %s: %s", url, e)
        return False
    finally:
        if tab:
            try:
                tab.close()
            except Exception:
                pass


# ── Lấy URL văn bản từ trang search (tab gốc) ────────────────────────────────
def get_doc_urls_from_search_page(driver: ChromiumPage, page: int) -> list[str]:
    url = search_url(page)
    try:
        driver.get(url)
        wait_cloudflare(driver, f"search p={page}")
        # Xử lý captcha nếu có
        if not handle_captcha(driver):
            log.warning("Bỏ qua trang %d do captcha không giải được.", page)
            return []
        # Chờ danh sách kết quả load — thử nhiều selector
        for sel in ["css:.result-item", "css:.list-vb li", "css:.nqTitle",
                    "css:a[href*='/van-ban/'][href*='.aspx']"]:
            el = driver.ele(sel, timeout=10)
            if el:
                break
        time.sleep(1)
    except Exception as e:
        log.warning("Không tải được trang search p=%d: %s", page, e)
        return []

    urls, seen = [], set()
    # Selector rộng: bắt mọi link /van-ban/*.aspx trên trang
    try:
        for a in driver.eles("css:a[href*='/van-ban/']"):
            href = a.attr("href") or ""
            if ".aspx" in href and "thuvienphapluat.vn/van-ban/" in href:
                clean = href.split("?")[0].split("#")[0]
                if clean not in seen:
                    seen.add(clean)
                    urls.append(clean)
    except Exception as e:
        log.debug("get_doc_urls: %s", e)

    log.info("  [search p=%d] %d URL văn bản", page, len(urls))
    return urls


# ── Worker ────────────────────────────────────────────────────────────────────
def worker(cfg: dict):
    wid        = cfg["id"]
    out_folder = cfg["out_folder"]
    page_start = cfg["page_start"]
    page_end   = cfg["page_end"]

    # Resume: lấy trang đã làm dở từ state
    state = load_state()
    resume_page = state.get(str(wid))
    if resume_page and resume_page > page_start:
        log.info("Worker %d resume từ trang %d (state)", wid, resume_page)
        page_start = resume_page

    driver = make_driver()
    log.info("Worker %d khởi động (trang %d–%d → %s/)", wid, page_start, page_end, out_folder)
    doc_count = 0

    try:
        for page in range(page_start, page_end + 1):
            if _done_event.is_set():
                break

            log.info("[W%d] === Trang tìm kiếm %d ===", wid, page)
            doc_urls = get_doc_urls_from_search_page(driver, page)

            if not doc_urls:
                log.warning("[W%d] Trang %d rỗng, thử trang tiếp.", wid, page)
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                save_state(wid, page + 1)
                continue

            for url in doc_urls:
                if _done_event.is_set():
                    break

                with _visited_lock:
                    if url in _visited:
                        log.debug("  [W%d] Bỏ qua (đã crawl): %s", wid, url.split("/")[-1][:50])
                        continue
                    _visited.add(url)

                log.info("  [W%d] → %s", wid, url.split("/")[-1][:60])
                ok = scrape_doc_in_new_tab(driver, url, out_folder)
                if ok:
                    doc_count += 1

                if doc_count % BATCH_SIZE == 0:
                    pause = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                    log.info("  [W%d] Nghỉ batch %.1fs...", wid, pause)
                    time.sleep(pause)
                else:
                    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            # Lưu state sau khi xong 1 trang
            save_state(wid, page + 1)
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX + 2))

    except Exception as e:
        log.error("Worker %d lỗi nghiêm trọng: %s", wid, e)
    finally:
        driver.quit()
        log.info("Worker %d dừng. Đã crawl %d văn bản.", wid, doc_count)


# ── Main ──────────────────────────────────────────────────────────────────────
def crawl(reset: bool = False):
    global _visited

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for cfg in WORKERS:
        os.makedirs(os.path.join(OUTPUT_DIR, cfg["out_folder"]), exist_ok=True)

    if reset:
        if os.path.exists(VISITED_FILE):
            os.remove(VISITED_FILE)
            log.info("Đã xóa: %s", VISITED_FILE)
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
            log.info("Đã xóa: %s", STATE_FILE)
        for cfg in WORKERS:
            folder = os.path.join(OUTPUT_DIR, cfg["out_folder"])
            for fname in [DATA_FILE, HTML_FILE]:
                fpath = os.path.join(folder, fname)
                if os.path.exists(fpath):
                    os.remove(fpath)
                    log.info("Đã xóa: %s", fpath)

    _visited = load_visited()
    _done_event.clear()

    threads = []
    for i, cfg in enumerate(WORKERS):
        t = threading.Thread(target=worker, args=(cfg,), daemon=True,
                             name=f"worker-{cfg['id']}")
        t.start()
        threads.append(t)
        if i < len(WORKERS) - 1:
            time.sleep(5)  # stagger Chrome

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        log.info("Dừng bởi người dùng.")
        _done_event.set()
        for t in threads:
            t.join(timeout=5)

    log.info("Hoàn thành.")


if __name__ == "__main__":
    crawl(reset="--reset" in sys.argv)
