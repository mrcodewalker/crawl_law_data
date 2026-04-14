import os
import sys
import json
import time
import random
import logging
import re
import threading
from queue import Queue, Empty
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.errors import ElementNotFoundError

# ── Config ──────────────────────────────────────────────────────────────────
START_URL = "https://thuvienphapluat.vn/van-ban/Giao-thong-Van-tai/Nghi-dinh-89-2026-ND-CP-dieu-kien-kinh-doanh-dich-vu-kiem-dinh-xe-co-gioi-688213.aspx"
OUTPUT_DIR = "output"
VISITED_FILE = "visited.json"
DATA_FILE = "data.jsonl"
HTML_FILE = "html.jsonl"
SAVE_HTML_FILE = True
DELAY_MIN = 3          # giây nghỉ tối thiểu sau mỗi URL
DELAY_MAX = 7          # giây nghỉ tối đa
BATCH_SIZE = 2         # mỗi worker crawl bao nhiêu URL rồi nghỉ dài
BATCH_PAUSE_MIN = 5    # nghỉ tối thiểu sau mỗi batch
BATCH_PAUSE_MAX = 12   # nghỉ tối đa sau mỗi batch
IDLE_TIMEOUT = 60      # giây chờ URL mới trước khi worker tự dừng
MAX_DOCS = 100
NUM_WORKERS = 3
INCLUDE_HREF = False   # True: giữ href trong HTML output

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Shared state ─────────────────────────────────────────────────────────────
_visited_lock = threading.Lock()
_write_lock = threading.Lock()
_counter_lock = threading.Lock()
_visited: set = set()
_counter: int = 0
_url_queue: Queue = Queue()
_done_event = threading.Event()  # set khi đạt MAX_DOCS hoặc queue cạn hẳn


# ── Helpers ──────────────────────────────────────────────────────────────────
def load_visited() -> set:
    if os.path.exists(VISITED_FILE):
        with open(VISITED_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_visited():
    with open(VISITED_FILE, "w", encoding="utf-8") as f:
        json.dump(list(_visited), f, ensure_ascii=False, indent=2)


def append_data(record: dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, DATA_FILE), "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def append_html(record: dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, HTML_FILE), "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def make_driver() -> ChromiumPage:
    opts = ChromiumOptions()
    opts.auto_port()
    opts.set_argument("--window-size=1280,900")
    opts.set_argument("--no-sandbox")
    opts.set_argument("--disable-dev-shm-usage")
    return ChromiumPage(addr_or_opts=opts)


# ── Core scraping ─────────────────────────────────────────────────────────────
def clean_content_html(driver: ChromiumPage) -> str:
    try:
        driver.run_js("""
            ['.__mucluc', '.NoiDungChiaSe', '#hdsdcondau', 'script'].forEach(sel => {
                document.querySelectorAll('#tab1 ' + sel).forEach(el => el.remove());
            });
        """)
        raw_html: str = driver.run_js(
            "var el=document.getElementById('tab1'); return el ? el.outerHTML : '';"
        )
        if not raw_html:
            return ""
        raw_html = re.sub(r'<!--.*?-->', '', raw_html, flags=re.DOTALL)
        raw_html = re.sub(r'\s+on\w+="[^"]*"', '', raw_html)
        raw_html = re.sub(r"\s+on\w+='[^']*'", '', raw_html)
        raw_html = re.sub(r'\s*background-image\s*:\s*url\((?:&quot;|\'|")?[^)]*(?:&quot;|\'|")?\)\s*;?', '', raw_html)
        if not INCLUDE_HREF:
            raw_html = re.sub(r'\s+href="[^"]*"', '', raw_html)
            raw_html = re.sub(r"\s+href='[^']*'", '', raw_html)
        raw_html = re.sub(r'\n{3,}', '\n\n', raw_html)
        return raw_html.strip()
    except Exception as e:
        log.debug("clean_content_html lỗi: %s", e)
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
  #tab1 {{ width: 100% !important; max-width: 900px !important; margin: 0 auto !important;
           background: #fff; padding: 30px !important; box-sizing: border-box; }}
  #divContentDoc, .cldivContentDocVn {{ width: 100% !important; float: none !important; margin: 0 !important; }}
  #ctl00_Content_ThongTinVB_pnlDocContent {{ width: 100% !important; }}
  #divContentDoc > div > div > table:first-of-type,
  .content1 > div > div > table:first-of-type {{ width: 100% !important; }}
  #divContentDoc > div > div > table:first-of-type td,
  .content1 > div > div > table:first-of-type td {{ width: 50% !important; }}
</style>
</head>
<body>
{content_html}
</body>
</html>"""


def save_html_file(title: str, content_html: str):
    html_dir = os.path.join(OUTPUT_DIR, "html")
    os.makedirs(html_dir, exist_ok=True)
    safe_name = re.sub(r'[\\/*?:"<>|]', '', title).strip()[:120] or "van_ban"
    with open(os.path.join(html_dir, safe_name + ".html"), "w", encoding="utf-8") as f:
        f.write(build_full_html(title, content_html))
    log.info("  → Đã lưu HTML: %s", safe_name)


def scrape_metadata(driver: ChromiumPage) -> dict:
    meta = {}
    label_map = {
        "Số hiệu": "so_hieu", "Loại văn bản": "loai_van_ban",
        "Nơi ban hành": "noi_ban_hanh", "Người ký": "nguoi_ky",
        "Ngày ban hành": "ngay_ban_hanh", "Ngày hiệu lực": "ngay_hieu_luc",
        "Ngày hết hiệu lực": "ngay_het_hieu_luc", "Tình trạng": "tinh_trang",
        "Cập nhật": "cap_nhat", "Lĩnh vực": "linh_vuc",
    }
    try:
        rows = driver.eles("css:.right-col tr, .box-thuoc-tinh tr, .thuoc-tinh tr, #tab2 tr")
        for row in rows:
            try:
                cells = row.eles("tag:td")
                if len(cells) >= 2:
                    label = cells[0].text.strip().rstrip(":")
                    key = label_map.get(label)
                    if key:
                        meta[key] = cells[1].text.strip()
            except Exception:
                continue
    except Exception as e:
        log.debug("scrape_metadata lỗi: %s", e)
    return meta


def scrape_page(driver: ChromiumPage, url: str) -> tuple[dict | None, dict | None]:
    try:
        driver.get(url)
        cf_warned = False
        for _ in range(60):
            t = driver.title
            if "Just a moment" in t or "Checking your browser" in t:
                if not cf_warned:
                    log.warning("  ⚠ [%s] Cloudflare — vui lòng click checkbox...", url.split("/")[-1][:40])
                    cf_warned = True
                time.sleep(2)
            else:
                if cf_warned:
                    log.info("  ✓ Đã qua Cloudflare")
                break
        if not driver.ele("#tab1", timeout=30):
            log.warning("Không tìm thấy #tab1: %s", url)
            return None, None
    except Exception as e:
        log.warning("Lỗi tải %s: %s", url, e)
        return None, None

    title = ""
    el = driver.ele("css:h1.title-vb, h1", timeout=3)
    if el:
        title = el.text.strip()

    meta = scrape_metadata(driver)
    data_record: dict = {"url": url, "title": title}
    content_el = driver.ele("css:#tab1", timeout=3)
    data_record["content"] = content_el.text.strip() if content_el else ""
    data_record["meta"] = meta

    raw_html = clean_content_html(driver)
    doc_title = title or url.split("/")[-1]
    html_record: dict = {"url": url, "title": title,
                         "content_html": build_full_html(doc_title, raw_html)}
    html_record.update(meta)
    if SAVE_HTML_FILE and raw_html:
        save_html_file(doc_title, raw_html)

    return data_record, html_record


def collect_related_urls(driver: ChromiumPage) -> list[str]:
    urls, seen = [], set()
    selectors = [".GridBaseVBCT .nqTitle a", ".GridBaseVBCT a",
                 ".vb-related a", ".list-vb a", "a[href*='/van-ban/']"]
    try:
        for sel in selectors:
            for a in driver.eles(f"css:{sel}"):
                href = a.attr("href") or ""
                if "thuvienphapluat.vn/van-ban/" in href and ".aspx" in href:
                    clean = href.split("?")[0].split("#")[0]
                    if clean not in seen:
                        seen.add(clean)
                        urls.append(clean)
    except Exception as e:
        log.debug("collect_related_urls lỗi: %s", e)
    return urls


# ── Worker ────────────────────────────────────────────────────────────────────
def worker(worker_id: int):
    global _counter
    driver = make_driver()
    log.info("Worker %d khởi động.", worker_id)
    batch_count = 0  # đếm số URL đã làm trong batch hiện tại

    try:
        while not _done_event.is_set():
            # Lấy URL từ queue, chờ tối đa IDLE_TIMEOUT giây
            try:
                url = _url_queue.get(timeout=IDLE_TIMEOUT)
            except Empty:
                log.info("Worker %d: không có URL mới sau %ds, dừng.", worker_id, IDLE_TIMEOUT)
                break

            # Kiểm tra đã visited chưa (atomic check-and-mark)
            with _visited_lock:
                if url in _visited:
                    _url_queue.task_done()
                    continue
                _visited.add(url)

            # Kiểm tra giới hạn
            with _counter_lock:
                if MAX_DOCS and _counter >= MAX_DOCS:
                    _done_event.set()
                    _url_queue.task_done()
                    break

            log.info("[W%d] Crawling: %s", worker_id, url.split("/")[-1][:60])
            data_record, html_record = scrape_page(driver, url)

            if data_record:
                # Thu thập URL liên quan trước khi ghi
                related = collect_related_urls(driver)

                with _write_lock:
                    append_data(data_record)
                    append_html(html_record)
                    save_visited()

                with _counter_lock:
                    _counter += 1
                    current = _counter
                    if MAX_DOCS and _counter >= MAX_DOCS:
                        _done_event.set()

                log.info("  [W%d] ✓ %d/%s — %s", worker_id, current,
                         str(MAX_DOCS) if MAX_DOCS else "∞",
                         data_record.get("title", url)[:60])

                # Thêm URL mới vào queue (chỉ những URL chưa visited)
                with _visited_lock:
                    new_urls = [u for u in related if u not in _visited]
                random.shuffle(new_urls)
                for u in new_urls:
                    if not _done_event.is_set():
                        _url_queue.put(u)
                if new_urls:
                    log.info("  [W%d] → %d URL mới vào queue", worker_id, len(new_urls))

            _url_queue.task_done()
            batch_count += 1

            if _done_event.is_set():
                break

            # Sau mỗi BATCH_SIZE URL, nghỉ dài hơn để tránh bị detect
            if batch_count % BATCH_SIZE == 0:
                pause = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                log.info("  [W%d] Nghỉ batch %.1fs...", worker_id, pause)
                time.sleep(pause)
            else:
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    except Exception as e:
        log.error("Worker %d lỗi nghiêm trọng: %s", worker_id, e)
    finally:
        driver.quit()
        log.info("Worker %d dừng. Đã xử lý %d URL.", worker_id, batch_count)


# ── Main crawler ──────────────────────────────────────────────────────────────
def crawl(reset: bool = False):
    global _visited, _counter

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if reset:
        for f in [VISITED_FILE, os.path.join(OUTPUT_DIR, DATA_FILE),
                  os.path.join(OUTPUT_DIR, HTML_FILE)]:
            if os.path.exists(f):
                os.remove(f)
                log.info("Đã xóa: %s", f)

    _visited = load_visited()
    _counter = 0
    _done_event.clear()
    _url_queue.put(START_URL)

    # Khởi động workers, stagger 3s để tránh 3 Chrome mở cùng lúc
    threads = []
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=worker, args=(i + 1,), daemon=True, name=f"worker-{i+1}")
        t.start()
        threads.append(t)
        if i < NUM_WORKERS - 1:
            time.sleep(3)

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        log.info("Dừng bởi người dùng.")
        _done_event.set()
        for t in threads:
            t.join(timeout=5)

    log.info("Hoàn thành. Đã crawl %d văn bản.", _counter)


if __name__ == "__main__":
    reset_flag = "--reset" in sys.argv
    crawl(reset=reset_flag)
