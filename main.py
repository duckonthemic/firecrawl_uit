import os
import json
import logging
import time
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

import yaml
from firecrawl import FirecrawlApp
import requests

OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/data"))
LOG_PATH = Path(os.environ.get("LOG_PATH", "/logs/firecrawl.log"))
CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config.yaml")

FIRECRAWL_URL = os.environ.get("FIRECRAWL_URL", "http://api:3002")
SCHEDULE_HOURS = float(os.environ.get("SCHEDULE_HOURS", "24"))
RUN_ONCE = os.environ.get("RUN_ONCE", "false").lower() == "true"

META_JSONL = OUTPUT_DIR / "metadata.jsonl"
META_JSON = OUTPUT_DIR / "metadata.json"
CRAWLED_CACHE = OUTPUT_DIR / "crawled_urls.txt"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
FAILED_URLS_FILE = OUTPUT_DIR / "failed_urls.jsonl"
STATS_FILE = OUTPUT_DIR / "crawl_stats.json"

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("firecrawl-uit")


class CrawlStats:
    """Track crawl statistics and performance metrics"""
    
    def __init__(self):
        self.total_pages = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.total_size_bytes = 0
        self.start_time = datetime.now()
        self.seed_stats = {}
        self.error_categories = {}
    
    def add_page(self, seed_url: str, size_bytes: int = 0, success: bool = True):
        """Record a page crawl"""
        self.total_pages += 1
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
        
        self.total_size_bytes += size_bytes
        
        if seed_url not in self.seed_stats:
            self.seed_stats[seed_url] = {
                "pages": 0,
                "size_mb": 0,
                "errors": 0
            }
        
        self.seed_stats[seed_url]["pages"] += 1
        self.seed_stats[seed_url]["size_mb"] += size_bytes / (1024**2)
        if not success:
            self.seed_stats[seed_url]["errors"] += 1
    
    def add_skipped(self):
        """Record a skipped page (cached)"""
        self.skipped_count += 1
    
    def add_error(self, category: str):
        """Record an error by category"""
        self.error_count += 1
        self.error_categories[category] = self.error_categories.get(category, 0) + 1
    
    def get_report(self) -> dict:
        """Generate comprehensive statistics report"""
        duration = (datetime.now() - self.start_time).total_seconds()
        
        return {
            "summary": {
                "total_pages": self.total_pages,
                "success_count": self.success_count,
                "error_count": self.error_count,
                "skipped_count": self.skipped_count,
                "success_rate": round(self.success_count / max(self.total_pages, 1) * 100, 2),
                "total_size_mb": round(self.total_size_bytes / (1024**2), 2),
            },
            "performance": {
                "duration_seconds": round(duration, 2),
                "duration_minutes": round(duration / 60, 2),
                "pages_per_minute": round(self.total_pages / max(duration / 60, 1), 2),
                "mb_per_minute": round((self.total_size_bytes / (1024**2)) / max(duration / 60, 1), 2),
            },
            "seeds": self.seed_stats,
            "errors_by_category": self.error_categories,
            "timestamp": datetime.now().isoformat()
        }
    
    def save_report(self):
        """Save statistics to file"""
        report = self.get_report()
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info(f"Statistics saved to {STATS_FILE}")

# Global stats instance
crawl_stats = CrawlStats()


def save_checkpoint(seed_url: str, seed_index: int):
    """Save checkpoint for recovery"""
    checkpoint = {
        "seed_url": seed_url,
        "seed_index": seed_index,
        "timestamp": datetime.now().isoformat(),
        "stats": crawl_stats.get_report()
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)

def load_checkpoint() -> Optional[dict]:
    """Load checkpoint to resume from interruption"""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                checkpoint = json.load(f)
                logger.info(f"Found checkpoint: {checkpoint['seed_url']} at {checkpoint['timestamp']}")
                return checkpoint
        except Exception as e:
            logger.warning(f"Failed to load checkpoint: {e}")
    return None

def clear_checkpoint():
    """Clear checkpoint after successful completion"""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("Checkpoint cleared")

def mark_failed_url(url: str, error: str, seed_url: str, attempts: int = 3):
    """Record failed URL for later retry"""
    with open(FAILED_URLS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "url": url,
            "seed_url": seed_url,
            "error": str(error)[:200],  # Truncate long errors
            "attempts": attempts,
            "timestamp": datetime.now().isoformat()
        }, ensure_ascii=False) + "\n")

def categorize_error(exception: Exception) -> str:
    """Categorize error type for better debugging"""
    error_str = str(exception).lower()
    
    if "timeout" in error_str:
        return "timeout"
    elif any(x in error_str for x in ["403", "401", "unauthorized"]):
        return "permission_denied"
    elif "404" in error_str:
        return "not_found"
    elif any(x in error_str for x in ["500", "502", "503"]):
        return "server_error"
    elif any(x in error_str for x in ["connection", "network"]):
        return "network_error"
    
    return "unknown"


def wait_for_firecrawl(max_retries: int = 30, delay: int = 10) -> bool:
    """Wait for Firecrawl API to be ready with health checks"""
    logger.info("Waiting for Firecrawl services to start...")
    
    for i in range(max_retries):
        try:
            logger.info(f"Attempting health check {i+1}/{max_retries} to {FIRECRAWL_URL}/v1")
            response = requests.get(f"{FIRECRAWL_URL}/v1", timeout=5)
            logger.info(f"Got response status: {response.status_code}")
            if response.status_code in [200, 404]:
                logger.info(f"Connected to Firecrawl at {FIRECRAWL_URL}")
                return True
            else:
                logger.warning(f"Unexpected status code: {response.status_code}, retrying...")
        except requests.exceptions.RequestException as e:
            logger.info(f"RequestException: {type(e).__name__}: {str(e)[:100]}")
            if i < max_retries - 1:
                logger.warning(f"Firecrawl not ready yet ({i+1}/{max_retries}), waiting {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Failed to connect to Firecrawl after {max_retries} attempts: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error during health check: {type(e).__name__}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    return False

def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    
    def env_list(name: str, default: List[str]) -> List[str]:
        raw = os.environ.get(name)
        if raw is None or raw.strip() == "":
            return default
        return [x.strip() for x in raw.split(",") if x.strip()]
    
    cfg["seed_urls"] = env_list("SEED_URLS", cfg.get("seed_urls", []))
    cfg["include_patterns"] = env_list("INCLUDE_PATTERNS", cfg.get("include_patterns", []))
    cfg["exclude_patterns"] = env_list("EXCLUDE_PATTERNS", cfg.get("exclude_patterns", []))
    cfg["max_depth"] = int(os.environ.get("MAX_DEPTH", cfg.get("max_depth", 3)))
    
    return cfg

def ensure_dirs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_crawled_urls() -> set:
    if CRAWLED_CACHE.exists():
        with open(CRAWLED_CACHE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def mark_url_crawled(url: str):
    """Mark URL as crawled in cache"""
    with open(CRAWLED_CACHE, "a", encoding="utf-8") as f:
        f.write(url + "\n")

def should_recrawl(url: str, days_threshold: int = 7) -> bool:
    """Determine if URL should be recrawled (incremental crawling)"""
    if url not in load_crawled_urls():
        return True
    
    # Check if metadata exists and get last crawl date
    if META_JSON.exists():
        try:
            with open(META_JSON, "r", encoding="utf-8") as f:
                items = json.load(f)
                for item in items:
                    if item.get("url") == url:
                        last_crawl = item.get("date")
                        if last_crawl:
                            crawl_date = datetime.fromisoformat(last_crawl)
                            days_since = (datetime.now() - crawl_date).days
                            if days_since > days_threshold:
                                logger.info(f"Recrawling old URL ({days_since} days): {url}")
                                return True
                        break
        except Exception as e:
            logger.warning(f"Error checking recrawl status: {e}")
    
    return False

CURRENT_SEED_URL = None

def get_content_folder(url: str, title: str = "") -> str:
    text = (url + " " + title).lower()
    
    if any(x in text for x in ["quy-trinh", "qui-trinh"]):
        if any(x in text for x in ["giang-vien", "giang-day", "can-bo"]):
            return "daa/quy-trinh/quy-trinh-cho-giang-vien"
        elif any(x in text for x in ["sinh-vien", "sv"]):
            return "daa/quy-trinh/quy-trinh-cho-sinh-vien"
        else:
            return "daa/quy-trinh/quy-trinh-chung"
    
    elif any(x in text for x in ["quy-dinh", "qui-dinh", "quy-che", "qui-che"]):
        if any(x in text for x in ["giang-vien", "giang-day", "can-bo"]):
            return "daa/quy-dinh/quy-dinh-cho-giang-vien"
        elif any(x in text for x in ["sinh-vien", "sv"]):
            return "daa/quy-dinh/quy-dinh-cho-sinh-vien"
        elif any(x in text for x in ["dhqg", "dai-hoc-quoc-gia"]):
            return "daa/quy-dinh/quy-dinh-dhqg-hcm"
        elif any(x in text for x in ["bo-gddt", "bo-giao-duc"]):
            return "daa/quy-dinh/quy-dinh-bo-gddt"
        else:
            return "daa/quy-dinh/quy-dinh-chung"
    
    elif any(x in text for x in ["thong-bao", "thongbao"]):
        if any(x in text for x in ["chinh-quy", "chinhquy"]):
            return "daa/thong-bao/thong-bao-chinh-quy"
        elif any(x in text for x in ["tu-xa", "tuxa"]):
            return "daa/thong-bao/thong-bao-tu-xa"
        else:
            return "daa/thong-bao/thong-bao-chung"
    
    elif any(x in text for x in ["huong-dan", "huongdan"]):
        if any(x in text for x in ["tra-cuu", "van-bang", "xac-minh"]):
            return "daa/huong-dan/huong-dan-tra-cuu-van-bang"
        elif any(x in text for x in ["online", "qua-mang", "hoc-online"]):
            return "daa/huong-dan/huong-dan-hoc-online"
        elif any(x in text for x in ["sinh-vien", "sv"]):
            return "daa/huong-dan/huong-dan-cho-sinh-vien"
        else:
            return "daa/huong-dan/huong-dan-chung"
    
    elif any(x in text for x in ["ctdt", "chuong-trinh-dao-tao"]):
        if any(x in text for x in ["chinh-quy", "cqui"]):
            return "daa/chuong-trinh-dao-tao/ctdt-chinh-quy"
        elif any(x in text for x in ["tu-xa"]):
            return "daa/chuong-trinh-dao-tao/ctdt-tu-xa"
        else:
            return "daa/chuong-trinh-dao-tao/ctdt-khac"
    
    elif any(x in text for x in ["dao-tao-ngan-han", "ngan-han"]):
        return "daa/dao-tao/dao-tao-ngan-han"
    
    else:
        return "daa/khac/chua-phan-loai"

def append_jsonl(obj: Dict[str, Any]):
    with open(META_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def rebuild_metadata_json():
    items = []
    if META_JSONL.exists():
        with open(META_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except Exception as e:
                    logger.warning(f"Failed to parse JSON line: {e}")
    
    with open(META_JSON, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

def find_download_links(html: str, base_url: str) -> List[str]:
    if not html:
        return []
    
    links = []
    patterns = [
        r'href=["\']([^"\']*\.pdf[^"\']*)["\']',
        r'href=["\']([^"\']*\.docx?[^"\']*)["\']',
        r'href=["\']([^"\']*\.xlsx?[^"\']*)["\']',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for match in matches:
            full_url = urljoin(base_url, match)
            if full_url not in links:
                links.append(full_url)
    
    return links

def download_file(url: str, output_dir: Path, category: str = "files") -> bool:
    try:
        parsed = urlparse(url)
        filename = parsed.path.split('/')[-1]
        if not filename or len(filename) > 200:
            url_hash = hashlib.sha1(url.encode()).hexdigest()[:8]
            ext = url.split('.')[-1].lower() if '.' in url else 'bin'
            filename = f"{url_hash}.{ext}"
        
        output_path = output_dir / filename
        
        if output_path.exists():
            logger.debug(f"File already exists: {filename}")
            return True
        
        response = requests.get(url, stream=True, timeout=30, verify=False)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"Downloaded [{category}]: {filename} ({size_mb:.2f} MB)")
        
        metadata = {
            "title": filename,
            "url": url,
            "type": "file",
            "seed": CURRENT_SEED_URL if CURRENT_SEED_URL else "unknown",
            "seed_folder": category,
            "file_path": str(output_path),
            "size_mb": round(size_mb, 2),
            "date": datetime.now().isoformat(),
        }
        append_jsonl(metadata)
        
        return True
    
    except Exception as e:
        logger.warning(f"Failed to download {url}: {e}")
        return False

def save_content(url: str, data: Dict[str, Any]):
    global CURRENT_SEED_URL
    
    title = data.get("metadata", {}).get("title", "")
    content_folder = get_content_folder(url, title)
    
    mark_url_crawled(url)
    
    content_dir = OUTPUT_DIR / content_folder
    html_dir = content_dir / "html"
    markdown_dir = content_dir / "markdown"
    pdf_dir = content_dir / "pdf"
    docx_dir = content_dir / "docx"
    
    html_dir.mkdir(parents=True, exist_ok=True)
    markdown_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    docx_dir.mkdir(parents=True, exist_ok=True)
    
    safe_name = url.replace("https://", "").replace("http://", "")
    safe_name = safe_name.replace("/", "_").replace(":", "_")[:200]
    
    total_size = 0
    
    if data.get("html"):
        html_file = html_dir / f"{safe_name}.html"
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(data["html"])
        total_size += html_file.stat().st_size
        logger.info(f"Saved HTML [{content_folder}]: {html_file.name}")
        
        download_links = find_download_links(data["html"], url)
        for link in download_links:
            if link.lower().endswith('.pdf'):
                download_file(link, pdf_dir, content_folder)
            elif link.lower().endswith(('.doc', '.docx')):
                download_file(link, docx_dir, content_folder)
    
    if data.get("markdown"):
        md_file = markdown_dir / f"{safe_name}.md"
        with open(md_file, "w", encoding="utf-8") as f:
            f.write(data["markdown"])
        total_size += md_file.stat().st_size
        logger.info(f"Saved Markdown [{content_folder}]: {md_file.name}")
    
    # Update stats
    crawl_stats.add_page(CURRENT_SEED_URL, total_size, success=True)
    
    metadata = {
        "title": data.get("metadata", {}).get("title", ""),
        "url": url,
        "type": "html",
        "seed": CURRENT_SEED_URL if CURRENT_SEED_URL else "unknown",
        "content_folder": content_folder,
        "content": data.get("markdown", data.get("text", ""))[:10000],
        "source_url": data.get("metadata", {}).get("sourceURL", url),
        "status_code": data.get("metadata", {}).get("statusCode", 200),
        "size_bytes": total_size,
        "date": datetime.now().isoformat(),
    }
    append_jsonl(metadata)

def crawl_single_seed(app: FirecrawlApp, seed_url: str, cfg: Dict[str, Any], crawled_urls: set) -> dict:
    """Crawl a single seed URL (for parallel execution)"""
    global CURRENT_SEED_URL
    CURRENT_SEED_URL = seed_url
    
    max_retries = 3
    retry_delay = 5
    result = {"seed_url": seed_url, "success": False, "pages": 0, "errors": 0}
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Starting crawl for: {seed_url} (attempt {attempt + 1}/{max_retries})")
            
            crawl_params = {
                "limit": 500,
                "maxDepth": cfg.get("max_depth", 3),
                "scrapeOptions": {
                    "formats": ["markdown", "html"],
                    "waitFor": 1000,
                    "timeout": 30000,
                }
            }
            
            if cfg.get("include_patterns"):
                crawl_params["includePaths"] = cfg["include_patterns"]
            if cfg.get("exclude_patterns"):
                crawl_params["excludePaths"] = cfg["exclude_patterns"]
            
            crawl_result = app.crawl_url(seed_url, params=crawl_params, poll_interval=5)
            
            if crawl_result.get("success"):
                data = crawl_result.get("data", [])
                logger.info(f"Crawled {len(data)} pages from {seed_url}")
                
                success_count = 0
                skipped_count = 0
                for page in data:
                    try:
                        page_url = page.get("metadata", {}).get("sourceURL", seed_url)
                        if page_url in crawled_urls:
                            skipped_count += 1
                            crawl_stats.add_skipped()
                            logger.debug(f"Skipped (cached): {page_url}")
                            continue
                        save_content(page_url, page)
                        success_count += 1
                    except Exception as e:
                        error_cat = categorize_error(e)
                        crawl_stats.add_error(error_cat)
                        logger.error(f"[{error_cat}] Failed to save page: {e}")
                        result["errors"] += 1
                
                result["success"] = True
                result["pages"] = success_count
                logger.info(f"Saved {success_count}/{len(data)} pages, skipped {skipped_count} (cached) from {seed_url}")
                break
            else:
                error_msg = crawl_result.get('error', 'Unknown error')
                error_cat = categorize_error(Exception(error_msg))
                crawl_stats.add_error(error_cat)
                logger.error(f"[{error_cat}] Crawl failed for {seed_url}: {error_msg}")
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Max retries reached for {seed_url}")
                    mark_failed_url(seed_url, error_msg, seed_url, max_retries)
        
        except Exception as e:
            error_cat = categorize_error(e)
            crawl_stats.add_error(error_cat)
            logger.error(f"[{error_cat}] Error crawling {seed_url}: {e}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"Max retries reached for {seed_url}")
                mark_failed_url(seed_url, str(e), seed_url, max_retries)
                result["errors"] += 1
    
    return result

def crawl_with_firecrawl(app: FirecrawlApp, seed_urls: List[str], cfg: Dict[str, Any]):
    """Crawl multiple seeds with parallel execution and checkpoint support"""
    global CURRENT_SEED_URL
    
    crawled_urls = load_crawled_urls()
    logger.info(f"Loaded {len(crawled_urls)} URLs from cache")
    
    checkpoint = load_checkpoint()
    start_index = 0
    
    if checkpoint:
        try:
            checkpoint_url = checkpoint.get("seed_url")
            if checkpoint_url in seed_urls:
                start_index = seed_urls.index(checkpoint_url)
                logger.info(f"Resuming from checkpoint: {checkpoint_url} (index {start_index})")
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not resume from checkpoint: {e}")
    
    seeds_to_crawl = seed_urls[start_index:]
    logger.info(f"Crawling {len(seeds_to_crawl)} seeds (starting from index {start_index})")
    
    max_workers = int(os.environ.get("MAX_WORKERS", "3"))
    logger.info(f"Using {max_workers} parallel workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_seed = {
            executor.submit(crawl_single_seed, app, seed_url, cfg, crawled_urls): (i + start_index, seed_url)
            for i, seed_url in enumerate(seeds_to_crawl)
        }
        
        # Process completed tasks
        for future in as_completed(future_to_seed):
            seed_index, seed_url = future_to_seed[future]
            
            try:
                result = future.result()
                save_checkpoint(seed_url, seed_index)
                logger.info(f"Progress: {seed_index + 1}/{len(seed_urls)} seeds completed")
                
                if (seed_index + 1) % 5 == 0:
                    crawl_stats.save_report()
                
            except Exception as e:
                error_cat = categorize_error(e)
                crawl_stats.add_error(error_cat)
                logger.error(f"[{error_cat}] Failed to process seed {seed_url}: {e}")
            
            time.sleep(1)
    
    clear_checkpoint()
    logger.info("All seeds crawled successfully")

def scrape_with_firecrawl(app: FirecrawlApp, urls: List[str]):
    for url in urls:
        try:
            logger.info(f"Scraping: {url}")
            
            scrape_result = app.scrape_url(url, params={
                "formats": ["markdown", "html"],
            })
            
            if scrape_result.get("success"):
                save_content(url, scrape_result)
            else:
                logger.error(f"Scrape failed for {url}")
        
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
        
        time.sleep(1)

def crawl_once():
    """Execute one crawl cycle with improved error handling and stats"""
    cfg = load_config()
    ensure_dirs()
    
    if not wait_for_firecrawl():
        logger.error("Firecrawl services not available, aborting")
        return
    
    try:
        app = FirecrawlApp(api_url=FIRECRAWL_URL)
        
        seed_urls = cfg.get("seed_urls", [])
        if not seed_urls:
            logger.warning("No seed URLs configured")
            return
        
        logger.info(f"Starting crawl with {len(seed_urls)} seed URLs")
        
        global crawl_stats
        crawl_stats = CrawlStats()
        
        crawl_with_firecrawl(app, seed_urls, cfg)
        
        rebuild_metadata_json()
        logger.info(f"Rebuilt {META_JSON}")
        
        crawl_stats.save_report()
        report = crawl_stats.get_report()
        
        logger.info("=" * 80)
        logger.info("CRAWL SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total Pages: {report['summary']['total_pages']}")
        logger.info(f"Success: {report['summary']['success_count']} ({report['summary']['success_rate']}%)")
        logger.info(f"Errors: {report['summary']['error_count']}")
        logger.info(f"Skipped (cached): {report['summary']['skipped_count']}")
        logger.info(f"Total Size: {report['summary']['total_size_mb']} MB")
        logger.info(f"Duration: {report['performance']['duration_minutes']} minutes")
        logger.info(f"Speed: {report['performance']['pages_per_minute']} pages/min")
        if report['errors_by_category']:
            logger.info(f"Error Categories: {report['errors_by_category']}")
        logger.info("=" * 80)
    
    except Exception as e:
        error_cat = categorize_error(e)
        crawl_stats.add_error(error_cat)
        logger.error(f"[{error_cat}] Crawl failed: {e}")
        
        crawl_stats.save_report()
        mark_failed_url("CRAWL_PROCESS", str(e), "SYSTEM", 1)

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("UIT CRAWLER - FIRECRAWL SELF-HOSTED")
    logger.info("=" * 80)
    logger.info(f"Firecrawl endpoint: {FIRECRAWL_URL}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Schedule: Every {SCHEDULE_HOURS} hours")
    logger.info(f"Run once mode: {RUN_ONCE}")
    logger.info("=" * 80)
    
    crawl_once()
    
    if RUN_ONCE:
        logger.info("Run-once complete. Exiting.")
    else:
        hours = max(SCHEDULE_HOURS, 1)
        while True:
            logger.info(f"Next crawl in {hours} hours")
            try:
                time.sleep(hours * 3600)
                logger.info("Starting scheduled crawl...")
                crawl_once()
            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Scheduled crawl failed: {e}")
                time.sleep(300)
    
    logger.info("=" * 80)
    logger.info("UIT crawler stopped")
    logger.info("=" * 80)