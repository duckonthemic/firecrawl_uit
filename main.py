import os
import json
import logging
import time
import re
import threading
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import parse_qsl, unquote, urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import unicodedata

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
PAGE_ARTIFACTS_JSONL = OUTPUT_DIR / "page_artifacts.jsonl"
CRAWLED_CACHE = OUTPUT_DIR / "crawled_urls.txt"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
FAILED_URLS_FILE = OUTPUT_DIR / "failed_urls.jsonl"
STATS_FILE = OUTPUT_DIR / "crawl_stats.json"
CRAWLED_LOG_FILE = OUTPUT_DIR / "crawled.txt"
COMPLETED_SEEDS_FILE = OUTPUT_DIR / "completed_seeds.json"

DIRECT_FILE_EXTENSIONS = {".pdf", ".doc", ".docx", ".xls", ".xlsx"}
DEFAULT_SEED_POLICIES = {
    "single_page_patterns": [
        "/content/bang-tom-tat-mon-hoc",
        "/content/chuc-nang-nhiem-vu-cua-phong-dao-tao-dai-hoc",
        "/content/quy-dinh-dao-tao-ngan-han",
        "/content/quy-trinh-danh-cho-can-bo-giang-day",
        "/mot-so-quy-trinh-danh-cho-sinh-vien",
        "/content/huong-dan-sinh-vien-dai-hoc-he-chinh-quy-thuc-hien-cac-quy-dinh-ve-chuan-qua-trinh-va-chuan",
    ],
    "listing_page_patterns": [
        "/content/cong-thong-tin-dao-tao",
        "tuyensinh.uit.edu.vn/nganh-dao-tao",
        "/thongbaochinhquy",
        "/thong-bao-vb2",
        "/thongbaotuxa",
        "/kehoachnam",
        "/content/chuong-trinh-dao-tao-cu",
    ],
    "listing_with_detail_fanout_patterns": [
        "/qui-che-qui-dinh-qui-trinh",
        "/quy-che-quy-dinh-dao-tao-dai-hoc-cua-dhqg-hcm",
        "/quy-che-quy-dinh-dao-tao-dai-hoc-cua-bo-gddt",
        "/loai-bai-viet/de-mo-nganh",
    ],
    "ctdt_index_patterns": [
        "/chuong-trinh-dao-tao/ctdt-khoa-",
        "/tu-xa/ctdt-khoa-",
        "/cqui/ctdt-khoa-",
    ],
    "slow_lane_patterns": [
        "/danh-muc-mon-hoc-dai-hoc",
    ],
    "listing_page_limit": 60,
    "listing_page_depth": 2,
    "ctdt_program_limit": 4,
    "ctdt_program_depth": 1,
    "batch_scrape_detail_fanout": True,
}

# Cache for numbered document URLs mapping
NUMBERED_DOCS_CACHE = {}
 
CRAWLED_URLS_SET = set()
JOB_STRATEGY_OVERRIDES = {}
# Content deduplication cache: maps MD5 hash to first URL that had this content
CONTENT_HASH_CACHE = {}
# File to persist content hashes across runs
CONTENT_HASH_FILE = OUTPUT_DIR / "content_hashes.json"

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


class HostRateLimiter:
    """Reserve request slots independently for each source host."""

    def __init__(self, delay_seconds: float):
        self.delay_seconds = max(float(delay_seconds), 0.0)
        self._next_request_by_host: Dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, url: str):
        if self.delay_seconds <= 0:
            return
        host = urlparse(url).netloc.lower()
        if not host:
            return
        now = time.monotonic()
        with self._lock:
            request_at = max(now, self._next_request_by_host.get(host, now))
            self._next_request_by_host[host] = request_at + self.delay_seconds
        wait_seconds = request_at - now
        if wait_seconds > 0:
            logger.debug(f"Rate limit: waiting {wait_seconds:.2f}s for {host}")
            time.sleep(wait_seconds)


SOURCE_HOST_RATE_LIMITER = HostRateLimiter(
    float(os.environ.get("SOURCE_HOST_DELAY_SECONDS", "10"))
)


class CrawlStats:
    """Track crawl statistics and performance metrics"""
    
    def __init__(self):
        self.total_pages = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.download_count = 0
        self.total_size_bytes = 0
        self.start_time = datetime.now()
        self.seed_stats = {}
        self.error_categories = {}
        self._lock = threading.Lock()

    def ensure_seed(self, seed_url: str) -> Dict[str, Any]:
        if seed_url not in self.seed_stats:
            self.seed_stats[seed_url] = {
                "strategy": "unknown",
                "pages": 0,
                "size_mb": 0,
                "errors": 0,
                "skipped": 0,
                "downloads": 0,
                "download_size_mb": 0,
                "status": "pending",
            }
        return self.seed_stats[seed_url]

    def start_seed(self, seed_url: str, strategy: str):
        with self._lock:
            seed_stats = self.ensure_seed(seed_url)
            seed_stats["strategy"] = strategy
            seed_stats["status"] = "running"
            seed_stats["_started_at"] = time.time()

    def finish_seed(self, seed_url: str, success: bool) -> float:
        with self._lock:
            seed_stats = self.ensure_seed(seed_url)
            started_at = seed_stats.pop("_started_at", None)
            duration = round(time.time() - started_at, 2) if started_at else 0
            seed_stats["duration_seconds"] = duration
            seed_stats["status"] = "success" if success else "failed"
            return duration

    def add_page(self, seed_url: str, size_bytes: int = 0, success: bool = True):
        """Record a page crawl"""
        with self._lock:
            self.total_pages += 1
            if success:
                self.success_count += 1
            else:
                self.error_count += 1

            self.total_size_bytes += size_bytes
            seed_stats = self.ensure_seed(seed_url)
            seed_stats["pages"] += 1
            seed_stats["size_mb"] += size_bytes / (1024**2)
            if not success:
                seed_stats["errors"] += 1

    def add_download(self, seed_url: str, size_bytes: int = 0):
        with self._lock:
            self.download_count += 1
            seed_stats = self.ensure_seed(seed_url)
            seed_stats["downloads"] += 1
            seed_stats["download_size_mb"] += size_bytes / (1024**2)

    def add_skipped(self, seed_url: str = ""):
        """Record a skipped page (cached)"""
        with self._lock:
            self.skipped_count += 1
            if seed_url:
                seed_stats = self.ensure_seed(seed_url)
                seed_stats["skipped"] += 1

    def add_error(self, category: str, seed_url: str = ""):
        """Record an error by category"""
        with self._lock:
            self.error_count += 1
            self.error_categories[category] = self.error_categories.get(category, 0) + 1
            if seed_url:
                seed_stats = self.ensure_seed(seed_url)
                seed_stats["errors"] += 1
    
    def get_report(self) -> dict:
        """Generate comprehensive statistics report"""
        duration = (datetime.now() - self.start_time).total_seconds()
        with self._lock:
            seeds_report = {}
            for seed_url, stats in self.seed_stats.items():
                cleaned_stats = {}
                for key, value in stats.items():
                    if key.startswith("_"):
                        continue
                    if isinstance(value, float):
                        cleaned_stats[key] = round(value, 2)
                    else:
                        cleaned_stats[key] = value
                seeds_report[seed_url] = cleaned_stats

            errors_by_category = dict(self.error_categories)
            summary = {
                "total_pages": self.total_pages,
                "success_count": self.success_count,
                "error_count": self.error_count,
                "skipped_count": self.skipped_count,
                "download_count": self.download_count,
                "success_rate": round(self.success_count / max(self.total_pages, 1) * 100, 2),
                "total_size_mb": round(self.total_size_bytes / (1024**2), 2),
            }
        
        return {
            "summary": summary,
            "performance": {
                "duration_seconds": round(duration, 2),
                "duration_minutes": round(duration / 60, 2),
                "pages_per_minute": round(summary["total_pages"] / max(duration / 60, 1), 2),
                "downloads_per_minute": round(summary["download_count"] / max(duration / 60, 1), 2),
                "mb_per_minute": round((self.total_size_bytes / (1024**2)) / max(duration / 60, 1), 2),
            },
            "seeds": seeds_report,
            "errors_by_category": errors_by_category,
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


def save_checkpoint(seed_url: str, seed_index: int, completed_count: int, total_seeds: int):
    """Save checkpoint for recovery"""
    checkpoint = {
        "seed_url": seed_url,
        "seed_index": seed_index,
        "timestamp": datetime.now().isoformat(),
        "completed_count": completed_count,
        "total_seeds": total_seeds,
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


def load_completed_seeds() -> Dict[str, Dict[str, Any]]:
    """Load successfully completed seeds for reliable resume across parallel workers."""
    if not COMPLETED_SEEDS_FILE.exists():
        return {}

    try:
        with open(COMPLETED_SEEDS_FILE, "r", encoding="utf-8") as f:
            completed = json.load(f)
            if isinstance(completed, dict):
                return completed
    except Exception as e:
        logger.warning(f"Failed to load completed seeds: {e}")

    return {}


def save_completed_seed(seed_url: str, seed_index: int, result: Dict[str, Any]):
    """Persist a successfully completed seed so resume never skips unfinished work."""
    completed = load_completed_seeds()
    completed[seed_url] = {
        "seed_index": seed_index,
        "timestamp": datetime.now().isoformat(),
        "pages": result.get("pages", 0),
        "errors": result.get("errors", 0),
    }

    with open(COMPLETED_SEEDS_FILE, "w", encoding="utf-8") as f:
        json.dump(completed, f, ensure_ascii=False, indent=2)


def clear_completed_seeds():
    """Clear persisted seed-completion state after a fully successful crawl."""
    if COMPLETED_SEEDS_FILE.exists():
        COMPLETED_SEEDS_FILE.unlink()
        logger.info("Completed seed state cleared")


def resolve_crawl_mode() -> str:
    """Resolve crawl mode from environment."""
    crawl_mode = os.environ.get("CRAWL_MODE", "incremental").strip().lower()
    valid_modes = {"incremental", "fresh", "resume_or_fresh"}

    if crawl_mode not in valid_modes:
        logger.warning(
            f"Unknown CRAWL_MODE '{crawl_mode}', falling back to incremental"
        )
        return "incremental"

    return crawl_mode


def clear_fresh_crawl_state():
    """Clear run-state files so the next crawl starts from a clean logical state."""
    global CONTENT_HASH_CACHE, CRAWLED_URLS_SET, NUMBERED_DOCS_CACHE

    for state_file in (
        META_JSONL,
        META_JSON,
        CRAWLED_CACHE,
        CHECKPOINT_FILE,
        FAILED_URLS_FILE,
        STATS_FILE,
        CRAWLED_LOG_FILE,
        CONTENT_HASH_FILE,
        COMPLETED_SEEDS_FILE,
    ):
        if state_file.exists():
            state_file.unlink()
            logger.info(f"Cleared state file: {state_file}")

    CONTENT_HASH_CACHE = {}
    CRAWLED_URLS_SET = set()
    NUMBERED_DOCS_CACHE = {}


def prepare_crawl_state(crawl_mode: str):
    """Prepare on-disk crawl state before a run starts."""
    checkpoint = load_checkpoint()
    completed_seeds = load_completed_seeds()

    if crawl_mode == "fresh":
        logger.info("CRAWL_MODE=fresh -> clearing prior crawl state")
        clear_fresh_crawl_state()
        return

    if crawl_mode == "resume_or_fresh":
        if checkpoint or completed_seeds:
            logger.info(
                "CRAWL_MODE=resume_or_fresh -> resuming existing crawl state "
                f"(checkpoint={bool(checkpoint)}, completed_seeds={len(completed_seeds)})"
            )
        else:
            logger.info(
                "CRAWL_MODE=resume_or_fresh -> no resume state found, clearing caches for a fresh crawl"
            )
            clear_fresh_crawl_state()
        return

    logger.info("CRAWL_MODE=incremental -> keeping existing crawl caches")

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

def slugify_vietnamese(text: str) -> str:
    """
    Convert Vietnamese text with diacritics to lowercase ASCII with hyphens.
    Example: "Quyết định về việc" -> "quyet-dinh-ve-viec"
    """
    # Vietnamese character mapping
    vietnamese_map = {
        'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
        'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
        'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
        'đ': 'd',
        'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
        'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
        'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
        'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
        'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
        'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
        'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
        'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
        'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
        'À': 'A', 'Á': 'A', 'Ả': 'A', 'Ã': 'A', 'Ạ': 'A',
        'Ă': 'A', 'Ằ': 'A', 'Ắ': 'A', 'Ẳ': 'A', 'Ẵ': 'A', 'Ặ': 'A',
        'Â': 'A', 'Ầ': 'A', 'Ấ': 'A', 'Ẩ': 'A', 'Ẫ': 'A', 'Ậ': 'A',
        'Đ': 'D',
        'È': 'E', 'É': 'E', 'Ẻ': 'E', 'Ẽ': 'E', 'Ẹ': 'E',
        'Ê': 'E', 'Ề': 'E', 'Ế': 'E', 'Ể': 'E', 'Ễ': 'E', 'Ệ': 'E',
        'Ì': 'I', 'Í': 'I', 'Ỉ': 'I', 'Ĩ': 'I', 'Ị': 'I',
        'Ò': 'O', 'Ó': 'O', 'Ỏ': 'O', 'Õ': 'O', 'Ọ': 'O',
        'Ô': 'O', 'Ồ': 'O', 'Ố': 'O', 'Ổ': 'O', 'Ỗ': 'O', 'Ộ': 'O',
        'Ơ': 'O', 'Ờ': 'O', 'Ớ': 'O', 'Ở': 'O', 'Ỡ': 'O', 'Ợ': 'O',
        'Ù': 'U', 'Ú': 'U', 'Ủ': 'U', 'Ũ': 'U', 'Ụ': 'U',
        'Ư': 'U', 'Ừ': 'U', 'Ứ': 'U', 'Ử': 'U', 'Ữ': 'U', 'Ự': 'U',
        'Ỳ': 'Y', 'Ý': 'Y', 'Ỷ': 'Y', 'Ỹ': 'Y', 'Ỵ': 'Y',
    }
    
    # Replace Vietnamese characters
    result = ''.join(vietnamese_map.get(c, c) for c in text)
    
    # Convert to lowercase
    result = result.lower()
    
    # Replace spaces and special characters with hyphens
    result = re.sub(r'[^\w\s-]', '', result)  # Remove special chars except space and hyphen
    result = re.sub(r'[-\s]+', '-', result)   # Replace spaces and multiple hyphens with single hyphen
    result = result.strip('-')                # Remove leading/trailing hyphens
    
    return result


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    ordered = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def merge_seed_policies(raw_policies: Any) -> Dict[str, Any]:
    merged = dict(DEFAULT_SEED_POLICIES)
    if not isinstance(raw_policies, dict):
        return merged

    for key, default_value in DEFAULT_SEED_POLICIES.items():
        if key not in raw_policies:
            continue

        value = raw_policies[key]
        if isinstance(default_value, list) and isinstance(value, list):
            merged[key] = [str(item).strip().lower() for item in value if str(item).strip()]
        elif isinstance(default_value, bool):
            if isinstance(value, str):
                merged[key] = value.strip().lower() == "true"
            else:
                merged[key] = bool(value)
        elif isinstance(default_value, int):
            merged[key] = int(value)
        else:
            merged[key] = value

    return merged


def safe_slug(text: str, max_length: int = 48, fallback: str = "item") -> str:
    slug = slugify_vietnamese(text or "") or fallback
    hash_suffix = hashlib.sha1((text or fallback).encode("utf-8")).hexdigest()[:8]
    if len(slug) > max_length:
        trim_length = max(max_length - len(hash_suffix) - 1, 8)
        slug = f"{slug[:trim_length].rstrip('-')}-{hash_suffix}"
    return slug or fallback


def get_url_extension(url: str) -> str:
    parsed = urlparse(url)
    return Path(unquote(parsed.path)).suffix.lower()


def get_url_path_depth(url: str) -> int:
    parsed = urlparse(url)
    return len([part for part in parsed.path.split("/") if part.strip()])


def resolve_absolute_crawl_depth(url: str, relative_depth: int) -> int:
    relative_depth = max(int(relative_depth), 0)
    return max(relative_depth, get_url_path_depth(url) + relative_depth)


def is_direct_file_url(url: str) -> bool:
    return get_url_extension(url) in DIRECT_FILE_EXTENSIONS


def sanitize_path_component(text: str, max_length: int = 80, fallback: str = "item") -> str:
    normalized = unicodedata.normalize("NFKD", unescape(text or ""))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = re.sub(r"[<>:\"/\\\\|?*\x00-\x1F]", " ", ascii_text)
    ascii_text = re.sub(r"[^\w\s.-]", " ", ascii_text)
    ascii_text = re.sub(r"[\s.-]+", "-", ascii_text).strip("-._")

    if not ascii_text:
        ascii_text = fallback

    hash_suffix = hashlib.sha1((text or fallback).encode("utf-8")).hexdigest()[:8]
    if len(ascii_text) > max_length:
        trim_length = max(max_length - len(hash_suffix) - 1, 8)
        ascii_text = f"{ascii_text[:trim_length].rstrip('-')}-{hash_suffix}"

    return ascii_text or fallback


def build_safe_url_basename(url: str, max_length: int = 100) -> str:
    parsed = urlparse(url)
    host = sanitize_path_component(parsed.netloc or "site", max_length=24, fallback="site")
    path_parts = [
        sanitize_path_component(unquote(part), max_length=24, fallback="part")
        for part in parsed.path.split("/")
        if part.strip()
    ]

    query_parts = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        key_part = sanitize_path_component(key, max_length=12, fallback="key")
        value_part = sanitize_path_component(value, max_length=16, fallback="value")
        query_parts.append(f"{key_part}-{value_part}")

    base_name = "_".join([host] + path_parts + query_parts) or host
    hash_suffix = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]

    if len(base_name) > max_length:
        trim_length = max(max_length - len(hash_suffix) - 1, 16)
        base_name = f"{base_name[:trim_length].rstrip('_-')}_{hash_suffix}"
    elif query_parts:
        base_name = f"{base_name}_{hash_suffix}"

    return base_name


def build_safe_download_name(url: str, max_length: int = 96) -> str:
    parsed = urlparse(url)
    original_name = Path(unquote(parsed.path)).name
    suffix = Path(original_name).suffix.lower()
    stem = Path(original_name).stem if original_name else ""
    safe_stem = sanitize_path_component(stem or parsed.netloc or "file", max_length=max_length, fallback="file")
    hash_suffix = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]

    candidate = f"{safe_stem}-{hash_suffix}{suffix}"
    if len(candidate) > max_length:
        trim_length = max(max_length - len(hash_suffix) - len(suffix) - 1, 12)
        safe_stem = safe_stem[:trim_length].rstrip("-")
        candidate = f"{safe_stem}-{hash_suffix}{suffix}"

    return candidate

def load_seed_urls_from_jobs(path: Path, limit: int = 0) -> List[str]:
    if not path.exists():
        logger.warning(f"CRAWL_JOBS_PATH does not exist: {path}")
        return []

    JOB_STRATEGY_OVERRIDES.clear()
    seed_urls = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                job = json.loads(line)
            except Exception as e:
                logger.warning(f"Failed to parse crawl job line: {e}")
                continue
            if str(job.get("status") or "scheduled") != "scheduled":
                continue
            url = str(job.get("url") or "").strip()
            if not url:
                continue
            strategy = str(job.get("strategy") or "").strip()
            if strategy:
                JOB_STRATEGY_OVERRIDES[url] = strategy
            if url not in seed_urls:
                seed_urls.append(url)
            if limit and len(seed_urls) >= limit:
                break
    return seed_urls

def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    
    def env_list(name: str, default: List[str]) -> List[str]:
        raw = os.environ.get(name)
        if raw is None or raw.strip() == "":
            return default
        return [x.strip() for x in raw.split(",") if x.strip()]
    
    cfg["seed_urls"] = env_list("SEED_URLS", cfg.get("seed_urls", []))
    crawl_jobs_path = os.environ.get("CRAWL_JOBS_PATH", "").strip()
    if crawl_jobs_path:
        job_limit = int(os.environ.get("CRAWL_JOB_LIMIT", "0"))
        job_seed_urls = load_seed_urls_from_jobs(Path(crawl_jobs_path), limit=job_limit)
        if job_seed_urls:
            cfg["seed_urls"] = job_seed_urls
            logger.info(f"Loaded {len(job_seed_urls)} seed URL(s) from CRAWL_JOBS_PATH={crawl_jobs_path}")
    cfg["include_patterns"] = env_list("INCLUDE_PATTERNS", cfg.get("include_patterns", []))
    cfg["exclude_patterns"] = env_list("EXCLUDE_PATTERNS", cfg.get("exclude_patterns", []))
    cfg["max_depth"] = int(os.environ.get("MAX_DEPTH", cfg.get("max_depth", 3)))
    cfg["seed_policies"] = merge_seed_policies(cfg.get("seed_policies", {}))

    batch_scrape_override = os.environ.get("BATCH_SCRAPE_DETAIL_FANOUT")
    if batch_scrape_override is not None:
        cfg["seed_policies"]["batch_scrape_detail_fanout"] = (
            batch_scrape_override.strip().lower() == "true"
        )
    
    return cfg


def classify_seed_strategy(seed_url: str, cfg: Dict[str, Any]) -> str:
    policies = cfg.get("seed_policies", DEFAULT_SEED_POLICIES)
    normalized_url = unquote(seed_url or "").lower()
    job_strategy = JOB_STRATEGY_OVERRIDES.get(seed_url)
    if job_strategy:
        return job_strategy

    if is_direct_file_url(seed_url):
        return "direct_file"

    if any(pattern in normalized_url for pattern in policies.get("slow_lane_patterns", [])):
        return "slow_lane"

    if any(pattern in normalized_url for pattern in policies.get("ctdt_index_patterns", [])):
        return "ctdt_index"

    if any(pattern in normalized_url for pattern in policies.get("listing_with_detail_fanout_patterns", [])):
        return "listing_with_detail_fanout"

    if any(pattern in normalized_url for pattern in policies.get("single_page_patterns", [])):
        return "single_page"

    if any(pattern in normalized_url for pattern in policies.get("listing_page_patterns", [])):
        return "listing_page"

    return "listing_page"


def build_request_headers() -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }


def build_scrape_options(strategy: str = "default", html_only: bool = False) -> Dict[str, Any]:
    wait_for = 1500 if strategy == "slow_lane" else 1000
    timeout = 90000 if strategy == "slow_lane" else 45000 if strategy == "single_page" else 30000
    return {
        "formats": ["html"] if html_only else ["markdown", "html"],
        "waitFor": wait_for,
        "timeout": timeout,
        "headers": build_request_headers(),
    }


def build_crawl_params(cfg: Dict[str, Any], strategy: str) -> Dict[str, Any]:
    policies = cfg.get("seed_policies", DEFAULT_SEED_POLICIES)

    limit = 500
    max_depth = cfg.get("max_depth", 3)

    if strategy == "listing_page":
        limit = int(policies.get("listing_page_limit", 60))
        max_depth = int(policies.get("listing_page_depth", 2))
    elif strategy == "ctdt_program":
        limit = int(policies.get("ctdt_program_limit", 4))
        max_depth = int(policies.get("ctdt_program_depth", 1))

    crawl_params = {
        "limit": limit,
        "maxDepth": max_depth,
        "scrapeOptions": build_scrape_options(strategy),
    }

    if cfg.get("include_patterns"):
        crawl_params["includePaths"] = cfg["include_patterns"]
    if cfg.get("exclude_patterns"):
        crawl_params["excludePaths"] = cfg["exclude_patterns"]

    return crawl_params


def normalize_scraped_page(scrape_result: Dict[str, Any], fallback_url: str) -> Dict[str, Any]:
    if not isinstance(scrape_result, dict):
        raise ValueError(f"Unexpected scrape result type: {type(scrape_result).__name__}")

    if "data" in scrape_result and isinstance(scrape_result["data"], dict):
        page = dict(scrape_result["data"])
    else:
        page = dict(scrape_result)

    metadata = dict(page.get("metadata", {}))
    metadata.setdefault("sourceURL", fallback_url)
    page["metadata"] = metadata
    return page


def extract_title_from_html(html: str, fallback: str = "") -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if not match:
        return fallback
    title = re.sub(r"\s+", " ", unescape(match.group(1))).strip()
    return title or fallback


def html_to_basic_markdown(html: str) -> str:
    if not html:
        return ""

    text = re.sub(
        r"<(script|style|noscript)\b[^>]*>.*?</\1>",
        "",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|section|article|li|tr|h[1-6]|table)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_seed_page_via_http(url: str, strategy: str) -> Dict[str, Any]:
    timeout = 120 if strategy == "slow_lane" else 90
    SOURCE_HOST_RATE_LIMITER.wait(url)
    response = requests.get(
        url,
        timeout=timeout,
        verify=False,
        headers=build_request_headers(),
    )
    response.raise_for_status()

    html = response.text or ""
    title = extract_title_from_html(html, fallback=url)
    return {
        "html": html,
        "markdown": html_to_basic_markdown(html),
        "metadata": {
            "title": title,
            "sourceURL": url,
            "statusCode": response.status_code,
        },
    }


def load_seed_pages_with_fallback(
    app: FirecrawlApp,
    url: str,
    strategy: str,
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    try:
        return [scrape_seed_page(app, url, strategy)]
    except Exception as scrape_error:
        if categorize_error(scrape_error) != "timeout" or strategy not in {"single_page", "slow_lane"}:
            raise

        logger.warning(
            f"Scrape timed out for {url}; falling back to direct HTTP fetch with a larger timeout"
        )
        try:
            return [fetch_seed_page_via_http(url, strategy)]
        except Exception as http_error:
            logger.warning(
                f"Direct HTTP fallback also failed for {url}; retrying with bounded crawl: {http_error}"
            )
            fallback_params = build_crawl_params(cfg, "listing_page")
            fallback_params["limit"] = 1 if strategy == "single_page" else 3
            fallback_params["maxDepth"] = resolve_absolute_crawl_depth(
                url,
                1 if strategy == "single_page" else 2,
            )
            pages = normalize_crawl_pages(
                app.crawl_url(url, params=fallback_params, poll_interval=2)
            )
            if not pages:
                raise RuntimeError(f"Timeout fallback returned 0 pages for {url}") from http_error
            return pages


def normalize_crawl_pages(crawl_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(crawl_result, dict):
        raise ValueError(f"Unexpected crawl result type: {type(crawl_result).__name__}")

    if crawl_result.get("success") is False:
        error_msg = crawl_result.get("error", "Unknown crawl error")
        raise RuntimeError(error_msg)

    pages = crawl_result.get("data", [])
    if not isinstance(pages, list):
        return []

    normalized_pages = []
    for page in pages:
        if not isinstance(page, dict):
            continue
        normalized_pages.append(normalize_scraped_page(page, page.get("url", "")))

    return normalized_pages


def extract_detail_urls_from_page(
    page_url: str,
    page: Dict[str, Any],
    seed_context_url: str,
    strategy: str,
) -> Dict[str, Any]:
    detail_kind = None
    detail_urls: List[str] = []

    if strategy not in {"listing_with_detail_fanout", "ctdt_index"}:
        return {"kind": detail_kind, "urls": detail_urls}

    html_content = page.get("html", "")
    page_title = page.get("metadata", {}).get("title", "")
    content_folder = get_content_folder(
        page_url,
        page_title,
        seed_context_url=seed_context_url,
    )

    if strategy == "ctdt_index":
        detail_kind = "ctdt_program"
        detail_urls = parse_ctdt_program_links_from_html(html_content, page_url)
    elif "dean" in content_folder:
        detail_kind = "detail_page"
        detail_urls = parse_de_mo_nganh_list_from_html(html_content, page_url)
    elif "quydinh_huongdan" in content_folder:
        detail_kind = "detail_page"
        detail_urls = parse_numbered_list_from_html(html_content, page_url)

    return {"kind": detail_kind, "urls": dedupe_preserve_order(detail_urls)}


def try_batch_scrape_pages(
    app: FirecrawlApp,
    urls: List[str],
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    policies = cfg.get("seed_policies", DEFAULT_SEED_POLICIES)
    if not policies.get("batch_scrape_detail_fanout"):
        return []

    scrape_options = build_scrape_options("detail_page")
    for method_name in ("batch_scrape_urls", "batch_scrape"):
        batch_method = getattr(app, method_name, None)
        if not callable(batch_method):
            continue

        attempts = [
            lambda: batch_method(urls=urls, params=scrape_options),
            lambda: batch_method(urls=urls, params={"formats": scrape_options["formats"]}),
            lambda: batch_method(urls=urls, scrape_options=scrape_options),
            lambda: batch_method(urls=urls, options=scrape_options),
        ]

        for attempt in attempts:
            try:
                raw_result = attempt()
            except TypeError:
                continue
            except Exception as e:
                logger.warning(f"{method_name} failed, falling back to sequential scrape: {e}")
                return []

            pages: List[Dict[str, Any]] = []
            if isinstance(raw_result, dict):
                if raw_result.get("success") is False:
                    logger.warning(
                        f"{method_name} returned an error, falling back to sequential scrape: "
                        f"{raw_result.get('error', 'unknown')}"
                    )
                    return []
                candidates = raw_result.get("data") or raw_result.get("results") or raw_result.get("pages") or []
            elif isinstance(raw_result, list):
                candidates = raw_result
            else:
                candidates = []

            for index, item in enumerate(candidates):
                if not isinstance(item, dict):
                    continue
                nested_page = item.get("data") if isinstance(item.get("data"), dict) else item
                fallback_url = item.get("url") or urls[min(index, len(urls) - 1)]
                pages.append(normalize_scraped_page(nested_page, fallback_url))

            if pages:
                logger.info(f"Using {method_name} for {len(pages)} detail pages")
                return pages

    return []

def ensure_dirs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_crawled_urls() -> set:
    if CRAWLED_CACHE.exists():
        with open(CRAWLED_CACHE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def mark_url_crawled(url: str):
    """Mark URL as crawled in cache"""
    try:
        with open(CRAWLED_CACHE, "a", encoding="utf-8") as f:
            f.write(url + "\n")
    except Exception:
        # Best effort write; don't crash the whole crawler if disk write fails
        logger.warning(f"Failed to write crawled URL to cache file: {url}")

    # Keep the in-memory set in sync so the running process doesn't
    # re-process URLs that were just saved.
    try:
        CRAWLED_URLS_SET.add(url)
    except Exception:
        logger.debug(f"Failed to add URL to in-memory crawled set: {url}")

def log_crawled_file(local_path: Path, source_url: str):
    """
    Log crawled file to crawled.txt with format:
    <local_path> - url: <source_url>
    """
    relative_path = local_path.relative_to(OUTPUT_DIR)

    with open(CRAWLED_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{relative_path} - url: {source_url}\n")

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

def get_content_folder(url: str, title: str = "", seed_context_url: str = "") -> str:
    """Map URL to folder structure based on new requirements"""
    text = (url + " " + title).lower()
    source_hint = (url + " " + seed_context_url).lower()

    if "ctsv.uit.edu.vn" in source_hint:
        if "/van-ban/thong-bao" in source_hint:
            return "ctsv/van-ban/thong-bao"
        if "/van-ban/ke-hoach" in source_hint:
            return "ctsv/van-ban/ke-hoach"
        if "/van-ban" in source_hint:
            return "ctsv/van-ban"
        if "/quy-trinh" in source_hint:
            return "ctsv/quy-trinh"
        if "/bai-viet" in source_hint:
            return "ctsv/bai-viet"
        if is_direct_file_url(url):
            return "ctsv/files"
        return "ctsv/khac/chua-phan-loai"
    
    # Giới thiệu
    if any(x in text for x in ["cong-thong-tin-dao-tao", "content/cong-thong-tin"]):
        return "daa/gioithieu/cong-thong-tin-dao-tao"
    elif any(x in text for x in ["nganh-dao-tao", "tuyensinh.uit.edu.vn/nganh"]):
        return "daa/gioithieu/nganh-dao-tao"
    elif any(x in text for x in ["chuc-nang-nhiem-vu"]):
        return "daa/gioithieu/chuc-nang-nhiem-vu"
    
    # Quy định - Hướng dẫn (CHECK BEFORE "Thông báo" to avoid false positives from /thongbao/huong-dan-*)
    elif any(x in text for x in ["qui-che-qui-dinh-qui-trinh", "quy-che-quy-dinh-quy-trinh"]):
        return "daa/quydinh_huongdan/qui-che-qui-dinh-qui-trinh"
    elif any(x in text for x in ["quy-che-quy-dinh-dao-tao-dai-hoc-cua-dhqg-hcm", "dhqg-hcm"]):
        return "daa/quydinh_huongdan/quyche-dhqg-hcm"
    elif any(x in text for x in ["quy-che-quy-dinh-dao-tao-dai-hoc-cua-bo-gddt", "bo-gddt"]):
        return "daa/quydinh_huongdan/quyche-bogddt"
    elif any(x in text for x in ["quy-dinh-giao-trinh", "53_qd_dhcntt"]):
        return "daa/quydinh_huongdan/quy-dinh-giao-trinh"
    elif any(x in text for x in ["quy-dinh-dao-tao-ngan-han", "dao-tao-ngan-han"]):
        return "daa/quydinh_huongdan/quy-dinh-dao-tao-ngan-han"
    elif any(x in text for x in ["quy-trinh-danh-cho-can-bo-giang-day", "can-bo-giang-day"]):
        return "daa/quydinh_huongdan/quy-trinh-can-bo-giang-day"
    elif any(x in text for x in ["quy-trinh-danh-cho-sinh-vien", "mot-so-quy-trinh-danh-cho-sinh-vien"]):
        return "daa/quydinh_huongdan/quy-trinh-sinh-vien"
    elif any(x in text for x in ["huong-dan-tra-cuu-va-xac-minh-van-bang", "tra-cuu-van-bang"]):
        return "daa/quydinh_huongdan/huong-dan-tra-cuu-van-bang"
    elif any(x in text for x in ["huong-dan-sinh-vien-dai-hoc-he-chinh-quy", "chuan-qua-trinh"]):
        return "daa/quydinh_huongdan/huong-dan-chuan-qua-trinh"
    elif any(x in text for x in ["huong-dan-trien-khai-day-va-hoc-qua-mang", "day-va-hoc-online", "covid"]):
        return "daa/quydinh_huongdan/huong-dan-day-va-hoc-online"
    
    # Thông báo (checked AFTER hướng dẫn)
    elif any(x in text for x in ["thongbaochinhquy", "thongbao-chinhquy"]):
        return "daa/thongbao/thongbao-chinhquy"
    elif any(x in text for x in ["thong-bao-vb2", "thongbao-vb2"]):
        return "daa/thongbao/thongbao-vb2"
    elif any(x in text for x in ["thongbaotuxa", "thongbao-tuxa"]):
        return "daa/thongbao/thongbao-tuxa"
    
    # Kế hoạch năm
    elif any(x in text for x in ["kehoachnam", "ke-hoach-nam"]):
        return "daa/kehoachnam"
    
    # Chương trình đào tạo - Hệ chính quy
    # Check for ANY cu-nhan, chuong-trinh, or ky-su program pages FIRST
    # IMPORTANT: Exclude hệ từ xa (those have "tu-xa" or "hinh-thuc-dao-tao-tu-xa" in URL/text)
    elif (("/cu-nhan-" in url or "/chuong-trinh-" in url or "/ky-su-" in url) and 
          "tu-xa" not in url.lower() and 
          "hinh-thuc-dao-tao-tu-xa" not in text.lower() and
          "qua-mang" not in url.lower()):
        # For CTDT program pages: use SEED year (folder) instead of URL year
        # This ensures all programs from a seed go into same folder
        year_folder = "khac"
        
        # Try to get year from the seed currently being processed first.
        if seed_context_url:
            seed_year_match = re.search(r'ctdt-khoa-(\d{4})', seed_context_url)
            if seed_year_match:
                seed_year = seed_year_match.group(1)
                year_folder = f"khoa_{seed_year}"
        
        # Fallback to URL year if seed year not found
        if year_folder == "khac":
            for year in range(2008, 2026):
                if f"khoa-{year}" in text or f"{year}" in text:
                    year_folder = f"khoa_{year}"
                    break
        
        # Extract program/major name from URL path
        # Strategy: Extract full URL path after /content/, then clean it
        # Examples:
        # - /content/cu-nhan-nganh-cong-nghe-thong-tin-ap-dung-tu-khoa-19-2024 → cong-nghe-thong-tin
        # - /content/chuong-trinh-tien-tien-nganh-he-thong-thong-tin-ap-dung-tu-khoa-18-2023 → he-thong-thong-tin
        # - /content/cu-nhan-nganh-mang-may-tinh-va-toan-thong-tin-chuong-trinh-lien-ket-voi... → mang-may-tinh-va-an-toan-thong-tin
        
        url_path_match = re.search(r'/content/([^/?#]+)', url)
        if url_path_match:
            full_path = url_path_match.group(1).lower()
            
            # Mapping chuẩn: URL pattern → Folder name (17 programs theo danh sách chuẩn)
            # Pattern matching: kiểm tra keywords trong URL để xác định program
            
            # Special cases: Song ngành và Chương trình đặc biệt
            if 'song-nganh' in full_path and 'thuong-mai' in full_path:
                major_name = 'songnganhthuongmaidientu'
            elif 'tien-tien' in full_path and 'he-thong-thong-tin' in full_path:
                major_name = 'chuongtrinhtientienhethongthongtin'
            
            # Birmingham City programs
            elif 'birmingham' in full_path:
                if 'mang-may-tinh' in full_path or ('mang' in full_path and 'an' in full_path):
                    major_name = 'mangmaytinhvaantoanthongtinbirminghamcity'
                else:  # Khoa học Máy tính Birmingham
                    major_name = 'khoahocmaytinhbirminghamcity'
            
            # Newcastle program
            elif 'newcastle' in full_path or ('ky-thuat-he-thong-may-tinh' in full_path and 'lien-ket' in full_path):
                major_name = 'kythuathethongmaytinhnewcastle'
            
            # Regular programs - extract core name
            elif 'khoa-hoc-du-lieu' in full_path:
                major_name = 'khoahocdulieu'
            elif 'an-toan-thong-tin' in full_path or 'toan-thong-tin' in full_path:
                major_name = 'antoanthongtin'
            elif 'thuong-mai-dien-tu' in full_path:
                major_name = 'thuongmaidientu'
            elif 'mang-may-tinh' in full_path and 'truyen-thong-du-lieu' in full_path:
                major_name = 'mangmaytinhvatruyenthongdulieu'
            elif 'truyen-thong-da-phuong-tien' in full_path:
                major_name = 'truyenthongdaphuongtien'
            elif 'thiet-ke-vi-mach' in full_path:
                major_name = 'thietkevimach'
            elif 'ky-thuat-may-tinh' in full_path:
                major_name = 'kythuatmaytinh'
            elif 'tri-tue-nhan-tao' in full_path:
                major_name = 'trituenhantao'
            elif 'ky-thuat-phan-mem' in full_path:
                major_name = 'kythuatphanmem'
            elif 'khoa-hoc-may-tinh' in full_path:
                major_name = 'khoahocmaytinh'
            elif 'he-thong-thong-tin' in full_path:
                major_name = 'hethongthongtin'
            elif 'cong-nghe-thong-tin' in full_path:
                major_name = 'congnghethongtin'
            else:
                # Fallback: extract name and clean
                cleaned = full_path
                for prefix in ['cu-nhan-khoa-hoc-nganh-', 'cu-nhan-nganh-', 'chuong-trinh-tien-tien-nganh-', 'chuong-trinh-dao-tao-song-nganh-nganh-']:
                    if cleaned.startswith(prefix):
                        cleaned = cleaned[len(prefix):]
                        break
                for marker in ['ap-dung-tu', 'chuong-trinh-lien-ket', 'hinh-thuc']:
                    if marker in cleaned:
                        cleaned = cleaned.split(marker)[0].rstrip('-')
                        break
                major_name = cleaned.replace('-', '')
            
            return f"daa/chuongtrinh_daotao/he-chinhquy/{year_folder}/{major_name}"
        
        return f"daa/chuongtrinh_daotao/he-chinhquy/{year_folder}"
    
    # Then check for CTDT index pages (these should stay at khoa-YYYY level, not in subfolders)
    elif "/chuong-trinh-dao-tao/" in text or "/cqui/" in text or ("ctdt-khoa-" in text and "chinh-quy" in text):
        # Extract year from URL
        year_folder = "khac"
        for year in range(2011, 2026):
            if f"khoa-{year}" in text:
                year_folder = f"khoa_{year}"
                break
        
        return f"daa/chuongtrinh_daotao/he-chinhquy/{year_folder}"
    
    # Chương trình đào tạo cũ - simplified routing, just categorize files
    elif any(x in text for x in ["chuong-trinh-dao-tao-cu", "ctdt-cu"]):
        return "daa/chuongtrinh_daotao/chuongtrinhdaotaocu"
    
    elif any(x in text for x in ["danh-muc-mon-hoc-dai-hoc", "danh-muc-mon-hoc"]):
        return "daa/chuongtrinh_daotao/he-chinhquy/danh-muc-mon-hoc"
    elif any(x in text for x in ["bang-tom-tat-mon-hoc", "tom-tat-mon-hoc"]):
        return "daa/chuongtrinh_daotao/he-chinhquy/bang-tom-tat-mon-hoc"
    
    # Chương trình đào tạo - Hệ từ xa
    # Check for tu-xa in URL OR keywords indicating tu-xa programs
    elif ("/tu-xa/" in url or "tuxa" in url or 
          "hinh-thuc-dao-tao-tu-xa" in text.lower() or 
          "qua-mang" in url.lower()):
        # For programs: use SEED year instead of URL year
        year_folder = "khac"
        
        # Try to get year from the seed currently being processed first.
        if seed_context_url and "/tu-xa/" in seed_context_url:
            seed_year_match = re.search(r'ctdt-khoa-(\d{4})', seed_context_url)
            if seed_year_match:
                seed_year = seed_year_match.group(1)
                year_folder = f"khoa_{seed_year}"  # Changed to underscore for consistency
        
        # Fallback to URL year if seed year not found
        if year_folder == "khac":
            for year in range(2008, 2026):
                if f"khoa-{year}" in text:
                    year_folder = f"khoa_{year}"  # Changed to underscore
                    break
        
        # Check if this is a program page (has /content/ in URL)
        if "/content/" in url and "cu-nhan" in url.lower():
            # Extract program name using keyword mapping (6 programs for he-tuxa)
            program_match = re.search(r'/content/([^/?#]+)', url)
            if program_match:
                full_path = program_match.group(1).lower()
                
                # Keyword-based mapping for 6 he-tuxa programs
                # Order matters: check most specific patterns first
                
                # 1. Trí tuệ nhân tạo - Văn bằng 2
                if 'tri-tue' in full_path and 'van-bang' in full_path:
                    program_name = 'trituenhantaovanbang2'
                # 2. Trí tuệ nhân tạo - Liên thông
                elif 'tri-tue' in full_path and 'lien-thong' in full_path:
                    program_name = 'trituenhantaolienthong'
                # 3. Trí tuệ nhân tạo - Thường (must check after văn bằng 2 and liên thông)
                elif 'tri-tue' in full_path:
                    program_name = 'trituenhantao'
                # 4. CNTT - Văn bằng 2 (without ngành in pattern)
                elif 'van-bang' in full_path and ('cong-nghe' in full_path or 'cntt' in full_path):
                    program_name = 'congnghethongtinvanbang2'
                # 5. CNTT - Liên thông (without ngành in pattern)
                elif 'lien-thong' in full_path and ('cong-nghe' in full_path or 'cntt' in full_path):
                    program_name = 'congnghethongtinlienthong'
                # 6. CNTT - Thường (must check after văn bằng 2 and liên thông)
                elif 'cong-nghe-thong-tin' in full_path or 'cntt' in full_path:
                    program_name = 'congnghethongtin'
                else:
                    # Fallback: clean the path
                    cleaned_path = full_path
                    for prefix in ['cu-nhan-van-bang-', 'cu-nhan-lien-thong-', 'cu-nhan-nganh-', 'cu-nhan-']:
                        if cleaned_path.startswith(prefix):
                            cleaned_path = cleaned_path[len(prefix):]
                            break
                    for marker in ['hinh-thuc-dao-tao', 'qua-mang', 'khoa-']:
                        if marker in cleaned_path:
                            cleaned_path = cleaned_path.split(marker)[0].rstrip('-')
                            break
                    program_name = cleaned_path.replace('-', '')
                
                return f"daa/chuongtrinh_daotao/he-tuxa/{year_folder}/{program_name}"
        
        return f"daa/chuongtrinh_daotao/he-tuxa/{year_folder}"
    
    # Đề án mở ngành - phân loại giống quy định hướng dẫn (numbered docs)
    elif any(x in text for x in ["de-mo-nganh", "loai-bai-viet/de-mo-nganh"]):
        # For detail pages, extract number and create folder like: daa/chuongtrinh_daotao/he-chinhquy/dean/1-tri-tue-nhan-tao/
        # Check if URL is a detail page (not the listing page) by checking if it's in cache
        if url in NUMBERED_DOCS_CACHE:
            # Get index and title from cache (set during parsing)
            cache_data = NUMBERED_DOCS_CACHE.get(url)
            
            # Check if cache is tuple (new format for de-mo-nganh) or string (old format)
            if isinstance(cache_data, tuple) and len(cache_data) == 2:
                doc_index, cached_title = cache_data
                # Use cached title if current title is empty
                major_title = title or cached_title
            else:
                # Fallback for old format
                doc_index = cache_data if isinstance(cache_data, str) else str(cache_data)
                major_title = title
            
            # Extract major name: "Đề án mở ngành Trí tuệ nhân tạo" -> "Trí tuệ nhân tạo"
            major_title = title
            for prefix in ["Đề án mở ngành", "đề án mở ngành", "De an mo nganh", "Đề án"]:
                if prefix in major_title:
                    major_title = major_title.replace(prefix, "").strip()
            
            major_slug = safe_slug(major_title, max_length=48, fallback=f"de-an-{doc_index}")
            
            if major_slug:
                # Create folder: {index}-{major_slug} under he-chinhquy
                folder_name = f"{doc_index}-{major_slug}"
                return f"daa/chuongtrinh_daotao/dean/{folder_name}"
        
        # For listing page
        return "daa/chuongtrinh_daotao/dean"
    
    else:
        return "daa/khac/chua-phan-loai"

def append_jsonl(obj: Dict[str, Any]):
    with open(META_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def append_page_artifact_jsonl(obj: Dict[str, Any]):
    with open(PAGE_ARTIFACTS_JSONL, "a", encoding="utf-8") as f:
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

def extract_direct_file_url(raw_url: str, base_url: str) -> str:
    full_url = urljoin(base_url, unescape(raw_url or ""))
    parsed = urlparse(full_url)
    if Path(parsed.path).suffix.lower() in DIRECT_FILE_EXTENSIONS:
        return full_url
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() not in {"file", "url", "src", "download"}:
            continue
        candidate = urljoin(full_url, unescape(value))
        if Path(urlparse(candidate).path).suffix.lower() in DIRECT_FILE_EXTENSIONS:
            return candidate
    return ""


def find_download_links(html: str, base_url: str) -> List[str]:
    if not html:
        return []
    
    links = []
    patterns = [r'(?:href|src|data)=["\']([^"\']+)["\']']
    
    for pattern in patterns:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for match in matches:
            full_url = extract_direct_file_url(match, base_url)
            if full_url and full_url not in links:
                links.append(full_url)
    
    return links

def download_file(
    url: str,
    output_dir: Path,
    category: str = "files",
    seed_context_url: str = "",
    parent_page_url: str = "",
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {
        "url": url,
        "downloaded": False,
        "skipped": False,
        "size_bytes": 0,
        "path": "",
        "error": "",
    }

    try:
        filename = build_safe_download_name(url)
        output_path = output_dir / filename

        if output_path.exists():
            logger.debug(f"File already exists: {filename}")
            result["skipped"] = True
            result["path"] = str(output_path)
            result["size_bytes"] = output_path.stat().st_size
            metadata = {
                "title": filename,
                "url": url,
                "type": "file",
                "seed": seed_context_url if seed_context_url else "unknown",
                "seed_folder": category,
                "parent_page_url": parent_page_url,
                "file_path": str(output_path),
                "size_mb": round(result["size_bytes"] / (1024 * 1024), 2),
                "status": "skipped",
                "date": datetime.now().isoformat(),
            }
            append_jsonl(metadata)
            return result

        SOURCE_HOST_RATE_LIMITER.wait(url)
        response = requests.get(url, stream=True, timeout=30, verify=False)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        size_bytes = output_path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        logger.info(f"Downloaded [{category}]: {filename} ({size_mb:.2f} MB)")

        # Log to crawled.txt
        log_crawled_file(output_path, url)

        if seed_context_url:
            crawl_stats.add_download(seed_context_url, size_bytes=size_bytes)

        metadata = {
            "title": filename,
            "url": url,
            "type": "file",
            "seed": seed_context_url if seed_context_url else "unknown",
            "seed_folder": category,
            "parent_page_url": parent_page_url,
            "file_path": str(output_path),
            "size_mb": round(size_mb, 2),
            "status": "downloaded",
            "date": datetime.now().isoformat(),
        }
        append_jsonl(metadata)

        result["downloaded"] = True
        result["size_bytes"] = size_bytes
        result["path"] = str(output_path)
        return result

    except Exception as e:
        logger.warning(f"Failed to download {url}: {e}")
        result["error"] = str(e)
        response = getattr(e, "response", None)
        status_code = getattr(response, "status_code", "")
        metadata = {
            "title": build_safe_download_name(url),
            "url": url,
            "type": "file",
            "seed": seed_context_url if seed_context_url else "unknown",
            "seed_folder": category,
            "parent_page_url": parent_page_url,
            "file_path": "",
            "size_mb": 0,
            "status": "failed",
            "status_code": status_code,
            "error": str(e),
            "date": datetime.now().isoformat(),
        }
        append_jsonl(metadata)
        return result

def extract_numbered_title(title: str, content: str = "") -> Optional[tuple]:
    """
    Extract numbered document information from title or content.
    Returns (number, full_title) or None.
    
    Example:
        "01. Quyết định về việc ban hành..." -> ("01", "Quyết định về việc ban hành...")
        "02. Quy định về đánh giá..." -> ("02", "Quy định về đánh giá...")
    
    Note: Keeps the full Vietnamese title with original formatting.
    """
    # Try to find pattern like "01.", "02.", etc. at the beginning
    pattern = r'^(\d{1,3})[\.\)]\s*(.+?)(?:\s*$)'
    
    # Search in title first
    match = re.search(pattern, title.strip(), re.MULTILINE)
    if not match:
        # Try to find in content (first few lines)
        first_lines = content[:1000] if content else ""
        match = re.search(pattern, first_lines, re.MULTILINE)
    
    if match:
        number = match.group(1).zfill(2)  # Pad to 2 digits: "1" -> "01"
        doc_title = match.group(2).strip()
        
        # Clean up excessive whitespace but keep Vietnamese characters
        doc_title = re.sub(r'\s+', ' ', doc_title)
        # Remove any trailing punctuation
        doc_title = doc_title.rstrip('.,;:')
        # Limit length
        doc_title = doc_title[:200]
        
        return (number, doc_title)
    
    return None

def get_nhom_lon(url: str, title: str) -> str:
    """
    Determine the main category group (nhom_lon) for quydinh_huongdan.
    
    Categories:
    - qui-che-qui-dinh-qui-trinh: Official regulations and procedures
    - huong-dan-sinh-vien: Student guidance documents
    - bieumau-bangbieucuahang: Forms and templates
    
    Returns the category slug or default.
    """
    text = (url + " " + title).lower()
    
    # Check for forms/templates
    if any(x in text for x in ["biểu mẫu", "bieu-mau", "bảng biểu", "bang-bieu", "mẫu đơn"]):
        return "bieumau-bangbieucuahang"
    
    # Check for student guidance
    if any(x in text for x in ["hướng dẫn sinh viên", "huong-dan-sinh-vien", 
                                "đăng ký", "dang-ky", "tốt nghiệp", "tot-nghiep",
                                "học vụ", "hoc-vu"]):
        return "huong-dan-sinh-vien"
    
    # Default to qui-che-qui-dinh-qui-trinh (most common)
    return "qui-che-qui-dinh-qui-trinh"

def parse_numbered_list_from_html(html: str, base_url: str) -> List[str]:
    """
    Parse numbered document list from HTML content.
    Returns list of detail URLs found in this listing page.
    Also caches URL metadata in NUMBERED_DOCS_CACHE.
    
    Example HTML patterns:
    <a href="/content/123">01. Quyết định về việc...</a>
    <a href="/content/123">1) Quyết định về việc...</a>
    """
    global NUMBERED_DOCS_CACHE
    
    if not html:
        return []
    
    # Pattern to find numbered links like "01. Title" or "1) Title"
    # Match: <a href="...">01. Quyết định...</a>
    # Also match with optional whitespace and newlines
    pattern = r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>\s*(\d{1,3})[\.\)]\s*([^<]+)</a>'
    
    matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)
    
    if matches:
        logger.info(f"Found {len(matches)} potential numbered items in {base_url}")
    
    found_urls = []
    seen_urls = set()  # Avoid duplicates
    
    for href, number, title_text in matches:
        # Build full URL
        full_url = urljoin(base_url, href)
        
        # Skip if already processed in this page
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)
        
        # Clean title - handle HTML entities and excessive whitespace
        title_clean = title_text.strip()
        title_clean = re.sub(r'\s+', ' ', title_clean)
        title_clean = title_clean.rstrip('.,;:')[:200]
        
        # Skip if title is too short (likely garbage)
        if len(title_clean) < 5:
            continue
        
        # Pad number
        number_padded = number.zfill(2)
        
        # Determine nhom_lon from base_url
        nhom_lon = get_nhom_lon(base_url, title_text)
        
        # Store in cache
        NUMBERED_DOCS_CACHE[full_url] = (number_padded, title_clean, nhom_lon)
        found_urls.append(full_url)
        logger.info(f"Cached numbered doc: {full_url} -> {number_padded}. {title_clean}")
    
    if found_urls:
        logger.info(f"Successfully cached {len(found_urls)} numbered docs from {base_url}")
    
    return found_urls

def parse_de_mo_nganh_list_from_html(html: str, base_url: str) -> List[str]:
    """
    Parse "đề án mở ngành" (project proposal) list from HTML content.
    Extracts detail links with ordering information.
    Returns list of detail URLs found in this listing page.
    
    Pattern: Links to proposal pages, preserving order from page
    """
    global NUMBERED_DOCS_CACHE
    
    if not html:
        return []
    
    # For de-mo-nganh, look for links that start with "de-mo-nganh" or "de-song-nganh" in URL
    # Pattern: <a href="...de-mo-nganh...">Title</a> or <a href="...de-song-nganh...">Title</a>
    pattern = r'<a[^>]+href=["\']([^"\']*(?:de-mo-nganh|de-song-nganh)[^"\']*)["\'][^>]*>([^<]+)</a>'
    
    matches = re.findall(pattern, html, re.IGNORECASE)
    
    found_urls = []
    seen_urls = set()
    index = 1  # For numbering
    
    for href, title_text in matches:
        # Build full URL
        full_url = urljoin(base_url, href)
        
        # Skip if already processed
        if full_url in seen_urls:
            continue
        
        # Skip if URL is same as base (listing page itself)
        if full_url == base_url.rstrip('/'):
            continue
        
        seen_urls.add(full_url)
        
        # Clean title
        title_clean = title_text.strip()
        title_clean = re.sub(r'\s+', ' ', title_clean)
        title_clean = title_clean.rstrip('.,;:')[:200]
        
        # Include all detail URLs (de-mo-nganh URLs don't always have /content/ or /node/)
        # Just ensure it's not the listing page itself (already checked above)
        found_urls.append(full_url)
        
        # Store index and title in cache for later use in get_content_folder()
        # Use tuple format: (index, title) to distinguish from quydinh_huongdan cache format
        NUMBERED_DOCS_CACHE[full_url] = (str(index), title_clean)
        
        logger.info(f"Found de-mo-nganh detail #{index}: {full_url} -> {title_clean}")
        index += 1
    
    if found_urls:
        logger.info(f"Found {len(found_urls)} de-mo-nganh detail pages from {base_url}")
    
    return found_urls

def parse_ctdt_program_links_from_html(html: str, base_url: str) -> List[str]:
    """
    Parse program links from CTDT index pages (ctdt-khoa-YYYY).
    Extracts all /content/cu-nhan-nganh-*, /content/chuong-trinh-*, and /content/ky-su-* links
    
    Example HTML patterns from CTDT pages:
    <a href="/content/cu-nhan-nganh-cong-nghe-thong-tin-ap-dung-tu-khoa-19-2024">Cử nhân ngành...</a>
    <a href="/content/chuong-trinh-tien-tien-nganh-...">Chương trình tiên tiến...</a>
    <a href="/content/ky-su-va-cu-nhan-nganh-ky-thuat-may-tinh-...">Kỹ sư và cử nhân...</a>
    """
    if not html:
        return []
    
    # Pattern to find all program links: cu-nhan (any), chuong-trinh (any), ky-su (any)
    # More flexible pattern to catch: cu-nhan-nganh, cu-nhan-khoa-hoc-nganh, chuong-trinh-*, ky-su-*, etc.
    pattern = r'<a[^>]+href=["\']([^"\']*(?:cu-nhan-|chuong-trinh-|ky-su-)[^"\']*)["\'][^>]*>([^<]+)</a>'
    
    matches = re.findall(pattern, html, re.IGNORECASE)
    
    if matches:
        logger.info(f"Found {len(matches)} program links in {base_url}")
    
    found_urls = []
    seen_urls = set()
    
    for href, title_text in matches:
        if not href or href in seen_urls:
            continue
        seen_urls.add(href)
        
        # Build full URL
        full_url = urljoin(base_url, href)
        
        # Filter: only include /content/ URLs (program pages)
        # This catches: cu-nhan-nganh, cu-nhan-khoa-hoc-nganh, chuong-trinh-*, ky-su-*, etc.
        if '/content/' in full_url and ('cu-nhan-' in full_url.lower() or 'chuong-trinh-' in full_url.lower() or 'ky-su-' in full_url.lower()):
            found_urls.append(full_url)
            logger.info(f"Found CTDT program: {full_url}")
    
    if found_urls:
        logger.info(f"Successfully found {len(found_urls)} program links from {base_url}")
    
    return found_urls

def load_content_hash_cache():
    """Load content hash cache from disk"""
    global CONTENT_HASH_CACHE
    if CONTENT_HASH_FILE.exists():
        try:
            with open(CONTENT_HASH_FILE, "r", encoding="utf-8") as f:
                CONTENT_HASH_CACHE = json.load(f)
            logger.info(f"Loaded {len(CONTENT_HASH_CACHE)} content hashes from cache")
        except Exception as e:
            logger.warning(f"Failed to load content hash cache: {e}")
            CONTENT_HASH_CACHE = {}

def save_content_hash_cache():
    """Save content hash cache to disk"""
    try:
        CONTENT_HASH_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CONTENT_HASH_FILE, "w", encoding="utf-8") as f:
            json.dump(CONTENT_HASH_CACHE, f)
    except Exception as e:
        logger.warning(f"Failed to save content hash cache: {e}")

def is_duplicate_content(content: str, url: str) -> bool:
    """Record duplicate content for diagnostics without dropping source URLs."""
    if not content:
        return False
    
    # For program pages (cu-nhan-nganh), include year in hash to allow same content across different years
    # Example: CTĐT khóa 2023 = khóa 2024 → should save BOTH (different years)
    hash_key = content.strip()
    if "/cu-nhan-nganh-" in url or "/chuong-trinh-" in url:
        # Extract year from URL to make hash unique per year
        year_match = re.search(r'khoa-(\d{2})-(\d{4})', url)
        if year_match:
            year = year_match.group(2)  # e.g., "2024"
            hash_key = f"{year}:{content.strip()}"
    
    # Normalize content: strip whitespace and normalize unicode
    normalized = unicodedata.normalize('NFKD', hash_key)
    content_hash = hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    # Check if we've seen this hash before
    if content_hash in CONTENT_HASH_CACHE:
        original_url = CONTENT_HASH_CACHE[content_hash]
        if original_url != url:
            logger.info(f"Duplicate content observed: {url} (identical to {original_url}); preserving artifact")
            return True
    else:
        # First time seeing this content
        CONTENT_HASH_CACHE[content_hash] = url
        save_content_hash_cache()
    
    return False

def trim_markdown_content(markdown: str) -> str:
    """Trim markdown to main content only: remove navigation, skip-to links, footer, and summarize large tables"""
    if not markdown:
        return markdown
    
    lines = markdown.split('\n')
    trimmed_lines = []
    in_table = False
    table_lines = []
    skip_related_section = False
    in_footer = False
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        # Skip empty lines at the beginning
        if not trimmed_lines and not stripped:
            continue
        
        # Detect footer start - only real footer patterns (strict matching)
        # Must have specific address/contact info, not just generic keywords
        if any(pattern in stripped.lower() for pattern in [
            "© 20", "copyright ©", "all rights reserved",
            "facebook.com/", "youtube.com/", "zalo.me/",
            "khu phố 6", "linh trung", "thủ đức",  # UIT specific address
            "điện thoại: ", "email: ", "fax: ",  # Must have colon (contact info format)
        ]):
            in_footer = True
            continue
        
        # Skip content after footer detected
        if in_footer:
            continue
        
        # Skip navigation/skip-to links (including variations)
        if stripped.lower().startswith('[skip'):
            continue
        if any(kw in stripped.lower() for kw in ["skip to", "skip navigation", "skip link"]):
            continue
        
        # Detect "Bài viết liên quan" section - skip everything after this heading
        # Support both markdown heading (# Text) and underline heading (Text\n---)
        is_markdown_heading = stripped.startswith('#')
        is_underline_heading = stripped and len(stripped) > 3 and all(c == '-' for c in stripped)
        is_heading = is_markdown_heading or is_underline_heading
        
        # Also check if CURRENT line is a heading keyword and NEXT line is underline (2-line heading pattern)
        is_two_line_heading = False
        if i + 1 < len(lines):
            next_line_stripped = lines[i + 1].strip()
            if next_line_stripped and all(c == '-' for c in next_line_stripped) and len(next_line_stripped) > 3:
                # Next line is underline, check if current line is a keyword
                if any(pattern in stripped.lower() for pattern in [
                    "bài viết liên quan", "related articles", "related posts", "trang"
                ]):
                    is_two_line_heading = True
        
        if (is_heading or is_two_line_heading) and any(pattern in stripped.lower() for pattern in [
            "bài viết liên quan",
            "related articles",
            "related posts",
            "trang",  # Pagination section
        ]):
            skip_related_section = True
            continue
        
        # Also skip "Back to top" links (exact match at line start)
        if stripped.lower() in ["back to top", "quay lại đầu trang", "lên đầu trang"]:
            skip_related_section = True
            continue
        
        # Skip pagination links and related content
        if skip_related_section:
            continue
        
        # Detect table start (markdown table lines start with |)
        if '|' in line and not in_table:
            in_table = True
            table_lines = [line]
        elif in_table:
            if '|' in line:
                table_lines.append(line)
            else:
                # Table ended, decide whether to include it
                in_table = False
                # Include table only if not too large (> 30 rows = skip/summarize)
                if len(table_lines) <= 30:
                    trimmed_lines.extend(table_lines)
                else:
                    # For large tables, add a summary and skip inline content
                    col_count = len([c for c in table_lines[0].split('|') if c.strip()])
                    trimmed_lines.append(f"_[Bảng lớn: {len(table_lines)} dòng, {col_count} cột - bỏ qua để tiết kiệm dung lượng]_\n")
                table_lines = []
                # Add the non-table line that ended this table
                if stripped:
                    trimmed_lines.append(line)
        else:
            trimmed_lines.append(line)
    
    # Handle table at end of file
    if in_table and table_lines:
        if len(table_lines) <= 30:
            trimmed_lines.extend(table_lines)
        else:
            col_count = len([c for c in table_lines[0].split('|') if c.strip()])
            trimmed_lines.append(f"_[Bảng lớn: {len(table_lines)} dòng, {col_count} cột - bỏ qua để tiết kiệm dung lượng]_")
    
    result = '\n'.join(trimmed_lines)
    # Clean up excessive newlines (more than 3 in a row)
    while '\n\n\n\n' in result:
        result = result.replace('\n\n\n\n', '\n\n\n')
    
    return result.strip()

def save_content(
    url: str,
    data: Dict[str, Any],
    skip_global_cache: bool = False,
    seed_context_url: str = "",
):
    """
    Save content to disk. 
    
    Args:
        url: URL of the content
        data: Page data from Firecrawl
        skip_global_cache: If True, don't add to global CRAWLED_URLS_SET (for CTDT programs that need re-crawl per year)
        seed_context_url: Seed URL that owns this page for folder mapping and stats attribution
    """
    global NUMBERED_DOCS_CACHE
    result = {
        "saved": False,
        "skipped": False,
        "downloads": 0,
        "size_bytes": 0,
    }
    
    title = data.get("metadata", {}).get("title", "")
    content_folder = get_content_folder(url, title, seed_context_url=seed_context_url)
    
    html_content = data.get("html", "")
    
    # Check if this URL is in the numbered docs cache (from listing page)
    numbered_info = None
    if url in NUMBERED_DOCS_CACHE:
        cache_data = NUMBERED_DOCS_CACHE[url]
        
        # Check if this is de-mo-nganh (tuple with 2 elements) or quydinh_huongdan (tuple with 3 elements)
        if isinstance(cache_data, tuple):
            if len(cache_data) == 2:
                # This is de-mo-nganh - content_folder already set correctly by get_content_folder()
                # No need to override content_folder here
                pass
            elif len(cache_data) == 3:
                # This is quydinh_huongdan - override content_folder
                number, doc_title, nhom_lon = cache_data
                # Use slugified title (no diacritics, with hyphens)
                doc_title_slug = safe_slug(doc_title, max_length=48, fallback=f"doc-{number}")
                numbered_folder = f"{number}-{doc_title_slug}"
                content_folder = f"daa/quydinh_huongdan/{nhom_lon}/{numbered_folder}"
                numbered_info = (number, doc_title)
                logger.info(f"Using cached numbered doc: {numbered_folder} in {nhom_lon}")
    
    # Otherwise, try to detect from current page content
    elif "quydinh_huongdan" in content_folder:
        markdown_content = data.get("markdown", "")
        content_text = markdown_content or html_content
        
        numbered_info = extract_numbered_title(title, content_text)
        if numbered_info:
            number, doc_title = numbered_info
            # Determine nhom_lon (main category group)
            nhom_lon = get_nhom_lon(url, title)
            # Use slugified title (no diacritics, with hyphens)
            doc_title_slug = safe_slug(doc_title, max_length=48, fallback=f"doc-{number}")
            numbered_folder = f"{number}-{doc_title_slug}"
            content_folder = f"daa/quydinh_huongdan/{nhom_lon}/{numbered_folder}"
            logger.info(f"Detected numbered document: {numbered_folder} in {nhom_lon}")
    
    content_dir = OUTPUT_DIR / content_folder
    
    # For quydinh_huongdan with numbered docs, save directly in the folder (no html/pdf subfolders)
    if numbered_info and "quydinh_huongdan" in content_folder:
        # Files go directly in the numbered folder
        content_dir.mkdir(parents=True, exist_ok=True)
        html_dir = content_dir
        markdown_dir = content_dir
        firecrawl_markdown_dir = content_dir / "firecrawl_markdown"
        pdf_dir = content_dir
        docx_dir = content_dir
        xlsx_dir = content_dir
        firecrawl_markdown_dir.mkdir(parents=True, exist_ok=True)
    else:
        # For other categories, use traditional structure with subfolders
        html_dir = content_dir / "html"
        markdown_dir = content_dir / "markdown"
        firecrawl_markdown_dir = content_dir / "firecrawl_markdown"
        pdf_dir = content_dir / "pdf"
        docx_dir = content_dir / "docx"
        xlsx_dir = content_dir / "xlsx"
        
        html_dir.mkdir(parents=True, exist_ok=True)
        markdown_dir.mkdir(parents=True, exist_ok=True)
        firecrawl_markdown_dir.mkdir(parents=True, exist_ok=True)
        pdf_dir.mkdir(parents=True, exist_ok=True)
        docx_dir.mkdir(parents=True, exist_ok=True)
        xlsx_dir.mkdir(parents=True, exist_ok=True)
    
    safe_name = build_safe_url_basename(url)
    
    # Track duplicate content for QC. Every source URL still keeps its raw artifact.
    if data.get("markdown"):
        is_duplicate_content(data["markdown"], url)
    
    total_size = 0
    download_count = 0
    html_file = None
    raw_markdown_file = None
    normalized_markdown_file = None
    normalized_markdown_content = ""
    attachment_results = []
    
    if data.get("html"):
        html_file = html_dir / f"{safe_name}.html"
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(data["html"])
        total_size += html_file.stat().st_size
        logger.info(f"Saved HTML [{content_folder}]: {html_file.name}")
        # Don't log HTML to crawled.txt (only MD and PDF)
        
        download_links = find_download_links(data["html"], url)
        for link in download_links:
            if link.lower().endswith('.pdf'):
                download_result = download_file(
                    link,
                    pdf_dir,
                    content_folder,
                    seed_context_url=seed_context_url,
                    parent_page_url=url,
                )
            elif link.lower().endswith(('.doc', '.docx')):
                download_result = download_file(
                    link,
                    docx_dir,
                    content_folder,
                    seed_context_url=seed_context_url,
                    parent_page_url=url,
                )
            elif link.lower().endswith(('.xls', '.xlsx')):
                download_result = download_file(
                    link,
                    xlsx_dir,
                    content_folder,
                    seed_context_url=seed_context_url,
                    parent_page_url=url,
                )
            else:
                download_result = {"url": link, "downloaded": False, "skipped": False, "path": "", "error": ""}

            if download_result.get("downloaded"):
                download_count += 1
            attachment_results.append({
                "url": link,
                "local_path": download_result.get("path", ""),
                "status": (
                    "downloaded"
                    if download_result.get("downloaded")
                    else "skipped"
                    if download_result.get("skipped")
                    else "failed"
                ),
                "size_bytes": download_result.get("size_bytes", 0),
                "error": download_result.get("error", ""),
            })
    
    if data.get("markdown"):
        raw_markdown_file = firecrawl_markdown_dir / f"{safe_name}.md"
        with open(raw_markdown_file, "w", encoding="utf-8") as f:
            f.write(data["markdown"])
        total_size += raw_markdown_file.stat().st_size
        logger.info(f"Saved Firecrawl Markdown [{content_folder}]: {raw_markdown_file.name}")

        md_file = markdown_dir / f"{safe_name}.md"
        markdown_content = data["markdown"]
        
        # Trim markdown to main content only
        normalized_markdown_content = trim_markdown_content(markdown_content)
        with open(md_file, "w", encoding="utf-8") as f:
            f.write(normalized_markdown_content)
        total_size += md_file.stat().st_size
        normalized_markdown_file = md_file
        logger.info(f"Saved Markdown [{content_folder}]: {md_file.name}")
        log_crawled_file(md_file, url)  # Log to crawled.txt
    
    # Update stats
    if seed_context_url and total_size > 0:
        crawl_stats.add_page(seed_context_url, total_size, success=True)
    
    artifact_content = normalized_markdown_content or data.get("markdown", "") or html_content
    artifact_hash = hashlib.md5(artifact_content.encode("utf-8")).hexdigest() if artifact_content else ""
    page_artifact = {
        "canonical_url": url,
        "raw_html_path": str(html_file) if html_file else "",
        "firecrawl_markdown_path": str(raw_markdown_file) if raw_markdown_file else "",
        "normalized_markdown_path": str(normalized_markdown_file) if normalized_markdown_file else "",
        "text_path": "",
        "links_path": "",
        "attachments": attachment_results,
        "content_hash": artifact_hash,
        "status_code": data.get("metadata", {}).get("statusCode", 200),
        "crawl_at": datetime.now().isoformat(),
    }
    append_page_artifact_jsonl(page_artifact)

    metadata = {
        "title": data.get("metadata", {}).get("title", ""),
        "url": url,
        "type": "html",
        "seed": seed_context_url if seed_context_url else "unknown",
        "content_folder": content_folder,
        "content": artifact_content[:10000],
        "source_url": data.get("metadata", {}).get("sourceURL", url),
        "html_path": str(html_file) if html_file else "",
        "firecrawl_markdown_path": str(raw_markdown_file) if raw_markdown_file else "",
        "markdown_path": str(normalized_markdown_file) if normalized_markdown_file else "",
        "content_hash": artifact_hash,
        "attachments": attachment_results,
        "status_code": data.get("metadata", {}).get("statusCode", 200),
        "size_bytes": total_size,
        "date": datetime.now().isoformat(),
    }
    append_jsonl(metadata)
    result["saved"] = total_size > 0
    result["downloads"] = download_count
    result["size_bytes"] = total_size
    if result["saved"] and not skip_global_cache:
        mark_url_crawled(url)
    return result

def merge_execution_summary(target: Dict[str, Any], source: Dict[str, Any]):
    for key in ("pages", "downloads", "skipped", "errors"):
        target[key] = target.get(key, 0) + source.get(key, 0)


def process_seed_pages(
    pages: List[Dict[str, Any]],
    seed_url: str,
    crawled_urls: set,
    strategy: str,
    *,
    skip_global_cache: bool = False,
    allow_global_cache: bool = True,
) -> Dict[str, Any]:
    summary = {
        "pages": 0,
        "downloads": 0,
        "skipped": 0,
        "errors": 0,
        "detail_kind": None,
        "detail_urls": [],
    }
    seen_detail_urls = set()
    seen_page_urls = set()

    for raw_page in pages:
        page_url = seed_url

        try:
            if isinstance(raw_page, dict):
                page_url = raw_page.get("metadata", {}).get("sourceURL") or raw_page.get("url") or seed_url

            page = normalize_scraped_page(raw_page, seed_url)
            page_url = page.get("metadata", {}).get("sourceURL", seed_url) or seed_url

            if page_url in seen_page_urls:
                continue
            seen_page_urls.add(page_url)

            detail_info = extract_detail_urls_from_page(page_url, page, seed_url, strategy)
            if detail_info["kind"] and not summary["detail_kind"]:
                summary["detail_kind"] = detail_info["kind"]
            for detail_url in detail_info["urls"]:
                if detail_url not in seen_detail_urls:
                    seen_detail_urls.add(detail_url)
                    summary["detail_urls"].append(detail_url)

            if allow_global_cache and (page_url in crawled_urls or page_url in CRAWLED_URLS_SET):
                summary["skipped"] += 1
                crawl_stats.add_skipped(seed_url)
                logger.debug(f"Skipped (cached): {page_url}")
                continue

            save_result = save_content(
                page_url,
                page,
                skip_global_cache=skip_global_cache,
                seed_context_url=seed_url,
            )
            if save_result.get("saved"):
                summary["pages"] += 1
            if save_result.get("skipped"):
                summary["skipped"] += 1
            summary["downloads"] += save_result.get("downloads", 0)
        except Exception as e:
            error_cat = categorize_error(e)
            crawl_stats.add_error(error_cat, seed_url)
            summary["errors"] += 1
            logger.error(f"[{error_cat}] Failed to process page {page_url} in seed {seed_url}: {e}")
            continue

    return summary


def scrape_seed_page(app: FirecrawlApp, url: str, strategy: str) -> Dict[str, Any]:
    scrape_result = app.scrape_url(url, params=build_scrape_options(strategy))
    if isinstance(scrape_result, dict) and scrape_result.get("success") is False:
        raise RuntimeError(scrape_result.get("error", f"Scrape failed for {url}"))
    return normalize_scraped_page(scrape_result, url)


def process_direct_file_seed(seed_url: str) -> Dict[str, Any]:
    content_folder = get_content_folder(seed_url, seed_context_url=seed_url)
    content_dir = OUTPUT_DIR / content_folder
    file_ext = get_url_extension(seed_url)
    subfolder = "pdf"
    if file_ext in {".doc", ".docx"}:
        subfolder = "docx"
    elif file_ext in {".xls", ".xlsx"}:
        subfolder = "xlsx"

    output_dir = content_dir / subfolder
    download_result = download_file(seed_url, output_dir, content_folder, seed_context_url=seed_url)

    result = {
        "success": download_result.get("downloaded") or download_result.get("skipped"),
        "pages": 0,
        "downloads": 1 if download_result.get("downloaded") else 0,
        "skipped": 0,
        "errors": 0,
    }

    if download_result.get("skipped"):
        crawl_stats.add_skipped(seed_url)
        result["skipped"] = 1

    if result["success"]:
        mark_url_crawled(seed_url)

    return result


def crawl_numbered_details(
    app: FirecrawlApp,
    detail_urls: List[str],
    cfg: Dict[str, Any],
    crawled_urls: set,
    seed_context_url: str,
) -> Dict[str, Any]:
    """Scrape detail pages for numbered documents and de-mo-nganh fanout."""
    delay = int(os.environ.get("DELAY_BETWEEN_REQUESTS", "2"))
    summary = {"pages": 0, "downloads": 0, "skipped": 0, "errors": 0}
    detail_urls = dedupe_preserve_order(detail_urls)

    batch_pages = try_batch_scrape_pages(app, detail_urls, cfg)
    batch_urls = {
        page.get("metadata", {}).get("sourceURL", "")
        for page in batch_pages
        if isinstance(page, dict)
    }
    if batch_pages:
        batch_summary = process_seed_pages(batch_pages, seed_context_url, crawled_urls, "detail_page")
        merge_execution_summary(summary, batch_summary)

    remaining_urls = [detail_url for detail_url in detail_urls if detail_url not in batch_urls]

    for detail_url in remaining_urls:
        try:
            logger.info(f"Scraping numbered detail: {detail_url}")
            page = scrape_seed_page(app, detail_url, "detail_page")
            detail_summary = process_seed_pages([page], seed_context_url, crawled_urls, "detail_page")
            merge_execution_summary(summary, detail_summary)
            logger.info(f"✓ Processed numbered detail: {detail_url}")
        except Exception as e:
            error_cat = categorize_error(e)
            crawl_stats.add_error(error_cat, seed_context_url)
            summary["errors"] += 1
            logger.error(f"[{error_cat}] Error scraping detail {detail_url}: {e}")

        time.sleep(delay)

    return summary


def crawl_ctdt_program_details(
    app: FirecrawlApp,
    detail_urls: List[str],
    cfg: Dict[str, Any],
    crawled_urls: set,
    seed_context_url: str,
) -> Dict[str, Any]:
    """Crawl explicit CTDT program URLs with a much smaller recursive budget."""
    delay = int(os.environ.get("DELAY_BETWEEN_REQUESTS", "2"))
    summary = {"pages": 0, "downloads": 0, "skipped": 0, "errors": 0}

    for program_url in dedupe_preserve_order(detail_urls):
        try:
            logger.info(f"Crawling CTDT program (bounded): {program_url}")
            crawl_params = build_crawl_params(cfg, "ctdt_program")
            crawl_params["maxDepth"] = resolve_absolute_crawl_depth(
                program_url,
                crawl_params.get("maxDepth", 1),
            )
            crawl_result = app.crawl_url(
                program_url,
                params=crawl_params,
                poll_interval=2,
            )
            program_pages = normalize_crawl_pages(crawl_result)
            program_summary = process_seed_pages(
                program_pages,
                seed_context_url,
                crawled_urls,
                "ctdt_program",
                skip_global_cache=True,
                allow_global_cache=False,
            )
            merge_execution_summary(summary, program_summary)
            logger.info(f"✓ Saved {program_summary['pages']} pages from CTDT program: {program_url}")
        except Exception as e:
            error_cat = categorize_error(e)
            crawl_stats.add_error(error_cat, seed_context_url)
            summary["errors"] += 1
            logger.error(f"[{error_cat}] Error crawling CTDT program {program_url}: {e}")

        time.sleep(delay)

    return summary


def crawl_single_seed(app: FirecrawlApp, seed_url: str, cfg: Dict[str, Any], crawled_urls: set) -> dict:
    """Crawl a single seed URL using the reviewed seed-aware routing strategy."""
    max_retries = max(int(os.environ.get("MAX_RETRIES", "3")), 1)
    retry_delay = 10
    strategy = classify_seed_strategy(seed_url, cfg)
    result = {
        "seed_url": seed_url,
        "strategy": strategy,
        "success": False,
        "pages": 0,
        "downloads": 0,
        "skipped": 0,
        "errors": 0,
    }

    crawl_stats.start_seed(seed_url, strategy)

    for attempt in range(max_retries):
        try:
            SOURCE_HOST_RATE_LIMITER.wait(seed_url)
            logger.info(
                f"Starting {strategy} crawl for: {seed_url} "
                f"(attempt {attempt + 1}/{max_retries})"
            )

            if strategy == "direct_file":
                direct_result = process_direct_file_seed(seed_url)
                if direct_result["success"]:
                    merge_execution_summary(result, direct_result)
                    result["success"] = True
                    break

                crawl_stats.add_error("unknown", seed_url)
                result["errors"] += 1
                logger.warning(f"Direct file crawl failed for {seed_url}")

                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue

                logger.error(f"Max retries reached for direct file seed {seed_url}")
                mark_failed_url(seed_url, "Direct file download failed", seed_url, max_retries)
                continue

            if strategy in {"single_page", "slow_lane", "listing_with_detail_fanout", "ctdt_index"}:
                seed_pages = load_seed_pages_with_fallback(app, seed_url, strategy, cfg)
                page_summary = process_seed_pages(seed_pages, seed_url, crawled_urls, strategy)
                merge_execution_summary(result, page_summary)
                fanout_complete = page_summary["errors"] == 0

                if strategy == "listing_with_detail_fanout" and page_summary["detail_urls"]:
                    logger.info(
                        f"Found {len(page_summary['detail_urls'])} detail pages to scrape from {seed_url}"
                    )
                    detail_summary = crawl_numbered_details(
                        app,
                        page_summary["detail_urls"],
                        cfg,
                        crawled_urls,
                        seed_url,
                    )
                    merge_execution_summary(result, detail_summary)
                    fanout_complete = fanout_complete and detail_summary["errors"] == 0
                elif strategy == "ctdt_index" and page_summary["detail_urls"]:
                    logger.info(
                        f"Found {len(page_summary['detail_urls'])} CTDT program pages to crawl from {seed_url}"
                    )
                    detail_summary = crawl_ctdt_program_details(
                        app,
                        page_summary["detail_urls"],
                        cfg,
                        crawled_urls,
                        seed_url,
                    )
                    merge_execution_summary(result, detail_summary)
                    fanout_complete = fanout_complete and detail_summary["errors"] == 0

                if strategy in {"listing_with_detail_fanout", "ctdt_index"} and not fanout_complete:
                    raise RuntimeError(
                        f"Fanout incomplete for {seed_url} "
                        f"(page_errors={page_summary['errors']}, total_errors={result['errors']})"
                    )

                if strategy in {"single_page", "slow_lane"} and page_summary["errors"] > 0:
                    raise RuntimeError(
                        f"Seed page processing incomplete for {seed_url} "
                        f"(page_errors={page_summary['errors']})"
                    )

                result["success"] = True
                break

            crawl_result = app.crawl_url(
                seed_url,
                params=build_crawl_params(cfg, strategy),
                poll_interval=5,
            )
            pages = normalize_crawl_pages(crawl_result)
            logger.info(f"Crawled {len(pages)} pages from {seed_url}")

            page_summary = process_seed_pages(pages, seed_url, crawled_urls, strategy)
            merge_execution_summary(result, page_summary)
            result["success"] = True
            logger.info(
                f"Saved {page_summary['pages']}/{len(pages)} pages, "
                f"skipped {page_summary['skipped']} from {seed_url}"
            )
            break

        except Exception as e:
            error_cat = categorize_error(e)
            crawl_stats.add_error(error_cat, seed_url)
            result["errors"] += 1
            logger.error(f"[{error_cat}] Error crawling {seed_url}: {e}")

            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"Max retries reached for {seed_url}")
                mark_failed_url(seed_url, str(e), seed_url, max_retries)

    result["duration_seconds"] = crawl_stats.finish_seed(seed_url, result["success"])
    return result


def interleave_seed_urls_by_host(seed_urls: List[str]) -> List[str]:
    buckets: Dict[str, List[str]] = {}
    for seed_url in seed_urls:
        host = urlparse(seed_url).netloc.lower()
        buckets.setdefault(host, []).append(seed_url)

    ordered: List[str] = []
    while any(buckets.values()):
        for host in sorted(buckets):
            if buckets[host]:
                ordered.append(buckets[host].pop(0))
    return ordered


def crawl_with_firecrawl(
    app: FirecrawlApp,
    seed_urls: List[str],
    cfg: Dict[str, Any],
    crawl_mode: str = "incremental",
):
    """Crawl multiple seeds with parallel execution and checkpoint support"""
    crawled_urls = load_crawled_urls()
    # Initialize the global in-memory cache so mark_url_crawled() can keep it
    # up-to-date during the run. We still pass the local set to worker funcs
    # for compatibility.
    global CRAWLED_URLS_SET
    # Use the same set object for both the local variable and the global so
    # updates via mark_url_crawled() are immediately visible to worker code.
    CRAWLED_URLS_SET = crawled_urls
    logger.info(f"Loaded {len(crawled_urls)} URLs from cache")
    
    checkpoint = load_checkpoint()
    completed_seeds = load_completed_seeds()
    completed_seed_urls = set(completed_seeds.keys())

    if checkpoint:
        logger.info(
            "Checkpoint metadata loaded: "
            f"{checkpoint.get('seed_url')} "
            f"({checkpoint.get('completed_count', len(completed_seed_urls))}/{checkpoint.get('total_seeds', len(seed_urls))})"
        )

    if completed_seed_urls:
        logger.info(f"Loaded {len(completed_seed_urls)} completed seeds from resume state")

    seeds_to_crawl = interleave_seed_urls_by_host(
        [seed_url for seed_url in seed_urls if seed_url not in completed_seed_urls]
    )
    logger.info(
        f"Crawling {len(seeds_to_crawl)} remaining seeds "
        f"({len(completed_seed_urls)} already complete, mode={crawl_mode})"
    )

    if not seeds_to_crawl:
        clear_checkpoint()
        clear_completed_seeds()
        logger.info("No remaining seeds to crawl")
        return
    
    max_workers = int(os.environ.get("MAX_WORKERS", "3"))
    logger.info(f"Using {max_workers} parallel workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_seed = {
            executor.submit(crawl_single_seed, app, seed_url, cfg, crawled_urls): (seed_urls.index(seed_url), seed_url)
            for seed_url in seeds_to_crawl
        }
        
        # Process completed tasks
        for future in as_completed(future_to_seed):
            seed_index, seed_url = future_to_seed[future]
            
            try:
                result = future.result()
                if result.get("success"):
                    save_completed_seed(seed_url, seed_index, result)
                    completed_seed_urls.add(seed_url)

                save_checkpoint(
                    seed_url,
                    seed_index,
                    len(completed_seed_urls),
                    len(seed_urls),
                )
                logger.info(f"Progress: {len(completed_seed_urls)}/{len(seed_urls)} seeds completed")
                
                if len(completed_seed_urls) % 5 == 0:
                    crawl_stats.save_report()
                
            except Exception as e:
                error_cat = categorize_error(e)
                crawl_stats.add_error(error_cat)
                logger.error(f"[{error_cat}] Failed to process seed {seed_url}: {e}")
            
            time.sleep(1)
    
    clear_checkpoint()
    clear_completed_seeds()
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
    crawl_mode = resolve_crawl_mode()
    
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
        logger.info(f"Using crawl mode: {crawl_mode}")
        
        global crawl_stats
        crawl_stats = CrawlStats()

        prepare_crawl_state(crawl_mode)
        
        # Load content deduplication cache
        load_content_hash_cache()
        
        crawl_with_firecrawl(app, seed_urls, cfg, crawl_mode=crawl_mode)
        
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
        logger.info(f"Downloads: {report['summary']['download_count']}")
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
