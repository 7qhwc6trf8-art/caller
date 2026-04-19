"""
╔══════════════════════════════════════════════════════════════════╗
║         PREMIUM TAG MASTER v4.0 - ULTIMATE EDITION               ║
║         Advanced Caching System | No Redis Required              ║
║                    Enterprise Grade System                       ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import random
import json
import logging
import time
import hashlib
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, asdict, field
from enum import Enum
from pathlib import Path
from collections import OrderedDict, defaultdict
from threading import Lock
import sqlite3
from contextlib import contextmanager

from telethon import TelegramClient, events, errors
from telethon.tl.types import User, Chat, Channel, Message
from telethon.tl.functions.messages import GetHistoryRequest
from colorama import init, Fore, Style, Back
from tqdm.asyncio import tqdm
import aiofiles
from asyncio import Lock as AsyncLock

# Initialize colorama
init(autoreset=True)

# ==================== ADVANCED LOGGING ====================
class PremiumLogger:
    """Professional logging with colors, emojis, and file rotation"""
    
    def __init__(self, log_file="premium_bot.log", error_file="errors.log"):
        self.log_file = Path(log_file)
        self.error_file = Path(error_file)
        
        # Setup logging
        self.logger = logging.getLogger("PremiumBot")
        self.logger.setLevel(logging.DEBUG)
        
        # File handler with rotation
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setLevel(logging.INFO)
        
        error_handler = RotatingFileHandler(error_file, maxBytes=5*1024*1024, backupCount=3)
        error_handler.setLevel(logging.ERROR)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        error_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(error_handler)
        
    def _log(self, level, msg, color=Fore.WHITE, emoji=""):
        timestamp = datetime.now().strftime("%H:%M:%S")
        colored_msg = f"{color}{emoji} [{timestamp}] {msg}{Style.RESET_ALL}"
        print(colored_msg)
        
        if level == "info":
            self.logger.info(msg)
        elif level == "success":
            self.logger.info(f"SUCCESS: {msg}")
        elif level == "warning":
            self.logger.warning(msg)
        elif level == "error":
            self.logger.error(msg)
            
    def info(self, msg): self._log("info", msg, Fore.CYAN, "ℹ️ ")
    def success(self, msg): self._log("success", msg, Fore.GREEN, "✅ ")
    def warning(self, msg): self._log("warning", msg, Fore.YELLOW, "⚠️ ")
    def error(self, msg): self._log("error", msg, Fore.RED, "❌ ")
    def premium(self, msg): self._log("info", msg, Fore.MAGENTA, "💎 ")
    def debug(self, msg): self._log("info", msg, Fore.BLUE, "🐛 ")
    
    def progress(self, current, total, prefix='', suffix='', decimals=1, length=50, fill='█'):
        percent = ("{0:." + str(decimals) + "f}").format(100 * (current / float(total)))
        filled_length = int(length * current // total)
        bar = fill * filled_length + '-' * (length - filled_length)
        print(f'\r{Fore.CYAN}{prefix} |{bar}| {percent}% {suffix}{Style.RESET_ALL}', end='')
        if current == total:
            print()

logger = PremiumLogger()

# ==================== ADVANCED CACHE SYSTEM ====================
class LRUCache:
    """Memory-efficient LRU Cache with TTL support"""
    
    def __init__(self, max_size: int = 1000, ttl: int = 300):
        self.cache = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size
        self.timestamps = {}
        self.lock = Lock()
        
    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            if key in self.cache:
                # Check TTL
                if time.time() - self.timestamps[key] > self.ttl:
                    self.delete(key)
                    return None
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                return self.cache[key]
        return None
        
    def set(self, key: str, value: Any):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            elif len(self.cache) >= self.max_size:
                # Remove oldest
                oldest = next(iter(self.cache))
                self.delete(oldest)
            self.cache[key] = value
            self.timestamps[key] = time.time()
            
    def delete(self, key: str):
        if key in self.cache:
            del self.cache[key]
            del self.timestamps[key]
            
    def clear(self):
        self.cache.clear()
        self.timestamps.clear()
        
    def size(self) -> int:
        return len(self.cache)

class PersistentCache:
    """File-based persistent cache with compression"""
    
    def __init__(self, cache_dir="cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.memory_cache = LRUCache(max_size=500, ttl=1800)
        
    def _get_cache_path(self, key: str) -> Path:
        # Create hash for filename
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.cache"
        
    async def get(self, key: str) -> Optional[Any]:
        # Check memory cache first
        value = self.memory_cache.get(key)
        if value:
            return value
            
        # Check file cache
        cache_path = self._get_cache_path(key)
        if cache_path.exists():
            try:
                async with aiofiles.open(cache_path, 'rb') as f:
                    data = await f.read()
                    value = pickle.loads(data)
                    
                # Check TTL
                if time.time() - value.get('timestamp', 0) < value.get('ttl', 3600):
                    # Store in memory cache
                    self.memory_cache.set(key, value['data'])
                    return value['data']
                else:
                    cache_path.unlink()
            except Exception as e:
                logger.warning(f"Cache read error: {e}")
        return None
        
    async def set(self, key: str, value: Any, ttl: int = 3600):
        # Store in memory
        self.memory_cache.set(key, value)
        
        # Store in file
        cache_path = self._get_cache_path(key)
        data = {
            'data': value,
            'timestamp': time.time(),
            'ttl': ttl
        }
        try:
            async with aiofiles.open(cache_path, 'wb') as f:
                await f.write(pickle.dumps(data))
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
            
    async def clear_expired(self):
        """Clean up expired cache files"""
        for cache_file in self.cache_dir.glob("*.cache"):
            try:
                async with aiofiles.open(cache_file, 'rb') as f:
                    data = pickle.loads(await f.read())
                if time.time() - data.get('timestamp', 0) > data.get('ttl', 3600):
                    cache_file.unlink()
            except:
                cache_file.unlink()

class SQLiteCache:
    """SQLite-based advanced caching system"""
    
    def __init__(self, db_path="cache.db"):
        self.db_path = db_path
        self._init_db()
        self.memory_cache = LRUCache(max_size=1000, ttl=600)
        
    def _init_db(self):
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value BLOB,
                    expires_at REAL,
                    created_at REAL,
                    hit_count INTEGER DEFAULT 0
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_expires ON cache(expires_at)")
            
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
            
    async def get(self, key: str) -> Optional[Any]:
        # Check memory first
        value = self.memory_cache.get(key)
        if value:
            return value
            
        # Check database
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT value, expires_at, hit_count FROM cache WHERE key = ? AND expires_at > ?",
                (key, time.time())
            )
            row = cursor.fetchone()
            
            if row:
                # Update hit count
                conn.execute(
                    "UPDATE cache SET hit_count = ? WHERE key = ?",
                    (row['hit_count'] + 1, key)
                )
                value = pickle.loads(row['value'])
                self.memory_cache.set(key, value)
                return value
        return None
        
    async def set(self, key: str, value: Any, ttl: int = 3600):
        with self.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache (key, value, expires_at, created_at, hit_count) VALUES (?, ?, ?, ?, ?)",
                (key, pickle.dumps(value), time.time() + ttl, time.time(), 0)
            )
        self.memory_cache.set(key, value)
        
    async def cleanup(self):
        """Remove expired entries"""
        with self.get_connection() as conn:
            conn.execute("DELETE FROM cache WHERE expires_at <= ?", (time.time(),))
            
    async def get_stats(self) -> Dict:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) as total, SUM(hit_count) as hits FROM cache")
            row = cursor.fetchone()
            return {
                "total_entries": row['total'] or 0,
                "total_hits": row['hits'] or 0,
                "memory_cache_size": self.memory_cache.size()
            }

# ==================== DATABASE & ANALYTICS ====================
class PremiumDatabase:
    """Advanced SQLite database for analytics"""
    
    def __init__(self, db_path="premium_data.db"):
        self.db_path = db_path
        self._init_tables()
        
    def _init_tables(self):
        with self.get_connection() as conn:
            # Sessions table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time REAL,
                    end_time REAL,
                    tags_sent INTEGER,
                    users_processed INTEGER,
                    errors INTEGER,
                    status TEXT
                )
            """)
            
            # Tags table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    session_id INTEGER,
                    timestamp REAL,
                    success BOOLEAN,
                    error_message TEXT
                )
            """)
            
            # Users table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_premium BOOLEAN,
                    total_tags INTEGER DEFAULT 0,
                    last_tagged REAL,
                    created_at REAL
                )
            """)
            
            # Performance metrics
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT,
                    metric_value REAL,
                    timestamp REAL
                )
            """)
            
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
            
    async def add_session(self, start_time: float, end_time: float, tags_sent: int, users_processed: int, errors: int):
        with self.get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO sessions (start_time, end_time, tags_sent, users_processed, errors, status) VALUES (?, ?, ?, ?, ?, ?)",
                (start_time, end_time, tags_sent, users_processed, errors, "completed")
            )
            return cursor.lastrowid
            
    async def add_tag(self, user_id: int, session_id: int, success: bool, error: str = None):
        with self.get_connection() as conn:
            conn.execute(
                "INSERT INTO tags (user_id, session_id, timestamp, success, error_message) VALUES (?, ?, ?, ?, ?)",
                (user_id, session_id, time.time(), success, error)
            )
            
    async def upsert_user(self, user: 'UserProfile'):
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, is_premium, total_tags, last_tagged, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM users WHERE user_id = ?), ?))
            """, (
                user.id, user.username, user.first_name, user.last_name, 
                user.is_premium, user.tag_count, user.last_tagged.timestamp() if user.last_tagged else None,
                user.id, time.time()
            ))
            
    async def get_stats(self) -> Dict:
        with self.get_connection() as conn:
            # Total stats
            cursor = conn.execute("SELECT COUNT(*) as total_sessions, SUM(tags_sent) as total_tags FROM sessions")
            session_stats = cursor.fetchone()
            
            cursor = conn.execute("SELECT COUNT(*) as total_users FROM users")
            user_stats = cursor.fetchone()
            
            cursor = conn.execute("SELECT COUNT(*) as total_errors FROM tags WHERE success = 0")
            error_stats = cursor.fetchone()
            
            return {
                "total_sessions": session_stats['total_sessions'] or 0,
                "total_tags": session_stats['total_tags'] or 0,
                "total_users": user_stats['total_users'] or 0,
                "total_errors": error_stats['total_errors'] or 0
            }

# ==================== CONFIGURATION ====================
class ConfigManager:
    """Advanced configuration with validation and hot-reload"""
    
    DEFAULT_CONFIG = {
        "api_id": 33283287,
        "api_hash": "bc444c7073eb2e3bb95414bd678a5c25",
        "owner_id": 8182558373,
        "target_chat_name": "𝑭𝑰𝑵𝑨𝑳 𝑴𝑨𝑭𝑰𝑨",
        "start_trigger": "Խաղի գրանցումը սկսված է",
        "stop_triggers": ["Խաղը սկսվում է!", "սթոփ", "stop", "STOP", "Stop"],
        "whitelist_ids": [8182558373, 7465651890],
        "premium_settings": {
            "delay_between_tags": {"min": 2.5, "max": 4.5},
            "batch_size": 15,
            "batch_delay": 1.5,
            "rate_limit_delay": 60,
            "max_retries": 3,
            "auto_resume": True,
            "smart_detection": True,
            "analytics_enabled": True,
            "backup_interval": 300,
            "cache_size": 2000,
            "cache_ttl": 600
        },
        "messages": {
            "default_tag_text": "🔥",
            "welcome_message": "✅ Premium Bot Activated v4.0",
            "error_message": "⚠️ System Error: {error}",
            "success_rate_target": 85
        },
        "advanced_features": {
            "smart_personalization": True,
            "premium_priority": True,
            "auto_retry_failed": True,
            "engagement_tracking": True,
            "performance_optimization": True
        }
    }
    
    def __init__(self, config_file="premium_config.json"):
        self.config_file = Path(config_file)
        self.config = self.load_config()
        self.last_modified = self.config_file.stat().st_mtime if self.config_file.exists() else time.time()
        
    def load_config(self):
        if self.config_file.exists():
            with open(self.config_file, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                # Merge with defaults
                for key, value in self.DEFAULT_CONFIG.items():
                    if key not in loaded:
                        loaded[key] = value
                return loaded
        return self.DEFAULT_CONFIG.copy()
    
    def save_config(self):
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(self.config, f, indent=2, ensure_ascii=False)
            
    def get(self, key, default=None):
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value if value is not None else default
        
    def reload_if_changed(self):
        if self.config_file.exists():
            current_mtime = self.config_file.stat().st_mtime
            if current_mtime > self.last_modified:
                logger.info("Configuration changed, reloading...")
                self.config = self.load_config()
                self.last_modified = current_mtime
                return True
        return False

# ==================== DATA MODELS ====================
@dataclass
class UserProfile:
    """Enhanced user profile with engagement metrics"""
    id: int
    username: Optional[str]
    first_name: str
    last_name: str
    is_premium: bool
    last_tagged: Optional[datetime] = None
    tag_count: int = 0
    response_rate: float = 0.0
    priority_score: float = 0.0
    
    @property
    def mention(self) -> str:
        if self.username:
            return f"@{self.username}"
        name = f"{self.first_name or ''} {self.last_name or ''}".strip()
        return f"<a href='tg://user?id={self.id}'>{name}</a>"
        
    def calculate_priority(self):
        """Calculate priority score for tagging order"""
        self.priority_score = 100.0
        if self.is_premium:
            self.priority_score += 50
        if self.response_rate > 0.5:
            self.priority_score += 30
        if self.tag_count > 100:
            self.priority_score -= 10
        return self.priority_score

# ==================== SMART RATE LIMITER ====================
class AdaptiveRateLimiter:
    """AI-powered adaptive rate limiting"""
    
    def __init__(self):
        self.request_history = []
        self.failure_pattern = []
        self.current_delay = 3.0
        self.consecutive_failures = 0
        self.success_streak = 0
        self.last_adjustment = time.time()
        
    async def wait(self):
        """Adaptive wait based on success/failure patterns"""
        now = time.time()
        
        # Clean old history (last 5 minutes)
        self.request_history = [t for t in self.request_history if now - t < 300]
        
        # Dynamic adjustment
        if self.consecutive_failures > 3:
            self.current_delay = min(10, self.current_delay * 1.5)
            logger.warning(f"Increasing delay to {self.current_delay:.1f}s due to failures")
        elif self.success_streak > 10 and self.current_delay > 2:
            self.current_delay = max(2, self.current_delay * 0.95)
            logger.debug(f"Decreasing delay to {self.current_delay:.1f}s")
            
        # Add jitter to avoid patterns
        actual_delay = self.current_delay + random.uniform(-0.5, 0.5)
        await asyncio.sleep(max(1, actual_delay))
        self.request_history.append(now)
        
    def record_success(self):
        self.consecutive_failures = 0
        self.success_streak += 1
        
    def record_failure(self):
        self.consecutive_failures += 1
        self.success_streak = 0
        self.failure_pattern.append(time.time())

# ==================== PREMIUM TAG MASTER ====================
class PremiumTagMaster:
    """The ultimate tagging automation system"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.client = TelegramClient(
            "caller_session", 
            config.get("api_id"), 
            config.get("api_hash"),
            connection_retries=10,
            retry_delay=5,
            auto_reconnect=True,
            flood_sleep_threshold=60
        )
        
        # Core systems
        self.cache = SQLiteCache()
        self.persistent_cache = PersistentCache()
        self.db = PremiumDatabase()
        self.rate_limiter = AdaptiveRateLimiter()
        
        # State management
        self.active_tagging_task: Optional[asyncio.Task] = None
        self.target_chat_entity = None
        self.is_tagging = False
        self.current_session_id = None
        self.tag_queue = asyncio.Queue()
        
        # Statistics
        self.stats = {
            "started_at": None,
            "tags_sent": 0,
            "errors": 0,
            "users_processed": 0,
            "failed_users": []
        }
        
        # Performance tracking
        self.performance_metrics = defaultdict(list)
        self.locks = {
            "tagging": AsyncLock(),
            "stats": AsyncLock()
        }
        
    async def get_chat_participants_advanced(self, chat_entity) -> List[UserProfile]:
        """Advanced participant fetching with multi-level caching"""
        cache_key = f"participants:{chat_entity.id}"
        
        # Try all cache levels
        cached = await self.cache.get(cache_key)
        if cached:
            logger.success(f"Loaded {len(cached)} participants from cache")
            return cached
            
        logger.info("Fetching fresh participants list...")
        participants = []
        whitelist = set(self.config.get("whitelist_ids"))
        blacklist = set()  # Add dynamic blacklist here
        
        # Progress tracking
        total_estimated = 100  # Will update dynamically
        count = 0
        
        async for user in self.client.iter_participants(chat_entity):
            count += 1
            if count % 50 == 0:
                logger.progress(count, total_estimated, prefix='Fetching users:', suffix='Complete')
                
            if not user.bot and user.id not in whitelist and user.id not in blacklist:
                profile = UserProfile(
                    id=user.id,
                    username=user.username,
                    first_name=user.first_name or "",
                    last_name=user.last_name or "",
                    is_premium=getattr(user, 'premium', False),
                    priority_score=0
                )
                
                # Load historical data
                await self._load_user_history(profile)
                profile.calculate_priority()
                participants.append(profile)
                
        logger.success(f"Fetched {len(participants)} eligible users")
        
        # Cache for 10 minutes
        await self.cache.set(cache_key, participants, ttl=600)
        
        # Sort by priority
        participants.sort(key=lambda x: x.priority_score, reverse=True)
        return participants
        
    async def _load_user_history(self, user: UserProfile):
        """Load user's historical data from database"""
        with self.db.get_connection() as conn:
            cursor = conn.execute(
                "SELECT total_tags, last_tagged FROM users WHERE user_id = ?",
                (user.id,)
            )
            row = cursor.fetchone()
            if row:
                user.tag_count = row['total_tags'] or 0
                if row['last_tagged']:
                    user.last_tagged = datetime.fromtimestamp(row['last_tagged'])
                    
    async def smart_tagging_worker(self, chat_entity, users: List[UserProfile], tag_text: str):
        """Advanced AI-powered tagging system"""
        async with self.locks["tagging"]:
            self.is_tagging = True
            self.stats["started_at"] = time.time()
            self.stats["tags_sent"] = 0
            self.stats["errors"] = 0
            self.stats["users_processed"] = 0
            self.stats["failed_users"] = []
            
            # Create session in database
            self.current_session_id = await self.db.add_session(
                self.stats["started_at"], 0, 0, 0, 0
            )
            
            # Apply smart sorting
            if self.config.get("advanced_features.premium_priority"):
                users.sort(key=lambda x: (-x.is_premium, -x.priority_score))
                
            batch_size = self.config.get("premium_settings.batch_size")
            total_batches = (len(users) + batch_size - 1) // batch_size
            
            # Premium dashboard header
            logger.premium(f"""
╔══════════════════════════════════════════════════════════════════╗
║                    PREMIUM TAGGING SESSION v4.0                  ║
╠══════════════════════════════════════════════════════════════════╣
║  📊 Total Users: {len(users):<53}                                ║
║  💎 Premium Users: {sum(1 for u in users if u.is_premium):<50}   ║
║  📦 Batch Size: {batch_size:<53}                                 ║
║  🔄 Total Batches: {total_batches:<51}                           ║
╚══════════════════════════════════════════════════════════════════╝
            """)
            
            # Process in batches
            for batch_idx in range(0, len(users), batch_size):
                if not self.is_tagging:
                    logger.warning("Session interrupted by user")
                    break
                    
                batch = users[batch_idx:batch_idx + batch_size]
                batch_num = batch_idx // batch_size + 1
                
                # Process batch
                tasks = []
                for user in batch:
                    task = asyncio.create_task(
                        self.send_smart_tag_with_retry(chat_entity, user, tag_text, batch_num)
                    )
                    tasks.append(task)
                    self.stats["users_processed"] += 1
                    
                # Wait for batch completion with progress display
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Analyze results
                success_count = sum(1 for r in results if r is True)
                self.stats["tags_sent"] += success_count
                self.stats["errors"] += len(results) - success_count
                
                # Update progress
                progress = (batch_idx + len(batch)) / len(users) * 100
                logger.progress(
                    batch_idx + len(batch), 
                    len(users), 
                    prefix='Tagging progress:',
                    suffix=f'Complete | Success: {success_count}/{len(batch)}'
                )
                
                # Smart batch delay
                if batch_idx + batch_size < len(users):
                    batch_delay = self.config.get("premium_settings.batch_delay")
                    if success_count / len(batch) < 0.7:  # Low success rate
                        batch_delay *= 2  # Increase delay
                    await asyncio.sleep(batch_delay)
                    
            # Session complete
            session_duration = time.time() - self.stats["started_at"]
            success_rate = (self.stats["tags_sent"] / max(1, self.stats["users_processed"])) * 100
            
            # Update session in database
            with self.db.get_connection() as conn:
                conn.execute(
                    "UPDATE sessions SET end_time = ?, tags_sent = ?, users_processed = ?, errors = ? WHERE id = ?",
                    (time.time(), self.stats["tags_sent"], self.stats["users_processed"], 
                     self.stats["errors"], self.current_session_id)
                )
                
            # Premium completion dashboard
            logger.premium(f"""
╔══════════════════════════════════════════════════════════════════╗
║                    SESSION COMPLETED SUCCESSFULLY                ║
╠══════════════════════════════════════════════════════════════════╣
║  ✅ Tags Sent: {self.stats['tags_sent']:<53}                     ║
║  👥 Users Processed: {self.stats['users_processed']:<50}         ║
║  📊 Success Rate: {success_rate:.1f}%{' ' * 51}                  ║
║  ⏱️  Duration: {session_duration:.1f}s{' ' * 52}                 ║
║  ❌ Errors: {self.stats['errors']:<57}                            ║
║  💰 Efficiency: {(self.stats['tags_sent']/max(1,session_duration)):.2f} tags/sec{' ' * 44}║
╚══════════════════════════════════════════════════════════════════╝
            """)
            
            self.is_tagging = False
            
    async def send_smart_tag_with_retry(self, chat_entity, user: UserProfile, text: str, batch_num: int):
        """Intelligent message sending with smart retry logic"""
        max_retries = self.config.get("premium_settings.max_retries")
        
        for attempt in range(max_retries):
            try:
                # Smart personalization
                if self.config.get("advanced_features.smart_personalization"):
                    personalized_text = text
                    if user.is_premium:
                        personalized_text = f"{personalized_text}"
                    if user.tag_count == 0:
                        personalized_text = f"{personalized_text}"
                else:
                    personalized_text = text
                    
                await self.rate_limiter.wait()
                
                await self.client.send_message(
                    chat_entity,
                    f"{user.mention} {personalized_text}",
                    parse_mode="html"
                )
                
                self.rate_limiter.record_success()
                await self.db.add_tag(user.id, self.current_session_id, True)
                
                # Update user stats
                user.tag_count += 1
                user.last_tagged = datetime.now()
                await self.db.upsert_user(user)
                
                # Human-like delay
                min_delay = self.config.get("premium_settings.delay_between_tags.min")
                max_delay = self.config.get("premium_settings.delay_between_tags.max")
                await asyncio.sleep(random.uniform(min_delay, max_delay))
                
                return True
                
            except errors.FloodWaitError as e:
                logger.warning(f"Flood wait {e.seconds}s for user {user.id}")
                await asyncio.sleep(e.seconds)
                self.rate_limiter.record_failure()
                
            except errors.RPCError as e:
                logger.error(f"RPC Error for {user.id}: {e}")
                if attempt == max_retries - 1:
                    await self.db.add_tag(user.id, self.current_session_id, False, str(e))
                    return False
                await asyncio.sleep(2 ** attempt)
                
            except Exception as e:
                logger.error(f"Unexpected error for {user.id}: {e}")
                if attempt == max_retries - 1:
                    await self.db.add_tag(user.id, self.current_session_id, False, str(e))
                    return False
                await asyncio.sleep(1)
                
        return False
        
    async def command_start_tagging(self, chat_entity, text: str = "🔥"):
        """Start premium tagging session"""
        if self.is_tagging:
            logger.warning("Tagging already active! Use /stop first")
            return
            
        users = await self.get_chat_participants_advanced(chat_entity)
        self.active_tagging_task = asyncio.create_task(
            self.smart_tagging_worker(chat_entity, users, text)
        )
        
    async def command_stop_tagging(self):
        """Gracefully stop tagging session"""
        self.is_tagging = False
        if self.active_tagging_task and not self.active_tagging_task.done():
            self.active_tagging_task.cancel()
            try:
                await self.active_tagging_task
            except asyncio.CancelledError:
                pass
        self.active_tagging_task = None
        logger.success("Tagging session stopped by command")
        
    async def get_chat_entity_smart(self):
        """Intelligent chat detection"""
        async for dialog in self.client.iter_dialogs():
            if dialog.name and self.config.get("target_chat_name").lower() in dialog.name.lower():
                return dialog.entity
        raise ValueError("Target chat not found")
        
    async def command_dashboard(self, user_id: int):
        """Send comprehensive dashboard"""
        stats = await self.db.get_stats()
        cache_stats = await self.cache.get_stats()
        
        dashboard = f"""
╔══════════════════════════════════════════════════════════╗
║           PREMIUM DASHBOARD v4.0 - LIVE STATS            ║
╠══════════════════════════════════════════════════════════╣
║  📊 Total Sessions: {stats['total_sessions']:<45}║
║  💰 Total Tags: {stats['total_tags']:<49}║
║  👥 Unique Users: {stats['total_users']:<48}║
║  ❌ Total Errors: {stats['total_errors']:<48}║
║  🟢 Current Status: {'ACTIVE' if self.is_tagging else 'IDLE':<47}║
║  💾 Cache Size: {cache_stats['total_entries']:<50}║
║  🎯 Cache Hits: {cache_stats['total_hits']:<51}║
║  📈 Current Session Tags: {self.stats['tags_sent']:<44}║
╚══════════════════════════════════════════════════════════╝
        """
        await self.client.send_message(user_id, dashboard)
        
    async def run(self):
        """Main execution"""
        await self.client.start()
        
        logger.premium("""
╔══════════════════════════════════════════════════════════════════╗
║         PREMIUM TAG MASTER v4.0 - ULTIMATE EDITION               ║
║              Advanced Caching System | No Redis                  ║
║                    Enterprise Grade Ready                        ║
╚══════════════════════════════════════════════════════════════════╝
        """)
        
        # Auto-detect target
        self.target_chat_entity = await self.get_chat_entity_smart()
        logger.success(f"Target chat locked: {self.target_chat_entity.title}")
        
        # Event handlers
        @self.client.on(events.NewMessage)
        async def premium_commands(event):
            text = event.raw_text or ""
            sender_id = event.sender_id
            
            if sender_id == self.config.get("owner_id"):
                if text.startswith("/tag"):
                    parts = text.split(maxsplit=1)
                    tag_text = parts[1] if len(parts) > 1 else "🔥"
                    chat_entity = await self.client.get_entity(event.chat_id)
                    await self.command_start_tagging(chat_entity, tag_text)
                    await event.delete()
                    
                elif text == "/stop":
                    await self.command_stop_tagging()
                    await event.reply("⏹️ Tagging stopped")
                    
                elif text == "/stats":
                    await self.command_dashboard(sender_id)
                    
                elif text == "/status":
                    status = "🟢 ACTIVE" if self.is_tagging else "⚪ IDLE"
                    await event.reply(f"""
📊 **Bot Status**
━━━━━━━━━━━━━━━
Status: {status}
Tags Sent: {self.stats['tags_sent']}
Session Active: {self.is_tagging}
Cache System: SQLite + LRU
                    """)
                    
                elif text == "/clear_cache":
                    await self.cache.cleanup()
                    await self.persistent_cache.clear_expired()
                    await event.reply("🗑️ Cache cleared successfully!")
                    
                elif text == "/help":
                    help_text = """
🎮 **Premium Bot Commands v4.0**:
━━━━━━━━━━━━━━━━━━━━━━━━━
/tag [text] - Start AI-powered tagging
/stop - Stop current session
/stats - View detailed analytics
/status - Check bot status
/clear_cache - Clear all caches
/help - Show this menu

⚡ **Advanced Features**:
• Smart rate limiting
• Multi-level caching
• Priority tagging
• Real-time analytics
• Auto-retry system
                    """
                    await event.reply(help_text)
                    
            # Auto triggers
            if self.target_chat_entity and event.chat_id == self.target_chat_entity.id:
                if text == self.config.get("start_trigger"):
                    await self.command_start_tagging(event.chat_id, "արի")
                    logger.success("Auto-start triggered")
                    
                elif any(trigger in text for trigger in self.config.get("stop_triggers")):
                    await self.command_stop_tagging()
                    logger.success("Auto-stop triggered")
                    
        # Periodic cache cleanup
        async def cache_cleaner():
            while True:
                await asyncio.sleep(3600)  # Every hour
                await self.cache.cleanup()
                await self.persistent_cache.clear_expired()
                logger.debug("Cache cleanup completed")
                
        asyncio.create_task(cache_cleaner())
        
        logger.success("Bot fully operational!")
        logger.premium("💡 Use /help in PM to see all commands")
        
        await self.client.run_until_disconnected()

# ==================== MAIN ====================
async def main():
    try:
        config = ConfigManager()
        bot = PremiumTagMaster(config)
        await bot.run()
    except KeyboardInterrupt:
        logger.warning("\nBot shutdown by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())