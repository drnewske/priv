#!/usr/bin/env python3
"""
Xtream Sports Channel Scanner - Production Version
Fast AND accurate. Scans external playlists first, then lovestory.json for matching channels.
"""

import json
import re
import requests
import argparse
import sys
import shutil
import subprocess
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from difflib import SequenceMatcher
import concurrent.futures
import time
import zlib
import os
import threading

# Configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEDULE_FILE = os.path.join(SCRIPT_DIR, 'weekly_schedule.json')
LOVESTORY_FILE = os.path.join(SCRIPT_DIR, '..', 'lovestory.json')
EXTERNAL_PLAYLISTS_FILE = os.path.join(SCRIPT_DIR, '..', 'external_playlists.txt')
MAX_STREAMS_PER_CHANNEL = 5
QUALITY_PRIORITY = ['4K', 'FHD', 'HD', 'SD']
TEST_TIMEOUT_SECONDS = 8
TEST_RETRY_FAILED = 1
TEST_RETRY_DELAY_SECONDS = 0.35
TEST_FFMPEG_FALLBACK = True
TEST_WORKERS = 20
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

if not os.path.exists(SCHEDULE_FILE):
    SCHEDULE_FILE = 'weekly_schedule.json'

NON_BROADCAST_WORD_RE = re.compile(r"\b(app|website|web\s*site|youtube|radio)\b", re.IGNORECASE)
DOMAIN_RE = re.compile(
    r"\b[a-z0-9][a-z0-9.-]{0,251}\.(com|net|org|io|tv|co|app|gg|me|fm|uk|us|au|de|fr)\b",
    re.IGNORECASE,
)

def infer_server_type(url: str, declared_type: Optional[str] = None) -> str:
    """Infer server scan mode from URL + optional declared type."""
    declared = (declared_type or "").strip().lower()
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    params = parse_qs(parsed.query)
    query_type = ((params.get("type") or [""])[0] or "").strip().lower()

    if path.endswith((".m3u", ".m3u8")):
        return "direct"
    if query_type in {"m3u", "m3u8", "m3u_plus"}:
        return "direct"
    if declared in {"direct", "m3u", "m3u8", "playlist"}:
        return "direct"
    if "githubusercontent.com" in (parsed.netloc or "").lower():
        return "direct"

    if "username" in params and "password" in params:
        return "api"
    return "direct"


def is_usable_channel_name(name: str) -> bool:
    cleaned = (name or "").strip()
    if not cleaned:
        return False
    if NON_BROADCAST_WORD_RE.search(cleaned):
        return False
    if DOMAIN_RE.search(cleaned):
        return False
    return True


def safe_int(value, default: int = 0) -> int:
    """Parse integer-like values safely."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def load_external_servers(file_path: str) -> List[Dict]:
    """Load direct/API playlist URLs from a plain-text file in repo root.

    Accepted line formats:
      - URL
      - Name|URL
    Empty lines and lines starting with '#' are ignored.
    """
    servers: List[Dict] = []
    if not os.path.exists(file_path):
        print(f"External playlist file not found: {file_path}. Skipping external sources.")
        return servers

    try:
        with open(file_path, "r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue

                name = ""
                url = line
                if "|" in line:
                    name, url = line.split("|", 1)
                    name = name.strip()
                    url = url.strip()

                if not url:
                    continue
                parsed = urlparse(url)
                if not parsed.scheme or not parsed.netloc:
                    print(f"  - Skipping invalid external playlist line {line_no}: {line}")
                    continue

                server_type = infer_server_type(url)
                servers.append(
                    {
                        "url": url,
                        "name": name or f"External Playlist {len(servers) + 1}",
                        "type": server_type,
                        "stream_count": 0,
                        "source": "external",
                    }
                )
    except Exception as e:
        print(f"Error loading external playlists from {file_path}: {e}")
        return []

    print(f"Loaded {len(servers)} external playlist sources from {file_path}.")
    return servers


def load_servers_from_lovestory(file_path: str) -> List[Dict]:
    """Load servers from lovestory.json featured_content."""
    servers: List[Dict] = []

    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found. Skipping lovestory sources.")
        return servers

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        featured = data.get('featured_content', [])
        for item in featured:
            url = item.get('url')
            if not url:
                continue

            server_type = infer_server_type(url, declared_type=item.get("type"))
            
            stream_count = safe_int(
                item.get('channel_count', item.get('stream_count', item.get('streams', 0))),
                0,
            )

            servers.append({
                'url': url,
                'name': item.get('name', 'Unknown'),
                'type': server_type,
                'stream_count': stream_count,
                'source': 'lovestory',
            })

        # Prioritize largest lovestory playlists first (external list order is preserved separately).
        servers.sort(key=lambda s: safe_int(s.get('stream_count', 0), 0), reverse=True)

        print(f"Loaded {len(servers)} servers from lovestory.json.")
        top_n = min(10, len(servers))
        if top_n:
            print("Top lovestory playlists by stream count:")
            for i, server in enumerate(servers[:top_n], start=1):
                print(f"  {i}. {server.get('name', 'Unknown')} ({safe_int(server.get('stream_count', 0), 0)} streams)")

    except Exception as e:
        print(f"Error loading {file_path}: {e}")

    return servers


def load_scan_servers(external_file_path: str, lovestory_file_path: str) -> List[Dict]:
    """Build final scan order: external playlists first, then lovestory playlists."""
    external_servers = load_external_servers(external_file_path)
    lovestory_servers = load_servers_from_lovestory(lovestory_file_path)

    servers: List[Dict] = []
    seen_urls = set()
    for server in external_servers + lovestory_servers:
        url = str(server.get("url", "")).strip()
        if not url:
            continue
        url_key = url.lower()
        if url_key in seen_urls:
            continue
        seen_urls.add(url_key)
        servers.append(server)

    print(
        f"Final server list: {len(servers)} total "
        f"({len(external_servers)} external + {len(lovestory_servers)} lovestory, deduped)."
    )
    return servers

class XtreamAPI:
    """Xtream Codes API client."""
    
    def __init__(self, url: str):
        """Parse credentials from M3U URL."""
        parsed = urlparse(url)
        
        # Try query parameters
        params = parse_qs(parsed.query)
        if 'username' in params and 'password' in params:
            self.username = params['username'][0]
            self.password = params['password'][0]
        else:
            # Try path-based format
            parts = [p for p in parsed.path.split('/') if p]
            if len(parts) >= 2:
                self.username = parts[0]
                self.password = parts[1]
            else:
                # Fallback for non-standard URLs or lack of creds
                self.username = ""
                self.password = ""
                # print(f"Warning: Cannot parse credentials from URL: {url}")
        
        # Construct Base URL
        # IMPORTANT: Use netloc to preserve Basic Auth (user@host) if present
        scheme = parsed.scheme if parsed.scheme else "http"
        self.base_url = f"{scheme}://{parsed.netloc}"
        self.timeout = 30
    
    def _api_call(self, action: str, **params) -> Optional[List[Dict]]:
        """Make API call."""
        if not self.username or not self.password:
            return None

        url = f"{self.base_url}/player_api.php?username={self.username}&password={self.password}&action={action}"
        for key, value in params.items():
            url += f"&{key}={value}"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            # print(f"API Call Failed ({action}): {e}")
            return None
    
    def get_live_categories(self) -> List[Dict]:
        """Get all live categories."""
        result = self._api_call('get_live_categories')
        return result if result is not None else []
    
    def get_live_streams(self, category_id: Optional[int] = None) -> List[Dict]:
        """Get live streams, optionally filtered by category."""
        params = {'category_id': category_id} if category_id else {}
        result = self._api_call('get_live_streams', **params)
        return result if result is not None else []
    
    def get_stream_url(self, stream_id: int) -> str:
        """Build stream URL."""
        # Xtream codes usually exposes live streams at /live/username/password/stream_id.ts
        if not self.username or not self.password:
             return ""
        return f"{self.base_url}/live/{self.username}/{self.password}/{stream_id}.ts"


class ChannelNormalizer:
    """Normalize and group channel names."""
    
    def __init__(self, similarity_threshold: float = 0.85):
        """Initialize with similarity threshold (0-1)."""
        self.similarity_threshold = similarity_threshold
        
        # Quality patterns
        self.quality_regex = re.compile(
            r'\b(4k|uhd|ultra\s*hd|2160p?|fhd|full\s*hd|1080p?|hd|720p?|sd|480p?|360p?|hevc|h\.?26[45])\b',
            re.IGNORECASE
        )
        
        # Junk patterns (compile once for speed)
        self.junk_patterns = [
            re.compile(r'[#\-_*+=:|~]{2,}'),  # Repeated chars
            re.compile(r'\[(?:vip|hd|fhd|4k|sd|server|backup|link|premium|multi|test|raw|direct|dn)\s*\d*\]', re.I),
            re.compile(r'\((?:vip|hd|fhd|4k|sd|server|backup|link|premium|multi|test|raw|direct|dn)\s*\d*\)', re.I),
            re.compile(r'^\s*[#\-_*+=:|~]+\s*'),  # Leading junk
            re.compile(r'\s*[#\-_*+=:|~]+\s*$'),  # Trailing junk
            re.compile(r'\s*\|+\s*'),  # Pipes
            re.compile(r'\s*-\s*'),     # Hyphens as separators
            re.compile(r'\b(ca|us|uk|de|fr|it|es|pt|br|ar|mx|tr|ru|nl|be|ch|at|pl|ro|bg|hr|rs|ba|mk|si|hu|cz|sk|ua|gr|cy|il|ae|sa|kw|qa|bh|om|lb|jo|eg|ma|dz|tn|ly|sd|sy|iq|ir|pk|in|bd|lk|np|mm|th|vn|la|kh|my|sg|id|ph|cn|tw|hk|mo|kp|kr|jp|au|nz)\s*[:-]\s*', re.I), # Country prefixes
        ]
    
    def extract_quality(self, name: str) -> str:
        """Extract quality tier from channel name."""
        name_lower = name.lower()
        
        if re.search(r'\\b(4k|uhd|2160p?)\\b', name_lower):
            return '4K'
        elif re.search(r'\\b(fhd|1080p?)\\b', name_lower):
            return 'FHD'
        elif re.search(r'\\b(hd|720p?)\\b', name_lower):
            return 'HD'
        elif re.search(r'\\b(sd|480p?|360p?)\\b', name_lower):
            return 'SD'
        else:
            return 'HD'  # Default
    
    def normalize(self, name: str) -> str:
        """Normalize channel name - remove quality and junk."""
        if not name:
            return ""
        
        # Remove quality indicators
        cleaned = self.quality_regex.sub(' ', name)
        
        # Remove junk patterns
        for pattern in self.junk_patterns:
            cleaned = pattern.sub(' ', cleaned)
        
        # Clean whitespace
        cleaned = re.sub(r'\\s+', ' ', cleaned).strip()
        
        # Remove empty brackets
        cleaned = re.sub(r'\\[\\s*\\]|\\(\\s*\\)|\\{\\s*\\}', '', cleaned)
        
        # Final trim of special chars
        cleaned = cleaned.strip(' -_#|:;*+=~()[]{}.,\'"\\/')
        
        return cleaned
    
    def similarity(self, s1: str, s2: str) -> float:
        """Calculate similarity between two strings."""
        return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()
    
    def find_match(self, name: str, existing_names: List[str]) -> Optional[str]:
        """Find best match in existing names (if similarity >= threshold)."""
        if not existing_names:
            return None
        
        best_match = None
        best_score = 0.0
        
        for existing in existing_names:
            score = self.similarity(name, existing)
            if score > best_score:
                best_score = score
                best_match = existing
        
        return best_match if best_score >= self.similarity_threshold else None


class SportsScanner:
    """Main scanner class."""
    
    def __init__(
        self,
        target_channels: List[str],
        max_streams_per_channel: int = MAX_STREAMS_PER_CHANNEL,
        test_timeout: int = TEST_TIMEOUT_SECONDS,
        test_retry_failed: int = TEST_RETRY_FAILED,
        test_retry_delay: float = TEST_RETRY_DELAY_SECONDS,
        allow_ffmpeg_fallback: bool = TEST_FFMPEG_FALLBACK,
        test_workers: int = TEST_WORKERS,
        test_user_agent: str = DEFAULT_USER_AGENT,
        preserve_existing_streams: bool = False,
        existing_channels: Optional[Dict[str, Dict]] = None,
    ):
        """Initialize scanner with target channels."""
        # Normalize targets for matching while preserving stable display names.
        self.target_display_names = {}
        for name in target_channels:
            if not name:
                continue
            cleaned = name.strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key not in self.target_display_names:
                self.target_display_names[key] = cleaned
        self.targets = list(self.target_display_names.keys())
        self.total_targets = len(self.targets)
        self.use_indexed_target_match = self.total_targets > 128

        # Fast target-matching index:
        # - targets length < 3 are checked directly
        # - targets length >= 3 use a selected trigram anchor
        #   so each stream only checks a small candidate subset.
        self.short_target_indices = tuple()
        self.anchor_to_target_indices = {}
        if self.use_indexed_target_match:
            self._build_target_match_index()

        # Channel storage: {original_target_name: {qualities: {quality: set()}, logo: str}}
        self.channels = defaultdict(lambda: {'qualities': defaultdict(set), 'logo': None})
        self.channel_urls = defaultdict(set)
        self.channel_ids = {}
        self.normalizer = ChannelNormalizer(0)
        self.max_streams_per_channel = max(1, int(max_streams_per_channel))
        self.test_timeout = max(1, int(test_timeout))
        self.test_retry_failed = max(0, int(test_retry_failed))
        self.test_retry_delay = max(0.0, float(test_retry_delay))
        self.test_workers = max(1, int(test_workers))
        self.test_user_agent = test_user_agent
        self.preserve_existing_streams = bool(preserve_existing_streams)
        self.lock = threading.Lock()
        self.url_test_cache: Dict[str, bool] = {}
        self.completed_targets = set()
        self.ffprobe_bin = shutil.which('ffprobe')
        self.ffmpeg_bin = shutil.which('ffmpeg')
        self.allow_ffmpeg_fallback = bool(allow_ffmpeg_fallback and self.ffmpeg_bin)

        if not self.ffprobe_bin:
            raise RuntimeError("ffprobe not found in PATH. Install ffmpeg/ffprobe before scanning.")
        if allow_ffmpeg_fallback and not self.ffmpeg_bin:
            print("Warning: ffmpeg not found; proceeding with ffprobe-only stream validation.", flush=True)
        
        # Stats
        self.stats = {
            'servers_total': 0,
            'servers_success': 0,
            'servers_failed': 0,
            'categories_total': 0,
            'streams_total': 0,
            'channels_added': 0,
            'streams_skipped_cap': 0,
            'channels_trimmed_to_cap': 0,
            'streams_trimmed_to_cap': 0,
            'streams_tested': 0,
            'streams_alive': 0,
            'streams_dead': 0,
            'streams_cached': 0,
            'channels_completed': 0,
            'channels_refreshed_from_tested_streams': 0,
            'channels_cleared_no_working_streams': 0,
            'channels_seeded_from_existing': 0,
            'streams_seeded_from_existing': 0,
        }

        if self.preserve_existing_streams:
            self._seed_existing_channels(existing_channels or {})

    def _build_target_match_index(self) -> None:
        """Build a compact substring prefilter index for fast candidate selection."""
        gram_frequency = defaultdict(int)
        target_grams: List[Optional[set]] = []
        short_indices = []

        for idx, target in enumerate(self.targets):
            if len(target) < 3:
                short_indices.append(idx)
                target_grams.append(None)
                continue

            grams = {target[i:i + 3] for i in range(len(target) - 2)}
            target_grams.append(grams)
            for gram in grams:
                gram_frequency[gram] += 1

        anchor_map = defaultdict(list)
        for idx, grams in enumerate(target_grams):
            if not grams:
                continue
            # Use rarest trigram as anchor to minimize candidate fan-out.
            anchor = min(grams, key=lambda g: (gram_frequency[g], g))
            anchor_map[anchor].append(idx)

        self.short_target_indices = tuple(short_indices)
        self.anchor_to_target_indices = {
            anchor: tuple(indices)
            for anchor, indices in anchor_map.items()
        }

    def _get_channel_id(self, name: str) -> int:
        """Get or create STABLE channel ID (Hash of name)."""
        if name not in self.channel_ids:
            val = zlib.adler32(name.encode('utf-8')) & 0xffffffff
            self.channel_ids[name] = val
        return self.channel_ids[name]

    def _find_target_match(self, stream_name: str) -> Optional[str]:
        """
        Check if stream name contains any target channel (Substring Match).
        This is the inner loop hot-path.
        """
        name_lower = stream_name.lower()
        if not name_lower:
            return None

        if not self.use_indexed_target_match:
            for target in self.targets:
                if target in name_lower:
                    return target
            return None

        candidate_indices = set(self.short_target_indices)

        # Gather candidates for targets length >= 3 via trigram anchors.
        if len(name_lower) >= 3:
            for i in range(len(name_lower) - 2):
                gram = name_lower[i:i + 3]
                anchored = self.anchor_to_target_indices.get(gram)
                if anchored:
                    candidate_indices.update(anchored)

        if not candidate_indices:
            return None

        # Preserve existing behavior: return first target by original target order.
        for idx in sorted(candidate_indices):
            target = self.targets[idx]
            if target in name_lower:
                return target
        return None

    def _get_display_name(self, target_lower: str) -> str:
        """Recover display name using original schedule casing when available."""
        return self.target_display_names.get(target_lower, target_lower.title())

    def _mark_channel_complete_locked(self, channel_name: str) -> None:
        if channel_name not in self.completed_targets:
            self.completed_targets.add(channel_name)
            self.stats['channels_completed'] += 1

    def _seed_existing_channels(self, existing_channels: Dict[str, Dict]) -> None:
        """Preload existing URLs so scans add/refill instead of rebuilding from scratch."""
        if not isinstance(existing_channels, dict) or not existing_channels:
            return

        existing_name_by_lower = {name.lower(): name for name in existing_channels.keys()}
        seeded_channels = 0
        seeded_streams = 0

        for target_lower in self.targets:
            canonical_name = existing_name_by_lower.get(target_lower)
            if not canonical_name:
                continue

            channel_data = existing_channels.get(canonical_name)
            if not isinstance(channel_data, dict):
                continue

            existing_qualities = channel_data.get('qualities')
            if not isinstance(existing_qualities, dict):
                continue

            display_name = self._get_display_name(target_lower)
            ordered_qualities = [q for q in QUALITY_PRIORITY if q in existing_qualities]
            ordered_qualities.extend(
                sorted(q for q in existing_qualities.keys() if q not in QUALITY_PRIORITY)
            )

            had_any = False
            for quality in ordered_qualities:
                raw_urls = existing_qualities.get(quality) or []
                if not isinstance(raw_urls, list):
                    continue

                for raw_url in raw_urls:
                    if len(self.channel_urls[display_name]) >= self.max_streams_per_channel:
                        break
                    if not isinstance(raw_url, str):
                        continue

                    url = raw_url.strip()
                    if not url:
                        continue
                    if url in self.channel_urls[display_name]:
                        continue

                    self.channel_urls[display_name].add(url)
                    self.channels[display_name]['qualities'][quality].add(url)
                    seeded_streams += 1
                    had_any = True

                if len(self.channel_urls[display_name]) >= self.max_streams_per_channel:
                    break

            if not had_any:
                continue

            seeded_channels += 1
            logo = channel_data.get('logo')
            if isinstance(logo, str):
                logo = logo.strip()
                if logo and not self.channels[display_name]['logo']:
                    self.channels[display_name]['logo'] = logo

            if len(self.channel_urls[display_name]) >= self.max_streams_per_channel:
                with self.lock:
                    self._mark_channel_complete_locked(display_name)

        self.stats['channels_seeded_from_existing'] = seeded_channels
        self.stats['streams_seeded_from_existing'] = seeded_streams

        if seeded_channels > 0:
            print(
                f"Seeded {seeded_channels} target channels with {seeded_streams} existing streams before scanning.",
                flush=True,
            )

    def _is_channel_complete(self, channel_name: str) -> bool:
        with self.lock:
            urls = self.channel_urls.get(channel_name)
            return len(urls) >= self.max_streams_per_channel if urls is not None else False

    def _all_targets_complete(self) -> bool:
        with self.lock:
            return self.total_targets > 0 and len(self.completed_targets) >= self.total_targets

    def _run_ffprobe(self, url: str) -> bool:
        cmd = [
            self.ffprobe_bin,
            "-v",
            "error",
            "-rw_timeout",
            str(self.test_timeout * 1_000_000),
            "-analyzeduration",
            "1000000",
            "-probesize",
            "65536",
            "-user_agent",
            self.test_user_agent,
            "-show_entries",
            "stream=codec_type",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            url,
        ]
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=self.test_timeout + 2,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return False
        except Exception:
            return False
        return result.returncode == 0 and bool(result.stdout.strip())

    def _run_ffmpeg(self, url: str) -> bool:
        if not self.ffmpeg_bin:
            return False
        cmd = [
            self.ffmpeg_bin,
            "-v",
            "error",
            "-rw_timeout",
            str(self.test_timeout * 1_000_000),
            "-user_agent",
            self.test_user_agent,
            "-t",
            "6",
            "-i",
            url,
            "-f",
            "null",
            "-",
        ]
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=self.test_timeout + 4,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return False
        except Exception:
            return False
        return result.returncode == 0

    def _validate_stream_url(self, channel_name: str, stream_name: str, url: str, source_label: str) -> bool:
        with self.lock:
            cached = self.url_test_cache.get(url)
        if cached is not None:
            with self.lock:
                self.stats['streams_cached'] += 1
            cached_label = 'ALIVE' if cached else 'DEAD'
            print(
                f"[TEST][CACHED] {cached_label} | channel={channel_name} | source={source_label} | stream={stream_name} | url={url}",
                flush=True,
            )
            return cached

        attempts = self.test_retry_failed + 1
        ok = False
        method = "ffprobe"
        for attempt in range(1, attempts + 1):
            ok = self._run_ffprobe(url)
            if ok:
                method = f"ffprobe(attempt={attempt})"
                break
            if attempt < attempts and self.test_retry_delay > 0:
                time.sleep(self.test_retry_delay)

        if not ok and self.allow_ffmpeg_fallback:
            ffmpeg_ok = self._run_ffmpeg(url)
            if ffmpeg_ok:
                ok = True
                method = "ffmpeg-fallback"
            else:
                method = "ffprobe+ffmpeg-fallback"

        with self.lock:
            self.url_test_cache[url] = ok
            self.stats['streams_tested'] += 1
            if ok:
                self.stats['streams_alive'] += 1
            else:
                self.stats['streams_dead'] += 1

        status = 'ALIVE' if ok else 'DEAD'
        print(
            f"[TEST] {status} | channel={channel_name} | source={source_label} | method={method} | stream={stream_name} | url={url}",
            flush=True,
        )
        return ok

    def _apply_channel_cap(self, qualities: Dict[str, List[str]]) -> Tuple[Dict[str, List[str]], int]:
        """Ensure no channel exceeds max_streams_per_channel across all qualities."""
        if not isinstance(qualities, dict):
            return {}, 0

        limited = {}
        seen_urls = set()
        dropped = 0
        kept_count = 0
        ordered_keys = [q for q in QUALITY_PRIORITY if q in qualities]
        ordered_keys.extend(sorted(q for q in qualities.keys() if q not in QUALITY_PRIORITY))

        for quality in ordered_keys:
            urls = qualities.get(quality) or []
            if not isinstance(urls, list):
                continue

            limited_urls = []
            for raw in urls:
                if not isinstance(raw, str):
                    continue
                url = raw.strip()
                if not url or url in seen_urls:
                    continue

                if kept_count >= self.max_streams_per_channel:
                    dropped += 1
                    continue

                seen_urls.add(url)
                kept_count += 1
                limited_urls.append(url)

            if limited_urls:
                limited[quality] = limited_urls

        return limited, dropped

    def process_streams(
        self,
        streams: List[Dict],
        api_instance: Optional[XtreamAPI] = None,
        source_label: str = "Unknown",
    ):
        """Process one playlist batch: collect candidates, test in parallel, keep only alive."""
        found_in_batch = 0
        candidates = []
        seen_pairs = set()

        for stream in streams:
            if self._all_targets_complete():
                print("  [INFO] All target channels reached the working-stream cap. Skipping remaining streams.", flush=True)
                break

            stream_name = stream.get('name', '').strip()
            if not stream_name:
                continue

            matched_target_lower = self._find_target_match(stream_name)
            if not matched_target_lower:
                continue

            final_name = self._get_display_name(matched_target_lower)
            if self._is_channel_complete(final_name):
                with self.lock:
                    self.stats['streams_skipped_cap'] += 1
                continue

            if api_instance:
                stream_id = stream.get('stream_id')
                if not stream_id:
                    continue
                url = api_instance.get_stream_url(stream_id)
            else:
                url = stream.get('url')
                if not url:
                    continue

            url = url.strip()
            if not url:
                continue

            with self.lock:
                if url in self.channel_urls[final_name]:
                    continue

            key = (final_name, url)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)

            candidates.append(
                {
                    'channel': final_name,
                    'stream_name': stream_name,
                    'quality': self.normalizer.extract_quality(stream_name),
                    'logo': stream.get('stream_icon') or stream.get('logo'),
                    'url': url,
                }
            )

        if not candidates:
            return 0

        print(
            f"    - Testing {len(candidates)} candidate streams with {self.test_workers} workers for source '{source_label}'...",
            flush=True,
        )

        def _test_candidate(candidate: Dict[str, str]) -> Optional[Dict[str, str]]:
            channel_name = candidate['channel']
            if self._is_channel_complete(channel_name):
                with self.lock:
                    self.stats['streams_skipped_cap'] += 1
                return None
            is_alive = self._validate_stream_url(
                channel_name=channel_name,
                stream_name=candidate['stream_name'],
                url=candidate['url'],
                source_label=source_label,
            )
            return candidate if is_alive else None

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.test_workers) as executor:
            futures = [executor.submit(_test_candidate, candidate) for candidate in candidates]
            for future in concurrent.futures.as_completed(futures):
                try:
                    candidate = future.result()
                except Exception as e:
                    print(f"  ! Stream test worker error in source '{source_label}': {e}", flush=True)
                    continue

                if not candidate:
                    continue

                channel_name = candidate['channel']
                url = candidate['url']
                quality = candidate['quality']
                stream_logo = candidate['logo']

                with self.lock:
                    channel_urls = self.channel_urls[channel_name]
                    if url in channel_urls:
                        continue
                    if len(channel_urls) >= self.max_streams_per_channel:
                        self.stats['streams_skipped_cap'] += 1
                        continue

                    self.channels[channel_name]['qualities'][quality].add(url)
                    channel_urls.add(url)
                    found_in_batch += 1
                    self.stats['channels_added'] += 1

                    if not self.channels[channel_name]['logo'] and stream_logo:
                        self.channels[channel_name]['logo'] = stream_logo

                    if len(channel_urls) == self.max_streams_per_channel:
                        self._mark_channel_complete_locked(channel_name)
                        print(
                            f"[CAP] Channel '{channel_name}' reached {self.max_streams_per_channel} working streams.",
                            flush=True,
                        )

                self._get_channel_id(channel_name)

        return found_in_batch
    
    def scan_direct_m3u(self, url: str) -> Dict:
        """Scan a direct M3U file URL."""
        print(f"  > Starting scan: Direct M3U ({url})...", flush=True)
        result = {
            'success': False,
            'channels_added': 0,
            'error': None
        }
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            lines = response.text.splitlines()
            
            parsed_streams = []
            current_info = {}
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('#EXTINF:'):
                    current_info = {}
                    # Extract Logo
                    logo_match = re.search(r'tvg-logo="([^"]*)"', line)
                    if logo_match:
                        current_info['logo'] = logo_match.group(1)
                    
                    # Extract Name
                    name_match = re.search(r',([^,]*)$', line)
                    if name_match:
                        current_info['name'] = name_match.group(1).strip()
                
                elif not line.startswith('#'):
                    # It's a URL
                    if 'name' in current_info:
                        current_info['url'] = line
                        parsed_streams.append(current_info)
                        current_info = {} # Reset
                        
            print(f"    - Parsed {len(parsed_streams)} streams from M3U. Matching...", flush=True)
            self.stats['streams_total'] += len(parsed_streams)
            
            added = self.process_streams(
                parsed_streams,
                api_instance=None,
                source_label=f"Direct M3U: {url}",
            )
            
            result['success'] = True
            result['channels_added'] = added
            print(f"  v Direct M3U - Done. Added {added} relevant channels.", flush=True)
            
        except Exception as e:
            result['error'] = str(e)
            print(f"  ! Direct M3U - Error: {e}", flush=True)
            
        return result

    def scan_server(self, server: Dict) -> Dict:
        """Scan a single server."""
        if server.get('type') == 'direct':
            return self.scan_direct_m3u(server['url'])

        print(f"  > Starting scan: {server.get('name', 'Unknown')}...", flush=True)
        result = {
            'name': server.get('name', 'Unknown'),
            'success': False,
            'channels_added': 0,
            'error': None
        }
        
        try:
            # Connect to API
            api = XtreamAPI(server['url'])
            
            # STRATEGY: fetch full live stream list only.
            # Category iteration fallback is intentionally disabled for speed.
            
            all_streams = []
            
            # Method A: Get All Streams
            print(f"    - Attempting to fetch full stream list...", flush=True)
            all_streams = api.get_live_streams(category_id=None)
            
            if all_streams:
                print(f"    - Success. Got {len(all_streams)} streams. Matching...", flush=True)
                self.stats['streams_total'] += len(all_streams)
                added = self.process_streams(
                    all_streams,
                    api_instance=api,
                    source_label=server.get('name', 'Unknown'),
                )
                
                result['success'] = True
                result['channels_added'] = added
                print(f"  v {server.get('name')} - Done. Added {added} channels.", flush=True)
                return result

            print(
                "    - Full list fetch returned empty. Category fallback disabled; skipping server.",
                flush=True,
            )
            result['success'] = True
            result['channels_added'] = 0
            print(f"  v {server.get('name')} - Done. Added 0 channels.", flush=True)
            
        except Exception as e:
            result['error'] = str(e)
            print(f"  ! {server.get('name')} - Error: {e}", flush=True)
        
        return result
    
    def scan_all(self, servers: List[Dict]) -> None:
        """Scan all provided servers."""
        servers_ordered = list(servers)
        self.stats['servers_total'] = len(servers_ordered)
        
        print(f"\\n{'='*70}")
        print(f"Scanning {len(servers_ordered)} configured servers")
        print("Priority: configured order (external playlists first, then lovestory)")
        print(
            f"Flow: one playlist at a time, batch-test URLs with {self.test_workers} workers, then move on"
        )
        print(f"{'='*70}\\n", flush=True)

        for idx, server in enumerate(servers_ordered, start=1):
            if self._all_targets_complete():
                print("All target channels are fully populated with working streams. Ending scan early.", flush=True)
                break

            server_name = server.get('name', 'Unknown')
            print(f"Playlist {idx}/{len(servers_ordered)}: {server_name}", flush=True)
            try:
                result = self.scan_server(server)
                if result['success']:
                    self.stats['servers_success'] += 1
                else:
                    self.stats['servers_failed'] += 1
            except Exception as e:
                print(f"Exception scanning {server_name}: {e}")
                self.stats['servers_failed'] += 1
                    
        print(f"--- Scan complete. ---", flush=True)

    def save(self, output_path: str) -> None:
        """Save results to JSON (Merge with existing)."""
        
        # 1. Load Existing Data to preserve Manual/Previous entries
        existing_data = {}
        if os.path.exists(output_path):
            try:
                with open(output_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load existing {output_path}: {e}")
        
        # Prepare Output Structure
        output = {
            'metadata': {
                'scan_date': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                'stats': self.stats,
                # Unique channels is sum of existing + new (deduplicated)
            },
            'channels': existing_data.get('channels', {})
        }
        existing_name_by_lower = {name.lower(): name for name in output['channels'].keys()}
        
        # 2. Replace target-channel URLs with this run's tested-alive URLs only.
        scanned_name_by_lower = {name.lower(): name for name in self.channels.keys()}
        refreshed_channels = 0
        emptied_channels = 0

        for target_lower in self.targets:
            display_name = self._get_display_name(target_lower)
            canonical_name = existing_name_by_lower.get(target_lower, display_name)
            scanned_name = scanned_name_by_lower.get(target_lower)
            scanned_data = self.channels.get(scanned_name) if scanned_name else None

            has_existing = canonical_name in output['channels']
            if not has_existing and not scanned_data:
                continue

            if canonical_name not in output['channels']:
                output['channels'][canonical_name] = {
                    'id': None,
                    'logo': None,
                    'qualities': {}
                }
                existing_name_by_lower[canonical_name.lower()] = canonical_name

            node = output['channels'][canonical_name]

            if scanned_data and scanned_data.get('qualities'):
                fresh_qualities = {}
                for quality in QUALITY_PRIORITY:
                    urls = scanned_data['qualities'].get(quality, set())
                    if urls:
                        fresh_qualities[quality] = sorted(urls)

                node['qualities'] = fresh_qualities if fresh_qualities else {}
                if node.get('id') is None:
                    node['id'] = self._get_channel_id(canonical_name)
                if scanned_data.get('logo') and not node.get('logo'):
                    node['logo'] = scanned_data['logo']
                refreshed_channels += 1
            else:
                # Channel was targeted this run but no working stream survived testing.
                node['qualities'] = {}
                emptied_channels += 1

        # 2b. Enforce hard per-channel stream cap in final merged output.
        trimmed_channels = 0
        trimmed_urls = 0
        for target_lower in self.targets:
            channel_name = existing_name_by_lower.get(target_lower)
            if not channel_name:
                continue
            channel_data = output['channels'].get(channel_name)
            if not isinstance(channel_data, dict):
                continue
            qualities = channel_data.get('qualities')
            if not isinstance(qualities, dict):
                continue
            limited_qualities, dropped = self._apply_channel_cap(qualities)
            if dropped > 0:
                trimmed_channels += 1
                trimmed_urls += dropped
            channel_data['qualities'] = limited_qualities if limited_qualities else {}

        self.stats['channels_trimmed_to_cap'] = trimmed_channels
        self.stats['streams_trimmed_to_cap'] = trimmed_urls
        self.stats['channels_refreshed_from_tested_streams'] = refreshed_channels
        self.stats['channels_cleared_no_working_streams'] = emptied_channels

        # 3. Add missing channels from the current schedule as placeholders.
        # Existing channels are never removed.
        found_names_lower = set(existing_name_by_lower.keys())
        
        missing_count = 0
        for target_lower in self.targets:
            if target_lower not in found_names_lower:
                # Truly new/missing
                display_name = self._get_display_name(target_lower)
                output['channels'][display_name] = {
                    'id': None,
                    'logo': None,
                    'qualities': None 
                }
                existing_name_by_lower[target_lower] = display_name
                missing_count += 1

        output['metadata']['unique_channels'] = len(output['channels'])

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        # Console Reporting
        print(f"\\n{'='*70}", flush=True)
        print(f"[OK] Saved merged results to {output_path}", flush=True)
        print(f"  Total Channels in DB: {len(output['channels'])}", flush=True)
        print(f"  New/Updated in this scan: {len(self.channels)}", flush=True)
        print(f"  Missing (Added as null): {missing_count}", flush=True)
        print(f"  Max streams/channel cap: {self.max_streams_per_channel}", flush=True)
        if self.preserve_existing_streams:
            print(f"  Seeded channels from existing DB: {self.stats['channels_seeded_from_existing']}", flush=True)
            print(f"  Seeded streams from existing DB: {self.stats['streams_seeded_from_existing']}", flush=True)
        print(f"  Streams tested: {self.stats['streams_tested']}", flush=True)
        print(f"  Streams alive: {self.stats['streams_alive']}", flush=True)
        print(f"  Streams dead: {self.stats['streams_dead']}", flush=True)
        print(f"  Cached stream test hits: {self.stats['streams_cached']}", flush=True)
        print(f"  Channels completed at cap: {self.stats['channels_completed']}", flush=True)
        print(f"  Channels refreshed with tested streams: {self.stats['channels_refreshed_from_tested_streams']}", flush=True)
        print(f"  Channels cleared (no working streams): {self.stats['channels_cleared_no_working_streams']}", flush=True)
        print(f"  Streams skipped during scan due to cap: {self.stats['streams_skipped_cap']}", flush=True)
        print(f"  Channels trimmed to cap in final output: {trimmed_channels}", flush=True)
        print(f"  Streams trimmed to cap in final output: {trimmed_urls}", flush=True)
        print(f"{'='*70}\\n", flush=True)


def load_target_channels(schedule_file):
    """Load unique channel names from the weekly schedule."""
    try:
        with open(schedule_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        targets = set()
        for day in data.get('schedule', []):
            for event in day.get('events', []):
                for channel in event.get('channels', []):
                    if channel:
                        clean_name = channel.strip()
                        if not is_usable_channel_name(clean_name):
                            continue
                        if clean_name.lower() == 'laligatv': clean_name = 'Laliga Tv'
                        targets.add(clean_name)
        
        print(f"Loaded {len(targets)} unique target channels from schedule.")
        return list(targets)
    except Exception as e:
        print(f"Error loading schedule: {e}")
        return []

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output_file', nargs='?', default='channels.json', help='Output JSON file')
    parser.add_argument('--verify', action='store_true', help='Use only first 3 servers for verification')
    parser.add_argument(
        '--preserve-existing-streams',
        action='store_true',
        help='Pre-seed target channels from existing output file before scanning (useful after stream pruning).',
    )
    parser.add_argument(
        '--max-working-streams-per-channel',
        type=int,
        default=MAX_STREAMS_PER_CHANNEL,
        help='Hard cap of working streams to keep per channel',
    )
    parser.add_argument('--test-workers', type=int, default=TEST_WORKERS, help='Parallel workers per playlist for stream testing')
    parser.add_argument('--test-timeout', type=int, default=TEST_TIMEOUT_SECONDS, help='Per-stream ffprobe timeout in seconds')
    parser.add_argument('--test-retry-failed', type=int, default=TEST_RETRY_FAILED, help='Extra ffprobe retries before marking dead')
    parser.add_argument('--test-retry-delay', type=float, default=TEST_RETRY_DELAY_SECONDS, help='Delay between ffprobe retries')
    parser.add_argument('--no-ffmpeg-fallback', action='store_true', help='Disable ffmpeg fallback test')
    parser.add_argument('--test-user-agent', default=DEFAULT_USER_AGENT, help='HTTP User-Agent for ffprobe/ffmpeg')
    args = parser.parse_args()
    
    # 1. Load Targets
    targets = load_target_channels(SCHEDULE_FILE)
    if not targets:
        print("No target channels found. Exiting.")
        sys.exit(1)

    # 2. Load Servers
    servers = load_scan_servers(EXTERNAL_PLAYLISTS_FILE, LOVESTORY_FILE)
    if args.verify:
         print("VERIFY MODE: Limiting to first 3 configured playlist sources")
         servers = servers[:3]
         
    existing_channels = {}
    if args.preserve_existing_streams and os.path.exists(args.output_file):
        try:
            with open(args.output_file, 'r', encoding='utf-8') as handle:
                existing_data = json.load(handle)
            parsed_channels = existing_data.get('channels', {})
            if isinstance(parsed_channels, dict):
                existing_channels = parsed_channels
        except Exception as e:
            print(f"Warning: could not pre-load existing channels from {args.output_file}: {e}")

    # 3. Init Scanner (hard cap enforced per channel)
    scanner = SportsScanner(
        target_channels=targets,
        max_streams_per_channel=args.max_working_streams_per_channel,
        test_timeout=args.test_timeout,
        test_retry_failed=args.test_retry_failed,
        test_retry_delay=args.test_retry_delay,
        allow_ffmpeg_fallback=not args.no_ffmpeg_fallback,
        test_workers=args.test_workers,
        test_user_agent=args.test_user_agent,
        preserve_existing_streams=args.preserve_existing_streams,
        existing_channels=existing_channels,
    )
    
    # 4. Run Scan
    scanner.scan_all(servers)
    
    # 5. Save
    scanner.save(args.output_file)


if __name__ == '__main__':
    main()

