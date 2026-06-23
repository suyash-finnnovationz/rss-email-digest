"""RSS feed parser module."""
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Union
from datetime import datetime, timedelta, timezone
import time
import aiohttp
import feedparser
import asyncio
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_opml(opml_path: Path) -> List[Dict[str, str]]:
    if not opml_path.exists():
        raise FileNotFoundError(f"OPML file not found: {opml_path}")

    # Read raw text and fix common XML issues before parsing
    raw = opml_path.read_text(encoding='utf-8')
    # Fix unescaped & that aren't already &amp;
    import re as _re
    raw = _re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', raw)

    import io
    root = ET.parse(io.StringIO(raw)).getroot()

    feeds = []
    for outline in root.findall(".//outline[@xmlUrl]"):
        feeds.append({
            "title": outline.get("text") or outline.get("title"),
            "url": outline.get("xmlUrl"),
            "html_url": outline.get("htmlUrl", "")
        })

    return feeds


def is_recent(date_value: Union[datetime, time.struct_time, None]) -> bool:
    """Check if a date is from the last 24 hours (UTC)."""
    if date_value is None:
        return False

    if isinstance(date_value, time.struct_time):
        date_value = datetime(*date_value[:6], tzinfo=timezone.utc)

    if date_value.tzinfo is None:
        date_value = date_value.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(hours=24)

    return date_value >= yesterday


async def fetch_feed(feed_name: str, feed_url: str, timeout: int = 15, html_url: str = "") -> Dict:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(feed_url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                raw_bytes = await response.read()

        # Try multiple encodings
        content = None
        for encoding in ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1']:
            try:
                content = raw_bytes.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        if content is None:
            content = raw_bytes.decode('utf-8', errors='replace')

        # Parse feed
        feed = feedparser.parse(content)

        # Allow slightly malformed feeds if entries exist
        if feed.bozo and not feed.entries:
            return {
                "name": feed_name,
                "status": "error",
                "posts": [],
                "error_message": f"Invalid feed format: {feed.bozo_exception}",
                "site_url": html_url
            }

        site_url = feed.feed.get("link", "") if hasattr(feed, "feed") else ""

        # Filter for last 1 hour
        recent_posts = []
        for entry in feed.entries:
            pub_date = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)

            if pub_date and is_recent(pub_date):
                excerpt = ""
                if hasattr(entry, "summary"):
                    excerpt = entry.summary
                elif hasattr(entry, "content"):
                    excerpt = entry.content[0].value

                excerpt = re.sub(r'<[^>]+>', '', excerpt)
                excerpt = excerpt.strip()
                if len(excerpt) > 300:
                    excerpt = excerpt[:300] + "..."

                recent_posts.append({
                    "title": entry.title,
                    "link": entry.link,
                    "excerpt": excerpt
                })

        status = "success" if recent_posts else "no_updates"
        logger.info(f"{feed_name}: {len(recent_posts)} posts in last 1 hour")

        return {
            "name": feed_name,
            "status": status,
            "posts": recent_posts,
            "site_url": site_url
        }

    except asyncio.TimeoutError:
        logger.warning(f"{feed_name}: Timeout after {timeout}s")
        return {
            "name": feed_name,
            "status": "error",
            "posts": [],
            "error_message": f"Timeout after {timeout}s",
            "site_url": html_url
        }
    except Exception as e:
        logger.error(f"{feed_name}: Error - {str(e)}")
        return {
            "name": feed_name,
            "status": "error",
            "posts": [],
            "error_message": str(e),
            "site_url": html_url
        }


async def fetch_all_feeds(feeds: List[Dict[str, str]], batch_size: int = 10, timeout: int = 15) -> List[Dict]:
    results = []
    logger.info(f"Fetching {len(feeds)} feeds in batches of {batch_size}...")

    for i in range(0, len(feeds), batch_size):
        batch = feeds[i:i + batch_size]
        tasks = [fetch_feed(feed["title"], feed["url"], timeout, feed.get("html_url", "")) for feed in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for j, result in enumerate(batch_results):
            if isinstance(result, Exception):
                feed = batch[j]
                logger.error(f"{feed['title']}: Unexpected error - {result}")
                results.append({
                    "name": feed["title"],
                    "status": "error",
                    "posts": [],
                    "error_message": f"Unexpected error: {str(result)}",
                    "site_url": feed.get("html_url", "")
                })
            else:
                results.append(result)

    logger.info(f"Completed fetching {len(results)} feeds")
    return results
