import html
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests
import yaml
from trendradar.ai import forum_stream
from trendradar.crawler.rss import RSSFetcher, RSSFeedConfig
from trendradar.notification.formatters import strip_markdown
from trendradar.utils.time import is_within_days
from trendradar.utils.url import normalize_url

CONFIG = Path(os.getenv("FULL_RSS_CONFIG", "/app/config/config.yaml"))
STATE = Path(os.getenv("FULL_RSS_STATE", "/app/output/full-rss-seen.sqlite3"))
TOKEN = os.environ["TELEGRAM_FULL_BOT_TOKEN"]
CHAT_IDS = list(dict.fromkeys(chat_id.strip() for chat_id in (
    os.environ["TELEGRAM_FULL_CHAT_ID"],
    os.getenv("TELEGRAM_FULL_PRIVATE_CHAT_ID", ""),
) if chat_id.strip()))
POLL_SECONDS = int(os.getenv("FULL_RSS_POLL_SECONDS", "300"))
CACHE_DIR = Path(os.getenv("FULL_RSS_CACHE_DIR", "/app/output/rss-cache"))


def db():
    STATE.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(STATE)
    c.execute("create table if not exists seen (feed_id text not null, key text not null, primary key(feed_id,key))")
    c.execute("create table if not exists seen_urls (feed_id text not null, url text not null, primary key(feed_id,url))")
    if forum_stream.enabled():
        forum_stream.init_queue(c)
    # Backfill the URL index from the last cache so this fix does not replay old items.
    for cache in CACHE_DIR.glob("*.json"):
        feed_id = cache.stem
        try:
            entries = json.loads(cache.read_text(encoding="utf-8"))
        except Exception:
            continue
        c.executemany(
            "insert or ignore into seen_urls(feed_id,url) values(?,?)",
            [(feed_id, canonical_url(e.get("url", ""))) for e in entries if canonical_url(e.get("url", ""))],
        )
    c.commit()
    return c


def canonical_url(value):
    value = str(value or "").strip()
    if not value:
        return ""
    parts = urlsplit(normalize_url(value))
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, parts.query, ""))


def fetch_flarum(url):
    response = requests.get(url, timeout=30, headers={"User-Agent": "HorizonFullRSS/1.0"})
    response.raise_for_status()
    payload = response.json()
    posts = {post["id"]: post.get("attributes", {}) for post in payload.get("included", []) if post.get("type") == "posts"}
    base = url.split("/api/", 1)[0]
    entries = []
    for discussion in payload["data"]:
        identity = str(discussion["id"])
        attributes = discussion["attributes"]
        if not identity.isdecimal() or not attributes.get("title") or not attributes.get("createdAt"):
            continue
        stamp = datetime.fromisoformat(attributes["createdAt"].replace("Z", "+00:00"))
        post = discussion.get("relationships", {}).get("firstPost", {}).get("data") or {}
        entries.append({
            "id": identity, "link": f"{base}/d/{identity}", "title": attributes["title"],
            "published": attributes["createdAt"],
            "published_parsed": stamp.astimezone(timezone.utc).timetuple(),
            "summary": posts.get(post.get("id"), {}).get("contentHtml", ""),
        })
    return entries


def fetch_feed(name, url, feed_id=""):
    """Use TrendRadar fetching/parsing; only adapt to the existing queue/cache schema."""
    fetcher = RSSFetcher([], timeout=30)
    try:
        items, error = fetcher.fetch_feed(RSSFeedConfig(id=feed_id, name=name, url=url), use_cache=False)
    finally:
        fetcher.session.close()
    if error or not items:
        raise RuntimeError(error or "RSS 源没有有效条目")
    entries = []
    for item in items:
        parts = None
        if item.published_at:
            stamp = datetime.fromisoformat(item.published_at.replace("Z", "+00:00"))
            parts = stamp.replace(tzinfo=stamp.tzinfo or timezone.utc).astimezone(timezone.utc).timetuple()
        entries.append({
            "id": item.guid or item.url, "link": item.url, "title": item.title,
            "published": item.published_at, "published_parsed": parts,
            "summary": item.summary, "author": item.author,
        })
    return entries


def write_cache(feed_id, entries):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = []
    for entry in entries:
        parts = entry.get("published_parsed") or entry.get("updated_parsed")
        published_at = ""
        if parts:
            published_at = datetime(*parts[:6], tzinfo=timezone.utc).isoformat()
        else:
            published_at = entry.get("published") or entry.get("updated") or ""
        key = entry.get("id") or entry.get("guid") or entry.get("link")
        if not key:
            continue
        payload.append({
            "key": str(key),
            "title": entry.get("title", ""),
            "url": entry.get("link", ""),
            "guid": str(entry.get("id") or entry.get("guid") or entry.get("link") or ""),
            "published_at": published_at,
            "summary": entry.get("summary") or entry.get("description") or "",
            "author": entry.get("author") or "",
        })
    target = CACHE_DIR / f"{feed_id}.json"
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    temporary.replace(target)


def write_sources(cfg):
    """Publish only effective RSS subscriptions, never credentials."""
    rss = cfg.get("rss", {})
    freshness = rss.get("freshness_filter", {})
    sources = {}
    if rss.get("enabled", False) is True:
        for source in rss.get("feeds", []):
            if source.get("enabled", True) is False or not source.get("url"):
                continue
            feed_id = source.get("id", "")
            if not feed_id or feed_id in (".", "..") or Path(feed_id).name != feed_id:
                raise ValueError("Invalid RSS source id")
            age = source.get("max_age_days", freshness.get("max_age_days", 3)) if freshness.get("enabled", True) else 0
            if not isinstance(age, int) or age < 0:
                raise ValueError("Invalid RSS source max_age_days")
            sources[source["url"]] = {"id": feed_id, "title": source.get("name", feed_id), "max_age_days": age}
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    target = CACHE_DIR / "sources"
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(sources, ensure_ascii=False), encoding="utf-8")
    temporary.replace(target)


def send(items):
    groups = {"linuxdo": "LinuxDo论坛", "nodeseek": "NodeSeek论坛", "naixi-forum": "奶昔论坛", "lowendtalk": "LowEndTalk论坛", "v2ex-tech": "V2EX技术"}
    failures = 0
    for feed_name, entry in items:
        title = html.escape(entry.get("title", "(无标题)"))
        link = html.escape(entry.get("link", ""), quote=True)
        group = html.escape(groups.get(entry.get("feed_id", ""), feed_name))
        stamp = entry.get("published_display") or entry.get("published") or entry.get("updated") or ""
        text = (f"<b>{title}</b>\n\n" f"🌐 链接: <a href=\"{link}\">{link}</a>\n" f"🏷️ 分组: {group}\n" f"🕒 时间: {html.escape(str(stamp))}")
        for target_index, chat_id in enumerate(CHAT_IDS, 1):
            try:
                # ponytail: bounded flood-control retry; durable per-target outbox if other failures need replay.
                for attempt in range(5):
                    r = requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": False}, timeout=30)
                    if r.status_code != 429 or attempt == 4:
                        break
                    delay = max(1, min(300, int(r.json().get("parameters", {}).get("retry_after", 5))))
                    print(f"[全量RSS] 目标 {target_index} 限流，等待 {delay} 秒重试", flush=True)
                    time.sleep(delay)
                r.raise_for_status()
                if not r.json().get("ok"):
                    raise RuntimeError("Telegram 返回 ok=false")
            except Exception as ex:
                failures += 1
                status = getattr(getattr(ex, "response", None), "status_code", None)
                print(f"[全量RSS] 目标 {target_index} 发送失败: {type(ex).__name__} status={status}", flush=True)
            time.sleep(1)
    if failures:
        raise RuntimeError(f"Telegram 共 {failures} 次发送失败，其他目标已继续发送")
    print(f"[全量RSS] 发送 {len(items)} 条至 {len(CHAT_IDS)} 个目标，单条格式", flush=True)


def export_selected():
    """Publish both views from the local queue, independent of Telegram."""
    destination = os.getenv("FULL_RSS_PUBLIC_DIR")
    if not destination:
        return
    threshold = forum_stream.min_score()
    directory = Path(destination)
    history_path = directory / "channel-history.json"
    history = json.loads(history_path.read_text(encoding="utf-8"))["items"] if history_path.exists() else []
    source_aliases = {
        "LinuxDo 最新": "linuxdo", "LINUX DO": "linuxdo", "LinuxDo论坛": "linuxdo",
        "NodeSeek 最新": "nodeseek", "NodeSeek": "nodeseek", "NodeSeek论坛": "nodeseek",
        "奶昔论坛 最新": "naixi", "forum.naixi.net": "naixi",
        "LowEndTalk": "lowendtalk", "LowEndTalk论坛": "lowendtalk",
        "V2EX｜技术": "v2ex", "V2EX": "v2ex", "V2EX技术": "v2ex",
    }
    def key(item):
        source = item.get("source", "")
        return (source_aliases.get(source, source), item.get("published", ""))
    historical = {}
    for old in history:
        historical.setdefault(key(old), []).append(old)
    c = sqlite3.connect(f"{STATE.resolve().as_uri()}?mode=ro", uri=True, timeout=30)
    try:
        rows = c.execute("SELECT id,payload,score,tag,discovered_at FROM forum_ai_queue ORDER BY discovered_at DESC,id DESC").fetchall()
    finally:
        c.close()
    items, matched, urls = [], set(), set()
    for identity, payload, score, tag, discovered in rows:
        p = json.loads(payload)
        try:
            url = urlsplit(p.get("url", ""))
        except ValueError:
            continue
        if url.scheme.lower() not in {"http", "https"} or not url.netloc:
            continue
        canonical = canonical_url(p["url"])
        if canonical in urls:
            continue
        urls.add(canonical)
        # Channel previews truncate titles at punctuation; match only unique prefixes at the same source/time.
        title = html.unescape(p.get("title", "")).strip()
        candidates = [old for old in historical.get(key(p), [])
                      if old.get("title", "").strip() and title.startswith(html.unescape(old["title"]).strip())]
        old = candidates[0] if len(candidates) == 1 else None
        if old:
            matched.add(old["id"])
        items.append({
            "id": f"rss:{identity}", "title": p.get("title", ""), "url": canonical,
            "source": p.get("source", ""), "published": p.get("published", ""),
            "summary": html.unescape(strip_markdown(p.get("summary", "")))[:1000],
            "score": score if tag else None, "tag": tag or "", "origin": "rss",
            "score_status": "scored" if tag and score is not None else "pending" if score is None else "unmatched",
            "score_reason": p.get("score_reason", "") or ("旧规则未返回分类；0 为默认值，不是真实评分" if score is not None and not tag else ""),
            "discovered_at": discovered, "channel_url": old["url"] if old else None,
        })
    for old in history:
        if old["id"] not in matched:
            items.append({**old, "score": None, "tag": "", "origin": "channel-history"})
    def timestamp(item):
        try:
            value = datetime.fromisoformat(item.get("published", ""))
            if value.tzinfo is None:
                value = value.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
            return value.timestamp()
        except (ValueError, TypeError):
            return item.get("discovered_at", 0)
    items.sort(key=lambda item: (timestamp(item), str(item["id"])), reverse=True)
    selected = [item for item in items if isinstance(item["score"], (int, float)) and threshold <= item["score"] <= 1]
    updated = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()
    directory.mkdir(parents=True, exist_ok=True)
    for name, entries in (("all", items), ("selected", selected)):
        temporary = directory / f"{name}.json.tmp"
        temporary.write_text(json.dumps({"updated_at": updated, "min_score": threshold,
                                         "complete": True, "source": "backend-rss", "items": entries}, ensure_ascii=False), encoding="utf-8")
        temporary.replace(directory / f"{name}.json")
    print(f"[网站消息] 全量 {len(items)} 条，精选 {len(selected)} 条，共用后台数据", flush=True)



def poll(first):
    cfg = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    write_sources(cfg)
    rss = cfg.get("rss", {})
    if rss.get("enabled", False) is not True:
        print("[全量RSS] RSS 总开关关闭，跳过采集和推送；历史数据保留", flush=True)
        if forum_stream.enabled():
            export_selected()
        return
    feeds = [f for f in rss.get("feeds", []) if f.get("enabled", True) is not False]
    feeds += [f for f in cfg.get("forum_feeds", []) if f.get("enabled", True) is not False]
    freshness = rss.get("freshness_filter", {})
    timezone_name = cfg.get("app", {}).get("timezone", "Asia/Shanghai")
    c = db(); fresh = []
    # Normalize historical URLs too, without rewriting the existing per-source records.
    known_urls = {canonical_url(row[0]) for row in c.execute("select url from seen_urls")}
    for f in feeds:
        feed_id, name, url = f.get("id", ""), f.get("name", f.get("id", "")), f.get("url", "")
        if not url: continue
        # Release the queue write lock before the next potentially slow network fetch.
        c.commit()
        try:
            entries = fetch_flarum(url) if f.get("type") == "flarum" else fetch_feed(name, url, feed_id)
            write_cache(feed_id, entries)
            rows = []
            for e in entries:
                key = e.get("id") or e.get("guid") or e.get("link")
                if not key: continue
                rows.append((str(key), canonical_url(e.get("link", "")), e))
            feed_initialized = c.execute(
                "select 1 from seen where feed_id=? limit 1", (feed_id,)
            ).fetchone() is not None
            if first or not feed_initialized:
                c.executemany("insert or ignore into seen(feed_id,key) values(?,?)", [(feed_id, k) for k,_,_ in rows])
                c.executemany("insert or ignore into seen_urls(feed_id,url) values(?,?)", [(feed_id, u) for _,u,_ in rows if u])
                known_urls.update(u for _, u, _ in rows if u)
                if not first:
                    print(
                        f"[全量RSS] {name} 首次成功抓取，已初始化 {len(rows)} 条基线，跳过旧帖补发",
                        flush=True,
                    )
            else:
                for k,url,e in rows:
                    if c.execute("select 1 from seen where feed_id=? and key=?", (feed_id,k)).fetchone() is not None:
                        continue
                    c.execute("insert or ignore into seen(feed_id,key) values(?,?)", (feed_id,k))
                    if url:
                        c.execute("insert or ignore into seen_urls(feed_id,url) values(?,?)", (feed_id,url))
                        if url in known_urls:
                            continue
                        known_urls.add(url)
                    parts = e.get("published_parsed") or e.get("updated_parsed")
                    published = datetime(*parts[:6], tzinfo=timezone.utc).isoformat() if parts else ""
                    if freshness.get("enabled", True) and not is_within_days(
                        published, f.get("max_age_days", freshness.get("max_age_days", 3)), timezone_name
                    ):
                        continue
                    e["feed_id"] = feed_id
                    if parts:
                        e["published_display"] = datetime(*parts[:6], tzinfo=timezone.utc).astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        e["published_display"] = e.get("published") or e.get("updated") or ""
                    fresh.append((name, e))
                    if forum_stream.enabled():
                        forum_stream.enqueue(c, name, e, k, url)
        except Exception as ex:
            print(f"[全量RSS] {name} 抓取失败: {ex}", flush=True)
    c.commit(); c.close()
    if forum_stream.enabled():
        try:
            export_selected()  # Publish before Telegram and AI; neither blocks website messages.
        except Exception as ex:
            print(f"[网站消息] 导出失败: {type(ex).__name__}", flush=True)
    fresh.sort(key=lambda item: tuple(item[1].get("published_parsed") or item[1].get("updated_parsed") or ()))
    try:
        if fresh and cfg.get("notification", {}).get("enabled", True): send(fresh)
        elif fresh: print("[全量RSS] 通知关闭，保留采集数据，不发送", flush=True)
        else: print("[全量RSS] 本轮无新增", flush=True)
    finally:
        if forum_stream.enabled() and cfg.get("filter", {}).get("method", "ai") == "ai":
            from trendradar.core.loader import load_config
            try:
                forum_stream.score_pending(load_config())
            finally:
                export_selected()


if __name__ == "__main__":
    first = not STATE.exists()
    while True:
        # Align to wall-clock boundaries: :00, :05, :10, ...
        wait = POLL_SECONDS - (int(time.time()) % POLL_SECONDS)
        time.sleep(wait)
        try: poll(first); first = False
        except Exception as ex: print(f"[全量RSS] 轮询失败: {ex}", flush=True)
