"""Serve the existing editor and its three deployment files, never .env or output."""
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import runpy
import secrets
import tempfile
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import yaml

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
CONFIG = ROOT / "config"
FILES = {"config": "config.yaml", "frequency": "frequency_words.txt", "timeline": "timeline.yaml"}
MASK = "__SERVER_SECRET_PRESERVED__"
SECRET = re.compile(r"token|password|secret|api_key|access_key|webhook|chat_id|email_from|email_to", re.I)
CSRF = secrets.token_urlsafe(32)
# This native module is standalone; do not load the AI runtime into the editor.
Scheduler = runpy.run_path(str(ROOT / "trendradar/core/scheduler.py"))["Scheduler"]


def revision(content):
    return hashlib.sha256(content.encode()).hexdigest()


def scalars(content):
    # Reject aliases and duplicate keys: browser and runtime must parse identically.
    if any(isinstance(t, (yaml.tokens.AliasToken, yaml.tokens.AnchorToken)) for t in yaml.scan(content)):
        raise ValueError("不支持 YAML 锚点或别名，请使用普通配置")
    root = yaml.compose(content, Loader=yaml.SafeLoader)
    result = {}

    def walk(node, path=(), hidden=False):
        if isinstance(node, yaml.MappingNode):
            seen = set()
            for key, value in node.value:
                if not isinstance(key, yaml.ScalarNode) or key.value in seen:
                    raise ValueError("配置包含重复或非文本键")
                seen.add(key.value)
                walk(value, path + (key.value,), hidden or bool(SECRET.search(key.value)))
        elif isinstance(node, yaml.SequenceNode):
            for i, value in enumerate(node.value):
                identity = i
                if isinstance(value, yaml.MappingNode):
                    identity = next(("id:" + v.value for k, v in value.value if k.value == "id" and isinstance(v, yaml.ScalarNode)), i)
                walk(value, path + (identity,), hidden)
        elif isinstance(node, yaml.ScalarNode):
            private_url = node.value.startswith(("http://", "https://")) and (urlsplit(node.value).username or SECRET.search(urlsplit(node.value).query))
            result[path] = (node, bool(hidden or private_url))

    if root:
        walk(root)
    return result


def replace_scalars(content, replacements):
    for node, value in sorted(replacements, key=lambda pair: pair[0].start_mark.index, reverse=True):
        content = content[:node.start_mark.index] + value + content[node.end_mark.index:]
    return content


def public_content(content):
    return replace_scalars(content, [(node, json.dumps(MASK)) for node, hidden in scalars(content).values()
                                     if hidden and node.tag != "tag:yaml.org,2002:null" and node.value])


def restore_secrets(content, original):
    old = scalars(original)
    replacements = []
    new = scalars(content)
    for path, (node, _) in new.items():
        if node.value == MASK:
            previous, hidden = old.get(path, (None, False))
            if previous is None or not hidden:
                raise ValueError("密钥占位符位置错误，请重新读取服务器配置")
            replacements.append((node, original[previous.start_mark.index:previous.end_mark.index]))
    for path, (node, hidden) in old.items():
        if hidden and node.value and path not in new:
            raise ValueError("不能删除已有密钥字段；请保留占位符")
    return replace_scalars(content, replacements)


def validate(kind, content, original):
    if not content.strip():
        raise ValueError("不能保存空文件")
    if kind == "frequency":
        return content
    content = restore_secrets(content, original)
    data, before = yaml.safe_load(content), yaml.safe_load(original)
    if not isinstance(data, dict) or not set(before).issubset(data):
        raise ValueError("配置缺少已有顶层模块，请勿用空白或官网模板覆盖部署配置")

    def check(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "enabled" and not isinstance(value, bool):
                    raise ValueError("enabled 必须是 true 或 false")
                check(value)
        elif isinstance(node, list):
            for value in node:
                check(value)
    check(data)
    if kind == "config":
        for key in before:
            if not isinstance(data[key], dict):
                raise ValueError(f"{key} 必须是配置对象")
        ZoneInfo(data["app"].get("timezone", "Asia/Shanghai"))
        ids = set()
        feeds = data["rss"].get("feeds", [])
        if not isinstance(feeds, list):
            raise ValueError("RSS feeds 必须是列表")
        for feed in feeds:
            if not isinstance(feed, dict) or not isinstance(feed.get("id"), str) or not re.fullmatch(r"[A-Za-z0-9_-]+", feed["id"]):
                raise ValueError("RSS 源 ID 只能包含字母、数字、下划线和短横线")
            if feed["id"] in ids:
                raise ValueError("RSS 源 ID 不能重复")
            ids.add(feed["id"])
            url = urlsplit(feed.get("url", ""))
            if url.scheme not in ("http", "https") or not url.hostname:
                raise ValueError("RSS 地址必须是完整 HTTP/HTTPS 地址")
        for settings in [data["rss"].get("freshness_filter", {}), *feeds]:
            age = settings.get("max_age_days", 0)
            if isinstance(age, bool) or not isinstance(age, int) or age < 0:
                raise ValueError("max_age_days 必须是非负整数")
        score = data["ai_filter"].get("min_score", 0)
        if isinstance(score, bool) or not isinstance(score, (int, float)) or not 0 <= score <= 1:
            raise ValueError("min_score 必须是 0~1 的数值")
        if data["filter"].get("method", "keyword") not in ("keyword", "ai"):
            raise ValueError("filter.method 必须是 keyword 或 ai")
    deployment = data if kind == "config" else yaml.safe_load((CONFIG / "config.yaml").read_text())
    timeline = data if kind == "timeline" else yaml.safe_load((CONFIG / "timeline.yaml").read_text())
    Scheduler(dict(deployment["schedule"], enabled=True), timeline, None, lambda: None)
    return content


def save_file(kind, content, expected):
    path = CONFIG / FILES[kind]
    original = path.read_text(encoding="utf-8")
    if not hmac.compare_digest(expected, revision(original)):
        raise FileExistsError("服务器配置已改变；请重新读取后再编辑，未覆盖任何文件")
    content = validate(kind, content, original)
    if content != original:
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=CONFIG, prefix=".editor-", delete=False) as f:
                temporary = Path(f.name)
                os.fchmod(f.fileno(), path.stat().st_mode & 0o777)
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(CONFIG, os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary and temporary.exists():
                temporary.unlink()
    return {"content": public_content(content) if kind != "frequency" else content, "revision": revision(content)}


class Handler(SimpleHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DOCS), **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        super().end_headers()

    def authenticated(self):
        credentials = f"{os.environ['EDITOR_USER']}:{os.environ['EDITOR_PASSWORD']}"
        expected = "Basic " + base64.b64encode(credentials.encode()).decode()
        if hmac.compare_digest(self.headers.get("Authorization", "").encode(), expected.encode()):
            return True
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="TrendRadar management", charset="UTF-8"')
        self.end_headers()
        return False

    def json(self, status, value):
        encoded = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        if not self.authenticated():
            return
        if urlsplit(self.path).path == "/api/config":
            files = {}
            for kind, name in FILES.items():
                content = (CONFIG / name).read_text(encoding="utf-8")
                files[kind] = {"content": public_content(content) if kind != "frequency" else content, "revision": revision(content)}
            return self.json(200, {"files": files, "csrf": CSRF})
        return self.do_HEAD(body=True)

    def do_HEAD(self, body=False):
        if not body and not self.authenticated():
            return
        path = Path(self.translate_path(self.path)).resolve()
        if not path.is_relative_to(DOCS) or (path != DOCS and path != DOCS / "index.html" and not path.is_relative_to(DOCS / "assets")) or (path.is_dir() and path != DOCS):
            return self.send_error(404)
        if body:
            return super().do_GET()
        return super().do_HEAD()

    def do_PUT(self):
        if not self.authenticated():
            return
        origin = urlsplit(self.headers.get("Origin", ""))
        if not hmac.compare_digest(self.headers.get("X-CSRF-Token", "").encode(), CSRF.encode()) or origin.netloc != self.headers.get("Host") or origin.scheme not in ("http", "https"):
            return self.json(403, {"error": "请求校验失败，请刷新页面"})
        kind = urlsplit(self.path).path.removeprefix("/api/config/")
        if kind not in FILES:
            return self.json(404, {"error": "不允许访问此文件"})
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 1_048_576 or self.headers.get_content_type() != "application/json":
                raise ValueError("请求格式错误或文件过大")
            data = json.loads(self.rfile.read(size))
            if not isinstance(data.get("content"), str) or not isinstance(data.get("revision"), str):
                raise ValueError("缺少配置内容或版本")
            saved = save_file(kind, data["content"], data["revision"])
            print(f"[editor] saved {FILES[kind]} revision={saved['revision'][:12]}", flush=True)
            return self.json(200, saved)
        except FileExistsError as error:
            return self.json(409, {"error": str(error)})
        except (ValueError, TypeError, AttributeError, KeyError, yaml.YAMLError):
            return self.json(400, {"error": "配置校验失败：检查模块、开关类型、源 ID、地址、天数和评分阈值；原文件未改动"})
        except OSError:
            return self.json(500, {"error": "保存失败，原文件保留，请检查服务权限"})


if __name__ == "__main__":
    if not os.environ.get("EDITOR_USER") or not os.environ.get("EDITOR_PASSWORD"):
        raise SystemExit("EDITOR_USER and EDITOR_PASSWORD are required")
    HTTPServer(("127.0.0.1", 18183), Handler).serve_forever()
