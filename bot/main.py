"""SibStar Telegram IPA auto-updater.

User account watches the configured source channels (default @blatants; add/remove from the admin bot) and
IPA files sent/forwarded to the admin bot -> forwards each new .ipa to @linkbefileeTGbot -> gets the direct link ->
asks SibStar (/api/internal/auto-ipa-update) to stage-import it for the matching EXISTING app ->
draft (default) or publish. Admin bot (@erfanitt only) reports and controls everything.
"""
import asyncio
import hashlib
import datetime as dt
import html
import json
import plistlib
import os
import re
import signal
import struct
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
import zlib

from telethon import Button, TelegramClient, events, utils
from telethon.errors import UserAlreadyParticipantError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import Channel

DEFAULT_CHANNEL = os.environ.get("SOURCE_CHANNEL", "blatants").lower()
LINKBOT = os.environ.get("LINK_BOT", "linkbefileeTGbot")
ENDPOINT = os.environ["SIBSTAR_ENDPOINT"]
SECRET = os.environ.get("SIBSTAR_ENDPOINT_SECRET", "")
API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION = os.environ["TG_SESSION"]
BOT_TOKEN = os.environ["ADMIN_BOT_TOKEN"]
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "erfanitt").lower().lstrip("@")
ADMIN_ID_ENV = int(os.environ.get("ADMIN_ID", "0") or 0)
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", str(5 * 3600 - 15 * 60)))
STATE_DIR = os.environ.get("STATE_DIR", "state")
STATE_PUSH = os.environ.get("STATE_PUSH", "1") == "1"
STATE_KEY = os.environ.get("STATE_KEY", "")  # state is pushed ONLY encrypted (public repo)
# v3: the admin's mode button is the control; the server's SIBSTAR_AUTO_IPA_ALLOW_PUBLISH is the master kill switch.
FINAL_STATUSES = ("draft_ok", "published", "skipped", "rejected", "duplicate", "discarded")
SITE = os.environ.get("SITE_URL", "https://beta.sibstars.ir").rstrip("/")
TEST_ACTIONS = [a.strip() for a in os.environ.get("TEST_ACTIONS", "").split(",") if ":" in a]
# one-off test run (workflow_dispatch inputs): refs like "blatants/13480"; TEST_MODE=publish is explicit approval
TEST_REFS = [r.strip() if "/" in r else f"{DEFAULT_CHANNEL}/{r.strip()}" for r in os.environ.get("TEST_REFS", os.environ.get("TEST_MESSAGE_ID", "")).split(",") if r.strip() and r.strip() != "0"]
TEST_MODE = "publish" if os.environ.get("TEST_MODE") == "publish" else "draft"
PROXY = None
if os.environ.get("BOX_PROXY"):
    h, p = os.environ["BOX_PROXY"].rsplit(":", 1)
    PROXY = ("http", h, int(p))
TEHRAN = dt.timezone(dt.timedelta(hours=3, minutes=30))

STATE_FILE = os.path.join(STATE_DIR, "state.json")
DEFAULT_STATE = {"enabled": True, "mode": "draft", "admin_id": ADMIN_ID_ENV or None, "items": {}, "log": [],
                 "channels": [{"key": DEFAULT_CHANNEL, "peer": DEFAULT_CHANNEL, "title": DEFAULT_CHANNEL}], "last_ids": {}}

state = {}
dirty = False
queue: "asyncio.Queue[tuple[int, dict]]" = None  # type: ignore
user: TelegramClient = None  # type: ignore
bot: TelegramClient = None  # type: ignore
current = {"ref": None, "stage": None}


def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def fa_time(iso):
    try:
        return dt.datetime.fromisoformat(iso).astimezone(TEHRAN).strftime("%m-%d %H:%M")
    except Exception:
        return iso or ""


def log(msg):
    print(f"[{dt.datetime.now(TEHRAN).strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------- state
def load_state():
    global state
    try:
        with open(STATE_FILE, encoding="utf8") as f:
            state = json.load(f)
    except Exception:
        state = json.loads(json.dumps(DEFAULT_STATE))
    for k, v in DEFAULT_STATE.items():
        state.setdefault(k, json.loads(json.dumps(v)))
    if ADMIN_ID_ENV and not state.get("admin_id"):
        state["admin_id"] = ADMIN_ID_ENV
    # v2 migration: numeric item keys / last_msg_id belonged to @blatants
    if "last_msg_id" in state:
        state.setdefault("last_ids", {}).setdefault(DEFAULT_CHANNEL, int(state.pop("last_msg_id") or 0))
    for k in [k for k in state["items"] if "/" not in k]:
        it = state["items"].pop(k)
        it.update(ref=f"{DEFAULT_CHANNEL}/{k}", chan=DEFAULT_CHANNEL)
        state["items"][f"{DEFAULT_CHANNEL}/{k}"] = it
    # v3 migration: start in draft/manual once; afterwards only the admin's button changes the mode
    if not state.get("v3"):
        state["v3"] = True
        state["mode"] = "draft"
        event("v3: mode reset to draft (manual)")


def mark():
    global dirty
    dirty = True


def event(text):
    state["log"].append({"t": now_iso(), "m": text})
    state["log"] = state["log"][-300:]
    mark()


_pushed_hash = ""


def save_state(push=True):
    global dirty
    os.makedirs(STATE_DIR, exist_ok=True)
    items = state["items"]
    if len(items) > 400:
        old = [k for k in sorted(items, key=lambda x: (items[x].get("updated") or "")) if items[k].get("status") != "draft_ok"]
        for k in old[: len(items) - 400]:
            items.pop(k, None)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf8") as f:
        json.dump(state, f, ensure_ascii=False, indent=1)
    os.replace(tmp, STATE_FILE)
    dirty = False
    if push and STATE_PUSH:
        try:
            global _pushed_hash
            if not STATE_KEY:
                raise RuntimeError("no STATE_KEY")
            h = hashlib.sha256(open(STATE_FILE, "rb").read()).hexdigest()
            if h == _pushed_hash:
                return
            subprocess.run(["openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-iter", "200000", "-salt",
                            "-in", STATE_FILE, "-out", os.path.join(STATE_DIR, "state.enc"), "-pass", "env:STATE_KEY"],
                           check=True, capture_output=True)
            subprocess.run(["git", "-C", STATE_DIR, "add", "state.enc"], check=True, capture_output=True)
            subprocess.run(["git", "-C", STATE_DIR, "commit", "-q", "--amend", "-m", "state (encrypted)"], check=True, capture_output=True)
            subprocess.run(["git", "-C", STATE_DIR, "push", "-q", "-f", "origin", "HEAD:state"], check=True, capture_output=True, timeout=60)
            _pushed_hash = h
        except Exception as e:  # never crash on state push
            log(f"state push failed: {type(e).__name__}")


async def state_saver():
    while True:
        await asyncio.sleep(45)
        if dirty:
            save_state()


# ---------------------------------------------------------------- SibStar endpoint
def _post(payload):
    req = urllib.request.Request(
        ENDPOINT,
        data=json.dumps(payload).encode(),
        headers={"content-type": "application/json", "x-sibstar-auto-ipa-secret": SECRET, "user-agent": "sib-ipa-updater/1"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:400]
        try:
            j = json.loads(body)
        except Exception:
            j = {"reasonFa": f"HTTP {e.code}"}
        j.setdefault("ok", False)
        j.setdefault("outcome", "error")
        j.setdefault("reasonFa", j.get("error") or f"HTTP {e.code}")
        return j


async def sib(payload, tries=4):
    last = None
    for i in range(tries):
        try:
            return await asyncio.to_thread(_post, payload)
        except Exception as e:
            last = e
            await asyncio.sleep(10 * (i + 1))
    return {"ok": False, "outcome": "error", "reasonFa": f"اتصال به سایت ناموفق ({type(last).__name__})"}


# ---------------------------------------------------------------- parsing
def parse_post(m):
    if not m or not m.document:
        return None
    fn = None
    for a in m.document.attributes:
        if getattr(a, "file_name", None):
            fn = a.file_name
    if not fn or not fn.lower().endswith(".ipa"):
        return None
    cap = m.message or ""
    first = cap.strip().split("\n")[0].strip() if cap.strip() else ""
    b = re.search(r"(?:Bundle\s*ID|Bundle|باندل)\s*[:：]\s*([A-Za-z0-9][\w.-]+)", cap, re.I)
    v = re.search(r"(?:Updated to|Version|نسخه)\s*[:：]\s*(\S+)", cap, re.I)
    if not v:
        v = re.search(r"[_ ]v(\d[\d.]*\d|\d)(?=[_ .])", fn)
    name = first
    if not name:
        base = re.sub(r"\.ipa$", "", fn, flags=re.I)
        base = re.split(r"[_ ]v\d", base)[0]
        name = base.replace("_", " ").strip()
    return {
        "msg_id": m.id,
        "name": name[:200],
        "bundle": b.group(1) if b else None,
        "version": v.group(1) if v else None,
        "file": fn,
        "size": m.document.size,
        "posted": m.date.isoformat() if m.date else None,
    }


# ---------------------------------------------------------------- link bot
URL_RE = re.compile(r"https?://\S+")


async def get_link(msg):
    """Forward the channel post to the link bot, confirm, and wait for the direct link."""
    linkbot = await user.get_entity(LINKBOT)
    fwd = await user.forward_messages(linkbot, msg)
    fwd = fwd[0] if isinstance(fwd, list) else fwd
    deadline = time.time() + 25 * 60
    confirm = None
    # 1) wait for the confirm keyboard
    while time.time() < deadline and confirm is None:
        await asyncio.sleep(3)
        async for r in user.iter_messages(linkbot, min_id=fwd.id, limit=20):
            if r.out:
                continue
            if r.reply_to and r.reply_to.reply_to_msg_id == fwd.id and r.buttons:
                confirm = r
                break
            if r.reply_to and r.reply_to.reply_to_msg_id == fwd.id and ("دسترسی" in (r.message or "")):
                raise RuntimeError("لینک‌یار دسترسی نمی‌دهد: " + (r.message or "")[:120])
        if confirm is None and time.time() - fwd.date.timestamp() > 180:
            raise RuntimeError("لینک‌یار جواب نداد (احتمالاً خاموش است)")
    if confirm is None:
        raise RuntimeError("لینک‌یار جواب نداد")
    clicked = False
    for row in confirm.buttons:
        for btn in row:
            data = getattr(btn, "data", None) or b""
            if data.startswith(b"c:1:"):
                try:
                    await btn.click()
                except Exception:
                    pass  # answerCallback timeouts are fine; edits follow
                clicked = True
                break
        if clicked:
            break
    if not clicked:
        raise RuntimeError("دکمهٔ تأیید لینک‌یار پیدا نشد")
    # 2) wait for the ready edit
    while time.time() < deadline:
        await asyncio.sleep(5)
        cur = await user.get_messages(linkbot, ids=confirm.id)
        text = (cur.message or "") if cur else ""
        if cur and cur.buttons:
            for row in cur.buttons:
                for btn in row:
                    u = getattr(btn, "url", None)
                    if u and u.startswith("http"):
                        return u
        if "لینک آماده" in text:
            found = URL_RE.findall(text)
            if found:
                return found[-1]
        if "آپلود انجام نشد" in text or "لغو شد" in text:
            raise RuntimeError("لینک‌یار: " + text.replace("\n", " ")[:200])
    raise RuntimeError("ساخت لینک بیش از ۲۵ دقیقه طول کشید")


# ---------------------------------------------------------------- admin bot helpers
def admin_id():
    return state.get("admin_id")


async def notify(text, buttons=None):
    aid = admin_id()
    if not aid:
        return False
    try:
        await bot.send_message(aid, text, buttons=buttons, parse_mode="html", link_preview=False)
        if state.get("admin_reachable") is not True:
            state["admin_reachable"] = True
            mark()
        return True
    except Exception as e:
        log(f"notify failed: {type(e).__name__}")
        state["admin_reachable"] = False
        state["admin_error"] = type(e).__name__
        mark()
        return False


def esc(s):
    return html.escape(str(s or ""))


def item_title(it):
    app = it.get("app") or {}
    return esc(app.get("nameFa") or app.get("name") or it.get("name"))


def sid_of(ref):
    return hashlib.sha256(ref.encode()).hexdigest()[:10]


def ref_by_sid(sid):
    for k in state["items"]:
        if sid_of(k) == sid:
            return k
    return None


def item_buttons(it):
    ref = it.get("ref") or f"{DEFAULT_CHANNEL}/{it.get('msg_id')}"
    sid = sid_of(ref)
    st = it.get("status")
    if st in ("failed", "rejected", "error"):
        return [[Button.inline("🔁 تلاش دوباره", f"rt:{sid}".encode())]]
    if st == "skipped" and it.get("code") in ("same_version", "older_version"):
        return [[Button.inline("🔁 بازهم آپدیت کن", f"fc:{sid}".encode())]]
    if st == "draft_ok":
        return [[Button.inline("✅ انتشار", f"dp:{sid}".encode()), Button.inline("❌ رد", f"dr:{sid}".encode())]]
    if st == "duplicate":
        return [[Button.inline("🔁 دوباره بررسی", f"re:{sid}".encode())]]
    return None


def app_link(it):
    slug = (it.get("app") or {}).get("slug")
    return f"\n🔗 {SITE}/app/{esc(slug)}" if slug else ""


def fmt_fields(f):
    if not f:
        return ""
    parts = []
    if f.get("version"):
        parts.append(f"نسخه {esc(f['version'])}")
    if f.get("build"):
        parts.append(f"Build {esc(f['build'])}")
    if f.get("minIosVersion") or f.get("minIos"):
        parts.append(f"iOS {esc(f.get('minIosVersion') or f.get('minIos'))}+")
    if f.get("deviceSupportFa"):
        parts.append(esc(f["deviceSupportFa"]))
    sz = f.get("sizeBytes")
    if sz:
        parts.append(f"{sz / 1048576:.1f} MB")
    return " · ".join(parts)


def item_report(it):
    st = it.get("status")
    t = item_title(it)
    if st == "published":
        old = it.get("old_file") or {}
        gone = "بله ✅" if old.get("deleted") else "نه"
        return (f"✅ <b>{t}</b> منتشر شد (نسخه {esc((it.get('fields') or {}).get('version'))})\n{fmt_fields(it.get('fields'))}\n"
                f"🗑 فایل قبلی حذف شد: {gone} — {esc(old.get('reasonFa', ''))}" + app_link(it))
    if st == "duplicate":
        return f"♻️ <b>{t}</b> {esc(it.get('version') or '')}: {esc(it.get('reason'))}"
    if st == "discarded":
        return f"🗑 <b>{t}</b> {esc(it.get('version') or '')}: پیش‌نویس رد شد (سایت تغییری نکرد)\n{esc((it.get('discard') or {}).get('reasonFa', ''))}"
    if st == "draft_ok":
        fr = (it.get("plan") or {}).get("from") or {}
        same = " (همان فایل فعلی)" if (it.get("plan") or {}).get("sameFile") else ""
        return (
            f"📝 <b>{t}</b> آماده است — فقط پیش‌نویس، منتشر نشد{same}\n"
            f"سایت: نسخه {esc(fr.get('version'))} ← فایل جدید: {fmt_fields((it.get('plan') or {}).get('fields'))}\n"
            f"Bundle ID تأیید شد ✔︎ <code>{esc(((it.get('plan') or {}).get('fields') or {}).get('bundleId'))}</code>"
            + (f"\nℹ️ {esc(it.get('reason'))}" if it.get("reason") else "") + app_link(it)
        )
    if st == "skipped":
        return f"⚠️ <b>{t}</b>: {esc(it.get('reason'))}"
    if st == "rejected":
        return f"⛔️ <b>{t}</b> منتشر نشد: {esc(it.get('reason'))}"
    if st == "publish_disabled":
        return f"📝 <b>{t}</b>: {esc(it.get('reason'))}"
    return f"❌ <b>{t}</b> خطا: {esc(it.get('reason'))}"


# ---------------------------------------------------------------- Info.plist straight from Telegram (range reads)
async def tg_read(msg, start, end):
    """bytes [start, end) of the message's document via aligned ranged downloads (no full download)."""
    rs = 512 * 1024  # offsets aligned to the request size: never crosses a 1 MB boundary (MTProto rule)
    al = start - (start % rs)
    buf = bytearray()
    async for chunk in user.iter_download(msg.media, offset=al, request_size=rs):
        buf += chunk
        if al + len(buf) >= end:
            break
    return bytes(buf[start - al:end - al])


async def ipa_info(msg):
    """Read Payload/*.app/Info.plist of the IPA in `msg` -> {bundle, version, build, name}. Raises on failure."""
    size = msg.document.size
    tail_len = min(size, 65536 + 22)
    tail = await tg_read(msg, size - tail_len, size)
    p = tail.rfind(b"PK\x05\x06")
    if p < 0:
        raise RuntimeError("فایل IPA معتبر نیست (zip)")
    cd_size, cd_off = struct.unpack_from("<II", tail, p + 12)
    if cd_size == 0xFFFFFFFF or cd_off == 0xFFFFFFFF:
        q = tail.rfind(b"PK\x06\x07", 0, p)
        if q < 0:
            raise RuntimeError("zip64 نامعتبر")
        (z64,) = struct.unpack_from("<Q", tail, q + 8)
        rec = await tg_read(msg, z64, z64 + 56)
        cd_size, cd_off = struct.unpack_from("<QQ", rec, 40)
    if cd_size > 64 * 1024 * 1024:
        raise RuntimeError("فهرست zip خیلی بزرگ است")
    cd = await tg_read(msg, cd_off, cd_off + cd_size)
    i = 0
    target = None
    while i + 46 <= len(cd) and cd[i:i + 4] == b"PK\x01\x02":
        method, = struct.unpack_from("<H", cd, i + 10)
        csize, usize = struct.unpack_from("<II", cd, i + 20)
        nlen, elen, clen = struct.unpack_from("<HHH", cd, i + 28)
        (loff,) = struct.unpack_from("<I", cd, i + 42)
        name = cd[i + 46:i + 46 + nlen].decode("utf8", "replace")
        extra = cd[i + 46 + nlen:i + 46 + nlen + elen]
        if re.match(r"^Payload/[^/]+\.app/Info\.plist$", name):
            j = 0
            while j + 4 <= len(extra):
                hid, hlen = struct.unpack_from("<HH", extra, j)
                if hid == 1:
                    vals = extra[j + 4:j + 4 + hlen]
                    k = 0
                    if usize == 0xFFFFFFFF:
                        (usize,) = struct.unpack_from("<Q", vals, k); k += 8
                    if csize == 0xFFFFFFFF:
                        (csize,) = struct.unpack_from("<Q", vals, k); k += 8
                    if loff == 0xFFFFFFFF:
                        (loff,) = struct.unpack_from("<Q", vals, k); k += 8
                j += 4 + hlen
            target = (method, csize, loff)
            break
        i += 46 + nlen + elen + clen
    if not target:
        raise RuntimeError("Info.plist داخل IPA پیدا نشد")
    method, csize, loff = target
    lh = await tg_read(msg, loff, loff + 30)
    nlen, elen = struct.unpack_from("<HH", lh, 26)
    data = await tg_read(msg, loff + 30 + nlen + elen, loff + 30 + nlen + elen + csize)
    raw = zlib.decompressobj(-15).decompress(data) if method == 8 else data
    pl = plistlib.loads(raw)
    return {
        "bundle": str(pl.get("CFBundleIdentifier") or "") or None,
        "version": str(pl.get("CFBundleShortVersionString") or pl.get("CFBundleVersion") or "") or None,
        "build": str(pl.get("CFBundleVersion") or "") or None,
        "name": str(pl.get("CFBundleDisplayName") or pl.get("CFBundleName") or "") or None,
    }


# ---------------------------------------------------------------- sources (channels + admin DM relay)
def ref_of(key, mid):
    return f"{key}/{mid}"


def ref_split(ref):
    key, _, mid = str(ref).rpartition("/")
    return key, int(mid)


def channel_keys():
    return [c["key"] for c in state.get("channels", [])]


async def peer_for(key):
    if key == "dm":
        return await user.get_entity(BOT_USERNAME)
    for c in state.get("channels", []):
        if c["key"] == key:
            return await user.get_entity(c["peer"])
    raise RuntimeError("کانال در فهرست نیست")


async def resolve_channel_ids():
    try:
        await user.get_dialogs(limit=None)
    except Exception:
        pass
    for c in state.get("channels", []):
        try:
            ent = await user.get_entity(c["peer"])
            chan_ids[utils.get_peer_id(ent)] = c["key"]
        except Exception as e:
            log(f"channel {c['key']} not resolvable: {type(e).__name__}")


def same_app_version_done(ref, app_id=None, bundle=None, version=None):
    """cross-channel dedupe: another item with the same app (or bundle) + version already reported ok."""
    if not version:
        return None
    for k, v in state["items"].items():
        if k == ref or v.get("status") not in ("draft_ok", "published"):
            continue
        if str(v.get("version") or "") != str(version):
            continue
        if app_id and (v.get("app") or {}).get("id") == app_id:
            return k
        if bundle and (v.get("bundle") or "").lower() == bundle.lower():
            return k
    return None


# ---------------------------------------------------------------- processing
async def process(ref, opts):
    it = state["items"].get(ref)
    manual = bool(opts.get("manual"))
    if it is not None and not manual and it.get("status") in FINAL_STATUSES:
        log(f"item {ref} already done; not reprocessed")
        return
    key, mid = ref_split(ref)
    msg = await user.get_messages(await peer_for(key), ids=mid)
    post = parse_post(msg)
    if not post:
        if it is None:
            state["items"][ref] = {"ref": ref, "chan": key, "msg_id": mid, "name": "?", "status": "skipped", "reason": "پیام IPA پیدا نشد", "created": now_iso(), "updated": now_iso()}
        return
    post["ref"], post["chan"] = ref, key
    # caption is only a hint: the IPA's own Info.plist is the truth (any channel / any file sent to the bot)
    try:
        info = await ipa_info(msg)
        if info.get("bundle"):
            if post.get("bundle") and post["bundle"].lower() != info["bundle"].lower():
                log(f"item {ref}: caption bundle differs from Info.plist; using Info.plist")
            post["bundle"] = info["bundle"]
        if info.get("version"):
            post["version"] = info["version"]
        if info.get("name") and not (msg.message or "").strip():
            post["name"] = info["name"][:200]
        post["plist"] = info
    except Exception as e:
        log(f"item {ref}: Info.plist read failed ({type(e).__name__}); using caption")
        if not post.get("bundle"):
            post["plist_error"] = str(e)[:120]
    if not manual:
        dup = same_app_version_done(ref, bundle=post.get("bundle"), version=post.get("version"))
        if dup:
            state["items"][ref] = {**post, "status": "duplicate", "created": now_iso(), "updated": now_iso(), "reason": f"تکراری (همان برنامه و نسخه: {dup})"}
            event(f"{ref} duplicate of {dup}")
            save_state()
            return
    if it is None:
        it = {**post, "status": "queued", "created": now_iso(), "attempts": 0}
        state["items"][ref] = it
    it.update({k: post[k] for k in ("name", "bundle", "version", "file", "size", "ref", "chan", "msg_id")})
    it["plist"] = post.get("plist")
    it["attempts"] = it.get("attempts", 0) + 1
    it["updated"] = now_iso()
    mode = opts.get("mode") or state.get("mode", "draft")
    if mode != "publish":
        mode = "draft"
    force = bool(opts.get("force"))
    current.update(ref=ref, stage="match")
    reuse = mode == "publish" and it.get("status") == "draft_ok" and it.get("job_id") and (it.get("app") or {}).get("id")
    mark()
    silent = False
    try:
        if reuse:
            url = it.get("url") or ""
            current["stage"] = "publish"
            return await poll_job(it, url, post, mode)
        m = await sib({"action": "match", "name": post["name"], "bundleId": post["bundle"], "version": post["version"], "force": force})
        if m.get("outcome") == "skipped":
            it.update(status="skipped", code=m.get("code"), reason=m.get("reasonFa"), app=m.get("app"))
            return
        if m.get("outcome") != "match":
            raise RuntimeError(m.get("reasonFa") or "خطای تطبیق")
        it["app"] = m.get("app")
        if not manual:
            dup = same_app_version_done(ref, app_id=(m.get("app") or {}).get("id"), version=post.get("version"))
            if dup:
                it.update(status="duplicate", reason=f"تکراری (همان برنامه و نسخه: {dup})")
                silent = True
                return
        current["stage"] = "link"
        url = it.get("url") if it.get("url") and (time.time() - it.get("url_ts", 0) < 4 * 3600) else None
        if not url:
            url = await get_link(msg)
            it.update(url=url, url_ts=time.time())
            mark()
        current["stage"] = "import"
        s = await sib({"action": "start", "name": post["name"], "bundleId": post["bundle"], "version": post["version"], "fileName": post["file"], "sourceUrl": url, "force": force})
        if s.get("outcome") == "skipped":
            it.update(status="skipped", code=s.get("code"), reason=s.get("reasonFa"))
            return
        if s.get("outcome") != "queued":
            raise RuntimeError(s.get("reasonFa") or "شروع دریافت ناموفق")
        it.update(job_id=s["jobId"], app=s.get("app"), matched_by=s.get("matchedBy"), forced=force)
        mark()
        await poll_job(it, url, post, mode)
    except Exception as e:
        it.update(status="failed", reason=str(e)[:300])
        log(f"item {ref} failed: {type(e).__name__}")
        traceback.print_exc(limit=2)
    finally:
        it["updated"] = now_iso()
        it["mode"] = mode
        event(f"{ref} {it.get('status')} {it.get('name')}")
        current.update(ref=None, stage=None)
        report = item_report(it)
        rh = hashlib.sha256(report.encode()).hexdigest()[:16]
        repeat = it.get("reported") == rh
        it["reported"] = rh
        save_state()
        if silent:
            pass
        elif not repeat:
            await notify(report, buttons=item_buttons(it))
        else:
            log(f"item {ref} same result as last report; not re-sent")


async def poll_job(it, url, post, mode):
    if True:
        deadline = time.time() + 90 * 60
        while time.time() < deadline:
            await asyncio.sleep(15)
            r = await sib({"action": "status", "jobId": it["job_id"], "appId": it["app"]["id"], "sourceUrl": url, "bundleId": post["bundle"], "mode": mode, "force": bool(it.get("forced"))})
            oc = r.get("outcome")
            if oc == "pending":
                current["stage"] = f"import {r.get('progress', 0)}%"
                continue
            if oc == "draft_ok":
                it.update(status="draft_ok", plan=r.get("plan"), reason=None, fields=(r.get("plan") or {}).get("fields"))
                return
            if oc == "published":
                it.update(status="published", fields=r.get("fields"), old_file=r.get("oldFile"), reason=None, published_at=now_iso())
                return
            if oc == "publish_disabled":
                it.update(status="draft_ok", plan=r.get("plan"), fields=(r.get("plan") or {}).get("fields"), reason=r.get("reasonFa"))
                return
            if oc in ("rejected", "skipped"):
                it.update(status="rejected" if oc == "rejected" else "skipped", code=r.get("code"), reason=r.get("reasonFa"))
                return
            raise RuntimeError(r.get("reasonFa") or f"وضعیت نامعتبر: {oc}")
        raise RuntimeError("دریافت فایل در سایت بیش از ۹۰ دقیقه طول کشید")


async def worker():
    while True:
        ref, opts = await queue.get()
        try:
            if state.get("enabled") or opts.get("manual") or opts.get("batch"):
                await process(ref, opts)
        except Exception:
            traceback.print_exc()
        finally:
            inflight.discard(ref)
            await check_batches()


inflight = set()
batches = {}


def enqueue(ref, **opts):
    if ref in inflight:
        return False
    inflight.add(ref)
    queue.put_nowait((ref, opts))
    return True


STATUS_FA = {"published": "✅ آپدیت شد", "draft_ok": "📝 پیش‌نویس آماده", "skipped": "⚠️ رد شد", "rejected": "⛔️ رد (بررسی)",
             "failed": "❌ خطا", "duplicate": "♻️ تکراری", "queued": "⏳ در صف", "discarded": "🗑 پیش‌نویس رد شد"}


async def start_batch(label, refs, single=False):
    bid = f"b{int(time.time() * 1000)}"
    refs = list(dict.fromkeys(refs))
    already = {r for r in refs if (state["items"].get(r) or {}).get("status") in FINAL_STATUSES}
    batches[bid] = {"label": label, "refs": refs, "already": already, "single": single}
    for r in refs:
        if r not in already:
            enqueue(r, batch=bid)
    await check_batches()
    return bid


async def check_batches():
    for bid, b in list(batches.items()):
        if any(r in inflight for r in b["refs"]):
            continue
        batches.pop(bid, None)
        if b.get("single"):
            r = b["refs"][0] if b["refs"] else None
            it = state["items"].get(r) or {}
            if r in b["already"] or it.get("status") == "duplicate":
                # point at the existing draft (with its buttons) or offer a forced re-check
                orig = state["items"].get((it.get("reason") or "").split(": ")[-1].rstrip(")")) if it.get("status") == "duplicate" else None
                btns = item_buttons(orig) if orig and orig.get("status") == "draft_ok" else []
                btns = (btns or []) + [[Button.inline("🔁 دوباره بررسی", f"re:{sid_of(r)}".encode())]] if r else btns
                extra = "\n📝 پیش‌نویسش آماده است؛ همین‌جا منتشر یا رد کن:" if btns and len(btns) > 1 else ""
                await notify(f"☑️ {item_title(it)} {esc(it.get('version') or '')}: قبلاً بررسی شده/تکراری — {esc(it.get('reason') or STATUS_FA.get(it.get('status'), ''))}{extra}", buttons=btns or None)
            continue
        lines = [f"<b>{esc(b['label'])}</b>", f"پیدا شد: {len(b['refs'])} فایل IPA"]
        groups = {}
        for r in b["refs"]:
            it = state["items"].get(r) or {"status": "failed", "reason": "پردازش نشد"}
            st = "already" if r in b["already"] else it.get("status", "?")
            groups.setdefault(st, []).append(it)
        lab = dict(STATUS_FA, already="☑️ قبلاً بررسی شده")
        for st, its in groups.items():
            lines.append(f"\n{lab.get(st, st)}: {len(its)}")
            for it in its[:15]:
                why = "" if st in ("published", "draft_ok", "already") else f" — {esc((it.get('reason') or '')[:70])}"
                lines.append(f"• {item_title(it)} {esc(it.get('version') or '')}{why}")
        if not b["refs"]:
            lines.append("هیچ IPA در این بازه پیدا نشد.")
        await notify("\n".join(lines)[:3900])


def tehran_day_bounds(d):
    start = dt.datetime(d.year, d.month, d.day, tzinfo=TEHRAN)
    return start.astimezone(dt.timezone.utc), (start + dt.timedelta(days=1)).astimezone(dt.timezone.utc)


def jalali_to_gregorian(jy, jm, jd):
    jy += 1595
    days = -355668 + (365 * jy) + (jy // 33) * 8 + ((jy % 33) + 3) // 4 + jd + ((jm - 1) * 31 if jm < 7 else ((jm - 7) * 30) + 186)
    gy = 400 * (days // 146097)
    days %= 146097
    if days > 36524:
        days -= 1
        gy += 100 * (days // 36524)
        days %= 36524
        if days >= 365:
            days += 1
    gy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        gy += (days - 1) // 365
        days = (days - 1) % 365
    gd = days + 1
    leap = (gy % 4 == 0 and gy % 100 != 0) or gy % 400 == 0
    months = [0, 31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    gm = 1
    while gm <= 12 and gd > months[gm]:
        gd -= months[gm]
        gm += 1
    return dt.date(gy, gm, gd)


FA_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def parse_day(text):
    t = (text or "").strip().translate(FA_DIGITS)
    if t in ("امروز", "today"):
        return dt.datetime.now(TEHRAN).date()
    if t in ("دیروز", "yesterday"):
        return dt.datetime.now(TEHRAN).date() - dt.timedelta(days=1)
    m = re.match(r"^(\d{4})[/\-.](\d{1,2})[/\-.](\d{1,2})$", t)
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return jalali_to_gregorian(y, mo, d) if y < 1700 else dt.date(y, mo, d)
    except Exception:
        return None


async def scan_day(day, label):
    start, end = tehran_day_bounds(day)
    refs = []
    for c in list(state.get("channels", [])):
        try:
            peer = await user.get_entity(c["peer"])
            async for m in user.iter_messages(peer, offset_date=end, limit=500):
                if m.date and m.date < start:
                    break
                if parse_post(m):
                    refs.append(ref_of(c["key"], m.id))
        except Exception as e:
            await notify(f"⚠️ کانال {esc(c['key'])} خوانده نشد: {type(e).__name__}")
    refs.sort(key=lambda r: ref_split(r)[1])
    await start_batch(label, refs)


def parse_channel_input(text):
    t = (text or "").strip()
    m = re.search(r"(?:t\.me|telegram\.me)/(?:joinchat/|\+)([\w-]+)", t)
    if m:
        return {"invite": m.group(1)}
    m = re.search(r"(?:t\.me|telegram\.me)/(?:s/)?([A-Za-z][\w]{3,})", t) or re.match(r"^@?([A-Za-z][\w]{3,})$", t)
    if m:
        return {"username": m.group(1)}
    return None


async def add_channel(text):
    p = parse_channel_input(text)
    if not p:
        return "❌ آدرس کانال نامعتبر است. مثل @blatants یا https://t.me/blatants بفرست."
    if p.get("invite"):
        try:
            res = await user(ImportChatInviteRequest(p["invite"]))
            ent = res.chats[0]
        except UserAlreadyParticipantError:
            info = await user(CheckChatInviteRequest(p["invite"]))
            ent = getattr(info, "chat", None)
            if ent is None:
                return "❌ لینک دعوت معتبر نیست."
        key, peer = f"c{ent.id}", utils.get_peer_id(ent)
    else:
        ent = await user.get_entity(p["username"])
        if not isinstance(ent, Channel):
            return "❌ این آدرس کانال نیست."
        try:
            await user(JoinChannelRequest(ent))
        except Exception:
            pass  # already joined / public read is enough
        key, peer = p["username"].lower(), p["username"]
    if key in channel_keys():
        return f"ℹ️ کانال {esc(key)} از قبل در فهرست است."
    latest = 0
    async for m in user.iter_messages(ent, limit=1):
        latest = m.id
    state["channels"].append({"key": key, "peer": peer, "title": getattr(ent, "title", key)})
    state.setdefault("last_ids", {})[key] = latest
    chan_ids[utils.get_peer_id(ent)] = key
    event(f"channel added {key}")
    save_state()
    return f"✅ کانال «{esc(getattr(ent, 'title', key))}» اضافه شد. از پست‌های بعدی بررسی می‌شود (برای قبلی‌ها از «چک یک روز» استفاده کن)."


def channels_text():
    chs = state.get("channels", [])
    if not chs:
        return "هیچ کانالی تنظیم نشده."
    return "<b>کانال‌های منبع</b>\n" + "\n".join(f"• {esc(c.get('title') or c['key'])} ({esc(c['key'])})" for c in chs)


def channels_buttons():
    rows = [[Button.inline(f"🗑 حذف {c['key']}"[:60], f"chdel:{c['key']}".encode())] for c in state.get("channels", [])]
    rows.append([Button.inline("➕ افزودن کانال", b"chadd")])
    return rows


async def relay_admin_file(ev):
    """Admin sent/forwarded an IPA to the admin bot: relay it to the user account, then the normal flow."""
    doc = ev.message.document
    fn = next((a.file_name for a in doc.attributes if getattr(a, "file_name", None)), "")
    if not fn.lower().endswith(".ipa"):
        await ev.respond("فقط فایل .ipa قبول می‌شود.")
        return
    cap = ev.message.message or ""
    fwd = ev.message.fwd_from
    if fwd and getattr(fwd, "from_name", None):
        cap = cap or fwd.from_name
    botent = await user.get_entity(BOT_USERNAME)
    last = 0
    async for m in user.iter_messages(botent, limit=1):
        last = m.id
    await bot.send_file(MY_USER_ID, file=ev.message.media, caption=cap[:1000])
    found = None
    for _ in range(20):
        await asyncio.sleep(2)
        async for m in user.iter_messages(botent, min_id=last, limit=10):
            if m.document and m.document.size == doc.size:
                found = m
                break
        if found:
            break
    if not found:
        await ev.respond("❌ فایل به حساب کاربر نرسید؛ دوباره بفرست.")
        return
    ref = ref_of("dm", found.id)
    await ev.respond(f"📥 دریافت شد: {esc(fn)}\nدر صف بررسی…", parse_mode="html")
    await start_batch(f"فایل ارسالی: {fn}", [ref], single=True)


async def cleanup_sweeper():
    """publish mode only: hourly strict cleanup of previous files / superseded draft imports (server re-checks)."""
    while True:
        await asyncio.sleep(3600)
        if not state.get("enabled"):
            continue  # the server only deletes when SIBSTAR_AUTO_IPA_ALLOW_PUBLISH=1 (else dry-run)
        try:
            ping = await sib({"action": "ping"}, tries=2)
            state["server"] = {"ok": bool(ping.get("ok")), "allowPublish": ping.get("allowPublish"), "t": now_iso()}
            r = await sib({"action": "cleanup", "dryRun": False, "limit": 20})
            done = [x for x in r.get("results", []) if x.get("deleted")]
            if done:
                mb = sum(int(x.get("bytes") or 0) for x in done) / 1e6
                event(f"cleanup deleted {len(done)} files ({mb:.0f} MB)")
                await notify(f"🧹 {len(done)} فایل قبلی/پیش‌نویس جایگزین‌شده حذف شد ({mb:.0f} مگابایت).")
        except Exception as e:
            log(f"cleanup failed: {type(e).__name__}")


# ---------------------------------------------------------------- drafts (v3)
busy = set()  # refs with a publish/discard in flight (double-tap guard)
PAGE = 5


def drafts():
    return sorted((v for v in state["items"].values() if v.get("status") == "draft_ok"), key=lambda v: v.get("updated") or "", reverse=True)


def source_fa(it):
    ch = it.get("chan") or ""
    return "فوروارد/ارسال مستقیم" if ch == "dm" else f"@{esc(ch)}"


def draft_line(i, it):
    plan = it.get("plan") or {}
    fr = (plan.get("from") or {}).get("version") or "?"
    f = plan.get("fields") or it.get("fields") or {}
    to = f.get("version") or it.get("version") or "?"
    sz = f.get("sizeBytes") or it.get("size") or 0
    slug = (it.get("app") or {}).get("slug")
    link = f'\n   🔗 <a href="{SITE}/app/{esc(slug)}">صفحهٔ برنامه</a>' if slug else ""
    note = f"\n   ℹ️ {esc((it.get('reason') or '')[:90])}" if it.get("reason") else ""
    return (f"<b>{i}. {item_title(it)}</b>\n   {esc(fr)} ← <b>{esc(to)}</b> · {sz / 1048576:.1f} MB\n"
            f"   منبع: {source_fa(it)} · {fa_time(it.get('created') or it.get('updated'))}{link}{note}")


def drafts_view(page=0):
    ds = drafts()
    if not ds:
        return "📝 پیش‌نویسی در انتظار نیست.", [[Button.inline("🏠 منو", b"status")]]
    pages = (len(ds) + PAGE - 1) // PAGE
    page = max(0, min(page, pages - 1))
    chunk = ds[page * PAGE:(page + 1) * PAGE]
    lines = [f"<b>📝 پیش‌نویس‌ها ({len(ds)})</b> — صفحه {page + 1}/{pages}", "سایت تا وقتی «انتشار» نزنی تغییری نمی‌کند.\n"]
    rows = []
    for n, it in enumerate(chunk, start=page * PAGE + 1):
        lines.append(draft_line(n, it))
        sid = sid_of(it["ref"])
        rows.append([Button.inline(f"✅ انتشار {n}", f"dp:{sid}".encode()), Button.inline(f"❌ رد {n}", f"dr:{sid}".encode())])
    nav = []
    if page > 0:
        nav.append(Button.inline("« قبلی", f"dl:{page - 1}".encode()))
    if page < pages - 1:
        nav.append(Button.inline("بعدی »", f"dl:{page + 1}".encode()))
    if nav:
        rows.append(nav)
    rows.append([Button.inline(f"🚀 انتشار همه ({len(ds)})", b"dpall"), Button.inline("🔄 تازه‌سازی", f"dl:{page}".encode())])
    rows.append([Button.inline("🏠 منو", b"status")])
    return "\n".join(lines)[:4000], rows


async def publish_draft(ref, quiet=False):
    """Explicit admin action: publish one existing draft now (any mode). Idempotent (busy set + server sameFile check)."""
    it = state["items"].get(ref)
    if not it:
        return "پیدا نشد"
    if it.get("status") == "published":
        return "already"
    if it.get("status") != "draft_ok":
        return "not_draft"
    if ref in busy or ref in inflight:
        return "busy"
    if not (it.get("job_id") and (it.get("app") or {}).get("id")):
        return "no_job"
    busy.add(ref)
    try:
        r = await sib({"action": "status", "jobId": it["job_id"], "appId": it["app"]["id"], "sourceUrl": it.get("url") or "",
                       "bundleId": it.get("bundle"), "mode": "publish", "force": bool(it.get("forced"))})
        oc = r.get("outcome")
        if oc == "published":
            it.update(status="published", fields=r.get("fields"), old_file=r.get("oldFile"), reason=None, published_at=now_iso(), mode="manual")
        elif oc in ("skipped", "rejected"):
            it.update(status="skipped" if oc == "skipped" else "rejected", code=r.get("code"), reason=r.get("reasonFa"))
        elif oc == "failed":
            it.update(status="failed", reason="فایل پیش‌نویس روی سرور نیست: " + str(r.get("reasonFa") or ""))
        elif oc == "publish_disabled":
            it["reason"] = "انتشار روی سرور با کلید اضطراری خاموش است"
        else:
            it["reason"] = r.get("reasonFa") or f"پاسخ نامعتبر ({oc})"
        it["updated"] = now_iso()
        event(f"{ref} manual publish -> {it.get('status')}")
        it["reported"] = hashlib.sha256(item_report(it).encode()).hexdigest()[:16]
        save_state()
        if not quiet:
            msg = item_report(it) if it["status"] != "draft_ok" else f"⚠️ <b>{item_title(it)}</b> منتشر نشد: {esc(it.get('reason'))}"
            if r.get("alreadyPublished"):
                msg = f"☑️ <b>{item_title(it)}</b> قبلاً منتشر شده بود؛ دوباره کاری انجام نشد." + app_link(it)
            await notify(msg, buttons=item_buttons(it))
        return it["status"]
    finally:
        busy.discard(ref)


async def discard_draft(ref):
    it = state["items"].get(ref)
    if not it or it.get("status") != "draft_ok":
        return "not_draft"
    if ref in busy or ref in inflight:
        return "busy"
    busy.add(ref)
    try:
        r = {"outcome": "discarded", "file": {"deleted": False, "reasonFa": "فایلی روی سرور ثبت نشده بود"}}
        if it.get("job_id") and (it.get("app") or {}).get("id"):
            r = await sib({"action": "discard", "jobId": it["job_id"], "appId": it["app"]["id"]})
        oc = r.get("outcome")
        if oc == "discarded":
            it.update(status="discarded", discard=r.get("file") or {}, updated=now_iso(), reason=None)
        elif oc == "already_published":
            it.update(status="published", reason=r.get("reasonFa"), updated=now_iso())
        else:
            await notify(f"⚠️ رد <b>{item_title(it)}</b> انجام نشد: {esc(r.get('reasonFa'))}")
            return "error"
        event(f"{ref} discarded by admin")
        save_state()
        await notify(item_report(it))
        return it["status"]
    finally:
        busy.discard(ref)


async def publish_all():
    refs = [d["ref"] for d in drafts()]
    res = {}
    for ref in refs:
        st = await publish_draft(ref, quiet=True)
        res.setdefault(st, []).append(state["items"].get(ref) or {})
    lab = {"published": "✅ منتشر شد", "skipped": "⚠️ منتشر نشد", "rejected": "⛔️ رد (بررسی)", "failed": "❌ خطا", "draft_ok": "📝 هنوز پیش‌نویس", "busy": "⏳ در حال کار"}
    lines = [f"<b>🚀 انتشار همه — {len(refs)} پیش‌نویس</b>"]
    for st, its in res.items():
        lines.append(f"\n{lab.get(st, st)}: {len(its)}")
        for it in its[:20]:
            old = it.get("old_file") or {}
            extra = (" — فایل قبلی حذف شد ✅" if old.get("deleted") else f" — فایل قبلی ماند: {esc((old.get('reasonFa') or '')[:60])}") if st == "published" else f" — {esc((it.get('reason') or '')[:70])}"
            lines.append(f"• {item_title(it)} {esc((it.get('fields') or {}).get('version') or it.get('version') or '')}{extra}")
    await notify("\n".join(lines)[:3900])


# ---------------------------------------------------------------- admin bot UI
def menu_buttons():
    on = state.get("enabled")
    draft = state.get("mode", "draft") == "draft"
    return [
        [Button.inline("🔴 خاموش کن" if on else "🟢 روشن کن", b"toggle"), Button.inline("📊 وضعیت", b"status")],
        [Button.inline(f"📝 پیش‌نویس‌ها ({len(drafts())})", b"dl:0")],
        [Button.inline("📝 حالت: پیش‌نویس (دستی منتشر می‌کنم)" if draft else "🚀 حالت: انتشار خودکار", b"mode")],
        [Button.inline("🔎 چک آپدیت‌های امروز", b"scan_today"), Button.inline("📆 چک آپدیت‌های یک روز", b"scan_day")],
        [Button.inline("📡 کانال‌ها", b"channels"), Button.inline("🕘 آخرین کارها", b"recent")],
        [Button.inline("📅 گزارش امروز", b"today"), Button.inline("🔁 تلاش دوباره خطاها", b"retryall")],
    ]


def status_text():
    on = "روشن 🟢" if state.get("enabled") else "خاموش 🔴"
    mode = "پیش‌نویس (دستی منتشر می‌کنم) 📝" if state.get("mode", "draft") == "draft" else "انتشار خودکار 🚀"
    if (state.get("server") or {}).get("allowPublish") is False:
        mode += "\n⛔️ کلید اضطراری سرور: انتشار خاموش است"
    cur = f"\n⏳ در حال کار: {esc((state['items'].get(current['ref'] or '') or {}).get('name'))} ({esc(current['stage'])})" if current["ref"] else ""
    chs = "، ".join(esc(c["key"]) for c in state.get("channels", [])) or "—"
    return f"<b>آپدیت خودکار سیب‌استار</b>\nوضعیت: {on}\nحالت: {mode}\nپیش‌نویس‌های منتظر: {len(drafts())}\nصف: {queue.qsize()}\nکانال‌ها: {chs}{cur}\n\nمی‌توانی فایل IPA را مستقیم بفرستی یا فوروارد کنی."


def today_text():
    today = dt.datetime.now(TEHRAN).date()
    counts = {}
    names = {}
    for it in state["items"].values():
        try:
            d = dt.datetime.fromisoformat(it.get("updated") or it.get("created")).astimezone(TEHRAN).date()
        except Exception:
            continue
        if d != today:
            continue
        st = it.get("status", "?")
        counts[st] = counts.get(st, 0) + 1
        names.setdefault(st, []).append(item_title(it))
    if not counts:
        return "امروز هنوز کاری انجام نشده."
    lab = {"published": "✅ منتشر شد", "draft_ok": "📝 پیش‌نویس", "skipped": "⚠️ رد شد", "rejected": "⛔️ رد (بررسی)", "failed": "❌ خطا", "queued": "⏳ در صف"}
    lines = [f"<b>گزارش امروز ({today.isoformat()})</b>"]
    for st, n in counts.items():
        lines.append(f"{lab.get(st, st)}: {n} — " + "، ".join(names[st][:8]))
    return "\n".join(lines)


def recent_text():
    items = sorted(state["items"].values(), key=lambda x: x.get("updated") or "", reverse=True)[:10]
    if not items:
        return "هنوز کاری ثبت نشده."
    ico = {"published": "✅", "draft_ok": "📝", "skipped": "⚠️", "rejected": "⛔️", "failed": "❌", "queued": "⏳"}
    return "<b>آخرین کارها</b>\n" + "\n".join(
        f"{ico.get(i.get('status'), '•')} {fa_time(i.get('updated'))} {item_title(i)} {esc(i.get('version') or '')}" + (f" — {esc((i.get('reason') or '')[:60])}" if i.get("status") in ("failed", "skipped", "rejected") else "")
        for i in items
    )


def is_admin(ev):
    s = ev.sender
    if admin_id():
        return ev.sender_id == admin_id()
    return bool(s and (getattr(s, "username", "") or "").lower() == ADMIN_USERNAME)


awaiting = {"kind": None}


def register_bot_handlers():
    @bot.on(events.NewMessage(incoming=True))
    async def on_msg(ev):
        if not ev.is_private:
            return
        await ev.get_sender()
        if not is_admin(ev):
            if (ev.message.message or "").startswith("/start"):
                await ev.respond("این ربات خصوصی است.")
            return
        if not state.get("admin_id"):
            state["admin_id"] = ev.sender_id
        state["admin_seen"] = now_iso()
        mark()
        text = (ev.message.message or "").strip()
        if ev.message.document:
            awaiting["kind"] = None
            try:
                await relay_admin_file(ev)
            except Exception as e:
                await ev.respond(f"❌ خطا در دریافت فایل: {type(e).__name__}")
            return
        if text.startswith("/start") or text.startswith("/menu"):
            awaiting["kind"] = None
            await ev.respond(status_text(), buttons=menu_buttons(), parse_mode="html")
            return
        kind = awaiting["kind"]
        if kind == "day":
            d = parse_day(text)
            if not d:
                await ev.respond("تاریخ را نفهمیدم. مثل ۱۴۰۵/۰۸/۲۱ یا 2026-11-12 یا «دیروز» بفرست.")
                return
            awaiting["kind"] = None
            await ev.respond(f"🔎 بررسی پست‌های {d.isoformat()} در همهٔ کانال‌ها…")
            await scan_day(d, f"چک آپدیت‌های روز {d.isoformat()}")
            return
        if kind == "addch":
            awaiting["kind"] = None
            try:
                msg = await add_channel(text)
            except Exception as e:
                msg = f"❌ اضافه نشد: {type(e).__name__}"
            await ev.respond(msg, parse_mode="html", buttons=channels_buttons())
            return
        await ev.respond(status_text(), buttons=menu_buttons(), parse_mode="html")

    @bot.on(events.CallbackQuery())
    async def on_cb(ev):
        await ev.get_sender()
        if not is_admin(ev):
            await ev.answer("دسترسی نداری", alert=True)
            return
        d = ev.data.decode()
        if d == "toggle":
            state["enabled"] = not state.get("enabled")
            event(f"enabled={state['enabled']}")
            await ev.answer("روشن شد" if state["enabled"] else "خاموش شد")
            await ev.edit(status_text(), buttons=menu_buttons(), parse_mode="html")
        elif d == "mode":
            if state.get("mode", "draft") == "draft":
                await ev.answer()
                await ev.respond(
                    "⚠️ با «انتشار خودکار» هر آپدیت تأییدشده (همان برنامه، Bundle ID درست، نسخهٔ جدیدتر) بدون بررسی تو روی سایت منتشر می‌شود و فایل قبلی با همان کنترل‌ها پاک می‌شود. مطمئنی؟",
                    buttons=[[Button.inline("بله، انتشار خودکار", b"mode_pub"), Button.inline("نه", b"noop")]],
                )
            else:
                state["mode"] = "draft"
                event("mode=draft")
                await ev.answer("حالت پیش‌نویس (دستی)")
                await ev.edit(status_text(), buttons=menu_buttons(), parse_mode="html")
        elif d == "mode_pub":
            state["mode"] = "publish"
            event("mode=publish")
            save_state()
            await ev.answer("انتشار خودکار روشن شد")
            warn = "\n⛔️ ولی کلید اضطراری سرور خاموش است؛ تا روشن نشود فقط پیش‌نویس ساخته می‌شود." if (state.get("server") or {}).get("allowPublish") is False else ""
            await ev.edit("🚀 انتشار خودکار روشن شد." + warn + "\n" + status_text(), buttons=menu_buttons(), parse_mode="html")
        elif d.startswith("dl:"):
            await ev.answer()
            text, rows = drafts_view(int(d[3:] or 0))
            try:
                await ev.edit(text, buttons=rows, parse_mode="html", link_preview=False)
            except Exception:
                await ev.respond(text, buttons=rows, parse_mode="html", link_preview=False)
        elif d == "dpall":
            n = len(drafts())
            await ev.answer()
            if not n:
                await ev.respond("پیش‌نویسی در انتظار نیست.")
            else:
                await ev.respond(f"⚠️ همهٔ {n} پیش‌نویس الان روی سایت منتشر شوند؟ (هر کدام با همان کنترل‌ها؛ فایل قبلی هر برنامه فقط اگر امن باشد پاک می‌شود)",
                                 buttons=[[Button.inline(f"بله، {n} مورد منتشر شود", b"dpall_yes"), Button.inline("نه", b"noop")]])
        elif d == "dpall_yes":
            if "publish_all" in busy:
                await ev.answer("همین الان در حال انتشار است", alert=True)
            else:
                busy.add("publish_all")
                await ev.answer("شروع شد…")
                await ev.edit("⏳ انتشار همهٔ پیش‌نویس‌ها شروع شد…")

                async def _all():
                    try:
                        await publish_all()
                    finally:
                        busy.discard("publish_all")
                asyncio.create_task(_all())
        elif d[:3] in ("dp:", "dr:", "re:", "rt:", "fc:"):
            kind, ref = d[:2], ref_by_sid(d[3:])
            it = state["items"].get(ref or "") or {}
            if not ref:
                await ev.answer("این مورد دیگر وجود ندارد", alert=True)
            elif kind == "dp":
                if it.get("status") == "published":
                    await ev.answer("قبلاً منتشر شده ✅", alert=True)
                elif it.get("status") != "draft_ok":
                    await ev.answer(f"دیگر پیش‌نویس نیست ({STATUS_FA.get(it.get('status'), it.get('status'))})", alert=True)
                elif ref in busy or ref in inflight:
                    await ev.answer("همین الان در حال انجام است…")
                else:
                    await ev.answer("در حال انتشار…")
                    asyncio.create_task(publish_draft(ref))
            elif kind == "dr":
                if it.get("status") != "draft_ok":
                    await ev.answer(f"دیگر پیش‌نویس نیست ({STATUS_FA.get(it.get('status'), it.get('status'))})", alert=True)
                elif ref in busy or ref in inflight:
                    await ev.answer("همین الان در حال انجام است…")
                else:
                    await ev.answer("رد شد")
                    asyncio.create_task(discard_draft(ref))
            elif kind == "re":
                await ev.answer("دوباره در صف بررسی" if enqueue(ref, manual=True) else "همین الان در صف است")
            elif kind == "rt":
                await ev.answer("دوباره در صف" if enqueue(ref, manual=True) else "همین الان در صف است")
            elif kind == "fc":
                await ev.answer("دوباره در صف (اجباری)" if enqueue(ref, manual=True, force=True) else "همین الان در صف است")
        elif d == "noop":
            awaiting["kind"] = None
            await ev.answer("لغو شد")
            await ev.delete()
        elif d == "status":
            await ev.answer()
            await ev.edit(status_text(), buttons=menu_buttons(), parse_mode="html")
        elif d == "today":
            await ev.answer()
            await ev.respond(today_text(), parse_mode="html")
        elif d == "recent":
            await ev.answer()
            await ev.respond(recent_text(), parse_mode="html")
        elif d == "scan_today":
            await ev.answer("در حال بررسی…")
            day = dt.datetime.now(TEHRAN).date()
            await ev.respond(f"🔎 بررسی پست‌های امروز ({day.isoformat()}) در همهٔ کانال‌ها…")
            await scan_day(day, f"چک آپدیت‌های امروز ({day.isoformat()})")
        elif d == "scan_day":
            awaiting["kind"] = "day"
            await ev.answer()
            await ev.respond("تاریخ را بفرست (شمسی مثل ۱۴۰۵/۰۸/۲۱ یا میلادی مثل 2026-11-12 یا «دیروز»):",
                             buttons=[[Button.inline("لغو", b"noop")]])
        elif d == "channels":
            await ev.answer()
            await ev.respond(channels_text(), buttons=channels_buttons(), parse_mode="html")
        elif d == "chadd":
            awaiting["kind"] = "addch"
            await ev.answer()
            await ev.respond("آیدی یا لینک کانال را بفرست (مثل @blatants یا https://t.me/blatants یا لینک دعوت):",
                             buttons=[[Button.inline("لغو", b"noop")]])
        elif d.startswith("chdel:"):
            key = d.split(":", 1)[1]
            state["channels"] = [c for c in state.get("channels", []) if c["key"] != key]
            for pid, k in list(chan_ids.items()):
                if k == key:
                    chan_ids.pop(pid, None)
            event(f"channel removed {key}")
            await ev.answer("حذف شد")
            await ev.edit(channels_text(), buttons=channels_buttons(), parse_mode="html")
        elif d == "retryall":
            refs = [k for k, v in state["items"].items() if v.get("status") == "failed"]
            for r in sorted(refs):
                enqueue(r, manual=True)
            await ev.answer(f"{len(refs)} مورد دوباره در صف")
        elif ":" in d:
            kind, ref = d.split(":", 1)
            if kind == "retry":
                await ev.answer("دوباره در صف" if enqueue(ref, manual=True) else "همین الان در صف است")
            elif kind == "force":
                await ev.answer("دوباره در صف (اجباری)" if enqueue(ref, manual=True, force=True) else "همین الان در صف است")
            elif kind == "pub":  # old notifications
                await ev.answer("در حال انتشار…")
                asyncio.create_task(publish_draft(ref))
            else:
                await ev.answer()
        mark()


# ---------------------------------------------------------------- channel watch
async def catch_up():
    last_ids = state.setdefault("last_ids", {})
    for c in list(state.get("channels", [])):
        key = c["key"]
        last = int(last_ids.get(key) or 0)
        latest, found = None, []
        try:
            peer = await user.get_entity(c["peer"])
            async for m in user.iter_messages(peer, limit=60, min_id=last):
                latest = max(latest or 0, m.id)
                if last and parse_post(m):
                    found.append(m.id)
        except Exception as e:
            log(f"catch-up {key} failed: {type(e).__name__}")
            continue
        if not last:
            last_ids[key] = latest or 0
            event(f"{key}: starting after message {latest}")
            continue
        for mid in sorted(found):
            if ref_of(key, mid) not in state["items"]:
                enqueue(ref_of(key, mid))
        if latest:
            last_ids[key] = max(last, latest)
    mark()


chan_ids = {}
BOT_USERNAME = ""
MY_USER_ID = 0


async def main():
    global queue, user, bot, BOT_USERNAME, MY_USER_ID
    load_state()
    queue = asyncio.Queue()
    user = TelegramClient(StringSession(SESSION), API_ID, API_HASH, proxy=PROXY, connection_retries=10, retry_delay=5)
    bot = TelegramClient(StringSession(os.environ.get("ADMIN_BOT_SESSION", "")), API_ID, API_HASH, proxy=PROXY, connection_retries=10, retry_delay=5)
    await user.connect()
    if not await user.is_user_authorized():
        log("user session not authorized")
        sys.exit(2)
    await bot.start(bot_token=BOT_TOKEN)
    me = await user.get_me()
    MY_USER_ID = me.id
    BOT_USERNAME = (await bot.get_me()).username
    log(f"user ok id={me.id}; bot ok")
    if not state.get("admin_id"):
        try:
            ent = await user.get_entity(ADMIN_USERNAME)
            state["admin_id"] = ent.id
        except Exception:
            pass
    if not state.get("relay_ready"):
        try:
            await user.send_message(BOT_USERNAME, "/start")
            state["relay_ready"] = True
        except Exception as e:
            log(f"relay start failed: {type(e).__name__}")
    await resolve_channel_ids()
    register_bot_handlers()

    @user.on(events.NewMessage(incoming=True))
    async def on_post(ev):
        key = chan_ids.get(ev.chat_id)
        if not key or not parse_post(ev.message):
            return
        ref = ref_of(key, ev.message.id)
        last_ids = state.setdefault("last_ids", {})
        last_ids[key] = max(int(last_ids.get(key) or 0), ev.message.id)
        mark()
        if ref not in state["items"] and state.get("enabled"):
            enqueue(ref)

    ping = await sib({"action": "ping"}, tries=2)
    state["server"] = {"ok": bool(ping.get("ok")), "allowPublish": ping.get("allowPublish"), "t": now_iso()}
    log(f"sibstar ping ok={ping.get('ok')}")
    for p in state.pop("pending", []) or []:
        ref = p.pop("ref", None) or (ref_of(DEFAULT_CHANNEL, p.pop("id")) if p.get("id") else None)
        p.pop("id", None)
        p.pop("batch", None)
        p.pop("test_publish", None)
        p.pop("mode", None)  # the mode is read when the item is processed
        if ref:
            enqueue(ref, **p)
    await catch_up()
    for r in TEST_REFS:
        enqueue(r, manual=True, force=True, mode=TEST_MODE)
    save_state()
    tasks = [asyncio.create_task(worker()), asyncio.create_task(state_saver()), asyncio.create_task(cleanup_sweeper())]
    if not ping.get("ok"):
        await notify("⚠️ ربات بالا آمد ولی به سایت وصل نشد: " + esc(ping.get("reasonFa")))
    async def run_test_actions():
        for a in TEST_ACTIONS:
            kind, ref = a.split(":", 1)
            t0 = time.time()
            while (ref in inflight or (state["items"].get(ref) or {}).get("status") in (None, "queued")) and time.time() - t0 < 45 * 60:
                await asyncio.sleep(10)  # a test item queued in the same run must finish first
            try:
                st = await (publish_draft(ref) if kind == "publish" else discard_draft(ref) if kind == "discard" else asyncio.sleep(0))
                log(f"test action {kind} -> {st}")
            except Exception as e:
                log(f"test action {kind} failed: {type(e).__name__}")
    if TEST_ACTIONS:
        tasks.append(asyncio.create_task(run_test_actions()))
    if not state.get("hello_v3"):
        n = len(drafts())
        ok = await notify(f"🆕 مدیریت پیش‌نویس‌ها اضافه شد: دکمهٔ «📝 پیش‌نویس‌ها ({n})» در منو — هر مورد «✅ انتشار» / «❌ رد»، و «انتشار همه».\n"
                          "حالت الان: پیش‌نویس (دستی منتشر می‌کنم). با دکمهٔ حالت می‌توانی انتشار خودکار را روشن کنی.", buttons=menu_buttons())
        state["hello_v3"] = bool(ok)
        state["hello_v2"] = True
        mark()
    if not state.get("hello_v2"):
        ok = await notify("🆕 امکانات تازه: چک آپدیت‌های امروز / یک روز، مدیریت چند کانال، و ارسال مستقیم فایل IPA به همین ربات.\n/start را بزن.", buttons=menu_buttons())
        state["hello_v2"] = bool(ok)
        state["hello_sent"] = True
        mark()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    try:
        await asyncio.wait_for(stop.wait(), timeout=RUN_SECONDS)
    except asyncio.TimeoutError:
        pass
    t0 = time.time()
    while current["ref"] and time.time() - t0 < 1200:
        await asyncio.sleep(5)
    pend = []
    while not queue.empty():
        ref, opts = queue.get_nowait()
        pend.append({"ref": ref, **{k: v for k, v in opts.items() if k not in ("batch", "test_publish")}})
    state["pending"] = pend
    for t in tasks:
        t.cancel()
    save_state()
    await user.disconnect()
    await bot.disconnect()
    log("bye")


if __name__ == "__main__":
    asyncio.run(main())
