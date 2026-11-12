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
FINAL_STATUSES = ("draft_ok", "published", "skipped", "rejected", "duplicate", "discarded", "needs_confirm")
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
RUN_STARTED = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

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
    n = 0
    while True:
        await asyncio.sleep(45)
        n += 1
        state["heartbeat"] = now_iso()  # in memory every 45s; pushed at least every ~3 min (runner watchdog reads it)
        if dirty or n % 4 == 0:
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
        m = await bot.send_message(aid, text, buttons=buttons, parse_mode="html", link_preview=False)
        if state.get("admin_reachable") is not True:
            state["admin_reachable"] = True
            mark()
        return m.id or True
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
    if st == "needs_confirm":
        rows = []
        for i, c in enumerate((it.get("candidates") or [])[:6]):
            nm = (c.get("nameFa") or c.get("name") or "?")[:28]
            rows.append([Button.inline(f"✅ همین: {nm} ({c.get('version') or '?'})", f"cf:{sid}:{i}".encode())])
        rows.append([Button.inline("❌ هیچ‌کدام", f"dr:{sid}".encode())])
        return rows
    if st == "skipped" and it.get("code") in RECHECK_CODES:
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
    if st == "needs_confirm":
        return (f"⚠️ <b>{esc(it.get('name'))}</b> {esc(it.get('version') or '')}: {esc(it.get('reason'))}\n"
                f"Bundle: <code>{esc(it.get('bundle'))}</code> · منبع: {esc(it.get('ref'))}\nکدام برنامهٔ سایت را آپدیت کنم؟ (فقط پیش‌نویس ساخته می‌شود)")
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
    recheck = bool(opts.get("rescan")) and it is not None and it.get("status") == "skipped" and it.get("code") in RECHECK_CODES
    if it is not None and not manual and not recheck and it.get("status") in FINAL_STATUSES:
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
    current.update(ref=ref, stage="plist")
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
    if mode != "publish" or opts.get("app_id") or it.get("confirmed"):
        mode = "draft"  # an admin-picked (fuzzy) match always becomes a draft first
    force = bool(opts.get("force"))
    current.update(ref=ref, stage="match")
    reuse = mode == "publish" and it.get("status") == "draft_ok" and it.get("job_id") and (it.get("app") or {}).get("id")
    mark()
    silent = False
    try:
        if reuse:
            url = it.get("url") or ""
            current["stage"] = "publishing"
            return await poll_job(it, url, post, mode)
        pick = opts.get("app_id")
        if pick:
            it.update(confirmed=True, picked=pick)
        conf = {"appId": it.get("picked"), "confirmed": True} if it.get("confirmed") and it.get("picked") else {}
        m = await sib({"action": "match", "name": post["name"], "bundleId": post["bundle"], "version": post["version"], "force": force, **conf})
        if m.get("outcome") == "confirm":
            it.update(status="needs_confirm", code="confirm", reason=m.get("reasonFa"), candidates=m.get("candidates"))
            return
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
        s = await sib({"action": "start", "name": post["name"], "bundleId": post["bundle"], "version": post["version"], "fileName": post["file"], "sourceUrl": url, "force": force, **conf})
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
        prog = opts.get("progress")
        if prog:
            progress_regs.pop(prog, None)
            final = report if it.get("status") != "duplicate" else f"♻️ {item_title(it)} {esc(it.get('version') or '')}: {esc(it.get('reason'))}"
            await edit_msg(prog, final, buttons=item_buttons(it))
            it["notif"] = prog
            mark()
        elif silent:
            pass
        elif not repeat:
            mid_ = await notify(report, buttons=item_buttons(it))
            if isinstance(mid_, int) and not isinstance(mid_, bool):
                it["notif"] = mid_
                mark()
        else:
            log(f"item {ref} same result as last report; not re-sent")


async def poll_job(it, url, post, mode):
    if True:
        deadline = time.time() + 90 * 60
        while time.time() < deadline:
            await asyncio.sleep(15)
            r = await sib({"action": "status", "jobId": it["job_id"], "appId": it["app"]["id"], "sourceUrl": url, "bundleId": post["bundle"], "mode": mode,
                           "force": bool(it.get("forced")), "confirmed": bool(it.get("confirmed"))})
            oc = r.get("outcome")
            if oc != "pending":
                current["stage"] = "verify"
            if oc == "pending":
                current["stage"] = f"import:{r.get('status') or ''}:{r.get('progress', 0)}"
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
            if current["ref"] == ref:
                current.update(ref=None, stage=None)
            prog = opts.get("progress")
            if prog and prog in progress_regs:  # process() returned early (already done / not found / duplicate)
                progress_regs.pop(prog, None)
                it = state["items"].get(ref) or {}
                txt = item_report(it) if it else "⚠️ پیام IPA پیدا نشد."
                if it.get("status") in FINAL_STATUSES and not opts.get("manual"):
                    txt = f"☑️ {item_title(it)} {esc(it.get('version') or '')}: قبلاً بررسی شده — {esc(STATUS_FA.get(it.get('status'), ''))}\n" + txt
                await edit_msg(prog, txt, buttons=item_buttons(it) if it else None)
            await check_batches()


inflight = set()
batches = {}


# ---------------------------------------------------------------- live progress (one message edited per action)
progress_regs = {}  # bot message id -> [fn() -> (text, buttons) | None, last text]


def watch(msg_id, fn):
    if msg_id:
        progress_regs[msg_id] = [fn, None]


async def progress_msg(text, reply_to=None):
    try:
        m = await bot.send_message(admin_id(), text, parse_mode="html", reply_to=reply_to, link_preview=False)
        return m.id
    except Exception as e:
        log(f"progress msg failed: {type(e).__name__}")
        return None


async def progress_ticker():
    while True:
        await asyncio.sleep(4)
        for mid, reg in list(progress_regs.items()):
            try:
                r = reg[0]()
            except Exception:
                r = None
            if r is None:
                continue
            text, btn = r
            if text != reg[1]:
                reg[1] = text
                await edit_msg(mid, text, buttons=btn)


IMPORT_FA = {"queued": "در صف سرور", "rc_pending": "در صف سرور", "running": "شروع دریافت", "connecting": "اتصال به لینک", "downloading": "دانلود فایل روی سایت",
             "verifying": "بررسی فایل", "extracting": "استخراج اطلاعات IPA", "storing": "ذخیره در فضای سایت", "finalizing": "نهایی‌سازی"}


def stage_fa(stage):
    st = stage or ""
    if st.startswith("import:"):
        _, s2, pct = (st.split(":") + ["", ""])[:3]
        return f"⬇️ {IMPORT_FA.get(s2, 'دریافت روی سایت')} {pct}%"
    return {"plist": "📖 خواندن Info.plist فایل از تلگرام", "match": "🔍 تطبیق با برنامه‌های سایت", "link": "🔗 ساخت لینک مستقیم (لینک‌یار)",
            "import": "⬆️ ارسال لینک به سایت", "verify": "🔬 بررسی Bundle ID و استخراج نسخه", "publishing": "🚀 انتشار"}.get(st, st or "…")


def item_progress(ref):
    it = state["items"].get(ref) or {}
    name = esc(it.get("name") or ref)
    if current["ref"] == ref:
        return f"⏳ <b>{name}</b> {esc(it.get('version') or '')}\nمرحله: {stage_fa(current['stage'])}", None
    if ref in inflight:
        return f"⏳ <b>{name}</b>: در صف بررسی ({queue.qsize()} کار در صف){' — الان: ' + esc((state['items'].get(current['ref']) or {}).get('name') or '') if current['ref'] else ''}", None
    return None


def batch_progress(bid):
    b = batches.get(bid)
    if not b:
        return None
    todo = [r for r in b["refs"] if r not in b["already"]]
    done = sum(1 for r in todo if r not in inflight)
    lines = [f"⏳ <b>{esc(b['label'])}</b>", f"پیدا شد: {len(b['refs'])} فایل · بررسی‌شده: {done}/{len(todo)} · از قبل: {len(b['already'])}"]
    if current["ref"] in todo:
        it = state["items"].get(current["ref"]) or {}
        lines.append(f"الان: {esc(it.get('name') or current['ref'])} {esc(it.get('version') or '')} — {stage_fa(current['stage'])}")
    return "\n".join(lines), None


def enqueue(ref, **opts):
    if ref in inflight:
        return False
    inflight.add(ref)
    queue.put_nowait((ref, opts))
    return True


SKIP_FA = {"not_found": "روی سایت نیست", "same_version": "همان نسخه", "older_version": "نسخهٔ قدیمی‌تر", "ambiguous": "چندتایی", "bundle_conflict": "Bundle فرق دارد",
           "pick_invalid": "انتخاب نامعتبر", "app_gone": "برنامه حذف شده", "bundle_mismatch": "Bundle فرق دارد", "confirm": "نیاز به انتخاب"}
async def start_item(ev, ref, label, **opts):
    """Button -> instant ack + one live progress message for this item (stages edited in place)."""
    it = state["items"].get(ref) or {}
    if ref in inflight:
        return await ev.answer("همین الان در صف/در حال بررسی است ⏳ (پیام وضعیتش بالاتر است)")
    await ev.answer(f"⏳ {label}")
    prog = await progress_msg(f"⏳ <b>{esc(it.get('name') or ref)}</b>: {esc(label)} — در صف…", reply_to=ev.message_id)
    if not enqueue(ref, progress=prog, **opts):
        return await edit_msg(prog, "⏳ همین الان در صف است.")
    watch(prog, lambda: item_progress(ref))
    try:
        await ev.edit(buttons=[[Button.inline(f"⏳ {label}…", b"noop_")]])
    except Exception:
        pass


STATUS_FA = {"published": "✅ آپدیت شد", "draft_ok": "📝 پیش‌نویس آماده", "skipped": "⚠️ رد شد", "rejected": "⛔️ رد (بررسی)",
             "failed": "❌ خطا", "duplicate": "♻️ تکراری", "queued": "⏳ در صف", "discarded": "🗑 پیش‌نویس رد شد", "needs_confirm": "⚠️ نیاز به انتخاب برنامه"}


async def start_batch(label, refs, single=False, progress=None, **opts):
    bid = f"b{int(time.time() * 1000)}"
    refs = list(dict.fromkeys(refs))
    def recheckable(it):
        return it.get("status") == "skipped" and it.get("code") in RECHECK_CODES
    already = {r for r in refs if (state["items"].get(r) or {}).get("status") in FINAL_STATUSES and not recheckable(state["items"][r])}
    batches[bid] = {"label": label, "refs": refs, "already": already, "single": single, "progress": progress, "t0": time.time()}
    for r in refs:
        if r not in already:
            enqueue(r, batch=bid, rescan=True, **({"progress": progress} if single and progress else {}), **opts)
    if progress and single:
        watch(progress, lambda: item_progress(refs[0]) if refs else None)
    elif progress:
        watch(progress, lambda: batch_progress(bid))
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
                txt = f"☑️ {item_title(it)} {esc(it.get('version') or '')}: قبلاً بررسی شده/تکراری — {esc(it.get('reason') or STATUS_FA.get(it.get('status'), ''))}{extra}"
                if b.get("progress"):
                    progress_regs.pop(b["progress"], None)
                    await edit_msg(b["progress"], txt, buttons=btns or None)
                else:
                    await notify(txt, buttons=btns or None)
            continue
        groups = {}
        for r in b["refs"]:
            st = (state["items"].get(r) or {}).get("status") or "failed"
            groups.setdefault(st, []).append(r)
        state["last_scan"] = {"label": b["label"], "t": now_iso(), "refs": b["refs"], "groups": groups, "already": sorted(b["already"])}
        mark()
        n_new = len(b["refs"]) - len(b["already"])
        lines = [f"<b>{esc(b['label'])}</b>", f"🔎 پیدا شد: {len(b['refs'])} فایل IPA (تازه بررسی‌شده: {n_new}، از قبل: {len(b['already'])})"]
        order = ["draft_ok", "needs_confirm", "published", "skipped", "rejected", "duplicate", "failed", "discarded", "queued"]
        btns = []
        for st in sorted(groups, key=lambda x: order.index(x) if x in order else 99):
            refs_ = groups[st]
            why = ""
            if st == "skipped":
                cnt = {}
                for r in refs_:
                    c = (state["items"].get(r) or {}).get("code") or "?"
                    cnt[c] = cnt.get(c, 0) + 1
                why = " (" + "، ".join(f"{SKIP_FA.get(c, c)}: {n}" for c, n in cnt.items()) + ")"
            lines.append(f"{STATUS_FA.get(st, st)}: {len(refs_)}{why}")
            btns.append(Button.inline(f"📋 {STATUS_FA.get(st, st)} ({len(refs_)})", f"sl:{st}".encode()))
        if not b["refs"]:
            lines.append("هیچ IPA در این بازه پیدا نشد.")
        rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
        if groups.get("draft_ok"):
            rows.append([Button.inline(f"📝 پیش‌نویس‌ها ({len(drafts())})", b"dl:0")])
        if b.get("progress"):
            progress_regs.pop(b["progress"], None)
            await edit_msg(b["progress"], ("✅ تمام شد — " + "\n".join(lines))[:3900], buttons=rows or None)
        else:
            await notify("\n".join(lines)[:3900], buttons=rows or None)


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


def gregorian_to_jalali(gy, gm, gd):
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    gy2 = gy + 1 if gm > 2 else gy
    days = 355666 + (365 * gy) + ((gy2 + 3) // 4) - ((gy2 + 99) // 100) + ((gy2 + 399) // 400) + gd + g_d_m[gm - 1]
    jy = -1595 + (33 * (days // 12053))
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365
    if days < 186:
        jm, jd = 1 + days // 31, 1 + days % 31
    else:
        jm, jd = 7 + (days - 186) // 30, 1 + (days - 186) % 30
    return jy, jm, jd


WEEKDAY_FA = ["دوشنبه", "سه‌شنبه", "چهارشنبه", "پنجشنبه", "جمعه", "شنبه", "یکشنبه"]
TO_FA = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")


def day_label(d):
    jy, jm, jd = gregorian_to_jalali(d.year, d.month, d.day)
    return f"{WEEKDAY_FA[d.weekday()]} {jy}/{jm:02d}/{jd:02d}".translate(TO_FA)


def day_buttons():
    today = dt.datetime.now(TEHRAN).date()
    rows = []
    for i in range(5):
        d = today - dt.timedelta(days=i)
        pre = "امروز — " if i == 0 else "دیروز — " if i == 1 else ""
        rows.append([Button.inline(pre + day_label(d), f"sd:{d.isoformat()}".encode())])
    rows.append([Button.inline("📅 تاریخ دیگر (دستی)", b"sd_manual"), Button.inline("لغو", b"noop")])
    return rows


async def start_batch_notice(ms):
    await notify(f"⏳ ساخت پیش‌نویس برای {len(ms)} مورد جامانده شروع شد؛ هر کدام آماده شد همین‌جا با دکمهٔ «✅ انتشار / ❌ رد» می‌آید.")


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


def scan_list_text(st):
    ls = state.get("last_scan") or {}
    refs = (ls.get("groups") or {}).get(st) or []
    lines = [f"<b>{esc(ls.get('label'))}</b> — {STATUS_FA.get(st, st)} ({len(refs)})"]
    for r in refs[:40]:
        it = state["items"].get(r) or {}
        why = "" if st in ("published", "draft_ok") else f" — {esc((it.get('reason') or SKIP_FA.get(it.get('code'), '') or '')[:80])}"
        lines.append(f"• {item_title(it)} {esc(it.get('version') or '')} <i>({esc(r)})</i>{why}")
    return "\n".join(lines)[:4000]


async def scan_day(day, label, progress=None):
    state["last_check"] = now_iso()
    start, end = tehran_day_bounds(day)
    refs = []
    chans = list(state.get("channels", []))
    for ci, c in enumerate(chans, 1):
        await edit_msg(progress, f"⏳ <b>{esc(label)}</b>\n📡 خواندن پست‌های کانال {esc(c['key'])} ({ci}/{len(chans)})… پیدا شده تا حالا: {len(refs)}")
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
    await start_batch(label, refs, progress=progress)


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
    prog = await progress_msg(f"📥 دریافت فایل {esc(fn)}…", reply_to=ev.message.id)
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
        await edit_msg(prog, "❌ فایل به حساب کاربر نرسید؛ دوباره بفرست.")
        return
    ref = ref_of("dm", found.id)
    await edit_msg(prog, f"📥 دریافت شد: {esc(fn)}\n⏳ در صف بررسی…")
    await start_batch(f"فایل ارسالی: {fn}", [ref], single=True, progress=prog)


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


# ---------------------------------------------------------------- drafts / admin actions (v4)
busy = set()  # refs with a publish/discard running right now (double-tap guard)
PAGE = 5
stopping = {"v": False}
ACT_LABEL = {"queued": "⏳ در صف", "publishing": "🚀 در حال انتشار…", "deleting": "🧹 پاک کردن فایل قبلی…", "discarding": "🗑 در حال رد…"}


def drafts():
    return sorted((v for v in state["items"].values() if v.get("status") == "draft_ok"), key=lambda v: v.get("updated") or "", reverse=True)


def confirms():
    return sorted((v for v in state["items"].values() if v.get("status") == "needs_confirm"), key=lambda v: v.get("updated") or "", reverse=True)


def source_fa(it):
    ch = it.get("chan") or ""
    return "فوروارد/ارسال مستقیم" if ch == "dm" else f"@{esc(ch)}"


def pending_action(ref):
    for a in state.get("actions", []):
        if a.get("ref") == ref:
            return a
    return None


def draft_state_fa(it):
    a = pending_action(it.get("ref"))
    if a:
        return ACT_LABEL.get(a.get("stage") or "queued", "⏳ در صف") + (" (انتشار)" if a["kind"] == "publish" and not a.get("stage") else "")
    if it.get("reason"):
        return "⚠️ " + esc((it.get("reason") or "")[:60])
    return "📝 منتظر تأیید تو"


def draft_line(i, it):
    plan = it.get("plan") or {}
    fr = (plan.get("from") or {}).get("version") or "?"
    f = plan.get("fields") or it.get("fields") or {}
    to = f.get("version") or it.get("version") or "?"
    sz = f.get("sizeBytes") or it.get("size") or 0
    slug = (it.get("app") or {}).get("slug")
    link = f'\n   🔗 <a href="{SITE}/app/{esc(slug)}">صفحهٔ برنامه</a>' if slug else ""
    return (f"<b>{i}. {item_title(it)}</b>\n   {esc(fr)} ← <b>{esc(to)}</b> · {sz / 1048576:.1f} MB\n"
            f"   منبع: {source_fa(it)} · {fa_time(it.get('created') or it.get('updated'))}\n   وضعیت: {draft_state_fa(it)}{link}")


def drafts_view(page=0):
    ds = drafts()
    cs = confirms()
    if not ds and not cs:
        return "📝 پیش‌نویسی در انتظار نیست.", [[Button.inline("🔎 موارد جامانده", b"miss"), Button.inline("🏠 منو", b"status")]]
    pages = max(1, (len(ds) + PAGE - 1) // PAGE)
    page = max(0, min(page, pages - 1))
    chunk = ds[page * PAGE:(page + 1) * PAGE]
    lines = [f"<b>📝 پیش‌نویس‌ها ({len(ds)})</b> — صفحه {page + 1}/{pages}", "سایت تا وقتی «انتشار» نزنی تغییری نمی‌کند.\n"]
    rows = []
    for n, it in enumerate(chunk, start=page * PAGE + 1):
        lines.append(draft_line(n, it))
        sid = sid_of(it["ref"])
        if pending_action(it["ref"]) or it["ref"] in busy:
            rows.append([Button.inline(f"{n}: {ACT_LABEL.get((pending_action(it['ref']) or {}).get('stage') or 'queued')}", b"noop_")])
        else:
            rows.append([Button.inline(f"✅ انتشار {n}", f"dp:{sid}".encode()), Button.inline(f"❌ رد {n}", f"dr:{sid}".encode())])
    if cs and page == 0:
        lines.append(f"\n<b>⚠️ نیاز به انتخاب برنامه ({len(cs)})</b>")
        for it in cs[:5]:
            lines.append(f"• {esc(it.get('name'))} {esc(it.get('version') or '')} — {esc((it.get('reason') or '')[:70])}")
            rows.append([Button.inline(f"⚠️ انتخاب برای {(it.get('name') or '')[:22]}", f"cs:{sid_of(it['ref'])}".encode())])
    nav = []
    if page > 0:
        nav.append(Button.inline("« قبلی", f"dl:{page - 1}".encode()))
    if page < pages - 1:
        nav.append(Button.inline("بعدی »", f"dl:{page + 1}".encode()))
    if nav:
        rows.append(nav)
    if ds:
        rows.append([Button.inline(f"🚀 انتشار همه ({len(ds)})", b"dpall"), Button.inline("🔄 تازه‌سازی", f"dl:{page}".encode())])
    rows.append([Button.inline("🔎 موارد جامانده", b"miss"), Button.inline("🏠 منو", b"status")])
    return "\n".join(lines)[:4000], rows


async def edit_msg(msg_id, text=None, buttons=None):
    """Edit one of the bot's own messages in the admin chat (progress / draft card). Never raises."""
    if not msg_id or not admin_id():
        return
    try:
        if text is None:
            await bot.edit_message(admin_id(), msg_id, buttons=buttons)
        else:
            await bot.edit_message(admin_id(), msg_id, text, buttons=buttons, parse_mode="html", link_preview=False)
    except Exception as e:
        if "not modified" not in str(e).lower():
            log(f"edit failed: {type(e).__name__}")


def final_buttons(it):
    slug = (it.get("app") or {}).get("slug")
    st = it.get("status")
    lab = {"published": "✅ منتشر شد", "discarded": "🗑 رد شد", "skipped": "⚠️ منتشر نشد", "rejected": "⛔️ منتشر نشد", "failed": "❌ خطا"}.get(st)
    rows = []
    if lab:
        rows.append([Button.inline(lab, b"noop_")])
    if slug and st == "published":
        rows.append([Button.url("🔗 صفحهٔ برنامه", f"{SITE}/app/{slug}")])
    if st == "draft_ok":
        return item_buttons(it)
    return rows or None


async def queue_action(kind, ref, progress_id=None, card_id=None, quiet=False):
    """Persisted admin action (survives a restart): publish / discard. Returns False if already queued."""
    if pending_action(ref) or ref in busy:
        return False
    state.setdefault("actions", []).append({"kind": kind, "ref": ref, "t": now_iso(), "progress": progress_id, "card": card_id, "stage": None, "quiet": quiet})
    save_state()
    action_event.set()
    return True


action_event = asyncio.Event()


async def action_worker():
    while True:
        await action_event.wait()
        action_event.clear()
        while state.get("actions") and not stopping["v"]:
            a = state["actions"][0]
            try:
                if a["kind"] == "publish":
                    await publish_draft(a)
                elif a["kind"] == "discard":
                    await discard_draft(a)
            except Exception as e:
                log(f"action {a.get('kind')} failed: {type(e).__name__}")
                traceback.print_exc(limit=2)
                await edit_msg(a.get("progress"), f"❌ خطای داخلی ربات ({type(e).__name__}) — دوباره بزن.")
            finally:
                if state.get("actions") and state["actions"][0] is a:
                    state["actions"].pop(0)
                save_state()


async def set_stage(a, stage, text):
    a["stage"] = stage
    mark()
    await edit_msg(a.get("progress"), text)


async def publish_draft(a):
    """Explicit admin action: publish one existing draft now (any mode). Idempotent (busy set + server sameFile check)."""
    ref = a["ref"]
    it = state["items"].get(ref)
    t = item_title(it or {})
    if not it:
        return await edit_msg(a.get("progress"), "❌ این مورد پیدا نشد.")
    if it.get("status") == "published":
        await edit_msg(a.get("progress"), f"☑️ <b>{t}</b> قبلاً منتشر شده بود." + app_link(it))
        return await edit_msg(a.get("card"), buttons=final_buttons(it))
    if it.get("status") != "draft_ok" or not (it.get("job_id") and (it.get("app") or {}).get("id")):
        return await edit_msg(a.get("progress"), f"⚠️ <b>{t}</b> دیگر پیش‌نویس قابل انتشار نیست ({esc(STATUS_FA.get(it.get('status'), it.get('status')))}).")
    busy.add(ref)
    try:
        fr = ((it.get("plan") or {}).get("from") or {}).get("version") or "?"
        to = ((it.get("plan") or {}).get("fields") or {}).get("version") or it.get("version")
        await set_stage(a, "publishing", f"🚀 <b>{t}</b>\n{esc(fr)} ← {esc(to)}\nدر حال انتشار روی سایت…")
        r = await sib({"action": "status", "jobId": it["job_id"], "appId": it["app"]["id"], "sourceUrl": it.get("url") or "",
                       "bundleId": it.get("bundle"), "mode": "publish", "force": bool(it.get("forced")),
                       "confirmed": bool(it.get("confirmed")), "deferDelete": True})
        oc = r.get("outcome")
        if oc == "published":
            old = r.get("oldFile") or {}
            if r.get("alreadyPublished"):
                it.update(status="published", reason=None)
            else:
                it.update(status="published", fields=r.get("fields"), old_file=old, reason=None, published_at=now_iso(), mode="manual")
                save_state()
                if old.get("code") == "deferred" and old.get("objectKey"):
                    await set_stage(a, "deleting", f"🚀 <b>{t}</b> منتشر شد ✅\n{esc(fr)} ← {esc(to)}\n🧹 پاک کردن فایل قبلی…")
                    newkey = ((it.get("plan") or {}).get("newFile") or {}).get("objectKey")
                    d = await sib({"action": "deletePrev", "appId": it["app"]["id"], "key": old["objectKey"], "newKey": newkey}) if newkey else None
                    if d is None:
                        d = {"deleted": False, "reasonFa": "کلید فایل جدید معلوم نیست؛ پاک‌سازی ساعتی بررسی می‌کند"}
                    it["old_file"] = {"deleted": bool(d.get("deleted")), "code": d.get("code"), "reasonFa": d.get("reasonFa"), "objectKey": old.get("objectKey"), "bytes": d.get("bytes")}
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
        if it["status"] == "published":
            old = it.get("old_file") or {}
            gone = "بله ✅" if old.get("deleted") else "نه"
            why = "" if old.get("deleted") else f" — {esc(old.get('reasonFa') or '')}"
            msg = (f"✅ <b>{t}</b> منتشر شد\n{esc(fr)} ← <b>{esc((it.get('fields') or {}).get('version') or to)}</b>\n"
                   f"🗑 فایل قبلی حذف شد: {gone}{why}" + app_link(it))
            if r.get("alreadyPublished"):
                msg = f"☑️ <b>{t}</b> قبلاً منتشر شده بود؛ دوباره کاری انجام نشد." + app_link(it)
        elif it["status"] == "draft_ok":
            msg = f"❌ <b>{t}</b> منتشر نشد: {esc(it.get('reason'))}\nپیش‌نویس سر جایش ماند."
        else:
            msg = f"❌ <b>{t}</b> منتشر نشد: {esc(it.get('reason'))}"
        if a.get("progress"):
            await edit_msg(a["progress"], msg, buttons=final_buttons(it) if it["status"] != "draft_ok" else item_buttons(it))
        elif not a.get("quiet"):
            await notify(msg, buttons=final_buttons(it))
        await edit_msg(a.get("card"), buttons=final_buttons(it))
        if it.get("notif") and it.get("notif") != a.get("card"):
            await edit_msg(it["notif"], buttons=final_buttons(it))
        if a.get("late"):
            await notify(f"🔄 درخواست انتشار «{t}» که موقع ری‌استارت ربات زده بودی انجام شد.")
        await refresh_lists()
        return it["status"]
    finally:
        busy.discard(ref)


async def discard_draft(a):
    ref = a["ref"]
    it = state["items"].get(ref)
    t = item_title(it or {})
    if not it or it.get("status") not in ("draft_ok", "needs_confirm"):
        return await edit_msg(a.get("progress"), f"⚠️ <b>{t}</b> دیگر پیش‌نویس نیست.")
    busy.add(ref)
    try:
        await set_stage(a, "discarding", f"🗑 <b>{t}</b>: در حال رد پیش‌نویس…")
        r = {"outcome": "discarded", "file": {"deleted": False, "reasonFa": "فایلی روی سرور ثبت نشده بود"}}
        if it.get("job_id") and (it.get("app") or {}).get("id") and it.get("status") == "draft_ok":
            r = await sib({"action": "discard", "jobId": it["job_id"], "appId": it["app"]["id"]})
        oc = r.get("outcome")
        if oc == "discarded":
            it.update(status="discarded", discard=r.get("file") or {}, updated=now_iso(), reason=None)
        elif oc == "already_published":
            it.update(status="published", reason=r.get("reasonFa"), updated=now_iso())
        else:
            await edit_msg(a.get("progress"), f"❌ رد <b>{t}</b> انجام نشد: {esc(r.get('reasonFa'))}", buttons=item_buttons(it))
            return "error"
        event(f"{ref} discarded by admin")
        save_state()
        await edit_msg(a.get("progress"), item_report(it), buttons=final_buttons(it))
        await edit_msg(a.get("card"), buttons=final_buttons(it))
        if it.get("notif") and it.get("notif") != a.get("card"):
            await edit_msg(it["notif"], buttons=final_buttons(it))
        await refresh_lists()
        return it["status"]
    finally:
        busy.discard(ref)


async def request_action(ev, kind, ref):
    """Button press -> instant answer, card buttons show the state, one live progress message."""
    it = state["items"].get(ref) or {}
    t = item_title(it)
    if kind == "publish" and it.get("status") == "published":
        return await ev.answer("قبلاً منتشر شده ✅", alert=True)
    if it.get("status") not in ("draft_ok",) + (("needs_confirm",) if kind == "discard" else ()):
        return await ev.answer(f"دیگر پیش‌نویس نیست ({STATUS_FA.get(it.get('status'), it.get('status'))})", alert=True)
    if pending_action(ref) or ref in busy:
        return await ev.answer("همین الان در صف/در حال انجام است ⏳")
    await ev.answer("⏳ در صف انتشار" if kind == "publish" else "⏳ در صف رد")
    card = ev.message_id
    prog = None
    try:
        m = await bot.send_message(admin_id(), f"⏳ <b>{t}</b>: در صف {'انتشار' if kind == 'publish' else 'رد'}…", parse_mode="html", reply_to=card)
        prog = m.id
    except Exception:
        pass
    await queue_action(kind, ref, prog, card)
    try:
        if card in list_msgs:
            text, rows = drafts_view(list_msgs[card])
            await ev.edit(text, buttons=rows, parse_mode="html", link_preview=False)
        else:
            await ev.edit(buttons=[[Button.inline(ACT_LABEL["queued"], b"noop_")]])
    except Exception:
        pass


list_msgs = {}  # message id of a shown drafts list -> page (re-rendered when an item changes)


async def refresh_lists():
    for mid, page in list(list_msgs.items())[-3:]:
        text, rows = drafts_view(page)
        await edit_msg(mid, text, buttons=rows)


async def publish_all(progress=None):
    refs = [d["ref"] for d in drafts()]
    for ref in refs:
        await queue_action("publish", ref, quiet=True)

    def prog_fn():
        left = [r for r in refs if pending_action(r)]
        if not left:
            return None
        cur = state["actions"][0] if state.get("actions") else {}
        it = state["items"].get(cur.get("ref")) or {}
        return (f"⏳ <b>انتشار همه</b>: {len(refs) - len(left)}/{len(refs)} انجام شد\n"
                f"الان: {item_title(it)} — {ACT_LABEL.get(cur.get('stage') or 'queued', '')}"), None
    watch(progress, prog_fn)
    # wait for the queued publishes, then one summary
    t0 = time.time()
    while any(pending_action(r) for r in refs) and time.time() - t0 < 3600:
        await asyncio.sleep(3)
    res = {}
    for ref in refs:
        it = state["items"].get(ref) or {}
        res.setdefault(it.get("status"), []).append(it)
    lab = {"published": "✅ منتشر شد", "skipped": "⚠️ منتشر نشد", "rejected": "⛔️ رد (بررسی)", "failed": "❌ خطا", "draft_ok": "📝 هنوز پیش‌نویس"}
    lines = [f"<b>🚀 انتشار همه — {len(refs)} پیش‌نویس</b>"]
    for st, its in res.items():
        lines.append(f"\n{lab.get(st, st)}: {len(its)}")
        for it in its[:20]:
            old = it.get("old_file") or {}
            extra = (" — فایل قبلی حذف شد ✅" if old.get("deleted") else f" — فایل قبلی ماند: {esc((old.get('reasonFa') or '')[:60])}") if st == "published" else f" — {esc((it.get('reason') or '')[:70])}"
            lines.append(f"• {item_title(it)} {esc((it.get('fields') or {}).get('version') or it.get('version') or '')}{extra}")
    progress_regs.pop(progress, None)
    if progress:
        await edit_msg(progress, ("✅ " + "\n".join(lines))[:3900])
    else:
        await notify("\n".join(lines)[:3900])


# ---------------------------------------------------------------- coverage audit («موارد جامانده»)
AUDIT_HOURS = 48
RECHECK_CODES = ("not_found", "ambiguous", "bundle_conflict", "pick_invalid")


async def coverage_audit(hours=AUDIT_HOURS, notify_result=True, progress=None):
    """Every IPA post of every channel in the window + files sent to the bot: what did the bot do?
    Never-seen / re-checkable posts are dry-run matched (read-only) to find real misses."""
    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)
    rows = []
    for c in list(state.get("channels", [])):
        await edit_msg(progress, f"⏳ <b>بررسی پوشش {hours} ساعت اخیر</b>\n📡 خواندن کانال {esc(c['key'])}…")
        async def read_channel():
            peer = await user.get_entity(c["peer"])
            async for m in user.iter_messages(peer, limit=600):
                if m.date and m.date < since:
                    break
                p = parse_post(m)
                if p:
                    rows.append((ref_of(c["key"], m.id), m, p))
        try:
            trace(f"audit: reading {c['key']}")
            await asyncio.wait_for(read_channel(), 180)
        except Exception as e:
            await notify(f"⚠️ کانال {esc(c['key'])} خوانده نشد: {type(e).__name__}")
    trace(f"audit: {len(rows)} IPA posts in the last {hours}h")
    out = []
    for i, (ref, m, p) in enumerate(rows, 1):
        state["audit_progress"] = {"t": now_iso(), "i": i, "n": len(rows)}
        if i % 10 == 0:
            mark()
        if i == 1 or i % 3 == 0:
            await edit_msg(progress, f"⏳ <b>بررسی پوشش {hours} ساعت اخیر</b>\n🔍 مقایسه با سایت: {i}/{len(rows)} — {esc(p.get('name'))}")
        it = state["items"].get(ref)
        r = {"ref": ref, "name": p.get("name"), "version": p.get("version"), "bundle": p.get("bundle"), "posted": p.get("posted"),
             "bot": (it or {}).get("status") or "never_seen", "code": (it or {}).get("code"), "reason": (it or {}).get("reason")}
        if it and it.get("version"):
            r["version"], r["bundle"] = it.get("version"), it.get("bundle") or r["bundle"]
        recheck = it is None or it.get("status") in ("failed", "queued") or (it.get("status") == "skipped" and it.get("code") in RECHECK_CODES)
        if recheck:
            if it is None:
                try:
                    info = await asyncio.wait_for(ipa_info(m), 60)
                    r["bundle"] = info.get("bundle") or r["bundle"]
                    r["version"] = info.get("version") or r["version"]
                except Exception as e:
                    log(f"audit: plist {ref} skipped ({type(e).__name__})")
            try:
                mm = await asyncio.wait_for(sib({"action": "match", "name": r["name"], "bundleId": r["bundle"], "version": r["version"]}, tries=2), 90)
            except Exception as e:
                log(f"audit: match {ref} failed ({type(e).__name__})")
                mm = {"outcome": "error"}
            oc = mm.get("outcome")
            app = mm.get("app") or {}
            r["site"] = {"name": app.get("nameFa") or app.get("name"), "version": app.get("version"), "slug": app.get("slug")}
            r["match"] = oc if oc != "skipped" else mm.get("code")
            r["missed"] = oc in ("match", "confirm") and not (it and it.get("status") == "queued" and ref in inflight)
            if oc == "confirm":
                r["candidates"] = mm.get("candidates")
        out.append(r)
    for k, it in state["items"].items():
        if k.startswith("dm/") and (it.get("created") or "") >= since.isoformat(timespec="seconds"):
            out.append({"ref": k, "name": it.get("name"), "version": it.get("version"), "bundle": it.get("bundle"), "posted": it.get("created"),
                        "bot": it.get("status"), "code": it.get("code"), "reason": it.get("reason"), "dm": True})
    state["audit"] = {"t": now_iso(), "hours": hours, "rows": out}
    state.pop("audit_progress", None)
    trace(f"audit done: {len(out)} rows, missed {sum(1 for r in out if r.get('missed'))}")
    mark()
    save_state()
    if notify_result:
        text, rows_b = missed_view()
        if progress:
            await edit_msg(progress, "✅ " + audit_summary() + "\n\n" + text, buttons=rows_b)
        else:
            await notify(audit_summary() + "\n\n" + text, buttons=rows_b)
    return out


def trace(msg):
    """Short persisted trace (encrypted state) so a stalled step is visible from outside the runner."""
    log(msg)
    tl = state.setdefault("trace", [])
    tl.append(f"{now_iso()} {msg[:160]}")
    state["trace"] = tl[-60:]
    mark()


def audit_summary():
    a = state.get("audit") or {}
    rows = a.get("rows") or []
    cnt = {}
    for r in rows:
        k = "missed" if r.get("missed") else r.get("bot")
        cnt[k] = cnt.get(k, 0) + 1
    lab = dict(STATUS_FA, never_seen="👀 دیده نشده", missed="🔎 جامانده (روی سایت هست و نسخه جدیدتر است)", needs_confirm="⚠️ نیاز به انتخاب")
    lines = [f"<b>📋 پوشش {a.get('hours', AUDIT_HOURS)} ساعت اخیر</b> ({fa_time(a.get('t'))}) — {len(rows)} فایل"]
    for k, n in sorted(cnt.items(), key=lambda kv: -kv[1]):
        lines.append(f"{lab.get(k, k)}: {n}")
    return "\n".join(lines)


def missed_rows():
    return [r for r in (state.get("audit") or {}).get("rows", []) if r.get("missed") and (state["items"].get(r["ref"]) or {}).get("status") not in ("draft_ok", "published", "needs_confirm")]


def missed_view():
    ms = missed_rows()
    if not ms:
        return "✅ مورد جامانده‌ای نیست.", [[Button.inline("🔄 بررسی دوباره", b"audit"), Button.inline("🏠 منو", b"status")]]
    lines = [f"<b>🔎 موارد جامانده ({len(ms)})</b> — برنامه روی سایت هست و نسخهٔ کانال جدیدتر است، ولی پیش‌نویس ساخته نشده:"]
    rows = []
    for i, r in enumerate(ms[:15], 1):
        site = r.get("site") or {}
        conf = " ⚠️ نیاز به انتخاب برنامه" if r.get("match") == "confirm" else ""
        lines.append(f"{i}. <b>{esc(r.get('name'))}</b> {esc(r.get('version') or '?')} (سایت: {esc(site.get('version') or '?')}) · {esc(r['ref'])}{conf}")
        rows.append([Button.inline(f"➕ ساخت پیش‌نویس {i}", f"mk:{sid_of(r['ref'])}".encode())])
    rows.append([Button.inline(f"➕ ساخت همه ({len(ms)})", b"mkall"), Button.inline("🔄 بررسی دوباره", b"audit")])
    rows.append([Button.inline("🏠 منو", b"status")])
    return "\n".join(lines)[:4000], rows


def ref_by_sid_any(sid):
    r = ref_by_sid(sid)
    if r:
        return r
    for row in (state.get("audit") or {}).get("rows", []):
        if sid_of(row["ref"]) == sid:
            return row["ref"]
    for b in (state.get("last_scan") or {}).get("refs", []):
        if sid_of(b) == sid:
            return b
    return None


# ---------------------------------------------------------------- admin bot UI
def menu_buttons():
    on = state.get("enabled")
    draft = state.get("mode", "draft") == "draft"
    return [
        [Button.inline("🔴 خاموش کن" if on else "🟢 روشن کن", b"toggle"), Button.inline("📊 وضعیت", b"status")],
        [Button.inline(f"📝 پیش‌نویس‌ها ({len(drafts())}{' + ⚠️' + str(len(confirms())) if confirms() else ''})", b"dl:0"), Button.inline("🔎 موارد جامانده", b"miss")],
        [Button.inline("📝 حالت: پیش‌نویس (دستی منتشر می‌کنم)" if draft else "🚀 حالت: انتشار خودکار", b"mode")],
        [Button.inline("🔎 چک آپدیت‌های امروز", b"scan_today"), Button.inline("📆 چک آپدیت‌های یک روز", b"scan_day")],
        [Button.inline("📡 کانال‌ها", b"channels"), Button.inline("🕘 آخرین کارها", b"recent")],
        [Button.inline("📅 گزارش امروز", b"today"), Button.inline("🔁 تلاش دوباره خطاها", b"retryall")],
    ]


def today_counts():
    today = dt.datetime.now(TEHRAN).date()
    counts = {}
    for it in state["items"].values():
        try:
            d = dt.datetime.fromisoformat(it.get("updated") or it.get("created")).astimezone(TEHRAN).date()
        except Exception:
            continue
        if d == today:
            counts[it.get("status", "?")] = counts.get(it.get("status", "?"), 0) + 1
    return counts


def ago_fa(iso):
    try:
        sec = int((dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(iso)).total_seconds())
    except Exception:
        return "—"
    if sec < 90:
        return f"{sec} ثانیه پیش"
    if sec < 5400:
        return f"{sec // 60} دقیقه پیش"
    return f"{sec // 3600} ساعت پیش"


def status_text():
    on = "روشن 🟢" if state.get("enabled") else "خاموش 🔴"
    mode = "پیش‌نویس (دستی منتشر می‌کنم) 📝" if state.get("mode", "draft") == "draft" else "انتشار خودکار 🚀"
    srv = state.get("server") or {}
    kill = "روشن (انتشار مجاز) ✅" if srv.get("allowPublish") else ("خاموش ⛔️ — هیچ انتشاری انجام نمی‌شود" if srv.get("allowPublish") is False else "نامشخص")
    cur = "—"
    if current["ref"]:
        cur = f"{esc((state['items'].get(current['ref'] or '') or {}).get('name'))} ({esc(current['stage'])})"
    acts = state.get("actions") or []
    if acts:
        cur += f"\nکارهای دکمه‌ها در صف: {len(acts)} ({esc(ACT_LABEL.get(acts[0].get('stage') or 'queued', ''))})"
    chs = "، ".join(esc(c["key"]) for c in state.get("channels", [])) or "—"
    tc = today_counts()
    today = " · ".join(f"{STATUS_FA.get(k, k)} {n}" for k, n in tc.items()) or "هنوز کاری نشده"
    return (f"<b>آپدیت خودکار سیب‌استار</b>\n"
            f"🟢 آنلاین — آخرین ضربان: {ago_fa(state.get('heartbeat'))} (اجرا از {fa_time(RUN_STARTED)})\n"
            f"وضعیت: {on}\nحالت: {mode}\nکلید اضطراری سرور: {kill}\n"
            f"آخرین بررسی کانال‌ها: {ago_fa(state.get('last_check'))}\n"
            f"کار فعلی: {cur}\nصف بررسی: {queue.qsize()}\n"
            f"پیش‌نویس‌های منتظر: {len(drafts())}" + (f" · نیاز به انتخاب: {len(confirms())}" if confirms() else "") + "\n"
            f"امروز: {today}\nکانال‌ها: {chs}\n\nمی‌توانی فایل IPA را مستقیم بفرستی یا فوروارد کنی.")


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
            prog = await progress_msg(f"⏳ بررسی پست‌های {day_label(d)} در همهٔ کانال‌ها…")
            await scan_day(d, f"چک آپدیت‌های {day_label(d)}", progress=prog)
            return
        if kind == "addch":
            awaiting["kind"] = None
            prog = await progress_msg(f"⏳ در حال پیوستن/خواندن کانال {esc(text[:60])}…")
            try:
                msg = await add_channel(text)
            except Exception as e:
                msg = f"❌ اضافه نشد: {type(e).__name__}"
            if prog:
                await edit_msg(prog, msg, buttons=channels_buttons())
            else:
                await ev.respond(msg, parse_mode="html", buttons=channels_buttons())
            return
        await ev.respond(status_text(), buttons=menu_buttons(), parse_mode="html")

    async def on_cb(ev):
        await ev.get_sender()
        if not is_admin(ev):
            await ev.answer("دسترسی نداری", alert=True)
            return
        d = ev.data.decode()
        taps = state.setdefault("taps", [])
        taps.append({"t": now_iso(), "d": d[:40]})  # audit trail: every press is recorded and answered
        state["taps"] = taps[-80:]
        mark()
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
        elif d == "noop_":
            await ev.answer("⏳ در حال انجام است؛ نتیجه همین‌جا نوشته می‌شود." if busy or state.get("actions")
                            else "ℹ️ این دکمه فقط وضعیت را نشان می‌دهد؛ کار تمام شده است.")
        elif d.startswith("dl:"):
            await ev.answer()
            state["list_page"] = int(d[3:] or 0)
            text, rows = drafts_view(state["list_page"])
            try:
                await ev.edit(text, buttons=rows, parse_mode="html", link_preview=False)
                list_msgs[ev.message_id] = state["list_page"]
            except Exception as e:
                if "not modified" in str(e).lower():
                    list_msgs[ev.message_id] = state["list_page"]
                else:
                    mm = await ev.respond(text, buttons=rows, parse_mode="html", link_preview=False)
                    list_msgs[mm.id] = state["list_page"]
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
                await ev.answer("⏳ شروع شد")
                await ev.edit(f"⏳ <b>انتشار همه</b>: {len(drafts())} پیش‌نویس در صف…", parse_mode="html")
                pid = ev.message_id

                async def _all():
                    try:
                        await publish_all(progress=pid)
                    finally:
                        busy.discard("publish_all")
                asyncio.create_task(_all())
        elif d == "miss":
            await ev.answer()
            a = state.get("audit") or {}
            if not a or time.time() - dt.datetime.fromisoformat(a["t"]).timestamp() > 3 * 3600:
                prog = await progress_msg("⏳ بررسی پوشش ۴۸ ساعت اخیر همهٔ کانال‌ها… (چند دقیقه)", reply_to=ev.message_id)
                asyncio.create_task(coverage_audit(progress=prog))
            else:
                text, rows = missed_view()
                await ev.respond(audit_summary() + "\n\n" + text, buttons=rows, parse_mode="html", link_preview=False)
        elif d == "audit":
            await ev.answer("در حال بررسی…")
            prog = await progress_msg("⏳ بررسی پوشش ۴۸ ساعت اخیر همهٔ کانال‌ها… (چند دقیقه)", reply_to=ev.message_id)
            asyncio.create_task(coverage_audit(progress=prog))
        elif d == "mkall":
            ms = missed_rows()
            await ev.answer(f"⏳ {len(ms)} مورد در صف ساخت پیش‌نویس")
            if ms:
                prog = await progress_msg(f"⏳ ساخت پیش‌نویس برای {len(ms)} مورد جامانده…", reply_to=ev.message_id)
                await start_batch(f"ساخت پیش‌نویس موارد جامانده ({len(ms)})", [r["ref"] for r in ms], progress=prog, manual=True)
        elif d.startswith("sl:"):
            await ev.answer()
            await ev.respond(scan_list_text(d[3:]), parse_mode="html", link_preview=False,
                             buttons=[[Button.inline(f"📝 پیش‌نویس‌ها ({len(drafts())})", b"dl:0")]] if d[3:] in ("draft_ok", "needs_confirm") else None)
        elif d.startswith("sd:"):
            day = dt.date.fromisoformat(d[3:])
            await ev.answer("در حال بررسی…")
            prog = await progress_msg(f"⏳ بررسی پست‌های {day_label(day)} در همهٔ کانال‌ها…", reply_to=ev.message_id)
            await scan_day(day, f"چک آپدیت‌های {day_label(day)}", progress=prog)
        elif d == "sd_manual":
            awaiting["kind"] = "day"
            await ev.answer()
            await ev.respond("تاریخ را بفرست (شمسی مثل ۱۴۰۵/۰۸/۲۱ یا میلادی مثل 2026-11-12 یا «دیروز»):",
                             buttons=[[Button.inline("لغو", b"noop")]])
        elif d.startswith("cf:"):
            _, sid, idx = d.split(":", 2)
            ref = ref_by_sid(sid)
            it = state["items"].get(ref or "") or {}
            cands = it.get("candidates") or []
            if it.get("status") != "needs_confirm" or not idx.isdigit() or int(idx) >= len(cands):
                await ev.answer("این انتخاب دیگر معتبر نیست", alert=True)
            else:
                c = cands[int(idx)]
                await start_item(ev, ref, f"ساخت پیش‌نویس برای «{(c.get('nameFa') or c.get('name') or '')[:30]}»", manual=True, app_id=c["id"])
        elif d[:3] in ("dp:", "dr:", "re:", "rt:", "fc:", "cs:", "mk:"):
            kind, ref = d[:2], ref_by_sid_any(d[3:])
            it = state["items"].get(ref or "") or {}
            if not ref:
                await ev.answer("این مورد دیگر وجود ندارد", alert=True)
            elif kind == "dp":
                await request_action(ev, "publish", ref)
            elif kind == "dr":
                await request_action(ev, "discard", ref)
            elif kind == "cs":
                await ev.answer()
                await ev.respond(item_report(it), buttons=item_buttons(it), parse_mode="html")
            elif kind == "mk":
                await start_item(ev, ref, "ساخت پیش‌نویس", manual=True)
            elif kind == "re":
                await start_item(ev, ref, "بررسی دوباره", manual=True)
            elif kind == "rt":
                await start_item(ev, ref, "تلاش دوباره", manual=True)
            elif kind == "fc":
                await start_item(ev, ref, "آپدیت اجباری", manual=True, force=True)
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
            prog = await progress_msg(f"⏳ بررسی پست‌های امروز ({day_label(day)}) در همهٔ کانال‌ها…", reply_to=ev.message_id)
            await scan_day(day, f"چک آپدیت‌های امروز ({day_label(day)})", progress=prog)
        elif d == "scan_day":
            await ev.answer()
            await ev.respond("📆 کدام روز را بررسی کنم؟", buttons=day_buttons())
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
            refs = sorted(k for k, v in state["items"].items() if v.get("status") == "failed")
            await ev.answer(f"⏳ {len(refs)} مورد دوباره در صف")
            if refs:
                prog = await progress_msg(f"⏳ تلاش دوباره برای {len(refs)} مورد خطادار…", reply_to=ev.message_id)
                await start_batch(f"تلاش دوباره خطاها ({len(refs)})", refs, progress=prog, manual=True)
            else:
                await ev.respond("✅ هیچ مورد خطاداری نیست.")
        elif ":" in d:
            kind, ref = d.split(":", 1)
            if kind == "retry":
                await start_item(ev, ref, "تلاش دوباره", manual=True)
            elif kind == "force":
                await start_item(ev, ref, "آپدیت اجباری", manual=True, force=True)
            elif kind == "pub":  # old notifications
                await request_action(ev, "publish", ref)
            else:
                await ev.answer()
        mark()

    @bot.on(events.CallbackQuery())
    async def on_cb_safe(ev):
        try:
            await on_cb(ev)
        except Exception as e:
            log(f"button handler failed: {type(e).__name__}")
            traceback.print_exc(limit=3)
            try:
                await ev.answer(f"❌ خطا در اجرای دکمه ({type(e).__name__})؛ دوباره بزن.", alert=True)
            except Exception:
                try:
                    await notify(f"❌ اجرای دکمه با خطا روبه‌رو شد ({type(e).__name__})؛ دوباره بزن.")
                except Exception:
                    pass


# ---------------------------------------------------------------- channel watch
async def catch_up():
    state["last_check"] = now_iso()
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
        state["last_check"] = now_iso()
        last_ids = state.setdefault("last_ids", {})
        last_ids[key] = max(int(last_ids.get(key) or 0), ev.message.id)
        mark()
        if ref not in state["items"] and state.get("enabled"):
            enqueue(ref)

    prev_hb = state.get("heartbeat")
    state["heartbeat"] = now_iso()
    ping = await sib({"action": "ping"}, tries=2)
    state["server"] = {"ok": bool(ping.get("ok")), "allowPublish": ping.get("allowPublish"), "t": now_iso()}
    log(f"sibstar ping ok={ping.get('ok')}")
    for a in state.get("actions", []) or []:
        a["late"] = True  # pressed before the restart: done now and confirmed
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
    async def periodic_catch_up():
        while True:
            await asyncio.sleep(600)
            try:
                await catch_up()  # safety net for live events missed while reconnecting
            except Exception as e:
                log(f"periodic catch-up failed: {type(e).__name__}")
    tasks = [asyncio.create_task(worker()), asyncio.create_task(state_saver()), asyncio.create_task(cleanup_sweeper()),
             asyncio.create_task(action_worker()), asyncio.create_task(progress_ticker()), asyncio.create_task(periodic_catch_up())]
    if state.get("actions"):
        action_event.set()
    try:
        gap = int((dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(prev_hb)).total_seconds()) if prev_hb else 0
    except Exception:
        gap = 0
    if gap > 240:
        n_act = len(state.get("actions") or [])
        await notify(f"🔄 ربات دوباره آنلاین شد (قطعی حدود {gap // 60} دقیقه)."
                     + (f"\n{n_act} درخواست دکمه که قبل از قطعی زده بودی الان انجام می‌شود." if n_act else "")
                     + "\nاگر در این فاصله دکمه‌ای زدی و هیچ پیامی نگرفتی، دوباره بزن (تکرارش بی‌خطر است).")
    if not ping.get("ok"):
        await notify("⚠️ ربات بالا آمد ولی به سایت وصل نشد: " + esc(ping.get("reasonFa")))
    async def run_test_actions():
        for a in TEST_ACTIONS:
            kind, ref = a.split(":", 1)
            t0 = time.time()
            while kind in ("publish", "discard") and (ref in inflight or (state["items"].get(ref) or {}).get("status") in (None, "queued")) and time.time() - t0 < 45 * 60:
                await asyncio.sleep(10)  # a test item queued in the same run must finish first
            trace(f"test action {a} start (batches={len(batches)}, inflight={len(inflight)})")
            try:
                if kind in ("publish", "discard"):
                    prog = await progress_msg(f"⏳ (آزمایشی) {kind}: {esc(ref)}…")
                    st = await queue_action(kind, ref, prog)
                elif kind == "scan":
                    day = dt.datetime.now(TEHRAN).date() - dt.timedelta(days=1 if ref == "yesterday" else 0)
                    prog = await progress_msg(f"⏳ بررسی پست‌های {day_label(day)}…")
                    await scan_day(day, f"چک آپدیت‌های {day_label(day)}", progress=prog)
                    t1 = time.time()
                    trace(f"test scan {ref}: waiting for {len(batches)} batch(es) {[b['label'] for b in batches.values()]}")
                    while batches and time.time() - t1 < 3 * 3600:
                        await asyncio.sleep(10)
                    st = "scanned"
                elif kind == "audit":
                    prog = await progress_msg("⏳ بررسی پوشش ۴۸ ساعت اخیر…")
                    await coverage_audit(progress=prog)
                    st = "audited"
                else:
                    st = None
                trace(f"test action {kind} -> {st}")
            except Exception as e:
                trace(f"test action {kind} failed: {type(e).__name__}: {str(e)[:100]}")
    if TEST_ACTIONS:
        tasks.append(asyncio.create_task(run_test_actions()))
    if not state.get("hello_v4"):
        ok = await notify("🆕 به‌روزرسانی ربات:\n• هر دکمه فوراً جواب می‌دهد و یک پیام وضعیت زنده (مرحله به مرحله) دارد.\n"
                          "• «🔎 موارد جامانده»: پست‌های ۴۸ ساعت اخیر که برنامه‌شان روی سایت هست و نسخهٔ جدیدتر دارند ولی پیش‌نویس نشده‌اند.\n"
                          "• تطابق دقیق‌تر (Bundle فایل فعلی، نسخه‌های دستکاری‌شده و اسم‌های مشابه → با تأیید تو).\n"
                          "• «📆 یک روز» با دکمهٔ روزها. «📊 وضعیت» با ضربان، آخرین بررسی و آمار امروز.", buttons=menu_buttons())
        state["hello_v4"] = bool(ok)
        state["hello_v3"] = True
        mark()
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
    stopping["v"] = True  # new button presses are still accepted and persisted; the successor run executes them
    t0 = time.time()
    while (current["ref"] or busy) and time.time() - t0 < 600:
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
