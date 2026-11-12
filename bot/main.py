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
import os
import re
import signal
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request

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
# Publishing is LOCKED unless the repo variable PUBLISH_UNLOCKED=1 is set (Erfan's explicit approval).
PUBLISH_UNLOCKED = os.environ.get("PUBLISH_UNLOCKED", "0") == "1"
FINAL_STATUSES = ("draft_ok", "published", "skipped", "rejected", "duplicate")
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
    if not PUBLISH_UNLOCKED and state.get("mode") != "draft":
        state["mode"] = "draft"
        event("mode forced to draft (publish locked)")


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
        for k in sorted(items, key=lambda x: (items[x].get("updated") or ""))[:-400]:
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


def item_buttons(it):
    mid = it.get("ref") or f"{DEFAULT_CHANNEL}/{it.get('msg_id')}"
    st = it.get("status")
    if st in ("failed", "rejected", "error"):
        return [[Button.inline("🔁 تلاش دوباره", f"retry:{mid}".encode())]]
    if st == "skipped" and it.get("code") in ("same_version", "older_version"):
        return [[Button.inline("🔁 بازهم آپدیت کن", f"force:{mid}".encode())]]
    if st == "draft_ok" and PUBLISH_UNLOCKED:
        return [[Button.inline("🚀 انتشار همین", f"pub:{mid}".encode())]]
    return None


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
        return f"✅ <b>{t}</b> آپدیت شد (نسخه {esc((it.get('fields') or {}).get('version'))})\n{fmt_fields(it.get('fields'))}\n🗑 {esc(old.get('reasonFa', ''))}"
    if st == "draft_ok":
        fr = (it.get("plan") or {}).get("from") or {}
        same = " (همان فایل فعلی)" if (it.get("plan") or {}).get("sameFile") else ""
        return (
            f"📝 <b>{t}</b> آماده است — فقط پیش‌نویس، منتشر نشد{same}\n"
            f"سایت: نسخه {esc(fr.get('version'))} ← فایل جدید: {fmt_fields((it.get('plan') or {}).get('fields'))}\n"
            f"Bundle ID تأیید شد ✔︎ <code>{esc(((it.get('plan') or {}).get('fields') or {}).get('bundleId'))}</code>"
            + (f"\nℹ️ {esc(it.get('reason'))}" if it.get("reason") else "")
        )
    if st == "skipped":
        return f"⚠️ <b>{t}</b>: {esc(it.get('reason'))}"
    if st == "rejected":
        return f"⛔️ <b>{t}</b> منتشر نشد: {esc(it.get('reason'))}"
    if st == "publish_disabled":
        return f"📝 <b>{t}</b>: {esc(it.get('reason'))}"
    return f"❌ <b>{t}</b> خطا: {esc(it.get('reason'))}"


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
    it["attempts"] = it.get("attempts", 0) + 1
    it["updated"] = now_iso()
    mode = opts.get("mode") or state.get("mode", "draft")
    if not (PUBLISH_UNLOCKED or opts.get("test_publish")):
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
        it.update(job_id=s["jobId"], app=s.get("app"), matched_by=s.get("matchedBy"))
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
            r = await sib({"action": "status", "jobId": it["job_id"], "appId": it["app"]["id"], "sourceUrl": url, "bundleId": post["bundle"], "mode": mode})
            oc = r.get("outcome")
            if oc == "pending":
                current["stage"] = f"import {r.get('progress', 0)}%"
                continue
            if oc == "draft_ok":
                it.update(status="draft_ok", plan=r.get("plan"), reason=None, fields=(r.get("plan") or {}).get("fields"))
                return
            if oc == "published":
                it.update(status="published", fields=r.get("fields"), old_file=r.get("oldFile"), reason=None)
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
             "failed": "❌ خطا", "duplicate": "♻️ تکراری", "queued": "⏳ در صف"}


async def start_batch(label, refs):
    bid = f"b{int(time.time() * 1000)}"
    refs = list(dict.fromkeys(refs))
    already = {r for r in refs if (state["items"].get(r) or {}).get("status") in FINAL_STATUSES}
    batches[bid] = {"label": label, "refs": refs, "already": already}
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
    await start_batch(f"فایل ارسالی: {fn}", [ref])


async def cleanup_sweeper():
    """publish mode only: hourly strict cleanup of previous files / superseded draft imports (server re-checks)."""
    while True:
        await asyncio.sleep(3600)
        if not (PUBLISH_UNLOCKED and state.get("enabled") and state.get("mode") == "publish"):
            continue
        try:
            r = await sib({"action": "cleanup", "dryRun": False, "limit": 20})
            done = [x for x in r.get("results", []) if x.get("deleted")]
            if done:
                mb = sum(int(x.get("bytes") or 0) for x in done) / 1e6
                event(f"cleanup deleted {len(done)} files ({mb:.0f} MB)")
                await notify(f"🧹 {len(done)} فایل قبلی/پیش‌نویس جایگزین‌شده حذف شد ({mb:.0f} مگابایت).")
        except Exception as e:
            log(f"cleanup failed: {type(e).__name__}")


# ---------------------------------------------------------------- admin bot UI
def menu_buttons():
    on = state.get("enabled")
    draft = state.get("mode", "draft") == "draft"
    return [
        [Button.inline("🔴 خاموش کن" if on else "🟢 روشن کن", b"toggle"), Button.inline("📊 وضعیت", b"status")],
        [Button.inline("📝 حالت: فقط پیش‌نویس، منتشر نکن" if draft else "🚀 حالت: انتشار خودکار", b"mode")],
        [Button.inline("🔎 چک آپدیت‌های امروز", b"scan_today"), Button.inline("📆 چک آپدیت‌های یک روز", b"scan_day")],
        [Button.inline("📡 کانال‌ها", b"channels"), Button.inline("🕘 آخرین کارها", b"recent")],
        [Button.inline("📅 گزارش امروز", b"today"), Button.inline("🔁 تلاش دوباره خطاها", b"retryall")],
    ]


def status_text():
    on = "روشن 🟢" if state.get("enabled") else "خاموش 🔴"
    mode = "فقط پیش‌نویس 📝" if state.get("mode", "draft") == "draft" else "انتشار خودکار 🚀"
    if not PUBLISH_UNLOCKED:
        mode += " (انتشار قفل است 🔒)"
    cur = f"\n⏳ در حال کار: {esc((state['items'].get(current['ref'] or '') or {}).get('name'))} ({esc(current['stage'])})" if current["ref"] else ""
    chs = "، ".join(esc(c["key"]) for c in state.get("channels", [])) or "—"
    return f"<b>آپدیت خودکار سیب‌استار</b>\nوضعیت: {on}\nحالت: {mode}\nصف: {queue.qsize()}\nکانال‌ها: {chs}{cur}\n\nمی‌توانی فایل IPA را مستقیم بفرستی یا فوروارد کنی."


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
        elif d in ("mode", "mode_pub") and not PUBLISH_UNLOCKED:
            state["mode"] = "draft"
            await ev.answer("انتشار خودکار قفل است؛ فقط پیش‌نویس.", alert=True)
        elif d == "mode":
            if state.get("mode", "draft") == "draft":
                await ev.answer()
                await ev.respond(
                    "⚠️ با «انتشار خودکار» هر آپدیت بدون بررسی روی سایت منتشر می‌شود. مطمئنی؟",
                    buttons=[[Button.inline("بله، انتشار خودکار", b"mode_pub"), Button.inline("نه", b"noop")]],
                )
            else:
                state["mode"] = "draft"
                event("mode=draft")
                await ev.answer("حالت فقط پیش‌نویس")
                await ev.edit(status_text(), buttons=menu_buttons(), parse_mode="html")
        elif d == "mode_pub":
            state["mode"] = "publish"
            event("mode=publish")
            await ev.answer("انتشار خودکار روشن شد")
            await ev.edit("🚀 انتشار خودکار روشن شد.\n" + status_text(), buttons=menu_buttons(), parse_mode="html")
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
            elif kind == "pub":
                if not PUBLISH_UNLOCKED:
                    await ev.answer("انتشار قفل است؛ فقط پیش‌نویس.", alert=True)
                else:
                    await ev.answer("در صف انتشار" if enqueue(ref, manual=True, mode="publish") else "همین الان در صف است")
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
        if not PUBLISH_UNLOCKED:
            p.pop("mode", None)
        if ref:
            enqueue(ref, **p)
    await catch_up()
    for r in TEST_REFS:
        enqueue(r, manual=True, force=True, mode=TEST_MODE, test_publish=(TEST_MODE == "publish"))
    save_state()
    tasks = [asyncio.create_task(worker()), asyncio.create_task(state_saver()), asyncio.create_task(cleanup_sweeper())]
    if not ping.get("ok"):
        await notify("⚠️ ربات بالا آمد ولی به سایت وصل نشد: " + esc(ping.get("reasonFa")))
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
