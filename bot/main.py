"""SibStar Telegram IPA auto-updater.

User account watches @blatants -> forwards each new .ipa to @linkbefileeTGbot -> gets the direct link ->
asks SibStar (/api/internal/auto-ipa-update) to stage-import it for the matching EXISTING app ->
draft (default) or publish. Admin bot (@erfanitt only) reports and controls everything.
"""
import asyncio
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

from telethon import Button, TelegramClient, events
from telethon.sessions import StringSession

CHANNEL = os.environ.get("SOURCE_CHANNEL", "blatants")
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
TEST_MESSAGE_ID = int(os.environ.get("TEST_MESSAGE_ID", "0") or 0)
PROXY = None
if os.environ.get("BOX_PROXY"):
    h, p = os.environ["BOX_PROXY"].rsplit(":", 1)
    PROXY = ("http", h, int(p))
TEHRAN = dt.timezone(dt.timedelta(hours=3, minutes=30))

STATE_FILE = os.path.join(STATE_DIR, "state.json")
DEFAULT_STATE = {"enabled": True, "mode": "draft", "admin_id": ADMIN_ID_ENV or None, "last_msg_id": 0, "items": {}, "log": []}

state = {}
dirty = False
queue: "asyncio.Queue[tuple[int, dict]]" = None  # type: ignore
user: TelegramClient = None  # type: ignore
bot: TelegramClient = None  # type: ignore
current = {"msg_id": None, "stage": None}


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
        for k in sorted(items, key=lambda x: int(x))[:-400]:
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
            import hashlib
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
    b = re.search(r"Bundle ID:\s*(\S+)", cap)
    v = re.search(r"Updated to:\s*(\S+)", cap)
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
    mid = it["msg_id"]
    st = it.get("status")
    if st in ("failed", "rejected", "error"):
        return [[Button.inline("🔁 تلاش دوباره", f"retry:{mid}".encode())]]
    if st == "skipped" and it.get("code") in ("same_version", "older_version"):
        return [[Button.inline("🔁 بازهم آپدیت کن", f"force:{mid}".encode())]]
    if st == "draft_ok":
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


# ---------------------------------------------------------------- processing
async def process(mid, opts):
    it = state["items"].get(str(mid))
    msg = await user.get_messages(CHANNEL, ids=mid)
    post = parse_post(msg)
    if not post:
        return
    if it is None:
        it = {**post, "status": "queued", "created": now_iso(), "attempts": 0}
        state["items"][str(mid)] = it
    it.update({k: post[k] for k in ("name", "bundle", "version", "file", "size")})
    it["attempts"] = it.get("attempts", 0) + 1
    it["updated"] = now_iso()
    mode = opts.get("mode") or state.get("mode", "draft")
    force = bool(opts.get("force"))
    current.update(msg_id=mid, stage="match")
    reuse = mode == "publish" and it.get("status") == "draft_ok" and it.get("job_id") and (it.get("app") or {}).get("id")
    mark()
    try:
        if reuse:
            url = it.get("url") or ""
            current["stage"] = "publish"
            return await poll_job(it, url, post, mode)
        # 1) match only (cheap) before using the link bot
        m = await sib({"action": "match", "name": post["name"], "bundleId": post["bundle"], "version": post["version"], "force": force})
        if m.get("outcome") == "skipped":
            it.update(status="skipped", code=m.get("code"), reason=m.get("reasonFa"), app=m.get("app"))
            return
        if m.get("outcome") != "match":
            raise RuntimeError(m.get("reasonFa") or "خطای تطبیق")
        it["app"] = m.get("app")
        # 2) link
        current["stage"] = "link"
        url = it.get("url") if it.get("url") and (time.time() - it.get("url_ts", 0) < 4 * 3600) else None
        if not url:
            url = await get_link(msg)
            it.update(url=url, url_ts=time.time())
            mark()
        # 3) stage import
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
        log(f"item {mid} failed: {type(e).__name__}")
        traceback.print_exc(limit=2)
    finally:
        it["updated"] = now_iso()
        it["mode"] = mode
        event(f"{mid} {it.get('status')} {it.get('name')}")
        current.update(msg_id=None, stage=None)
        save_state()
        await notify(item_report(it), buttons=item_buttons(it))


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
        mid, opts = await queue.get()
        if not state.get("enabled") and not opts.get("manual"):
            continue
        try:
            await process(mid, opts)
        except Exception:
            traceback.print_exc()


def enqueue(mid, **opts):
    queue.put_nowait((mid, opts))


# ---------------------------------------------------------------- admin bot UI
def menu_buttons():
    on = state.get("enabled")
    draft = state.get("mode", "draft") == "draft"
    return [
        [Button.inline("🔴 خاموش کن" if on else "🟢 روشن کن", b"toggle"), Button.inline("📊 وضعیت", b"status")],
        [Button.inline("📝 حالت: فقط پیش‌نویس، منتشر نکن" if draft else "🚀 حالت: انتشار خودکار", b"mode")],
        [Button.inline("📅 گزارش امروز", b"today"), Button.inline("🕘 آخرین کارها", b"recent")],
        [Button.inline("🔁 تلاش دوباره خطاها", b"retryall")],
    ]


def status_text():
    on = "روشن 🟢" if state.get("enabled") else "خاموش 🔴"
    mode = "فقط پیش‌نویس 📝" if state.get("mode", "draft") == "draft" else "انتشار خودکار 🚀"
    cur = f"\n⏳ در حال کار: {esc((state['items'].get(str(current['msg_id'])) or {}).get('name'))} ({esc(current['stage'])})" if current["msg_id"] else ""
    return f"<b>آپدیت خودکار سیب‌استار</b>\nوضعیت: {on}\nحالت: {mode}\nصف: {queue.qsize()}\nکانال: @{CHANNEL}{cur}"


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


def register_bot_handlers():
    @bot.on(events.NewMessage(pattern=r"^/(start|menu)"))
    async def on_start(ev):
        await ev.get_sender()
        if not is_admin(ev):
            await ev.respond("این ربات خصوصی است.")
            return
        if not state.get("admin_id"):
            state["admin_id"] = ev.sender_id
            mark()
        state["admin_seen"] = now_iso()
        mark()
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
        elif d == "retryall":
            ids = [int(k) for k, v in state["items"].items() if v.get("status") == "failed"]
            for i in sorted(ids):
                enqueue(i, manual=True)
            await ev.answer(f"{len(ids)} مورد دوباره در صف")
        elif ":" in d:
            kind, mid = d.split(":", 1)
            mid = int(mid)
            if kind == "retry":
                enqueue(mid, manual=True)
                await ev.answer("دوباره در صف")
            elif kind == "force":
                enqueue(mid, manual=True, force=True)
                await ev.answer("دوباره در صف (اجباری)")
            elif kind == "pub":
                enqueue(mid, manual=True, mode="publish")
                await ev.answer("در صف انتشار")
            else:
                await ev.answer()
        mark()


# ---------------------------------------------------------------- channel watch
async def catch_up():
    last = int(state.get("last_msg_id") or 0)
    latest = None
    found = []
    async for m in user.iter_messages(CHANNEL, limit=60, min_id=last):
        latest = max(latest or 0, m.id)
        if last and parse_post(m):
            found.append(m.id)
    if not last:
        state["last_msg_id"] = latest or 0
        event(f"first run: starting after message {latest}")
        return
    for mid in sorted(found):
        if str(mid) not in state["items"]:
            enqueue(mid)
    if latest:
        state["last_msg_id"] = max(last, latest)
    mark()


async def main():
    global queue, user, bot
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
    log(f"user ok id={me.id}; bot ok")
    if not state.get("admin_id"):
        try:
            ent = await user.get_entity(ADMIN_USERNAME)
            state["admin_id"] = ent.id
        except Exception:
            pass
    register_bot_handlers()

    @user.on(events.NewMessage(chats=CHANNEL))
    async def on_post(ev):
        if parse_post(ev.message) and str(ev.message.id) not in state["items"]:
            state["last_msg_id"] = max(int(state.get("last_msg_id") or 0), ev.message.id)
            mark()
            if state.get("enabled"):
                enqueue(ev.message.id)

    ping = await sib({"action": "ping"}, tries=2)
    state["server"] = {"ok": bool(ping.get("ok")), "allowPublish": ping.get("allowPublish"), "t": now_iso()}
    log(f"sibstar ping ok={ping.get('ok')}")
    for p in state.pop("pending", []) or []:
        mid = p.pop("id", None)
        if mid:
            enqueue(int(mid), **p)
    await catch_up()
    if TEST_MESSAGE_ID:
        enqueue(TEST_MESSAGE_ID, manual=True, force=True, mode="draft")
    save_state()
    tasks = [asyncio.create_task(worker()), asyncio.create_task(state_saver())]
    if not ping.get("ok"):
        await notify("⚠️ ربات بالا آمد ولی به سایت وصل نشد: " + esc(ping.get("reasonFa")))
    if not state.get("hello_sent"):
        ok = await notify("🟢 ربات آپدیت خودکار سیب‌استار راه افتاد.\nحالت: فقط پیش‌نویس، چیزی منتشر نمی‌شود.\n/start را بزن تا منو بیاید.", buttons=menu_buttons())
        state["hello_sent"] = bool(ok)
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
    # finish the current item if one is running (max 20 min), then exit
    t0 = time.time()
    while current["msg_id"] and time.time() - t0 < 1200:
        await asyncio.sleep(5)
    # leave unprocessed queue items for the next run
    pend = []
    while not queue.empty():
        mid, opts = queue.get_nowait()
        pend.append({"id": mid, **opts})
    state["pending"] = pend
    for t in tasks:
        t.cancel()
    save_state()
    await user.disconnect()
    await bot.disconnect()
    log("bye")


if __name__ == "__main__":
    asyncio.run(main())
