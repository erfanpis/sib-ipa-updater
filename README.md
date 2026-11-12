# sib-ipa-updater

Telegram → SibStar IPA auto-updater (24/7 on GitHub Actions).

1. A dedicated Telegram user account watches `@blatants`.
2. Each new `.ipa` post is forwarded to `@linkbefileeTGbot` (Linkyar) to get a direct link.
3. SibStar's internal endpoint (`/api/internal/auto-ipa-update`, shared-secret header) matches an
   **existing** app only (bundle id first, exact name fallback), stage-imports the IPA with the normal
   ingest worker, verifies the bundle id, and either reports a draft (default) or publishes
   (technical fields only, never name/icon; publisher «عرفان»; guarded delete of the previous file).
4. Admin bot `@betagadminsjhbot` (only `@erfanitt`) reports every item and has the menu:
   on/off, status, today's report, draft-only ↔ auto-publish, recent items, retry.

State lives ENCRYPTED (`state.enc`, AES-256 with secret `STATE_KEY`) on the `state` branch; bot output is not printed to the public Actions log. Secrets: `TG_API_ID`, `TG_API_HASH`, `TG_SESSION`,
`ADMIN_BOT_TOKEN`, `SIBSTAR_ENDPOINT`, `SIBSTAR_ENDPOINT_SECRET`, `ADMIN_ID`, `STATE_KEY` (optional `RESPAWN_PAT`).

## Admin bot (v2)
- Sources: any number of channels (📡 کانال‌ها → add by @username / t.me link / invite link, remove, list). Stored in the encrypted state.
- 🔎 چک آپدیت‌های امروز / 📆 چک آپدیت‌های یک روز (Jalali `۱۴۰۵/۰۸/۲۱` or Gregorian `2026-11-12`): scans every channel for that Tehran day and sends a summary.
- Send or forward an .ipa file to the admin bot: it is relayed to the user account and goes through the same link → match → import → draft/publish flow.
- Dedupe: per message ref (`channel/msgid`), per app+version across channels, identical reports are not re-sent.
- Drafts (v3): menu button «📝 پیش‌نویس‌ها (N)» lists pending drafts (paginated) with old → new version, size, source, time and the app link; per item «✅ انتشار» (publishes that draft now, in any mode; idempotent) and «❌ رد» (no site change; the staged import is removed only under the strict per-file guards). «انتشار همه» asks for confirmation. Every new draft notification carries the same buttons; duplicate notices carry «🔁 دوباره بررسی».
- Mode: the admin's button switches «پیش‌نویس (دستی منتشر می‌کنم)» ↔ «انتشار خودکار» (admin-only). The server's `SIBSTAR_AUTO_IPA_ALLOW_PUBLISH` stays the emergency kill switch.
- One-off tests: dispatch with `test_refs` (+ `test_mode`) and/or `test_actions` like `publish:blatants/13483,discard:blatants/13444`.
- When publishing, the previous IPA is deleted only after the server's strict per-file check (fix116).

## v4 — coverage, confirm picks, live progress

- Every button answers instantly and edits a single progress message live
  (queued → downloading/linking → importing % → verifying → ✅/❌ with a Persian reason).
  Publish shows: در صف → در حال انتشار → پاک کردن فایل قبلی → ✅ منتشر شد (link, versions, old file deleted).
- Draft cards change their buttons to show state (⏳ queued / 🚀 publishing / ✅ published / 🗑 rejected).
- Button presses are persisted in the encrypted state (`actions`) and finished by the next run if a
  restart happens mid-way; a restart notice lists anything pending. Presses made while *no* run is
  online can't be re-delivered by Telegram, so the workflow keeps the hand-off gap short
  (successor is dispatched ~15 min before the end and waits at the gate until the old run drains).
- «📊 وضعیت»: heartbeat, last channel check, current job + stage, queues, mode, server kill switch, today's counts.
- Matching (server fix121): bundle → stored metadata bundle → exact name; shared/tweaked bundles or
  multiple same-name apps become «⚠️ نیاز به انتخاب» with candidate buttons (draft only); single-number
  versions (268 vs 269) and "1.2 (456)" are compared correctly.
- «🔎 موارد جامانده»: coverage audit of the last 48 h of channel posts vs the catalog, with «ساخت پیش‌نویس».
- «📆 چک آپدیت‌های یک روز»: buttons for the last 5 days (Jalali + weekday) plus manual date entry.
- Skipped items with not_found/ambiguous/bundle_conflict are re-checked on every rescan.
