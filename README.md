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
- Publishing is locked unless repo variable `PUBLISH_UNLOCKED=1` (and the server allows publishing). A one-off supervised test can be dispatched with inputs `test_refs` + `test_mode=publish`.
- When publishing, the previous IPA is deleted only after the server's strict per-file check (fix116).
