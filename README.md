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
