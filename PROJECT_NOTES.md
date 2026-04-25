# Project Notes

Open this file first when working on this bot from a new computer or a new Codex session.

## Active Bot

The active build is now `v2`.

- Active local folder: `C:\Users\Oliver\Desktop\oliverslittlehelper_bot-git-v2`
- GitHub repo: `oliboi123-ui/oliverslittlehelper_bot`
- Active branch: `v2-tiered-shop`
- Main bot file: `tiered_shop_bot_v2.py`
- Product brief: `V2_PRODUCT_BRIEF.md`
- Safe env template: `.env.example`
- Docker entrypoint: `Dockerfile`

## Parallel v1 Folder

The live v1 working folder is separate:

- v1 local folder: `C:\Users\Oliver\Desktop\oliverslittlehelper_bot-main-v1`
- v1 main bot file: `gatekeeper_bot.py`

If the user asks for `v1`, edit that folder. If the user asks for `v2`, edit this folder/branch. If the user asks for `both`, edit both active files directly.

## Archived Bot

The previous stable bot has been moved into:

- `archive/v1_gatekeeper_bot_stable_2026-04-24/`

Do not edit files in the archive unless the user explicitly asks for a v1 recovery or comparison.

## What v2 Is

This bot is a tiered Telegram commerce bot with a controlled premium funnel:

- `OnlyFans Verified`
- `Starter`
- `Plus`
- `Pro`

Important:

- `OnlyFans Verified` is not a paid tier.
- Existing OnlyFans verification logic should be preserved conceptually and adapted into v2.
- Admin can manually move buyers between tiers.
- The product experience should feel like a private premium shop, not an open chatroom.

## Current Tier Logic

These rules were confirmed by the user on 2026-04-24:

- Verified user can buy `starter unlock` for `$37`.
- `Starter` buying one individual `$67` Plus PPV upgrades them to `Plus`.
- `Starter` buying the `$97` Best Value Bundle upgrades them directly to `Pro`.
- `Plus` buying another `$67` PPV upgrades them to `Pro`.
- Higher tiers get a `25%` discount on lower-tier PPVs only.
- Pro products are fulfilled manually by admin request, not instantly from the vault.

## Deployment Rule

Do not replace the old production bot accidentally.

Preferred rollout:

1. Build and test `v2` using a new BotFather token.
2. Use a separate Railway test service/project for `v2`.
3. Keep `v1` archived and untouched.

## Working Rule For Future Codex Sessions

Before editing anything:

1. Read this file.
2. Read `V2_PRODUCT_BRIEF.md`.
3. Confirm you are working in `C:\Users\Oliver\Desktop\oliverslittlehelper_bot-git-v2`.
4. Avoid using `C:\Users\Oliver\Documents\Codex\oliverslittlehelper_bot-main`.
5. Do not edit `archive/v1_gatekeeper_bot_stable_2026-04-24/` unless explicitly asked to compare or recover archived v1.

## Current Priority

Build a clean `v2` around the tiered buyer journey, while keeping test mode foolproof and admin-only during development.

## Current Shared UX Commands

These exist in both active v1 and active v2 as of 2026-04-25:

- `/help`: admin help when used as admin, buyer help when a buyer/test-buyer is active.
- `/reportissue`: buyer-side debug report flow. The buyer explains what looks wrong and the bot forwards it to admin for debugging.
- Test-buyer mode should treat the admin as a buyer for normal buyer messages. Use `/testreset` to leave test-buyer mode.
