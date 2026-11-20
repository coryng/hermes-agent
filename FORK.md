# Becky and Sanchez Hermes fork

This branch maintains the private dashboard/Apple Shortcut integration on top
of NousResearch/Hermes Agent. The initial upstream base is `2281116525`.
Both assistants use the same source revision, with separate standalone gateway
processes, profile homes, credentials, databases, and loopback bridge ports.
Multiplexed hosting is not supported by this custom bridge.

The bridge code was recovered from the existing local deployment. It includes
authenticated loopback RPCs, Telegram topic handoff, optional topic closing,
private title generation, and delivery-aware automatic closing. Only source,
synthetic tests and maintenance documentation belong in this fork. Never commit
profile `.env`, auth files, session databases, conversations, launchd exports,
recovery backups, or personal skill/memory snapshots.

## Reconnection contract

Publishing a replacement primary Telegram adapter rebinds the bridge's topic
sender, Bot API controller, and close-command handler. The bridge listener and
request/idempotency state survive. Failed sends remain failed; recovery does
not replay user actions. Each standalone runner refreshes only its own bridge.
Version-2 dashboard clients receive the original six public methods; title
generation and new-topic handoff remain private authenticated RPCs.

## Readiness

Use the runtime Python environment and the explicit home of each profile:

```sh
python scripts/observability/becky_bridge_probe.py --hermes-home /path/to/profile
```

Exit 0 means the authenticated bridge reports usable topic replies and the
deployed dashboard contract is compatible. Exit 1 indicates a bounded,
non-secret failure code. This calls only capabilities: it never sends Telegram
messages, generates model responses, or replays requests. `topic_control_ready`
is separate: optional MTProto closing requires its own configured dependency
and credentials. Readiness does not prove external Telegram delivery.

## Updating and rollback

Maintain this deployment branch separately from upstream `main`. Review and
test upstream updates before changing the deployed revision. Use the same
reviewed commit for both bots and record it alongside the private deployment
configuration. Preserve a known-good revision and a consistent backup of each
profile's state before upgrades. Never replace the fork checkout with an
unpatched upstream install during a routine update.

Run `scripts/run_tests.sh` for the bridge, lifecycle, reconnect and Telegram
regressions. Use isolated test homes and synthetic transports. Roll out one
idle profile at a time, then run the readiness probe and confirm normal gateway
status before proceeding. A service process being alive is insufficient health
evidence. For rollback, restore the previous source revision and runtime
environment without overwriting profile data.

The dashboard remains a separate repository. Its action/queue redesign and any
Ubuntu migration are separate changes; this patch does not add a durable job
queue or an automatic restart monitor.
