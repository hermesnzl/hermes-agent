# LXMF / Reticulum

The LXMF adapter exposes Hermes over a [Reticulum](https://reticulum.network/) mesh using the [LXMF](https://github.com/markqvist/lxmf) message protocol. It implements a complete **request/response messaging system over the mesh**:

- Send an **LXMF message** to the gateway's LXMF delivery address and it is delivered to the agent as a user message (exactly like any other platform).
- The agent's **reply is wrapped as an LXMF message** and sent back to the originating Reticulum destination — so every query gets an answer over the mesh.

Reticulum is a mesh networking stack that runs over almost anything: LoRa, packet radio, TCP, WiFi, I2P, or a single shared host. That makes this adapter ideal for low-bandwidth, intermittently-connected, or privacy-sensitive environments where you still want to talk to your agent.

> LXMF carries **text only**. There is no voice, image, file, typing indicator, threading, or streaming. Replies are sent as plain LXMF messages, with long responses soft-capped to ~1 KB.

## Prerequisites

- Python packages **`rns`** and **`lxmf`** (`pip install rns lxmf`). The adapter makes these *optional*: it imports them lazily and auto-installs them the moment you enable the platform — the rest of the gateway is unaffected if they're missing.
- A working Reticulum configuration. The adapter defaults to `~/.reticulum`, so if your host is already part of a mesh (e.g. the `reticulum` container built for this gateway), the adapter joins that mesh automatically. Otherwise Reticulum creates a default config on first run.

## Configure Hermes

You can configure LXMF via `config.yaml` (recommended) or environment variables.

### Option A — gateway-config.yaml

```yaml
gateway:
  platforms:
    lxmf:
      enabled: true
      extra:
        config_dir: ""               # Reticulum config dir; "" = ~/.reticulum
        identity_file: ""             # optional fixed identity file; "" = auto-generate & persist
        display_name: "Hermes Agent"  # name announced for the gateway delivery destination
        propagation_node: ""          # optional outbound LXMF propagation node (hash hex)
        proof_strategy: "none"        # none | app | all
        inbound_stamp_cost: 0         # Reticulum stamp cost on inbound msgs (0 = none, <255)
        allowed_users: []             # list of RNS identity hashes allowed to talk to the bot
        max_message_length: 1024      # LXMF payload soft-cap
```

### Option B — environment variables

| Variable | Required | Description |
|----------|:--------:|-------------|
| `LXMF_CONFIG_DIR` | — | Reticulum config directory (defaults to `~/.reticulum`) |
| `LXMF_IDENTITY_FILE` | — | Fixed Reticulum identity file for the gateway's LXMF identity |
| `LXMF_DISPLAY_NAME` | — | Display name announced for the gateway delivery destination |
| `LXMF_PROPAGATION_NODE` | — | Outbound LXMF propagation node (RNS identity/delivery hash hex) |
| `LXMF_PROOF_STRATEGY` | — | `none` (default) / `app` / `all` |
| `LXMF_INBOUND_STAMP_COST` | — | Stamp cost demanded on inbound LXMF messages (`0` = none, `<255`) |
| `LXMF_ALLOWED_USERS` | — | Comma-separated RNS identity hashes allowed to talk to the bot |
| `LXMF_ALLOW_ALL_USERS` | — | Allow anyone on the mesh to talk to the bot (dev only) |
| `LXMF_HOME_DESTINATION` | — | Default RNS destination hash for cron / notification delivery |

## How a conversation flows

1. The gateway starts Reticulum, builds (or loads) an LXMF identity, and registers an `lxmf.delivery` destination for it. It then **announces** that destination on the mesh.
2. A peer sends an LXMF message to the gateway's delivery hash. Reticulum delivers it to the router's delivery callback.
3. The adapter turns the message into a gateway request — the **RNS identity hash** of the sender becomes the durable `chat_id`/`user_id`, and the LXMF message **title** (if set) becomes the display name.
4. The agent answers. The adapter wraps the response as an outbound LXMF message addressed to the **originating peer's** delivery destination (resolved via Reticulum's known-destinations store) and sends it back.

## Addressing

- Each Reticulum identity has a stable hex **identity hash** (e.g. `a1b2c3…`). That hash is the `chat_id` the gateway uses, so a given human/device keeps one conversation thread across reconnects.
- To talk to the gateway from another Reticulum node, address an LXMF message to the gateway's **`lxmf.delivery`** destination hash (use `rnsd`/`lxmf` tooling to resolve it from the gateway's announced identity).
- For cron / `send_message` delivery, set `LXMF_HOME_DESTINATION` to the RNS hash you want notifications routed to (defaults to the gateway's own hash).

## Access control

- By default, **all peers on the mesh may talk to the bot** (`allow_all_users: false` but `allowed_users: []` means "allow everyone"). For production meshes, set `allowed_users` to the specific RNS identity hashes you trust, or set `LXMF_ALLOW_ALL_USERS=false` with a populated `allowed_users` list.
- `proof_strategy` controls whether the gateway demands a proof of identity before accepting messages (`none` = anyone; `app`/`all` = stricter). `inbound_stamp_cost` can require proof-of-work on inbound messages to deter spam.

## Cron & standalone delivery

`deliver=lxmf` cron jobs and the `send_message` tool open a short-lived Reticulum + LXMRouter, send a single LXMF message to `LXMF_HOME_DESTINATION` (or an explicit `chat_id`), and tear down — so they work even when the gateway process is not co-resident.

## Notes & limitations

- The gateway's LXMF identity is **persisted** under `<config_dir>/storage/hermes_lxmf_identity` so its delivery hash (its address on the mesh) stays stable across restarts. Point `LXMF_IDENTITY_FILE` at a file to use your own identity instead.
- If you run other Reticulum-based Hermes platforms in the same process, they share the single process-global Reticulum instance (by design).
- LXMF is text-only; `send_image` degrades to sending the caption/link as text.
