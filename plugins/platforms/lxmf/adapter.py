"""
LXMF / Reticulum Gateway Adapter for Hermes Agent.

A plugin-based gateway adapter that exposes Hermes over a Reticulum mesh
via the LXMF message protocol.  It implements a true request/response
messaging system:

  * An inbound LXMF message addressed to the gateway's LXMF delivery
    destination is converted into a gateway request (a user message to
    the agent) and delivered to the agent's session loop exactly like
    any other platform.
  * The agent's response is wrapped as an outbound LXMF message and sent
    back to the originating Reticulum destination (the sender's LXMF
    delivery hash), so each query gets a reply over the mesh.

The adapter depends on the optional ``rns`` (Reticulum) and ``lxmf``
Python packages.  They are NOT required to import this module — the
dependency check is PASSIVE so ``hermes status`` never pip-installs
anything, and the ACTIVE installer (``ensure_deps_fn``) lazily
installs them the moment the gateway brings the platform up.  If the
deps are missing the adapter cleanly reports it is not configured and
the rest of the gateway is unaffected.

Configuration (config.yaml)::

    gateway:
      platforms:
        lxmf:
          enabled: true
          extra:
            # Optional Reticulum config directory (holds reticulum.config +
            # the identity if `identity_file` is not given).  Defaults to
            # ~/.reticulum so the gateway joins whatever mesh the host is
            # already part of.
            config_dir: ""
            # Optional path to a Reticulum identity file to use as the
            # gateway's LXMF identity.  If omitted, a per-run identity is
            # generated and persisted under the config_dir storage path.
            identity_file: ""
            # Display name announced for the gateway's delivery destination.
            display_name: "Hermes Agent"
            # Optional outbound LXMF propagation node (destination hash or
            # identity-hash hex) for relayed delivery beyond direct peers.
            propagation_node: ""
            # Reticulum proof strategy for the delivery destination.
            #   "none"  -> PROVE_NONE  (default, replies to anyone)
            #   "app"   -> PROVE_APP
            #   "all"   -> PROVE_ALL
            proof_strategy: "none"
            # Reticulum stamp cost the gateway demands on inbound LXMF
            # messages (0 = none, must be <255).  0 by default so any peer
            # can reach the agent without proof-of-work.
            inbound_stamp_cost: 0
            # Allowed senders: list of Reticulum identity hashes (hex) that
            # may talk to the agent.  Empty = allow all (recommended to set
            # for production meshes).
            allowed_users: []
            max_message_length: 1024   # LXMF payload soft-cap

Or via environment variables (override config.yaml):

    LXMF_CONFIG_DIR, LXMF_IDENTITY_FILE, LXMF_DISPLAY_NAME,
    LXMF_PROPAGATION_NODE, LXMF_PROOF_STRATEGY, LXMF_INBOUND_STAMP_COST,
    LXMF_ALLOWED_USERS, LXMF_ALLOW_ALL_USERS, LXMF_HOME_DESTINATION
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

from agent.secret_scope import UnscopedSecretError as _UnscopedSecretError
from agent.secret_scope import get_secret as _scoped_get_secret


def _get_scoped_secret(name, default=None):
    """Scope-aware credential read with the default-profile startup fallback.

    Mirrors the pattern used by the IRC and Slack adapters: secondary
    profiles construct their adapter under a profile secret scope (which
    is authoritative), while the DEFAULT profile falls back to os.environ
    for its own value.
    """
    try:
        val = _scoped_get_secret(name, default)
    except _UnscopedSecretError:
        val = os.getenv(name)
    return val if val is not None else default


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Optional dependency import.
#
# The whole module must import without rns/lxmf present (plugin discovery
# loads adapter.py at startup).  We therefore import lazily inside the
# functions that need it.  Module-level aliases are populated by
# ``_import_rns()`` the first time Reticulum is actually required.
# ---------------------------------------------------------------------------

_RNS = None
_LXMF = None
_LXMRouter = None
_LXMessage = None
_DEPS_IMPORTED = False


def _import_rns() -> bool:
    """Import the rns / lxmf packages into module-level aliases.

    Returns True on success, False if either package is unavailable.  Safe
    to call repeatedly; the heavy import happens once.
    """
    global _RNS, _LXMF, _LXMRouter, _LXMessage, _DEPS_IMPORTED
    if _DEPS_IMPORTED:
        return _RNS is not None
    _DEPS_IMPORTED = True
    try:
        import RNS as _rns  # noqa: N813
        from LXMF import LXMF as _lxmf  # noqa: N813
        from LXMF import LXMRouter as _lxmrouter  # noqa: N813
        from LXMF import LXMessage as _lxmessage  # noqa: N813
        _RNS = _rns
        _LXMF = _lxmf
        _LXMRouter = _lxmrouter
        _LXMessage = _lxmessage
        return True
    except Exception as exc:  # pragma: no cover - exercised only without deps
        logger.debug("LXMF: optional dependency import failed: %s", exc)
        _RNS = None
        _LXMF = None
        _LXMRouter = None
        _LXMessage = None
        return False


def ensure_deps() -> bool:
    """ACTIVE installer: pip-install rns + lxmf if not already importable.

    Called by the gateway's ``create_adapter`` only when ``check_requirements``
    returns False and the platform is enabled.  Never called from status
    displays (that path uses the PASSIVE check below).
    """
    if _import_rns():
        return True
    try:
        import subprocess
        import sys

        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "rns", "lxmf"]
        )
    except Exception as exc:  # pragma: no cover - network/install failure
        logger.warning("LXMF: failed to pip-install rns/lxmf: %s", exc)
        return False
    return _import_rns()


def check_requirements() -> bool:
    """PASSIVE probe: are rns/lxmf importable right now? No side effects."""
    if _RNS is not None:
        return True
    return _import_rns()


# ---------------------------------------------------------------------------
# Base adapter
# ---------------------------------------------------------------------------

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.config import Platform


class LXMFAdapter(BasePlatformAdapter):
    """Async LXMF/Reticulum adapter implementing the BasePlatformAdapter
    interface.

    Inbound LXMF messages are received by the LXMRouter's delivery callback
    (which runs on Reticulum's internal thread) and bridged into the
    gateway's asyncio loop via ``call_soon_threadsafe`` -> ``handle_message``.
    Outbound agent responses are wrapped as LXMessages addressed to the
    originating sender's LXMF delivery destination and handed to the router.
    """

    # LXMF content is plain UTF-8 text; markdown is passed through but the
    # platform hint tells the agent it is a constrained mesh channel.
    PLATFORM_NAME = "lxmf"

    def __init__(self, config, **kwargs):
        platform = Platform("lxmf")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}

        # ---- Connection / identity settings (env overrides config.yaml) ----
        self.config_dir = os.getenv("LXMF_CONFIG_DIR") or extra.get("config_dir", "")
        self.identity_file = (
            os.getenv("LXMF_IDENTITY_FILE") or extra.get("identity_file", "")
        )
        self.display_name = (
            os.getenv("LXMF_DISPLAY_NAME") or extra.get("display_name", "Hermes Agent")
        )
        prop = os.getenv("LXMF_PROPAGATION_NODE") or extra.get("propagation_node", "")
        self.propagation_node = prop.strip() if isinstance(prop, str) else ""

        proof_env = (os.getenv("LXMF_PROOF_STRATEGY") or "").strip().lower()
        proof_cfg = str(extra.get("proof_strategy", "none")).strip().lower()
        proof = proof_env or proof_cfg or "none"
        self.proof_strategy = proof if proof in ("none", "app", "all") else "none"

        try:
            cost_env = os.getenv("LXMF_INBOUND_STAMP_COST")
            self.inbound_stamp_cost = int(
                cost_env if cost_env is not None else extra.get("inbound_stamp_cost", 0)
            )
        except (TypeError, ValueError):
            self.inbound_stamp_cost = 0
        if not (0 <= self.inbound_stamp_cost < 255):
            self.inbound_stamp_cost = 0

        # ---- Auth / identity ----
        self.allowed_users = list(extra.get("allowed_users", []) or [])
        # Normalise allowed identity hashes to lowercase for lookups.
        self._allowed_users_lower = {
            str(u).lower() for u in self.allowed_users if isinstance(u, str) and u
        }
        # Allow-all toggle (env wins).
        allow_all_env = (os.getenv("LXMF_ALLOW_ALL_USERS") or "").strip().lower()
        if allow_all_env in ("1", "true", "yes"):
            self.allow_all = True
        else:
            self.allow_all = bool(extra.get("allow_all_users", False))

        # ---- Limits ----
        try:
            max_msg = int(extra.get("max_message_length", 1024))
        except (TypeError, ValueError):
            max_msg = 1024
        self.max_message_length = max_msg or 1024

        # Home destination for cron / outbound delivery (a Reticulum
        # destination hash).  Defaults to the gateway's own delivery hash
        # once connected, but can be pinned to a fixed peer via
        # LXMF_HOME_DESTINATION so cron jobs always reach the operator.
        self.home_destination = (
            os.getenv("LXMF_HOME_DESTINATION") or extra.get("home_destination", "")
        )

        # ---- Runtime state (set in connect) ----
        self._reticulum = None
        self._router = None
        self._delivery_destination = None
        self._identity = None
        self._own_delivery_hash_hex = None
        self._loop = None
        # Map of active RNS.Destination objects per reply target hash so we
        # don't rebuild them on every response.  Guarded by a lock because
        # the delivery callback may run on the Reticulum thread.
        self._dest_lock = threading.Lock()
        self._reply_destinations: Dict[str, Any] = {}

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Start Reticulum + the LXMF router, bind a delivery destination,
        and register the inbound delivery callback."""
        if not ensure_deps():
            logger.error(
                "LXMF: rns/lxmf packages are not installed and could not be "
                "installed; cannot start the LXMF gateway platform."
            )
            self._set_fatal_error(
                "deps_missing",
                "rns/lxmf not available",
                retryable=False,
            )
            return False

        try:
            # Resolve the Reticulum config dir.  Empty config_dir means "use the
            # default location" (~/.reticulum), which is what we want for the
            # common case.  We always need a concrete directory so the LXMF
            # router has a storage path and the gateway identity can persist.
            config_dir = os.path.expanduser(self.config_dir) if self.config_dir else None
            if not config_dir:
                config_dir = _RNS.Reticulum.configdir or os.path.expanduser("~/.reticulum")
            config_dir = os.path.abspath(config_dir)

            # Reticulum must be a singleton for the process lifetime; if it is
            # already initialised (e.g. another Reticulum-based platform), reuse
            # it rather than re-constructing.
            if getattr(_RNS.Reticulum, "_Reticulum__instance", None) is None:
                self._reticulum = _RNS.Reticulum(config_dir)
            else:
                self._reticulum = _RNS.Reticulum._Reticulum__instance

            # Resolve the gateway identity.
            identity = self._load_identity()

            # Build the LXMF router with our identity.
            storagepath = os.path.join(config_dir, "storage", "lxmf")
            os.makedirs(storagepath, exist_ok=True)
            self._router = _LXMRouter(identity=identity, storagepath=storagepath)
            self._identity = identity

            # Register the gateway's delivery identity (single per router) and
            # capture its delivery destination.
            self._delivery_destination = self._router.register_delivery_identity(
                identity,
                display_name=self.display_name,
                stamp_cost=self.inbound_stamp_cost if self.inbound_stamp_cost else None,
            )

            # Proof strategy.
            proof_map = {
                "none": _RNS.Destination.PROVE_NONE,
                "app": _RNS.Destination.PROVE_APP,
                "all": _RNS.Destination.PROVE_ALL,
            }
            self._delivery_destination.set_proof_strategy(proof_map[self.proof_strategy])

            # Optional outbound propagation node.
            if self.propagation_node:
                self._set_propagation_node(self.propagation_node)

            # Capture the loop so the threaded delivery callback can marshal
            # inbound messages back onto the event loop.
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = None

            # Register the inbound delivery callback.
            self._router.register_delivery_callback(self._on_lxmf_delivery)

            # Announce ourselves so peers can discover the gateway.
            try:
                self._router.announce(self._delivery_destination.hash)
            except Exception as exc:  # pragma: no cover - announce best-effort
                logger.debug("LXMF: announce failed (non-fatal): %s", exc)

            self._own_delivery_hash_hex = self._delivery_destination.hash.hex()

            # If home destination was not pinned, default it to our own hash
            # so cron delivery has a target (operator can still override).
            if not self.home_destination:
                self.home_destination = self._own_delivery_hash_hex

            self._mark_connected()
            logger.info(
                "LXMF: connected — gateway delivery hash %s (display name %r)",
                self._own_delivery_hash_hex,
                self.display_name,
            )
            return True

        except Exception as exc:
            logger.error("LXMF: failed to start: %s", exc, exc_info=True)
            self._set_fatal_error("connect_failed", str(exc), retryable=True)
            return False

    def _load_identity(self):
        """Load the configured Reticulum identity, or generate + persist one."""
        if self.identity_file and os.path.isfile(self.identity_file):
            ident = _RNS.Identity.from_file(self.identity_file)
            if ident is not None:
                return ident
            logger.warning(
                "LXMF: identity_file %s invalid; generating ephemeral identity",
                self.identity_file,
            )
        # Persist a stable identity in the reticulum storage path so the
        # gateway's delivery hash (its address on the mesh) is stable across
        # restarts.  Falls back to an in-memory identity if storage is
        # unavailable.
        config_dir = self.config_dir or (_RNS.Reticulum.configdir or os.path.expanduser("~/.reticulum"))
        config_dir = os.path.abspath(os.path.expanduser(config_dir))
        persist = os.path.join(config_dir, "storage", "hermes_lxmf_identity")
        try:
            os.makedirs(os.path.dirname(persist), exist_ok=True)
            if os.path.isfile(persist):
                ident = _RNS.Identity.from_file(persist)
                if ident is not None:
                    return ident
            ident = _RNS.Identity()
            ident.to_file(persist)
            return ident
        except Exception as exc:  # pragma: no cover - storage failure
            logger.debug("LXMF: could not persist identity (%s); ephemeral", exc)
        return _RNS.Identity()

    def _set_propagation_node(self, node: str) -> None:
        """Set an outbound propagation node from a hex identity hash or hash."""
        try:
            node = node.strip()
            if not node:
                return
            # A propagation node is identified by its LXMF delivery destination
            # hash.  Users may supply the full hex hash of the node's identity
            # (in which case we derive the delivery hash) or the delivery hash
            # directly.
            if len(node) == 64:
                digest = _RNS.Destination.hash_from_name_and_identity(
                    "lxmf.delivery", _RNS.Identity.recall(bytes.fromhex(node), from_identity_hash=True)
                )
            else:
                digest = bytes.fromhex(node)
            self._router.set_outbound_propagation_node(digest)
        except Exception as exc:  # pragma: no cover - resolution failure
            logger.warning("LXMF: could not resolve propagation node %r: %s", node, exc)

    async def disconnect(self) -> None:
        """Tear down the LXMF router and Reticulum instance."""
        self._mark_disconnected()
        try:
            if self._router is not None:
                # Best-effort shutdown; LXMRouter registers atexit handlers.
                with self._dest_lock:
                    self._reply_destinations.clear()
                self._router = None
        except Exception as exc:  # pragma: no cover
            logger.debug("LXMF: error during disconnect: %s", exc)
        self._delivery_destination = None
        self._identity = None
        # Note: we intentionally do NOT tear down the process-global Reticulum
        # singleton (it may be shared with other platforms); it cleans up on
        # process exit.

    # ── Inbound delivery ──────────────────────────────────────────────────

    def _on_lxmf_delivery(self, lxmessage) -> None:
        """LXMRouter delivery callback (runs on the Reticulum thread).

        Bridges the inbound LXMF message onto the gateway's asyncio loop so
        the agent session loop can process it.
        """
        try:
            source_hash = lxmessage.source_hash
            source_hex = source_hash.hex() if isinstance(source_hash, (bytes, bytearray)) else str(source_hash)
            content = lxmessage.content_as_string()
            if content is None:
                return
            title = ""
            try:
                title = lxmessage.title_as_string() or ""
            except Exception:
                title = ""

            # Authorisation (identity-hash allowlist).
            if not self.allow_all:
                if self._allowed_users_lower and source_hex.lower() not in self._allowed_users_lower:
                    logger.debug("LXMF: ignoring message from unauthorized source %s", source_hex)
                    return

            # Defensive: never echo our own messages back into the loop.
            if source_hex == self._own_delivery_hash_hex:
                return

            if self._loop is not None:
                self._loop.call_soon_threadsafe(
                    lambda lm=lxmessage, sh=source_hex, c=content, t=title: asyncio.ensure_future(
                        self._dispatch_lxmf(lm, sh, c, t)
                    )
                )
            else:  # pragma: no cover - loop missing only in tests
                logger.warning("LXMF: no event loop available; dropping inbound message")
        except Exception as exc:
            logger.warning("LXMF: error in delivery callback: %s", exc, exc_info=True)

    async def _dispatch_lxmf(self, lxmessage, source_hex: str, content: str, title: str) -> None:
        """Build a MessageEvent from an inbound LXMF message and dispatch it."""
        if not self._message_handler:
            return

        # The LXMF title (if present) is surfaced as the chat/user name so the
        # agent can tell humans apart on the mesh.  The RNS identity hash is
        # the durable chat_id and user_id — Reticulum has no usernames.
        display = (title.strip() or source_hex[:8]) if title else source_hex[:8]

        source = self.build_source(
            chat_id=source_hex,
            chat_name=title.strip() or source_hex,
            chat_type="dm",
            user_id=source_hex,
            user_name=display,
            message_id=lxmessage.hash.hex() if getattr(lxmessage, "hash", None) else None,
        )

        event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=source.message_id,
            timestamp=__import__("datetime").datetime.now(),
        )

        # Stash the originating LXMessage so send() can reply to the exact
        # source destination without re-deriving it.
        event._lxmf_source_hash = source_hex

        await self.handle_message(event)

    # ── Sending ───────────────────────────────────────────────────────────

    def _reply_destination_for(self, target_hash_hex: str):
        """Return (cached) OUTBOUND RNS.Destination for an LXMF peer.

        ``target_hash_hex`` is the sender's RNS identity hash (the ``chat_id``
        we stored at inbound time).  We resolve the peer's public key via
        ``RNS.Identity.recall`` (Reticulum remembers the public key from the
        inbound announce/LXMF delivery) and build the matching
        ``lxmf.delivery`` OUTBOUND destination.  If the peer isn't yet known we
        fall back to deriving the delivery hash from the identity hash hex and
        constructing a public-key-only destination — the path will be requested
        opportunistically when the message is sent.
        """
        with self._dest_lock:
            cached = self._reply_destinations.get(target_hash_hex)
            if cached is not None:
                return cached

        node = target_hash_hex.strip()
        try:
            # Resolve the peer's public key identity.  recall() looks up the
            # destination hash directly (the inbound LXMF delivery registered
            # it), or by identity hash.
            ident = None
            try:
                if len(node) == 64:
                    ident = _RNS.Identity.recall(bytes.fromhex(node), from_identity_hash=True)
                else:
                    ident = _RNS.Identity.recall(bytes.fromhex(node))
            except Exception as exc:
                logger.debug("LXMF: recall failed for %s: %s", node, exc)

            if ident is None:
                # Fallback: build a public-key-only identity from the hash so
                # an OUTBOUND destination can be constructed; Reticulum will
                # request the path on send.
                ident = _RNS.Identity(create_keys=False)
                # Wrap the hash as a placeholder public key surface so the
                # destination hash computation matches the peer's.  recall()
                # already did this internally; here we only reach this branch
                # when the peer is not yet known, in which case we still need
                # a valid destination object.  We derive the delivery hash and
                # let Reticulum resolve the key opportunistically.
                ident.load_public_key(_RNS.Identity.truncated_hash(bytes.fromhex(node))[:_RNS.Identity.KEYSIZE // 8 // 2] * 2
                                      if len(node) == 64 else bytes.fromhex(node))

            dest = _RNS.Destination(
                ident, _RNS.Destination.OUT, _RNS.Destination.SINGLE,
                _LXMF.APP_NAME, "delivery",
            )
            with self._dest_lock:
                self._reply_destinations[target_hash_hex] = dest
            return dest
        except Exception as exc:
            logger.warning("LXMF: could not build reply destination for %s: %s", target_hash_hex, exc)
            return None

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if self._router is None:
            return SendResult(success=False, error="LXMF gateway not connected")

        # Resolve the reply target.  Prefer the session-sourced RNS hash that
        # came in via chat_id (which is the sender's identity hash).  We use
        # chat_id directly as the target (it is the RNS identity hash).
        target_hash = chat_id
        if not target_hash:
            return SendResult(success=False, error="No LXMF destination (chat_id)")

        destination = self._reply_destination_for(target_hash)
        if destination is None:
            return SendResult(success=False, error="Could not resolve LXMF destination")

        # Soft-cap long messages.
        payload = content
        if self.max_message_length and len(payload) > self.max_message_length:
            payload = payload[: self.max_message_length]

        try:
            msg = _LXMessage(
                destination,
                self._delivery_destination,
                content=payload,
                title=self.display_name,
                desired_method=_LXMessage.DIRECT,
            )
            # Allow the router to handle propagation/opportunistic fallback.
            self._router.handle_outbound(msg)
            return SendResult(success=True, message_id=msg.hash.hex() if getattr(msg, "hash", None) else None)
        except Exception as exc:
            logger.error("LXMF: failed to send message: %s", exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """LXMF has no typing indicator — no-op."""
        pass

    async def send_image(self, chat_id: str, image_url: str, caption=None) -> SendResult:
        # LXMF carries text only; degrade gracefully to sending the caption or
        # a link text.
        text = caption or f"[image] {image_url}"
        return await self.send(chat_id, text)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {
            "name": chat_id[:8] if chat_id else chat_id,
            "type": "dm",
            "chat_id": chat_id,
        }

    # ── Misc helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _is_valid_hash(value: str) -> bool:
        """Return True if value is a hex Reticulum hash (delivery or identity)."""
        if not isinstance(value, str):
            return False
        return bool(re.fullmatch(r"[0-9a-fA-F]{1,64}", value.strip()))


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

def validate_config(config) -> bool:
    """Validate that the platform config is minimally usable."""
    extra = getattr(config, "extra", {}) or {}
    # LXMF needs Reticulum (deps).  If deps are present, the platform can
    # start (it generates/persists an identity automatically).  We treat the
    # config as valid when deps are importable.
    return check_requirements()


def is_connected(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    # Connected iff deps are present (the gateway will generate an identity).
    return check_requirements()


def _env_enablement() -> dict | None:
    """Seed PlatformConfig.extra from env vars during config load."""
    if not check_requirements():
        return None
    seed: dict = {}
    if os.getenv("LXMF_CONFIG_DIR"):
        seed["config_dir"] = os.getenv("LXMF_CONFIG_DIR")
    if os.getenv("LXMF_IDENTITY_FILE"):
        seed["identity_file"] = os.getenv("LXMF_IDENTITY_FILE")
    if os.getenv("LXMF_DISPLAY_NAME"):
        seed["display_name"] = os.getenv("LXMF_DISPLAY_NAME")
    if os.getenv("LXMF_PROPAGATION_NODE"):
        seed["propagation_node"] = os.getenv("LXMF_PROPAGATION_NODE")
    if os.getenv("LXMF_PROOF_STRATEGY"):
        seed["proof_strategy"] = os.getenv("LXMF_PROOF_STRATEGY")
    if os.getenv("LXMF_INBOUND_STAMP_COST"):
        try:
            seed["inbound_stamp_cost"] = int(os.getenv("LXMF_INBOUND_STAMP_COST"))
        except ValueError:
            pass
    if os.getenv("LXMF_HOME_DESTINATION"):
        seed["home_channel"] = {"chat_id": os.getenv("LXMF_HOME_DESTINATION")}
    return seed or None


def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process delivery for cron jobs / send_message tool.

    Opens a short-lived Reticulum + LXMRouter, sends a single LXMF message to
    ``chat_id`` (the target's RNS identity/delivery hash), and tears down.
    This is used when the cron/send process is separate from the gateway.
    """
    if not ensure_deps():
        return {"error": "LXMF standalone send: rns/lxmf not available"}

    extra = getattr(pconfig, "extra", {}) or {}
    config_dir = os.getenv("LXMF_CONFIG_DIR") or extra.get("config_dir", "") or None
    try:
        reticulum = _RNS.Reticulum(config_dir)
    except Exception as exc:
        return {"error": f"LXMF standalone send: Reticulum init failed: {exc}"}

    try:
        # Resolve identity (prefer persisted / configured).
        identity_file = os.getenv("LXMF_IDENTITY_FILE") or extra.get("identity_file", "")
        if identity_file and os.path.isfile(identity_file):
            identity = _RNS.Identity.from_file(identity_file)
        elif config_dir:
            persist = os.path.join(config_dir, "storage", "hermes_lxmf_identity")
            if os.path.isfile(persist):
                identity = _RNS.Identity.from_file(persist)
            else:
                identity = _RNS.Identity()
                identity.to_file(persist)
        else:
            identity = _RNS.Identity()

        storagepath = None
        if config_dir:
            storagepath = os.path.join(config_dir, "storage", "lxmf")
            os.makedirs(storagepath, exist_ok=True)
        router = _LXMRouter(identity=identity, storagepath=storagepath)

        display_name = os.getenv("LXMF_DISPLAY_NAME") or extra.get("display_name", "Hermes Agent")
        delivery = router.register_delivery_identity(identity, display_name=display_name)

        target = chat_id.strip()
        ident = None
        try:
            if len(target) == 64:
                ident = _RNS.Identity.recall(bytes.fromhex(target), from_identity_hash=True)
            else:
                ident = _RNS.Identity.recall(bytes.fromhex(target))
        except Exception as exc:
            logger.debug("LXMF standalone: recall failed for %s: %s", target, exc)
        if ident is None:
            ident = _RNS.Identity(create_keys=False)
            try:
                ident.load_public_key(
                    _RNS.Identity.truncated_hash(bytes.fromhex(target))[: _RNS.Identity.KEYSIZE // 8 // 2] * 2
                    if len(target) == 64 else bytes.fromhex(target)
                )
            except Exception:
                pass

        dest = _RNS.Destination(
            ident, _RNS.Destination.OUT, _RNS.Destination.SINGLE,
            _LXMF.APP_NAME, "delivery",
        )

        max_msg = int(extra.get("max_message_length", 1024)) or 1024
        payload = message
        if len(payload) > max_msg:
            payload = payload[:max_msg]

        msg = _LXMessage(destination=dest, source=delivery, content=payload, title=display_name)
        router.handle_outbound(msg)

        # Give the router a moment to dispatch before we tear down.
        time.sleep(2.0)
        return {"success": True, "message_id": msg.hash.hex() if getattr(msg, "hash", None) else None}
    except Exception as exc:
        logger.debug("LXMF standalone send raised", exc_info=True)
        return {"error": f"LXMF standalone send failed: {exc}"}
    finally:
        # Reticulum teardown is best-effort; process exit cleans up.
        pass


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="lxmf",
        label="LXMF / Reticulum",
        adapter_factory=lambda cfg: LXMFAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=[],
        install_hint="pip install rns lxmf",
        ensure_deps_fn=ensure_deps,
        setup_fn=None,  # config.yaml / env only for now
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="LXMF_HOME_DESTINATION",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="LXMF_ALLOWED_USERS",
        allow_all_env="LXMF_ALLOW_ALL_USERS",
        max_message_length=1024,
        emoji="📡",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting over LXMF on a Reticulum mesh — a text-only, "
            "low-bandwidth, often intermittently-connected radio/network layer. "
            "Keep responses concise and self-contained. Markdown is tolerated but "
            "may not render; prefer plain text. Long messages are soft-capped to "
            "~1KB. There is no voice, images, typing indicator, or threading."
        ),
    )
