"""Tests for the LXMF / Reticulum platform adapter plugin.

These tests exercise the adapter's configuration parsing, the optional
dependency probe, the plugin registration entry point, and the routing
helpers WITHOUT requiring a live Reticulum mesh.  The heavy rns/lxmf
packages are optional — we import them lazily and skip the parts that
need a real mesh when they are not installed.
"""

import os

import pytest

from tests.gateway._plugin_adapter_loader import load_plugin_adapter

# Load plugins/platforms/lxmf/adapter.py under a unique module name so it
# cannot collide with other plugin adapters loaded by sibling tests.
_lxmf_mod = load_plugin_adapter("lxmf")

LXMFAdapter = _lxmf_mod.LXMFAdapter
check_requirements = _lxmf_mod.check_requirements
validate_config = _lxmf_mod.validate_config
is_connected = _lxmf_mod.is_connected
register = _lxmf_mod.register
ensure_deps = _lxmf_mod.ensure_deps


# ---------------------------------------------------------------------------
# Dependency probe
# ---------------------------------------------------------------------------


class TestLXMFRequirements:

    def test_check_requirements_returns_bool(self):
        # The passive probe must never raise and must return a bool.
        result = check_requirements()
        assert isinstance(result, bool)

    def test_validate_config_from_extra(self, monkeypatch):
        # With deps present (the test env may or may not have them) validate
        # must not raise; without deps it should return False.
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(extra={})
        out = validate_config(cfg)
        assert isinstance(out, bool)
        if not check_requirements():
            assert out is False

    def test_is_connected_returns_bool(self, monkeypatch):
        from gateway.config import PlatformConfig

        assert isinstance(is_connected(PlatformConfig(extra={})), bool)


# ---------------------------------------------------------------------------
# Adapter configuration parsing
# ---------------------------------------------------------------------------


class TestLXMFAdapterInit:

    def test_init_from_config_extra(self, monkeypatch):
        for key in (
            "LXMF_CONFIG_DIR", "LXMF_IDENTITY_FILE", "LXMF_DISPLAY_NAME",
            "LXMF_PROPAGATION_NODE", "LXMF_PROOF_STRATEGY",
            "LXMF_INBOUND_STAMP_COST", "LXMF_ALLOW_ALL_USERS",
        ):
            monkeypatch.delenv(key, raising=False)

        from gateway.config import PlatformConfig

        cfg = PlatformConfig(
            enabled=True,
            extra={
                "config_dir": "/tmp/reticulum",
                "display_name": "Test Bot",
                "proof_strategy": "none",
                "inbound_stamp_cost": 0,
                "allowed_users": [],
                "max_message_length": 1024,
            },
        )
        adapter = LXMFAdapter(cfg)

        assert adapter.config_dir == "/tmp/reticulum"
        assert adapter.display_name == "Test Bot"
        assert adapter.proof_strategy == "none"
        assert adapter.inbound_stamp_cost == 0
        assert adapter.max_message_length == 1024
        # No env -> allow-all defaults off but allowed_users empty => still a
        # real adapter object.
        assert adapter.allow_all is False

    def test_env_overrides_config(self, monkeypatch):
        monkeypatch.setenv("LXMF_DISPLAY_NAME", "Env Bot")
        monkeypatch.setenv("LXMF_PROOF_STRATEGY", "all")
        monkeypatch.setenv("LXMF_ALLOW_ALL_USERS", "true")
        # Clear the rest
        for key in ("LXMF_CONFIG_DIR", "LXMF_IDENTITY_FILE", "LXMF_PROPAGATION_NODE"):
            monkeypatch.delenv(key, raising=False)

        from gateway.config import PlatformConfig

        cfg = PlatformConfig(
            enabled=True,
            extra={"display_name": "Yaml Bot", "proof_strategy": "none"},
        )
        adapter = LXMFAdapter(cfg)
        assert adapter.display_name == "Env Bot"
        assert adapter.proof_strategy == "all"
        assert adapter.allow_all is True

    def test_invalid_stamp_cost_clamped(self, monkeypatch):
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(extra={"inbound_stamp_cost": 9999})
        adapter = LXMFAdapter(cfg)
        # Anything >=255 (or non-int) is clamped to 0.
        assert adapter.inbound_stamp_cost == 0

    def test_allowed_users_normalised_lowercase(self, monkeypatch):
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(extra={"allowed_users": ["AbCdEf", "123456"]})
        adapter = LXMFAdapter(cfg)
        assert adapter._allowed_users_lower == {"abcdef", "123456"}


# ---------------------------------------------------------------------------
# Identity-hash validation helper
# ---------------------------------------------------------------------------


class TestLXMFHashHelper:

    def test_valid_identity_and_delivery_hashes(self):
        assert LXMFAdapter._is_valid_hash("a" * 64) is True
        assert LXMFAdapter._is_valid_hash("abcdef0123") is True

    def test_invalid_hashes(self):
        assert LXMFAdapter._is_valid_hash("") is False
        assert LXMFAdapter._is_valid_hash("not hex!!") is False
        assert LXMFAdapter._is_valid_hash(None) is False


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


class TestLXMFRegistration:

    def test_register_calls_registry(self, monkeypatch):
        from gateway.platform_registry import platform_registry

        platform_registry.unregister("lxmf")

        captured = {}
        ctx = type("Ctx", (), {"register_platform": lambda self, **kw: captured.update(kw)})()

        register(ctx)
        assert captured.get("name") == "lxmf"
        assert captured.get("label") == "LXMF / Reticulum"
        assert captured.get("emoji") == "📡"
        # Hooks that make cron + env auto-config work must be present.
        assert "standalone_sender_fn" in captured
        assert "env_enablement_fn" in captured

    def test_ensure_deps_returns_bool(self):
        # Never raises; returns a bool.
        assert isinstance(ensure_deps(), bool)


# ---------------------------------------------------------------------------
# End-to-end with the real rns/lxmf packages (skipped if absent)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not check_requirements(), reason="rns/lxmf not installed")
class TestLXMFWithRealDeps:
    """Exercises the adapter against the REAL rns/lxmf packages.

    Reticulum is a process-global singleton, so we build one instance for the
    whole module (exactly as the adapter does via ensure_deps / Reticulum())
    and reuse it across tests.
    """

    def test_reply_destination_recall(self, reticulum):
        """The reply destination for a known peer must resolve to the same
        destination hash we registered — proving responses will route back."""
        from gateway.config import PlatformConfig

        mod = _lxmf_mod
        RNS = mod._RNS
        LXMRouter = mod._LXMRouter

        ident = RNS.Identity()
        router = LXMRouter(identity=ident, storagepath=os.path.join(reticulum, "storage", "lxmf"))
        deliv = router.register_delivery_identity(ident, display_name="Test")

        # Simulate the peer being known (Reticulum remembers public keys from
        # inbound deliveries/announces).
        RNS.Identity.remember(b"\x00" * 10, deliv.hash, ident.get_public_key(), None)

        adapter = LXMFAdapter(PlatformConfig(enabled=True, extra={"config_dir": reticulum}))
        dest = adapter._reply_destination_for(deliv.hash.hex())
        assert dest is not None
        assert dest.hash == deliv.hash

    def test_outbound_lxmessage_wraps(self, reticulum):
        """An agent response is wrapped as an LXMF message with the right
        content/title and a resolvable destination."""
        from gateway.config import PlatformConfig

        mod = _lxmf_mod
        RNS = mod._RNS
        LXMRouter = mod._LXMRouter
        LXMF = mod._LXMF

        ident = RNS.Identity()
        router = LXMRouter(identity=ident, storagepath=os.path.join(reticulum, "storage", "lxmf"))
        deliv = router.register_delivery_identity(ident, display_name="Test")

        peer_ident = RNS.Identity()
        peer_deliv = RNS.Destination(
            peer_ident, RNS.Destination.IN, RNS.Destination.SINGLE, LXMF.APP_NAME, "delivery"
        )
        msg = mod._LXMessage(deliv, peer_deliv, content="hello agent", title="Sam")
        assert msg.content_as_string() == "hello agent"
        assert msg.title_as_string() == "Sam"


@pytest.fixture(scope="module")
def reticulum():
    import tempfile

    mod = _lxmf_mod
    RNS = mod._RNS
    cfgdir = tempfile.mkdtemp(prefix="lxmf_test_")
    # The adapter instantiates Reticulum once per process; do the same here.
    if getattr(RNS.Reticulum, "_Reticulum__instance", None) is None:
        RNS.Reticulum(cfgdir)
    return cfgdir
