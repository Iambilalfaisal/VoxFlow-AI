import db.session as session_module


def test_resolve_read_dsn_falls_back_to_write_dsn_when_unset(monkeypatch):
    monkeypatch.setattr(session_module.settings, "database_read_url", None)
    monkeypatch.setattr(
        session_module.settings, "database_url", "postgresql+asyncpg://write-dsn/voxflow"
    )

    assert session_module._resolve_read_dsn() == "postgresql+asyncpg://write-dsn/voxflow"


def test_resolve_read_dsn_uses_override_when_set(monkeypatch):
    monkeypatch.setattr(
        session_module.settings, "database_read_url", "postgresql+asyncpg://replica-dsn/voxflow"
    )
    monkeypatch.setattr(
        session_module.settings, "database_url", "postgresql+asyncpg://write-dsn/voxflow"
    )

    assert session_module._resolve_read_dsn() == "postgresql+asyncpg://replica-dsn/voxflow"


def test_read_engine_currently_falls_back_to_write_dsn():
    # Sanity check against the actual module-level engines built at import
    # time: with no database_read_url configured in this environment's
    # .env, the read engine must point at the same DSN as the write engine.
    if session_module.settings.database_read_url is not None:
        return  # environment has an override configured; nothing to assert here

    assert str(session_module.read_engine.url) == str(session_module.engine.url)
