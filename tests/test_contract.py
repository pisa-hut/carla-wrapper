from importlib.metadata import PackageNotFoundError
from pathlib import Path

import pytest
from pisa_api.conversions import init_response_from_proto, init_response_to_proto
from pisa_api.simulator import (
    GenericSimulatorService,
    InitRequest,
    InitResponse,
    SimulatorUnavailable,
)


def test_wrapper_version_uses_distribution_metadata(monkeypatch) -> None:
    from carla_wrapper import version

    monkeypatch.setattr(version, "version", lambda name: "9.8.7" if name == "carla-wrapper" else "")
    assert version.wrapper_version() == "9.8.7"


def test_wrapper_version_falls_back_to_pyproject(monkeypatch, tmp_path: Path) -> None:
    from carla_wrapper import version

    package = tmp_path / "carla_wrapper"
    package.mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "carla-wrapper"\nversion = "1.2.3"\n', encoding="utf-8"
    )
    monkeypatch.setattr(version, "__file__", str(package / "version.py"))
    monkeypatch.setattr(
        version, "version", lambda _name: (_ for _ in ()).throw(PackageNotFoundError())
    )
    assert version.wrapper_version() == "1.2.3"


def test_wrapper_version_is_non_empty() -> None:
    from carla_wrapper.version import wrapper_version

    assert wrapper_version().strip()


def test_server_passes_stable_identity_version_and_formats(monkeypatch) -> None:
    from carla_wrapper import server

    calls = []
    adapter = object()
    monkeypatch.setattr(server, "CarlaAdapter", lambda: adapter)
    monkeypatch.setattr(server, "wrapper_version", lambda: "2.3.4")
    monkeypatch.setattr(
        server, "serve_simulator", lambda simulator, **kwargs: calls.append((simulator, kwargs))
    )

    server.main()

    assert calls == [
        (
            adapter,
            {
                "name": "carla-wrapper",
                "version": "2.3.4",
                "scenario_formats": {"open_scenario1", "carla_lb_route"},
            },
        )
    ]


class _Context:
    def peer(self) -> str:
        return "test"


def test_ping_has_explicit_message_name_and_version() -> None:
    service = GenericSimulatorService(object(), name="carla-wrapper", version="2.3.4")
    pong = service.Ping(None, _Context())
    assert pong.msg == "carla-wrapper alive"
    assert pong.name == "carla-wrapper"
    assert pong.version == "2.3.4"


class _FakeClient:
    def get_client_version(self) -> str:
        return "0.9.16-client"


def _initialized_adapter(monkeypatch):
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = True
    adapter._server_version = "0.9.16-server"
    adapter._client = _FakeClient()
    calls = []
    monkeypatch.setattr(adapter, "_ensure_connected", lambda: calls.append("connect"))
    monkeypatch.setattr(adapter, "_prepare_reused_server_state", lambda: calls.append("prepare"))
    return adapter, calls


def _is_safe_struct_value(value) -> bool:
    if value is None or isinstance(value, (bool, int, float, str)):
        return True
    if isinstance(value, list):
        return all(_is_safe_struct_value(item) for item in value)
    if isinstance(value, dict):
        return all(
            isinstance(key, str) and _is_safe_struct_value(item) for key, item in value.items()
        )
    return False


def test_init_returns_round_trippable_component_metadata(monkeypatch) -> None:
    adapter, calls = _initialized_adapter(monkeypatch)
    monkeypatch.setenv("CARLA_HOST", "localhost")
    monkeypatch.setenv("CARLA_PORT", "2000")
    monkeypatch.setenv("CARLA_TM_PORT", "8000")

    response = adapter.init(InitRequest(dt=0.05))

    assert isinstance(response, InitResponse)
    assert response.name == "carla"
    assert isinstance(response.metadata, dict)
    assert response.metadata["client_version"] == "0.9.16-client"
    assert response.metadata["server_version"] == "0.9.16-server"
    assert response.metadata["traffic_manager_port"] == 8000
    assert response.metadata["config"]["synchronous_mode"] is True
    assert response.metadata["config"]["no_rendering_mode"] is True
    assert response.metadata["config"]["allow_async_world_lifecycle"] is False
    assert response.metadata["config"]["reload_world_between_episodes"] is False
    assert response.metadata["config"]["physics_substepping"] is True
    assert response.metadata["config"]["physics_max_substep_delta_seconds"] == 0.01
    assert response.metadata["config"]["physics_max_substeps"] == 10
    assert response.metadata["config"]["kinematic_speed_deadband_mps"] == 0.02
    assert response.metadata["config"]["ackermann_speed_kp"] == 0.5
    assert response.metadata["config"]["ackermann_brake_jerk_default"] == 8.0
    assert "carla_connect_timeout_seconds" not in response.metadata["config"]
    assert "retry_interval_seconds" not in response.metadata["config"]
    assert _is_safe_struct_value(response.metadata)
    assert init_response_from_proto(init_response_to_proto(response)) == response
    assert calls == ["connect", "prepare"]


@pytest.mark.parametrize(
    ("reload_value", "expected"),
    [(None, False), (False, False), (True, True)],
)
def test_reload_world_requires_explicit_opt_in(monkeypatch, reload_value, expected) -> None:
    adapter, _ = _initialized_adapter(monkeypatch)

    response = adapter.init(
        InitRequest(
            dt=0.05,
            config={
                "reload_world_between_episodes": reload_value,
            },
        )
    )

    assert response.metadata["config"]["allow_async_world_lifecycle"] is False
    assert response.metadata["config"]["reload_world_between_episodes"] is expected


@pytest.mark.parametrize(
    ("legacy_mode", "expected_async"),
    [("strict", False), ("standard", True)],
)
def test_legacy_determinism_mode_maps_to_lifecycle_option(
    monkeypatch, legacy_mode, expected_async
) -> None:
    adapter, _ = _initialized_adapter(monkeypatch)

    response = adapter.init(InitRequest(dt=0.05, config={"determinism_mode": legacy_mode}))

    assert response.metadata["config"]["allow_async_world_lifecycle"] is expected_async


def test_init_failure_returns_no_response_and_preserves_exception(monkeypatch) -> None:
    adapter, calls = _initialized_adapter(monkeypatch)

    def fail_connect() -> None:
        calls.append("connect")
        raise SimulatorUnavailable("CARLA unavailable")

    monkeypatch.setattr(adapter, "_ensure_connected", fail_connect)

    with pytest.raises(SimulatorUnavailable, match="CARLA unavailable"):
        adapter.init(InitRequest(dt=0.05))

    assert calls == ["connect"]
