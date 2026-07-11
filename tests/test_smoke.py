"""Smoke tests for the pisa-api simulator-friendly contract."""

import ast
import math
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from pisa_api.simulator import (
    ActorRole,
    ControlCommand,
    ControlMode,
    InvalidSimulatorRequest,
    RuntimeFrameData,
    SimulatorPreconditionFailed,
    SimulatorTimeout,
    SimulatorUnavailable,
    StepRequest,
)


def _parse_flat_yaml_config(path: Path) -> dict[str, object]:
    config = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        key, raw_value = (part.strip() for part in line.split(":", 1))
        lowered = raw_value.lower()
        if lowered in {"true", "false"}:
            value = lowered == "true"
        else:
            try:
                value = float(raw_value) if "." in raw_value else int(raw_value)
            except ValueError:
                value = raw_value.strip("'\"")
        config[key] = value
    return config


def test_default_config_matches_config_example() -> None:
    from carla_wrapper.simulation import DEFAULT_CONFIG

    example_path = Path(__file__).parents[1] / "config_example.yaml"
    example_config = _parse_flat_yaml_config(example_path)

    assert example_config == DEFAULT_CONFIG
    assert {key: type(value) for key, value in example_config.items()} == {
        key: type(value) for key, value in DEFAULT_CONFIG.items()
    }


def test_every_default_config_key_is_used_by_runtime() -> None:
    from carla_wrapper import simulation

    tree = ast.parse(Path(simulation.__file__).read_text(encoding="utf-8"))
    used_keys = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        key_arg = None
        if node.func.attr == "_config_value" and node.args:
            key_arg = node.args[0]
        elif node.func.attr == "_deadband_value" and len(node.args) >= 2:
            key_arg = node.args[1]
        if isinstance(key_arg, ast.Constant) and isinstance(key_arg.value, str):
            used_keys.add(key_arg.value)

    assert used_keys == set(simulation.DEFAULT_CONFIG)


def test_public_imports_use_pisa_api_simulator_contract() -> None:
    from pisa_api.simulator import RuntimeFrameData as PisaRuntimeFrameData

    from carla_wrapper.simulation import CarlaAdapter

    assert CarlaAdapter.init.__annotations__["request"].__name__ == "InitRequest"
    assert CarlaAdapter.reset.__annotations__["request"].__name__ == "ResetRequest"
    assert CarlaAdapter.step.__annotations__["request"].__name__ == "StepRequest"
    assert PisaRuntimeFrameData.__name__ == "RuntimeFrameData"
    assert CarlaAdapter is not None


def test_step_ticks_scenario_runner_then_world() -> None:
    from carla_wrapper import simulation

    calls = []
    simulation.carla = SimpleNamespace(VehicleControl=_FakeVehicleControl)
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._world = _FakeWorld(calls)
    adapter._sync = True
    adapter._time_ns = 0
    adapter._dt_ns = 50_000_000
    adapter._last_carla_frame = 0
    adapter._tick_scenario_runner_module = lambda: calls.append("scenario_runner")
    adapter._ego_vehicle = SimpleNamespace(
        apply_control=lambda control: calls.append(("control", control)),
    )
    adapter._yaw_sign = -1.0
    adapter._collect_runtime_frame = lambda **kwargs: RuntimeFrameData(
        sim_time_ns=kwargs["sim_time_ns"]
    )

    ctrl = ControlCommand(
        mode=ControlMode.THROTTLE_STEER_BREAK,
        payload={"throttle": 0.0, "brake": 1.0, "steer": 0.0},
    )

    response = adapter.step(StepRequest(ctrl_cmd=ctrl, timestamp_ns=50_000_000))

    assert calls[0] == "scenario_runner"
    assert calls[1][0] == "control"
    assert calls[2] == "world_tick"
    assert isinstance(response.frame, RuntimeFrameData)


def test_step_uses_absolute_timestamp_and_advances_multiple_fixed_ticks() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = _FakeWorld(calls)
    adapter._sync = True
    adapter._time_ns = 0
    adapter._dt_ns = 50_000_000
    adapter._last_carla_frame = 0
    adapter._ego_vehicle = object()
    adapter._tick_scenario_runner_module = lambda: calls.append("scenario_runner")
    adapter._apply_ctrl = lambda ctrl: calls.append("control")
    adapter._collect_runtime_frame = lambda **kwargs: RuntimeFrameData(
        sim_time_ns=kwargs["sim_time_ns"]
    )

    response = adapter.step(
        StepRequest(
            ctrl_cmd=ControlCommand(mode=ControlMode.NONE),
            timestamp_ns=100_000_000,
        )
    )

    assert calls == [
        "scenario_runner",
        "control",
        "world_tick",
        "scenario_runner",
        "control",
        "world_tick",
    ]
    assert response.frame.sim_time_ns == 100_000_000


@pytest.mark.parametrize("timestamp_ns", [-1, 25_000_000])
def test_step_rejects_regressing_or_off_grid_timestamp(timestamp_ns) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = object()
    adapter._sync = True
    adapter._time_ns = 0
    adapter._dt_ns = 50_000_000

    with pytest.raises(InvalidSimulatorRequest):
        adapter.step(
            StepRequest(
                ctrl_cmd=ControlCommand(mode=ControlMode.NONE),
                timestamp_ns=timestamp_ns,
            )
        )


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (
            AttributeError(
                "LanePosition 'roadId=22, laneId=-1, s=55.0, offset=0.4' does not exist"
            ),
            "LanePosition",
        ),
        (
            NotImplementedError("Negative target speeds are not yet supported"),
            "Negative target speeds",
        ),
    ],
)
def test_step_marks_deterministic_scenario_errors_as_precondition_failures(error, message) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = object()
    adapter._sync = True
    adapter._time_ns = 0
    adapter._dt_ns = 50_000_000
    adapter._tick_scenario_runner_module = lambda: (_ for _ in ()).throw(error)

    with pytest.raises(SimulatorPreconditionFailed, match=message):
        adapter.step(
            StepRequest(
                ctrl_cmd=ControlCommand(mode=ControlMode.NONE),
                timestamp_ns=50_000_000,
            )
        )


def test_should_quit_returns_response_message() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._quit_flag = True
    adapter._quit_msg = "ScenarioRunner finished with status SUCCESS"

    response = adapter.should_quit()

    assert response.should_quit is True
    assert response.msg == "ScenarioRunner finished with status SUCCESS"


def test_scenario_runner_tree_ticks_once_per_world_timestamp(monkeypatch) -> None:
    from carla_wrapper import simulation

    calls = []
    monkeypatch.setattr(
        simulation,
        "py_trees",
        SimpleNamespace(
            common=SimpleNamespace(
                Status=SimpleNamespace(RUNNING="RUNNING", FAILURE="FAILURE", INVALID="INVALID")
            )
        ),
    )
    monkeypatch.setattr(
        simulation,
        "GameTime",
        SimpleNamespace(on_carla_tick=lambda timestamp: calls.append("game_time")),
    )
    monkeypatch.setattr(
        simulation,
        "CarlaDataProvider",
        SimpleNamespace(on_carla_tick=lambda: calls.append("data_provider")),
    )

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._world = _FakeScenarioWorld()
    adapter._sr_scenario = object()
    adapter._sr_running = True
    adapter._sr_tree = _FakeScenarioTree(calls)
    adapter._ego_vehicle = SimpleNamespace(id=1)
    adapter._sr_last_tick_timestamp = None
    adapter._quit_flag = False

    adapter._tick_scenario_runner_module()
    adapter._tick_scenario_runner_module()

    assert calls == ["game_time", "data_provider", "tree_tick"]


def test_scenario_runner_completion_sets_quit_message(monkeypatch) -> None:
    from carla_wrapper import simulation

    calls = []
    monkeypatch.setattr(
        simulation,
        "py_trees",
        SimpleNamespace(
            common=SimpleNamespace(
                Status=SimpleNamespace(RUNNING="RUNNING", FAILURE="FAILURE", INVALID="INVALID")
            )
        ),
    )
    monkeypatch.setattr(
        simulation,
        "GameTime",
        SimpleNamespace(on_carla_tick=lambda timestamp: calls.append("game_time")),
    )
    monkeypatch.setattr(
        simulation,
        "CarlaDataProvider",
        SimpleNamespace(on_carla_tick=lambda: calls.append("data_provider")),
    )

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._world = _FakeScenarioWorld()
    adapter._sr_scenario = object()
    adapter._sr_running = True
    adapter._sr_tree = _FakeScenarioTree(calls, status="SUCCESS")
    adapter._ego_vehicle = SimpleNamespace(id=1)
    adapter._sr_last_tick_timestamp = None
    adapter._quit_flag = False
    adapter._quit_msg = ""

    adapter._tick_scenario_runner_module()

    response = adapter.should_quit()
    assert response.should_quit is True
    assert response.msg == "ScenarioRunner finished with status SUCCESS"


def test_finalize_destroys_collision_sensor_before_scenario_runner_cleanup() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._record = True
    adapter._client = SimpleNamespace(stop_recorder=lambda: calls.append("stop_recorder"))
    adapter._destroy_spawned_actors = lambda: calls.append("destroy_spawned_actors")
    adapter._stop_scenario_runner_module = lambda: calls.append("stop_scenario_runner")
    adapter._clear_dynamic_actors = lambda: calls.append("clear_dynamic_actors")

    adapter._finalize()

    assert calls == [
        "stop_recorder",
        "destroy_spawned_actors",
        "stop_scenario_runner",
        "clear_dynamic_actors",
    ]
    assert adapter._finalized is True


def test_finalize_continues_after_recorder_stop_failure() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._record = True
    adapter._client = SimpleNamespace(
        stop_recorder=lambda: (_ for _ in ()).throw(RuntimeError("recorder failed"))
    )
    adapter._destroy_spawned_actors = lambda: calls.append("destroy_spawned_actors")
    adapter._stop_scenario_runner_module = lambda: calls.append("stop_scenario_runner")
    adapter._clear_dynamic_actors = lambda: calls.append("clear_dynamic_actors")

    adapter._finalize()

    assert calls == [
        "destroy_spawned_actors",
        "stop_scenario_runner",
        "clear_dynamic_actors",
    ]
    assert adapter._finalized is True


def test_finalize_skips_recorder_when_record_disabled() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._record = False
    adapter._client = SimpleNamespace(stop_recorder=lambda: calls.append("stop_recorder"))
    adapter._destroy_spawned_actors = lambda: calls.append("destroy_spawned_actors")
    adapter._stop_scenario_runner_module = lambda: calls.append("stop_scenario_runner")
    adapter._clear_dynamic_actors = lambda: calls.append("clear_dynamic_actors")

    adapter._finalize()

    assert "stop_recorder" not in calls
    assert adapter._finalized is True


def test_finalize_clears_dynamic_actors_and_leaves_server_async() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    vehicle = _FakeActor(actor_id=1, type_id="vehicle.tesla.model3")
    traffic_light = _FakeActor(actor_id=2, type_id="traffic.traffic_light")
    world = _FakeSettingsWorld(
        [vehicle, traffic_light], synchronous_mode=True, fixed_delta_seconds=0.05
    )
    traffic_manager = _FakeTrafficManager()

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._client = SimpleNamespace(
        stop_recorder=lambda: None,
        get_trafficmanager=lambda port: traffic_manager,
    )
    adapter._world = world
    adapter._scenario_runner_tm_port = 8000
    adapter._destroy_spawned_actors = lambda: None
    adapter._stop_scenario_runner_module = lambda: None

    adapter._finalize()

    assert vehicle.destroy_calls == 1
    assert traffic_light.destroy_calls == 0
    assert world.settings.synchronous_mode is False
    assert world.settings.fixed_delta_seconds is None
    assert world.applied_settings is world.settings
    assert traffic_manager.sync_calls == [False]
    assert adapter._finalized is True


def test_init_finalizes_previous_run_and_prepares_reused_server() -> None:
    from pisa_api.simulator import InitResponse

    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = False
    adapter._server_version = "0.9.16-server"
    adapter._client = SimpleNamespace(get_client_version=lambda: "0.9.16-client")
    adapter._finalize = lambda: calls.append("finalize")
    adapter._ensure_connected = lambda: True
    adapter._prepare_reused_server_state = lambda: calls.append("prepare_reused_server")

    request = SimpleNamespace(
        output_dir="out",
        config={},
        scenario=SimpleNamespace(format="open_scenario1"),
        dt=0.05,
    )

    response = adapter.init(request)

    assert calls == ["finalize", "prepare_reused_server"]
    assert isinstance(response, InitResponse)
    assert response.name == "carla"
    assert adapter._wrapper_loaded_opendrive_digest is None


def test_connect_reports_local_carla_process_exit_as_unavailable(monkeypatch) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    monkeypatch.delenv("CARLA_HOST", raising=False)
    monkeypatch.delenv("CARLA_PORT", raising=False)
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter.config = {
        "carla_connect_timeout_seconds": 40,
        "retry_interval_seconds": 0,
    }
    adapter._server_version = None
    adapter._server_process = SimpleNamespace(pid=123, poll=lambda: 17)
    adapter._server_log_path = "/tmp/carla_server"
    adapter._connect = lambda timeout: (_ for _ in ()).throw(TimeoutError("RPC deadline"))

    with pytest.raises(SimulatorUnavailable) as exc_info:
        adapter._ensure_connected()

    message = str(exc_info.value)
    assert "process exited" in message
    assert "localhost:2000" in message
    assert "exit code 17" in message
    assert "TimeoutError: RPC deadline" in message
    assert "/tmp/carla_server/stderr.log" in message


def test_connect_reports_running_process_with_unresponsive_rpc_as_timeout(monkeypatch) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    monkeypatch.delenv("CARLA_HOST", raising=False)
    monkeypatch.delenv("CARLA_PORT", raising=False)
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter.config = {
        "carla_connect_timeout_seconds": 0,
        "retry_interval_seconds": 0,
    }
    adapter._server_version = None
    adapter._server_process = SimpleNamespace(pid=456, poll=lambda: None)
    adapter._connect = lambda timeout: (_ for _ in ()).throw(TimeoutError("RPC deadline"))

    with pytest.raises(SimulatorTimeout) as exc_info:
        adapter._ensure_connected()

    message = str(exc_info.value)
    assert "process is still running (pid 456)" in message
    assert "RPC did not respond" in message
    assert "TimeoutError: RPC deadline" in message


def test_connect_does_not_infer_remote_health_from_local_process(monkeypatch) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    monkeypatch.setenv("CARLA_HOST", "carla.example.test")
    monkeypatch.delenv("CARLA_PORT", raising=False)
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter.config = {
        "carla_connect_timeout_seconds": 0,
        "retry_interval_seconds": 0,
    }
    adapter._server_version = None
    adapter._server_process = SimpleNamespace(
        pid=789,
        poll=lambda: (_ for _ in ()).throw(AssertionError("must not inspect local process")),
    )
    adapter._connect = lambda timeout: (_ for _ in ()).throw(ConnectionError("refused"))

    with pytest.raises(SimulatorTimeout) as exc_info:
        adapter._ensure_connected()

    message = str(exc_info.value)
    assert "carla.example.test:2000" in message
    assert "remote CARLA server health cannot be inferred" in message


def test_connect_retries_then_succeeds(monkeypatch) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    monkeypatch.delenv("CARLA_HOST", raising=False)
    monkeypatch.delenv("CARLA_PORT", raising=False)
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter.config = {
        "carla_connect_timeout_seconds": 1,
        "retry_interval_seconds": 0,
    }
    adapter._server_version = None
    adapter._server_process = SimpleNamespace(pid=456, poll=lambda: None)
    calls = []

    def connect(timeout):
        calls.append(timeout)
        if len(calls) == 1:
            raise TimeoutError("not ready")
        adapter._server_version = "0.9.16"

    adapter._connect = connect

    adapter._ensure_connected()

    assert calls == [2, 2]


def test_init_rejects_unknown_open_scenario_map_loader() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = True
    adapter._ensure_connected = lambda: True
    adapter._prepare_reused_server_state = lambda: None
    request = SimpleNamespace(
        output_dir="out",
        config={"open_scenario_map_loader": "unknown"},
        scenario=SimpleNamespace(format="open_scenario1"),
        dt=0.05,
    )

    with pytest.raises(InvalidSimulatorRequest, match="Invalid open_scenario_map_loader"):
        adapter.init(request)


@pytest.mark.parametrize(
    ("config", "dt", "message"),
    [
        ({"synchronous_mode": False}, 0.05, "synchronous_mode must be true"),
        ({"yaw_sign": 1.0}, 0.05, "canonical coordinates"),
        ({"yaw_offset_deg": 1.0}, 0.05, "canonical coordinates"),
        ({}, 0.0, "finite positive"),
        ({}, float("nan"), "finite positive"),
        ({"ackermann_native_speed_kp": float("inf")}, 0.05, "finite number"),
    ],
)
def test_init_rejects_noncanonical_runtime_config(config, dt, message) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = True
    adapter._ensure_connected = lambda: True
    adapter._prepare_reused_server_state = lambda: None
    request = SimpleNamespace(
        output_dir="out",
        config=config,
        scenario=SimpleNamespace(format="open_scenario1"),
        dt=dt,
    )

    with pytest.raises(InvalidSimulatorRequest, match=message):
        adapter.init(request)


def test_prepare_reused_server_forces_async_and_clears_dynamic_actors() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    vehicle = _FakeActor(actor_id=1, type_id="vehicle.tesla.model3")
    traffic_light = _FakeActor(actor_id=2, type_id="traffic.traffic_light")
    world = _FakeSettingsWorld(
        [vehicle, traffic_light],
        synchronous_mode=True,
        fixed_delta_seconds=0.05,
    )
    traffic_manager = _FakeTrafficManager()
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._client = _FakeClient(world, traffic_manager=traffic_manager)
    adapter._scenario_runner_tm_port = 8000

    adapter._prepare_reused_server_state()

    assert adapter._world is world
    assert world.settings.synchronous_mode is False
    assert world.settings.fixed_delta_seconds is None
    assert world.applied_settings is world.settings
    assert traffic_manager.sync_calls == [False]
    assert vehicle.destroy_calls == 1
    assert traffic_light.destroy_calls == 0


def test_runtime_frame_uses_reset_relative_time() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    actor = _FakeKinematicActor(actor_id=1)
    world = _FakeRuntimeWorld(actor, frame=120, elapsed_seconds=12.5)
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = world
    adapter._ego_vehicle = actor
    adapter._objects_by_id = {}
    adapter._prev_yaw_rate = {}
    adapter._collision_lock = _FakeLock()
    adapter._collision_events = []
    adapter._last_applied_control = None
    adapter._yaw_sign = -1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}

    adapter._reset_episode_clock()
    frame = adapter._collect_runtime_frame()

    assert frame.sim_time_ns == 0
    assert frame.ego.tracking_id == 1
    assert frame.ego.object.state.kinematic.time_ns == 0
    assert frame.agents == {}
    assert frame.extras["carla_frame"] == 120
    assert frame.extras["carla_time_ns"] == 12_500_000_000
    assert frame.extras["episode_frame"] == 0

    world.frame = 122
    world.elapsed_seconds = 12.6
    frame = adapter._collect_runtime_frame()

    assert frame.sim_time_ns == 100_000_000
    assert frame.ego.object.state.kinematic.time_ns == 100_000_000
    assert frame.extras["episode_frame"] == 2


def test_runtime_frame_converts_carla_pose_and_yaw_to_canonical_frame() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    actor = _FakeKinematicActor(actor_id=1, x=1.0, y=2.0, z=3.0)
    actor.yaw = 90.0
    actor.yaw_rate = 30.0
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = _FakeRuntimeWorld(actor, frame=1, elapsed_seconds=0.0)
    adapter._ego_vehicle = actor
    adapter._objects_by_id = {}
    adapter._prev_yaw_rate = {}
    adapter._time_ns = 0
    adapter._episode_start_carla_time_ns = 0
    adapter._yaw_sign = -1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}

    ego, _, _, _, _ = adapter._collect_objects(sim_time_ns=0)
    kinematic = ego.object.state.kinematic

    assert (kinematic.x, kinematic.y, kinematic.z) == (1.0, -2.0, 3.0)
    assert kinematic.yaw == pytest.approx(-math.pi / 2)
    assert kinematic.yaw_rate == pytest.approx(-math.pi / 6)


def test_collect_objects_identity_is_independent_of_enumeration_order() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    ego = _FakeKinematicActor(actor_id=1, x=10.0, y=10.0)
    first = _FakeKinematicActor(actor_id=2, x=1.0, y=5.0)
    second = _FakeKinematicActor(actor_id=3, x=2.0, y=1.0)
    third = _FakeKinematicActor(actor_id=4, x=2.0, y=3.0)
    world = _FakeRuntimeWorld([third, ego, second, first], frame=1, elapsed_seconds=0.0)
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = world
    adapter._ego_vehicle = ego
    adapter._objects_by_id = {}
    adapter._prev_yaw_rate = {}
    adapter._time_ns = 0
    adapter._episode_start_carla_time_ns = 0
    adapter._yaw_sign = 1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}

    ego_frame, agents, _, _, _ = adapter._collect_objects()
    assert ego_frame.tracking_id == 1
    assert set(agents) == {2, 3, 4}
    assert {actor_id: obj.state.kinematic.x for actor_id, obj in agents.items()} == {
        2: 1.0,
        3: 2.0,
        4: 2.0,
    }
    refs_before_reorder = (adapter._actor_ref(1), adapter._actor_ref(3))
    first_shape = agents[2].state.shape
    first.bounding_box.extent.x = 99.0

    world.actors = _FakeActorList([first, second, ego, third])
    reordered_ego, reordered_agents, _, _, _ = adapter._collect_objects()
    assert reordered_ego.tracking_id == ego_frame.tracking_id
    assert set(reordered_agents) == set(agents)
    assert (adapter._actor_ref(1), adapter._actor_ref(3)) == refs_before_reorder
    assert reordered_agents[2].state.shape == first_shape


def test_collect_objects_reflects_add_remove_and_rejects_id_reuse() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    ego = _FakeKinematicActor(actor_id=1)
    original = _FakeKinematicActor(actor_id=2)
    added = _FakeKinematicActor(actor_id=3)
    world = _FakeRuntimeWorld([ego, original], frame=1, elapsed_seconds=0.0)
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = world
    adapter._ego_vehicle = ego
    adapter._objects_by_id = {}
    adapter._prev_yaw_rate = {}
    adapter._time_ns = 0
    adapter._episode_start_carla_time_ns = 0
    adapter._yaw_sign = 1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}

    _, agents, _, _, _ = adapter._collect_objects()
    assert set(agents) == {2}

    world.actors = _FakeActorList([ego, added])
    _, agents, _, _, _ = adapter._collect_objects()
    assert set(agents) == {3}
    assert agents[3].state.shape.reference_point == "carla_actor_origin"

    world.actors = _FakeActorList([ego, added, _FakeKinematicActor(actor_id=2)])
    with pytest.raises(SimulatorPreconditionFailed, match="reused retired actor ID"):
        adapter._collect_objects()


def test_shape_preserves_local_center_and_handedness() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    actor = _FakeKinematicActor(actor_id=7)
    actor.bounding_box.location = SimpleNamespace(x=1.0, y=0.5, z=0.25)
    actor.bounding_box.rotation = SimpleNamespace(roll=10.0, pitch=20.0, yaw=30.0)
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._yaw_sign = -1.0

    shape = adapter._shape_from_actor(actor)

    assert (
        shape.dimensions.x,
        shape.dimensions.y,
        shape.dimensions.z,
    ) == (4.0, 2.0, 1.5)
    assert shape.center.x == 1.0
    assert shape.center.y == -0.5
    assert shape.center.z == 0.25
    assert shape.center.roll == pytest.approx(-math.radians(10.0))
    assert shape.center.pitch == pytest.approx(math.radians(20.0))
    assert shape.center.yaw == pytest.approx(-math.radians(30.0))
    assert shape.reference_point == "carla_actor_origin"


def test_entity_name_only_uses_declared_xosc_scenario_object_name() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._xosc_entity_names = {"Ego", "NPC"}
    named = SimpleNamespace(attributes={"role_name": "NPC"})
    background = SimpleNamespace(attributes={"role_name": "background"})

    assert adapter._entity_name_from_actor(named) == "NPC"
    assert adapter._entity_name_from_actor(background) is None


@pytest.mark.parametrize("bad_extent", [0.0, -1.0, float("nan"), float("inf")])
def test_shape_rejects_non_positive_or_non_finite_dimensions(bad_extent) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    actor = _FakeKinematicActor(actor_id=7)
    actor.bounding_box.extent.x = bad_extent
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._yaw_sign = -1.0

    with pytest.raises(SimulatorPreconditionFailed):
        adapter._shape_from_actor(actor)


def test_runtime_frame_rejects_non_finite_kinematic_value() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    ego = _FakeKinematicActor(actor_id=1, x=float("nan"))
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = _FakeRuntimeWorld([ego], frame=1, elapsed_seconds=0.0)
    adapter._ego_vehicle = ego
    adapter._objects_by_id = {}
    adapter._prev_yaw_rate = {}
    adapter._time_ns = 0
    adapter._episode_start_carla_time_ns = 0
    adapter._yaw_sign = -1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}

    with pytest.raises(SimulatorPreconditionFailed, match="non-finite actor 1 x"):
        adapter._collect_objects(sim_time_ns=0)


def test_runtime_frame_rejects_actor_identity_mutation() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    ego = _FakeKinematicActor(actor_id=1)
    agent = _FakeKinematicActor(actor_id=2)
    agent.attributes = {"role_name": "NPC"}
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = _FakeRuntimeWorld([ego, agent], frame=1, elapsed_seconds=0.0)
    adapter._ego_vehicle = ego
    adapter._objects_by_id = {}
    adapter._prev_yaw_rate = {}
    adapter._time_ns = 0
    adapter._episode_start_carla_time_ns = 0
    adapter._yaw_sign = -1.0
    adapter._yaw_offset_deg = 0.0
    adapter._xosc_entity_names = {"NPC", "Other"}
    adapter.config = {}

    adapter._collect_objects(sim_time_ns=0)
    agent.attributes["role_name"] = "Other"

    with pytest.raises(SimulatorPreconditionFailed, match="identity changed"):
        adapter._collect_objects(sim_time_ns=0)


@pytest.mark.parametrize("actor_id", [-1, 1 << 64])
def test_tracking_id_must_fit_uint64(actor_id) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    with pytest.raises(SimulatorPreconditionFailed, match="uint64"):
        CarlaAdapter._require_tracking_id(actor_id)


def test_collision_details_include_episode_relative_time() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._episode_start_carla_time_ns = 12_500_000_000
    adapter._episode_start_carla_frame = 120
    adapter._collision_lock = _FakeLock()
    adapter._collision_events = [
        {
            "frame": 122,
            "timestamp": 12.6,
            "actor_id": 1,
            "other_actor_id": 2,
            "other_actor_type_id": "vehicle.test",
            "other_actor_semantic_tags": [],
            "normal_impulse": {"x": 1.0, "y": 2.0, "z": 2.0},
        }
    ]
    adapter._ego_vehicle = SimpleNamespace(id=1)
    adapter._actor_metadata_by_id = {1: ("Ego", ActorRole.EGO), 2: ("Agent", ActorRole.AGENT)}

    collisions = adapter._collect_collision_infos()

    assert len(collisions) == 1
    assert collisions[0].details["carla_frame"] == 122
    assert collisions[0].details["episode_frame"] == 2
    assert collisions[0].details["carla_timestamp_seconds"] == 12.6
    assert collisions[0].details["timestamp_seconds"] == pytest.approx(0.1)
    assert collisions[0].actor_a.tracking_id == 1
    assert collisions[0].actor_a.entity_name == "Ego"
    assert collisions[0].actor_a.role == ActorRole.EGO
    assert collisions[0].actor_b.tracking_id == 2
    assert collisions[0].actor_b.entity_name == "Agent"
    assert collisions[0].actor_b.role == ActorRole.AGENT
    assert adapter._actor_ref(99).role == ActorRole.ACTOR_ROLE_UNSPECIFIED


def test_collision_rejects_non_finite_impulse() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._episode_start_carla_time_ns = 0
    adapter._episode_start_carla_frame = 0
    adapter._collision_lock = _FakeLock()
    adapter._collision_events = [
        {
            "frame": 1,
            "timestamp": 0.1,
            "actor_id": 1,
            "other_actor_id": 2,
            "other_actor_type_id": "vehicle.test",
            "other_actor_semantic_tags": [],
            "normal_impulse": {"x": float("nan"), "y": 0.0, "z": 0.0},
        }
    ]
    adapter._ego_vehicle = SimpleNamespace(id=1)

    with pytest.raises(SimulatorPreconditionFailed, match="normal impulse x"):
        adapter._collect_collision_infos()


def test_reset_finalizes_partial_state_on_failure(tmp_path) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = True
    adapter._output_base = tmp_path
    adapter.scenario = SimpleNamespace(format="carla_lb_route")
    adapter._clear_collision_events = lambda: calls.append("clear_collision_events")
    adapter._ensure_world = lambda scenario_pack, generate_opendrive_world=True: calls.append(
        ("ensure_world", scenario_pack, generate_opendrive_world)
    )
    adapter._clear_dynamic_actors = lambda: calls.append("clear_dynamic_actors")
    adapter._apply_world_settings = lambda: calls.append("apply_world_settings")
    adapter._client = SimpleNamespace(start_recorder=lambda path: calls.append(("recorder", path)))

    def start_scenario_runner(scenario_pack, params):
        raise RuntimeError("scenario failed")

    adapter._start_scenario_runner = start_scenario_runner
    adapter._finalize = lambda: calls.append("finalize")

    request = SimpleNamespace(output_dir="run", scenario_pack=object(), params={})

    with pytest.raises(RuntimeError, match="scenario failed"):
        adapter.reset(request)

    assert calls[-1] == "finalize"
    assert calls[1][0] == "ensure_world"


def test_reset_marks_negative_initial_speed_as_precondition_failure(tmp_path) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = True
    adapter._output_base = tmp_path
    adapter.scenario = SimpleNamespace(format="open_scenario1")
    adapter._open_scenario_map_loader = "scenario_runner"
    adapter._clear_collision_events = lambda: None
    adapter._ensure_world = lambda *args, **kwargs: None
    adapter._clear_dynamic_actors = lambda: None
    adapter._start_scenario_runner = lambda *args, **kwargs: (_ for _ in ()).throw(
        AttributeError("Warning: Speed value of actor Ego must be positive. Speed set to 0.")
    )
    adapter._finalize = lambda: calls.append("finalize")

    request = SimpleNamespace(output_dir="run", scenario_pack=object(), params={})

    with pytest.raises(SimulatorPreconditionFailed, match="Speed value of actor Ego"):
        adapter.reset(request)

    assert calls == ["finalize"]


def test_reset_does_not_reclassify_unrecognized_attribute_error(tmp_path) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = True
    adapter._output_base = tmp_path
    adapter.scenario = SimpleNamespace(format="open_scenario1")
    adapter._open_scenario_map_loader = "scenario_runner"
    adapter._clear_collision_events = lambda: None
    adapter._ensure_world = lambda *args, **kwargs: None
    adapter._clear_dynamic_actors = lambda: None
    adapter._start_scenario_runner = lambda *args, **kwargs: (_ for _ in ()).throw(
        AttributeError("unexpected wrapper bug")
    )
    adapter._finalize = lambda: None

    request = SimpleNamespace(output_dir="run", scenario_pack=object(), params={})

    with pytest.raises(AttributeError, match="unexpected wrapper bug"):
        adapter.reset(request)


def test_open_scenario_wrapper_map_loader_generates_world_before_scenario_runner(tmp_path) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = True
    adapter._output_base = tmp_path
    adapter.scenario = SimpleNamespace(format="open_scenario1")
    adapter._open_scenario_map_loader = "wrapper"
    adapter._clear_collision_events = lambda: None
    adapter._ensure_world = lambda scenario_pack, generate_opendrive_world=True: calls.append(
        ("ensure_world", generate_opendrive_world)
    )
    adapter._clear_dynamic_actors = lambda: calls.append("clear_dynamic_actors")
    adapter._apply_world_settings = lambda: calls.append("apply_world_settings")
    adapter._start_scenario_runner = lambda scenario_pack, params: (_ for _ in ()).throw(
        RuntimeError("stop after map preparation")
    )
    adapter._finalize = lambda: calls.append("finalize")
    request = SimpleNamespace(output_dir="run", scenario_pack=object(), params={})

    with pytest.raises(RuntimeError, match="stop after map preparation"):
        adapter.reset(request)

    assert calls[:3] == [
        ("ensure_world", True),
        "clear_dynamic_actors",
        "apply_world_settings",
    ]
    assert calls[-1] == "finalize"


def test_open_scenario_reset_delegates_world_loading_to_scenario_runner(tmp_path) -> None:
    from carla_wrapper import simulation

    calls = []
    simulation.carla = SimpleNamespace(
        VehicleControl=_FakeVehicleControl,
        Vector3D=lambda x, y, z: SimpleNamespace(x=x, y=y, z=z),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._finalized = True
    adapter._output_base = tmp_path
    adapter.scenario = SimpleNamespace(format="open_scenario1")
    adapter._sync = True
    ego_vehicle = _FakeKinematicActor(11)
    other_vehicle = _FakeKinematicActor(12)
    adapter._ego_vehicle = ego_vehicle
    adapter._clear_collision_events = lambda: calls.append("clear_collision_events")
    adapter._ensure_world = lambda scenario_pack, generate_opendrive_world=True: calls.append(
        ("ensure_world", generate_opendrive_world)
    )
    adapter._clear_dynamic_actors = lambda: calls.append("clear_dynamic_actors")
    adapter._apply_world_settings = lambda: calls.append("apply_world_settings")
    adapter._record = True
    adapter._client = SimpleNamespace(start_recorder=lambda path: calls.append("start_recorder"))

    def start_scenario_runner(scenario_pack, params):
        calls.append("start_scenario_runner")
        adapter._xosc_entity_names = {"ego", "agent1"}

    adapter._start_scenario_runner = start_scenario_runner
    adapter._setup_collision_sensor = lambda: calls.append("setup_collision_sensor")
    adapter._tick_scenario_runner_module = lambda: calls.append("tick_scenario_runner")
    adapter._collect_collision_infos = lambda: []
    original_collect_runtime_frame = adapter._collect_runtime_frame

    def collect_runtime_frame(speed_overrides=None, sim_time_ns=None):
        calls.append(("collect_runtime_frame", speed_overrides))
        return original_collect_runtime_frame(
            speed_overrides=speed_overrides,
            sim_time_ns=sim_time_ns,
        )

    adapter._collect_runtime_frame = collect_runtime_frame
    adapter._finalize = lambda: calls.append("finalize")
    adapter._sr_scenario = SimpleNamespace(
        config=SimpleNamespace(
            ego_vehicles=[SimpleNamespace(rolename="ego", speed=5.0)],
            other_actors=[SimpleNamespace(rolename="agent1", speed=3.0)],
        ),
        ego_vehicles=[ego_vehicle],
        other_actors=[other_vehicle],
    )
    ego_vehicle.attributes = {"role_name": "ego"}
    other_vehicle.attributes = {"role_name": "agent1"}
    adapter._world = _FakeRuntimeWorld([ego_vehicle, other_vehicle], frame=10, elapsed_seconds=0.1)
    adapter._objects_by_id = {}
    adapter._prev_yaw_rate = {}
    adapter._last_applied_control = None
    adapter._yaw_sign = -1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}

    request = SimpleNamespace(output_dir="run", scenario_pack=object(), params={})

    response = adapter.reset(request)

    assert ("ensure_world", False) in calls
    assert "apply_world_settings" not in calls
    assert calls.index("start_scenario_runner") < calls.index("start_recorder")
    assert calls.index("start_recorder") < calls.index("setup_collision_sensor")
    assert "tick_scenario_runner" not in calls
    assert ("collect_runtime_frame", {11: 5.0, 12: 3.0}) in calls
    assert ego_vehicle.set_velocity_calls == [SimpleNamespace(x=5.0, y=0.0, z=0.0)]
    assert other_vehicle.set_velocity_calls == []
    assert response.frame.ego.tracking_id == 11
    assert response.frame.ego.object.entity_name == "ego"
    assert response.frame.ego.object.state.shape.reference_point == "carla_actor_origin"
    assert set(response.frame.agents) == {12}
    assert response.frame.agents[12].entity_name == "agent1"
    assert "object_index_by_actor_id" not in response.frame.extras
    assert "ego_actor_id" not in response.frame.extras
    adapter._world.frame = 11
    adapter._world.elapsed_seconds = 0.3
    ego_vehicle.forward_speed = 8.0
    step_frame = adapter._collect_runtime_frame()
    assert step_frame.ego.object.state.kinematic.acceleration == pytest.approx(15.0)
    assert step_frame.ego.object.state.kinematic.speed == pytest.approx(8.0)
    assert adapter._initial_speed_acceleration_pending is False

    adapter._world.frame = 12
    adapter._world.elapsed_seconds = 0.35
    ego_vehicle.forward_speed = 9.0
    ego_vehicle.forward_accel = 0.7
    next_frame = adapter._collect_runtime_frame()
    assert next_frame.ego.object.state.kinematic.acceleration == pytest.approx(0.7)
    assert isinstance(response.frame, RuntimeFrameData)


def test_route_reset_frame_starts_at_zero_after_initialization_tick(tmp_path) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    ego = _FakeKinematicActor(21)
    world = _FakeRuntimeWorld([ego], frame=10, elapsed_seconds=4.0)
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = True
    adapter._output_base = tmp_path
    adapter.scenario = SimpleNamespace(format="carla_lb_route")
    adapter._sync = True
    adapter._record = False
    adapter._world = world
    adapter._ego_vehicle = ego
    adapter._objects_by_id = {}
    adapter._prev_yaw_rate = {}
    adapter._last_applied_control = None
    adapter._yaw_sign = -1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}
    adapter._clear_collision_events = lambda: None
    adapter._ensure_world = lambda *args, **kwargs: None
    adapter._clear_dynamic_actors = lambda: None
    adapter._apply_world_settings = lambda: None
    adapter._start_scenario_runner = lambda *args, **kwargs: None
    adapter._setup_collision_sensor = lambda: None
    adapter._tick_scenario_runner_module = lambda: None
    adapter._collect_collision_infos = lambda: []
    adapter._finalize = lambda: None

    response = adapter.reset(SimpleNamespace(output_dir="run", scenario_pack=object(), params={}))

    assert world.frame == 11
    assert response.frame.sim_time_ns == 0
    assert response.frame.ego.object.state.kinematic.time_ns == 0


def test_ensure_world_can_skip_opendrive_generation() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    world = _FakeSettingsWorld()
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._server_version = "test"
    adapter._client = _FakeClient(world)

    adapter._ensure_world(SimpleNamespace(), generate_opendrive_world=False)

    assert adapter._world is world
    assert adapter._client.generated is False


def test_ensure_world_generates_opendrive_without_walls(monkeypatch) -> None:
    from carla_wrapper import simulation

    generated_world = object()
    generation_parameters = []

    class FakeGenerationParameters:
        def __init__(self, **kwargs):
            generation_parameters.append(kwargs)

    class FakeClient:
        def __init__(self):
            self.timeouts = []

        def set_timeout(self, timeout):
            self.timeouts.append(timeout)

        def generate_opendrive_world(self, opendrive, parameters):
            assert opendrive == "<OpenDRIVE/>"
            assert isinstance(parameters, FakeGenerationParameters)
            return generated_world

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(OpendriveGenerationParameters=FakeGenerationParameters),
    )
    monkeypatch.setattr(simulation.Path, "exists", lambda self: True)
    monkeypatch.setattr("builtins.open", lambda *args, **kwargs: StringIO("<OpenDRIVE/>"))
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._server_version = "test"
    adapter._client = FakeClient()

    adapter._ensure_world(SimpleNamespace(map_name="test_map"))

    assert adapter._world is generated_world
    assert generation_parameters[0]["wall_height"] == 0.0
    assert adapter._client.timeouts == [300.0, 30.0]
    assert adapter._wrapper_loaded_opendrive_digest == adapter._opendrive_digest("<OpenDRIVE/>")


def test_ensure_world_reuses_verified_wrapper_generated_opendrive(monkeypatch) -> None:
    from carla_wrapper import simulation

    opendrive = "<?xml version='1.0'?>\n<OpenDRIVE><road id='1'/></OpenDRIVE>"

    class FakeMap:
        name = "/Game/Carla/Maps/OpenDriveMap"

        def to_opendrive(self):
            return opendrive[opendrive.index("<OpenDRIVE") :]

    world = SimpleNamespace(get_map=lambda: FakeMap())

    class FakeClient:
        def __init__(self):
            self.generate_calls = 0

        def get_world(self):
            return world

        def generate_opendrive_world(self, *_args):
            self.generate_calls += 1
            return world

    monkeypatch.setattr(simulation.Path, "exists", lambda self: True)
    monkeypatch.setattr("builtins.open", lambda *args, **kwargs: StringIO(opendrive))
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._server_version = "test"
    adapter._client = FakeClient()
    adapter._wrapper_loaded_opendrive_digest = adapter._opendrive_digest(opendrive)

    adapter._ensure_world(SimpleNamespace(map_name="map_a"))

    assert adapter._world is world
    assert adapter._client.generate_calls == 0


def test_ensure_world_switches_between_different_opendrive_maps_with_same_carla_name(
    monkeypatch,
) -> None:
    from carla_wrapper import simulation

    old_opendrive = "<OpenDRIVE><road id='old'/></OpenDRIVE>"
    new_opendrive = "<OpenDRIVE><road id='new'/></OpenDRIVE>"
    generated_world = object()

    class FakeMap:
        name = "/Game/Carla/Maps/OpenDriveMap"

        def to_opendrive(self):
            return old_opendrive

    old_world = SimpleNamespace(get_map=lambda: FakeMap())

    class FakeClient:
        def __init__(self):
            self.generate_calls = 0
            self.timeouts = []

        def get_world(self):
            return old_world

        def set_timeout(self, timeout):
            self.timeouts.append(timeout)

        def generate_opendrive_world(self, opendrive, _parameters):
            self.generate_calls += 1
            assert opendrive == new_opendrive
            return generated_world

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(OpendriveGenerationParameters=lambda **kwargs: kwargs),
    )
    monkeypatch.setattr(simulation.Path, "exists", lambda self: True)
    monkeypatch.setattr("builtins.open", lambda *args, **kwargs: StringIO(new_opendrive))
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._server_version = "test"
    adapter._client = FakeClient()
    adapter._wrapper_loaded_opendrive_digest = adapter._opendrive_digest(old_opendrive)

    adapter._ensure_world(SimpleNamespace(map_name="map_b"))

    assert adapter._world is generated_world
    assert adapter._client.generate_calls == 1
    assert adapter._wrapper_loaded_opendrive_digest == adapter._opendrive_digest(new_opendrive)


def test_ensure_world_requires_scenario_pack() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)

    with pytest.raises(InvalidSimulatorRequest, match="ScenarioPack is required"):
        adapter._ensure_world(None)


def test_ensure_world_stops_when_reconnect_fails() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._server_version = None
    adapter._client = None
    adapter._ensure_connected = lambda: (_ for _ in ()).throw(
        SimulatorTimeout("Timed out connecting to CARLA")
    )

    with pytest.raises(SimulatorTimeout, match="Timed out connecting to CARLA"):
        adapter._ensure_world(SimpleNamespace(map_name="Town01"))


def test_open_scenario_missing_xosc_fails_before_scenario_runner(monkeypatch) -> None:
    from carla_wrapper import simulation

    traffic_manager = _FakeTrafficManager()
    monkeypatch.setattr(
        simulation,
        "CarlaDataProvider",
        SimpleNamespace(
            set_client=lambda client: None,
            set_world=lambda world: None,
            set_traffic_manager_port=lambda port: None,
        ),
    )

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._client = SimpleNamespace(get_trafficmanager=lambda port: traffic_manager)
    adapter._world = object()
    adapter._scenario_runner_tm_port = 8000
    adapter._scenario_runner_tm_seed = 0
    adapter._sync = True
    adapter._traffic_manager = None
    adapter._traffic_manager_sync_enabled = False
    adapter.scenario = SimpleNamespace(format="open_scenario1")

    with pytest.raises(InvalidSimulatorRequest, match="OpenSCENARIO file not found"):
        adapter._start_scenario_runner_module(SimpleNamespace(name="missing"), {})


def test_wrapper_map_loader_prevents_scenario_runner_from_replacing_world(monkeypatch) -> None:
    from carla_wrapper import simulation

    class FakeOpenScenarioConfiguration:
        def __init__(self, filename, client, params):
            client.generate_opendrive_world("different map")

    monkeypatch.setattr(simulation, "OpenScenarioConfiguration", FakeOpenScenarioConfiguration)
    monkeypatch.setattr(simulation.Path, "exists", lambda self: True)
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._client = SimpleNamespace(generate_opendrive_world=lambda *args: object())
    adapter._open_scenario_map_loader = "wrapper"

    with pytest.raises(InvalidSimulatorRequest, match="tried to replace"):
        adapter._prepare_open_scenario_config(SimpleNamespace(name="scenario"), {})


def test_open_scenario_uses_scenario_runner_reloaded_world(monkeypatch) -> None:
    from carla_wrapper import simulation

    old_world = _FakeSettingsWorld()
    new_world = _FakeSettingsWorld()
    actor = _FakeVehicle()
    calls = []

    class FakeDataProvider:
        current_world = old_world

        @staticmethod
        def set_client(client):
            calls.append("set_client")

        @staticmethod
        def set_world(world):
            FakeDataProvider.current_world = world
            calls.append(("set_world", world, world.applied_settings is not None))

        @staticmethod
        def get_world():
            return FakeDataProvider.current_world

        @staticmethod
        def set_traffic_manager_port(port):
            calls.append(("tm_port", port))

        @staticmethod
        def request_new_actor(*args, **kwargs):
            calls.append(("request_new_actor", FakeDataProvider.current_world, kwargs.get("tick")))
            return actor

    class FakeOpenScenarioConfiguration:
        def __init__(self, filename, client, params):
            calls.append("config")
            FakeDataProvider.current_world = new_world
            self.ego_vehicles = [
                SimpleNamespace(
                    model="vehicle.lincoln.mkz_2017",
                    transform=object(),
                    rolename="hero",
                    random_location=False,
                    color=None,
                    category="car",
                )
            ]
            self.name = "scenario"

    class FakeOpenScenario:
        def __init__(self, world, ego_vehicles, config, config_file, timeout):
            calls.append(("open_scenario", world, new_world.applied_settings is not None))
            self.scenario_tree = object()

    monkeypatch.setattr(simulation, "CarlaDataProvider", FakeDataProvider)
    monkeypatch.setattr(simulation, "OpenScenarioConfiguration", FakeOpenScenarioConfiguration)
    monkeypatch.setattr(simulation, "OpenScenario", FakeOpenScenario)
    monkeypatch.setattr(
        simulation, "GameTime", SimpleNamespace(restart=lambda: calls.append("restart"))
    )
    monkeypatch.setattr(simulation.Path, "exists", lambda self: True)

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._client = SimpleNamespace(
        get_world=lambda: new_world,
        get_trafficmanager=lambda port: _FakeTrafficManager(),
    )
    adapter._world = old_world
    adapter._scenario_runner_tm_port = 8000
    adapter._scenario_runner_tm_seed = 0
    adapter._sync = True
    adapter._fixed_delta_seconds = 0.05
    adapter._no_rendering = False
    adapter._traffic_manager = None
    adapter._traffic_manager_sync_enabled = False
    adapter._sr_ego_vehicles = []
    adapter.scenario = SimpleNamespace(format="open_scenario1")

    adapter._start_scenario_runner_module(SimpleNamespace(name="scenario", timeout_ns=0), {})

    assert adapter._world is new_world
    assert adapter._ego_vehicle is actor
    assert ("request_new_actor", new_world, None) in calls
    assert ("open_scenario", new_world, True) in calls
    assert new_world.tick_calls == 1
    assert old_world.tick_calls == 0
    set_world_calls = [call for call in calls if call[0] == "set_world"]
    assert set_world_calls[-1] == ("set_world", new_world, True)


def test_clear_dynamic_actors_only_destroys_runtime_actor_types() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    vehicle = _FakeActor(actor_id=1, type_id="vehicle.tesla.model3")
    walker = _FakeActor(actor_id=2, type_id="walker.pedestrian.0001")
    walker_controller = _FakeActor(actor_id=3, type_id="controller.ai.walker")
    sensor = _FakeActor(actor_id=4, type_id="sensor.other.collision")
    traffic_light = _FakeActor(actor_id=5, type_id="traffic.traffic_light")
    prop = _FakeActor(actor_id=6, type_id="static.prop.streetbarrier")
    world = _FakeSettingsWorld([vehicle, walker, walker_controller, sensor, traffic_light, prop])
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = world

    adapter._clear_dynamic_actors()

    assert vehicle.destroy_calls == 1
    assert walker.destroy_calls == 1
    assert walker_controller.destroy_calls == 1
    assert sensor.destroy_calls == 1
    assert traffic_light.destroy_calls == 0
    assert prop.destroy_calls == 0


def test_actor_type_uses_carla_vehicle_catalogue_base_types() -> None:
    from pisa_api.simulator import RoadObjectType

    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    cases = {
        "vehicle.mitsubishi.fusorosa": RoadObjectType.BUS,
        "vehicle.tesla.cybertruck": RoadObjectType.TRUCK,
        "vehicle.ford.ambulance": RoadObjectType.VAN,
        "vehicle.mercedes.sprinter": RoadObjectType.VAN,
        "vehicle.harley-davidson.low_rider": RoadObjectType.MOTORCYCLE,
        "vehicle.kawasaki.ninja": RoadObjectType.MOTORCYCLE,
        "vehicle.bh.crossbike": RoadObjectType.BICYCLE,
        "vehicle.diamondback.century": RoadObjectType.BICYCLE,
        "vehicle.tesla.model3": RoadObjectType.CAR,
    }

    for type_id, expected in cases.items():
        assert adapter._actor_type(SimpleNamespace(type_id=type_id)) == expected


def test_actor_type_keeps_fallbacks_for_non_catalogue_vehicle_ids() -> None:
    from pisa_api.simulator import RoadObjectType

    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    cases = {
        "walker.pedestrian.0001": RoadObjectType.PEDESTRIAN,
        "vehicle.custom.city_bus": RoadObjectType.BUS,
        "vehicle.custom.delivery_hgv": RoadObjectType.TRUCK,
        "vehicle.custom.long_trailer": RoadObjectType.TRAILER,
        "vehicle.custom.electric_sprinter": RoadObjectType.VAN,
        "vehicle.custom.naked_motorcycle": RoadObjectType.MOTORCYCLE,
        "vehicle.custom.cargo_bicycle": RoadObjectType.BICYCLE,
        "vehicle.custom.prototype": RoadObjectType.CAR,
        "sensor.other.collision": RoadObjectType.UNKNOWN,
    }

    for type_id, expected in cases.items():
        assert adapter._actor_type(SimpleNamespace(type_id=type_id)) == expected


def test_partial_scenario_cleanup_destroys_untracked_new_actors() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    existing_actor = _FakeActor(actor_id=1)
    leaked_actor = _FakeActor(actor_id=2)
    world = _FakeSettingsWorld([existing_actor])
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = world

    adapter._snapshot_existing_actors()
    world.actors.append(leaked_actor)
    adapter._destroy_new_scenario_actors()

    assert existing_actor.destroy_calls == 0
    assert leaked_actor.destroy_calls == 1
    assert adapter._pre_scenario_actor_ids is None


def test_partial_scenario_cleanup_handles_empty_initial_world() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    leaked_actor = _FakeActor(actor_id=2)
    world = _FakeSettingsWorld([])
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = world

    adapter._snapshot_existing_actors()
    world.actors.append(leaked_actor)
    adapter._destroy_new_scenario_actors()

    assert leaked_actor.destroy_calls == 1


def test_restore_traffic_manager_disables_sync_when_wrapper_enabled_it() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    traffic_manager = _FakeTrafficManager()
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._traffic_manager = traffic_manager
    adapter._traffic_manager_sync_enabled = True

    adapter._restore_traffic_manager_settings()

    assert traffic_manager.sync_calls == [False]
    assert adapter._traffic_manager is None
    assert adapter._traffic_manager_sync_enabled is False


def test_scenario_runner_cleanup_cleans_globals_even_when_not_running(monkeypatch) -> None:
    from carla_wrapper import simulation

    calls = []
    monkeypatch.setattr(
        simulation,
        "CarlaDataProvider",
        SimpleNamespace(cleanup=lambda: calls.append("data_provider_cleanup")),
    )
    monkeypatch.setattr(
        simulation,
        "py_trees",
        SimpleNamespace(
            blackboard=SimpleNamespace(
                Blackboard=SimpleNamespace(_Blackboard__shared_state=_FakeSharedState(calls))
            )
        ),
    )

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._restore_traffic_manager_settings = lambda: calls.append("restore_tm")
    adapter._sr_scenario = None
    adapter._sr_tree = None
    adapter._sr_ego_vehicles = []

    adapter._stop_scenario_runner_module()

    assert calls == ["restore_tm", "data_provider_cleanup", "blackboard_clear"]


def test_stop_does_not_call_carla_after_finalize() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalize = lambda: None
    adapter._world = SimpleNamespace(
        apply_settings=lambda settings: (_ for _ in ()).throw(
            AssertionError("stop must not call CARLA after finalize")
        )
    )
    adapter._original_settings = object()
    adapter._client = object()
    adapter._server_version = "test"
    adapter._ego_vehicle = object()

    adapter.stop()

    assert adapter._world is None
    assert adapter._client is None
    assert adapter._server_version is None
    assert adapter._ego_vehicle is None


class _FakeWorld:
    def __init__(self, calls):
        self._calls = calls
        self.frame = 0

    def tick(self):
        self._calls.append("world_tick")
        self.frame += 1

    def get_snapshot(self):
        return SimpleNamespace(frame=self.frame)

    def wait_for_tick(self):
        self._calls.append("world_wait_for_tick")


class _FakeScenarioWorld:
    def get_snapshot(self):
        return SimpleNamespace(timestamp=SimpleNamespace(frame=1))


class _FakeScenarioTree:
    def __init__(self, calls, status="RUNNING"):
        self.status = status
        self._calls = calls

    def tick_once(self):
        self._calls.append("tree_tick")


class _FakeLock:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeActorList(list):
    def filter(self, pattern):
        prefix = pattern.removesuffix("*")
        return [actor for actor in self if actor.type_id.startswith(prefix)]


class _FakeRuntimeWorld:
    def __init__(self, actors, frame, elapsed_seconds):
        if not isinstance(actors, list):
            actors = [actors]
        self.actors = _FakeActorList(actors)
        self.frame = frame
        self.elapsed_seconds = elapsed_seconds

    def get_snapshot(self):
        return SimpleNamespace(
            frame=self.frame,
            timestamp=SimpleNamespace(elapsed_seconds=self.elapsed_seconds),
        )

    def get_actors(self):
        return self.actors

    def tick(self):
        self.frame += 1
        self.elapsed_seconds += 0.05
        return self.frame


class _FakeKinematicActor:
    def __init__(self, actor_id, x=0.0, y=0.0, z=0.0):
        self.id = actor_id
        self.type_id = "vehicle.test"
        self.x = x
        self.y = y
        self.z = z
        self.forward_speed = 1.0
        self.forward_accel = 0.0
        self.yaw = 0.0
        self.yaw_rate = 0.0
        self.bounding_box = SimpleNamespace(extent=SimpleNamespace(x=2.0, y=1.0, z=0.75))
        self.set_velocity_calls = []

    def get_velocity(self):
        return SimpleNamespace(x=self.forward_speed, y=0.0, z=0.0)

    def get_acceleration(self):
        return SimpleNamespace(x=self.forward_accel, y=0.0, z=0.0)

    def get_angular_velocity(self):
        return SimpleNamespace(z=self.yaw_rate)

    def get_transform(self):
        return SimpleNamespace(
            location=SimpleNamespace(x=self.x, y=self.y, z=self.z),
            rotation=SimpleNamespace(yaw=self.yaw),
            get_forward_vector=lambda: SimpleNamespace(x=1.0, y=0.0, z=0.0),
        )

    def set_simulate_physics(self, enabled):
        self.simulate_physics_enabled = enabled

    def set_velocity(self, velocity):
        self.set_velocity_calls.append(velocity)


class _FakeTrafficManager:
    def __init__(self):
        self.sync_calls = []
        self.seed_calls = []

    def set_synchronous_mode(self, enabled):
        self.sync_calls.append(enabled)

    def set_random_device_seed(self, seed):
        self.seed_calls.append(seed)


class _FakeSharedState:
    def __init__(self, calls):
        self._calls = calls

    def clear(self):
        self._calls.append("blackboard_clear")


class _FakeSettingsWorld:
    def __init__(self, actors=None, synchronous_mode=False, fixed_delta_seconds=None):
        self.settings = SimpleNamespace(
            synchronous_mode=synchronous_mode,
            no_rendering_mode=False,
            fixed_delta_seconds=fixed_delta_seconds,
        )
        self.applied_settings = None
        self.actors = list(actors or [])
        self.tick_calls = 0
        self.wait_for_tick_calls = 0

    def get_settings(self):
        return self.settings

    def apply_settings(self, settings):
        self.applied_settings = settings

    def get_actors(self):
        return list(self.actors)

    def tick(self):
        self.tick_calls += 1

    def wait_for_tick(self):
        self.wait_for_tick_calls += 1


class _FakeActor:
    def __init__(self, actor_id, type_id="vehicle.test"):
        self.id = actor_id
        self.type_id = type_id
        self.destroy_calls = 0

    def destroy(self):
        self.destroy_calls += 1


class _DestroyFalseActor(_FakeActor):
    def destroy(self):
        self.destroy_calls += 1
        return False


class _FakeClient:
    def __init__(self, world, traffic_manager=None):
        self.world = world
        self.traffic_manager = traffic_manager or _FakeTrafficManager()
        self.generated = False

    def get_world(self):
        return self.world

    def get_trafficmanager(self, port):
        return self.traffic_manager

    def generate_opendrive_world(self, *args, **kwargs):
        self.generated = True
        raise AssertionError("generate_opendrive_world should not be called")


def test_throttle_steer_brake_preserves_brake_and_gives_it_priority(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle()
    adapter._yaw_sign = -1.0
    adapter._last_applied_control = None

    ctrl = ControlCommand(
        mode=ControlMode.THROTTLE_STEER_BREAK,
        payload={"throttle": 0.8, "brake": 0.3, "steer": 0.2},
    )
    adapter._apply_ctrl(ctrl)

    assert adapter._ego_vehicle.applied_control.throttle == 0.0
    assert adapter._ego_vehicle.applied_control.brake == 0.3
    assert adapter._ego_vehicle.applied_control.steer == -0.2


@pytest.mark.parametrize(
    "payload",
    [
        {"throttle": 0.0, "brake": 0.0},
        {"throttle": 1.1, "brake": 0.0, "steer": 0.0},
        {"throttle": 0.0, "brake": 0.0, "steer": float("nan")},
        {"throttle": 0.0, "brake": 0.0, "steer": 0.0, "break": 0.0},
    ],
)
def test_throttle_steer_brake_rejects_noncanonical_payload(monkeypatch, payload) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle()
    adapter._yaw_sign = -1.0

    with pytest.raises(InvalidSimulatorRequest):
        adapter._apply_ctrl(ControlCommand(mode=ControlMode.THROTTLE_STEER_BREAK, payload=payload))


def test_ackermann_vehicle_control_fallback_remains_available(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle(forward_speed=0.0)
    adapter._yaw_sign = -1.0
    adapter._max_steer_rad = 0.5
    adapter._last_applied_control = None
    adapter.config = {
        "ackermann_use_native_control": False,
        "ackermann_speed_kp": 0.1,
        "ackermann_launch_throttle": 0.5,
    }

    adapter._apply_ctrl(
        ControlCommand(
            mode=ControlMode.ACKERMANN,
            payload={"speed": 1.0, "steer": 0.25},
        )
    )

    assert adapter._ego_vehicle.applied_control.throttle == 0.5
    assert adapter._ego_vehicle.applied_control.brake == 0.0
    assert adapter._ego_vehicle.applied_control.steer == pytest.approx(-0.5)
    assert adapter._last_applied_control["backend"] == "vehicle_control"


def test_ackermann_native_backend_uses_decel_defaults_when_slowing(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(
            AckermannControllerSettings=_FakeAckermannControllerSettings,
            VehicleAckermannControl=_FakeVehicleAckermannControl,
        ),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle(forward_speed=5.0)
    adapter._scenario_runner_tm_port = 8000
    adapter._yaw_sign = -1.0
    adapter._max_steer_rad = None
    adapter._last_applied_control = None
    adapter._native_ackermann_settings_actor_id = None
    adapter._native_ackermann_settings_payload = None
    adapter.config = {
        "ackermann_use_native_control": True,
        "ackermann_decel_default": 3.5,
        "ackermann_brake_jerk_default": 9.0,
    }

    adapter._apply_ctrl(
        ControlCommand(
            mode=ControlMode.ACKERMANN,
            payload={"speed": 1.0, "steer": 0.1},
        )
    )

    assert adapter._ego_vehicle.applied_ackermann_control.speed == 1.0
    assert adapter._ego_vehicle.applied_ackermann_control.steer == pytest.approx(-0.1)
    assert adapter._ego_vehicle.applied_ackermann_control.acceleration == -3.5
    assert adapter._ego_vehicle.applied_ackermann_control.jerk == 9.0
    assert adapter._last_applied_control["backend"] == "native_ackermann"
    assert adapter._last_applied_control["decelerating"] is True


def test_native_ackermann_handoff_does_not_apply_raw_vehicle_control(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(
            AckermannControllerSettings=_FakeAckermannControllerSettings,
            VehicleAckermannControl=_FakeVehicleAckermannControl,
        ),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle(forward_speed=1.0)
    adapter._scenario_runner_tm_port = 8000
    adapter._yaw_sign = -1.0
    adapter._max_steer_rad = None
    adapter._last_applied_control = None
    adapter._native_ackermann_settings_actor_id = None
    adapter._native_ackermann_settings_payload = None
    adapter.config = {"ackermann_use_native_control": True}

    adapter._apply_ctrl(
        ControlCommand(
            mode=ControlMode.ACKERMANN,
            payload={
                "speed": 2.0,
                "steer": 0.2,
                "steer_speed": 0.4,
                "acceleration": 1.25,
                "jerk": -0.5,
            },
        )
    )

    assert adapter._ego_vehicle.applied_control is None
    assert adapter._ego_vehicle.applied_ackermann_control.speed == 2.0
    assert adapter._ego_vehicle.applied_ackermann_control.steer == pytest.approx(-0.2)
    assert adapter._ego_vehicle.applied_ackermann_control.steer_speed == 0.4
    assert adapter._ego_vehicle.applied_ackermann_control.acceleration == 1.25
    assert adapter._ego_vehicle.applied_ackermann_control.jerk == -0.5


@pytest.mark.parametrize(
    "payload",
    [
        {"steer": 0.0},
        {"steer": 0.0, "speed": -1.0},
        {"steer": 0.0, "speed": 1.0, "steer_speed": -0.1},
        {"steer": float("inf"), "speed": 1.0},
        {"steer": 0.0, "speed": 1.0, "steerAngle": 0.0},
    ],
)
def test_ackermann_rejects_noncanonical_payload(payload) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle()
    adapter._yaw_sign = -1.0

    with pytest.raises(InvalidSimulatorRequest):
        adapter._apply_ctrl(ControlCommand(mode=ControlMode.ACKERMANN, payload=payload))


@pytest.mark.parametrize(
    "mode",
    [
        ControlMode.TRAJECTORY,
        ControlMode.THROTTLE_STEER,
        ControlMode.WAYPOINTS,
        ControlMode.POSITION,
    ],
)
def test_legacy_control_modes_are_rejected(mode) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle()

    with pytest.raises(InvalidSimulatorRequest, match="legacy/reserved"):
        adapter._apply_ctrl(ControlCommand(mode=mode, payload={}))


def test_none_control_rejects_action_payload() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle()

    with pytest.raises(InvalidSimulatorRequest, match="must be empty"):
        adapter._apply_ctrl(ControlCommand(mode=ControlMode.NONE, payload={"speed": 0.0}))


def test_kinematic_deadbands_clamp_near_zero_values() -> None:
    from carla_wrapper import simulation

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter.config = {
        "kinematic_speed_deadband_mps": 0.02,
        "kinematic_acceleration_deadband_mps2": 0.15,
        "kinematic_yaw_rate_deadband_radps": 0.003,
        "kinematic_yaw_acceleration_deadband_radps2": 0.1,
    }

    assert adapter._apply_kinematic_deadbands(
        speed=0.01,
        acceleration=-0.12,
        yaw_rate=0.002,
        yaw_acceleration=-0.08,
    ) == (0.0, 0.0, 0.0, 0.0)

    assert adapter._apply_kinematic_deadbands(
        speed=0.03,
        acceleration=-0.2,
        yaw_rate=0.004,
        yaw_acceleration=-0.2,
    ) == (0.03, -0.2, 0.004, -0.2)


def test_kinematic_deadbands_use_default_config_values() -> None:
    from carla_wrapper import simulation

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter.config = {}

    assert adapter._apply_kinematic_deadbands(
        speed=0.01,
        acceleration=-0.12,
        yaw_rate=0.002,
        yaw_acceleration=-0.08,
    ) == (0.0, 0.0, 0.0, 0.0)


def test_kinematic_deadbands_can_be_disabled_explicitly() -> None:
    from carla_wrapper import simulation

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter.config = {
        "kinematic_speed_deadband_mps": 0.0,
        "kinematic_acceleration_deadband_mps2": 0.0,
        "kinematic_yaw_rate_deadband_radps": 0.0,
        "kinematic_yaw_acceleration_deadband_radps2": 0.0,
    }

    assert adapter._apply_kinematic_deadbands(
        speed=0.01,
        acceleration=-0.12,
        yaw_rate=0.002,
        yaw_acceleration=-0.08,
    ) == (0.01, -0.12, 0.002, -0.08)


def test_ackermann_native_backend_applies_controller_settings_once(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(
            AckermannControllerSettings=_FakeAckermannControllerSettings,
            VehicleAckermannControl=_FakeVehicleAckermannControl,
        ),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle(forward_speed=1.0)
    adapter._scenario_runner_tm_port = 8000
    adapter._yaw_sign = -1.0
    adapter._max_steer_rad = None
    adapter._last_applied_control = None
    adapter._native_ackermann_settings_actor_id = None
    adapter._native_ackermann_settings_payload = None
    adapter.config = {
        "ackermann_use_native_control": True,
        "ackermann_native_speed_kp": 0.3,
        "ackermann_native_speed_ki": 0.01,
        "ackermann_native_speed_kd": 0.4,
        "ackermann_native_accel_kp": 0.05,
        "ackermann_native_accel_ki": 0.02,
        "ackermann_native_accel_kd": 0.03,
    }

    ctrl = ControlCommand(
        mode=ControlMode.ACKERMANN,
        payload={"speed": 2.0, "steer": 0.0},
    )
    adapter._apply_ctrl(ctrl)
    adapter._apply_ctrl(ctrl)

    assert len(adapter._ego_vehicle.ackermann_settings_calls) == 1
    settings = adapter._ego_vehicle.ackermann_settings_calls[0]
    assert settings.speed_kp == 0.3
    assert settings.speed_ki == 0.01
    assert settings.speed_kd == 0.4
    assert settings.accel_kp == 0.05
    assert settings.accel_ki == 0.02
    assert settings.accel_kd == 0.03
    assert adapter._last_applied_control["controller_settings"] == {
        "speed_kp": 0.3,
        "speed_ki": 0.01,
        "speed_kd": 0.4,
        "accel_kp": 0.05,
        "accel_ki": 0.02,
        "accel_kd": 0.03,
    }


class _FakeVehicle:
    def __init__(self, forward_speed=0.0):
        self.id = 1
        self.applied_control = None
        self.applied_ackermann_control = None
        self.ackermann_settings_calls = []
        self.autopilot_calls = []
        self.simulate_physics_calls = []
        self.forward_speed = forward_speed

    def apply_control(self, control):
        self.applied_control = control

    def get_control(self):
        return self.applied_control or _FakeVehicleControl()

    def apply_ackermann_control(self, control):
        self.applied_ackermann_control = control

    def apply_ackermann_controller_settings(self, settings):
        self.ackermann_settings_calls.append(settings)

    def set_autopilot(self, enabled, port=None):
        self.autopilot_calls.append((enabled, port))

    def set_simulate_physics(self, enabled):
        self.simulate_physics_calls.append(enabled)

    def get_velocity(self):
        return SimpleNamespace(x=self.forward_speed, y=0.0, z=0.0)

    def get_transform(self):
        return SimpleNamespace(get_forward_vector=lambda: SimpleNamespace(x=1.0, y=0.0, z=0.0))


class _FakeVehicleControl:
    def __init__(self, throttle=0.0, steer=0.0, brake=0.0):
        self.throttle = throttle
        self.steer = steer
        self.brake = brake


class _FakeAckermannControllerSettings:
    def __init__(
        self,
        speed_kp=0.15,
        speed_ki=0.0,
        speed_kd=0.25,
        accel_kp=0.01,
        accel_ki=0.0,
        accel_kd=0.01,
    ):
        self.speed_kp = speed_kp
        self.speed_ki = speed_ki
        self.speed_kd = speed_kd
        self.accel_kp = accel_kp
        self.accel_ki = accel_ki
        self.accel_kd = accel_kd


class _FakeVehicleAckermannControl:
    def __init__(
        self,
        steer=0.0,
        steer_speed=0.0,
        speed=0.0,
        acceleration=0.0,
        jerk=0.0,
    ):
        self.steer = steer
        self.steer_speed = steer_speed
        self.speed = speed
        self.acceleration = acceleration
        self.jerk = jerk
