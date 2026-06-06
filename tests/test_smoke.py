"""Smoke tests for the pisa-api simulator-friendly contract."""

from types import SimpleNamespace

import pytest
from pisa_api.simulator import (
    ControlCommand,
    ControlMode,
    InvalidSimulatorRequest,
    RuntimeFrameData,
    SimulatorTimeout,
    StepRequest,
)


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
    adapter._tick_scenario_runner_module = lambda: calls.append("scenario_runner")
    adapter._ego_vehicle = SimpleNamespace(
        apply_control=lambda control: calls.append(("control", control)),
    )
    adapter._yaw_sign = 1.0
    adapter._collect_runtime_frame = lambda: RuntimeFrameData()

    ctrl = ControlCommand(
        mode=ControlMode.THROTTLE_STEER_BREAK,
        payload={"throttle": 0.0, "brake": 1.0, "steer": 0.0},
    )

    response = adapter.step(StepRequest(ctrl_cmd=ctrl))

    assert calls[0] == "scenario_runner"
    assert calls[1][0] == "control"
    assert calls[2] == "world_tick"
    assert isinstance(response.frame, RuntimeFrameData)


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
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = False
    adapter._finalize = lambda: calls.append("finalize")
    adapter._ensure_connected = lambda: True
    adapter._prepare_reused_server_state = lambda: calls.append("prepare_reused_server")

    request = SimpleNamespace(
        output_dir="out",
        config={},
        scenario=SimpleNamespace(format="open_scenario1"),
        dt=0.05,
    )

    adapter.init(request)

    assert calls == ["finalize", "prepare_reused_server"]


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
    adapter._last_object_index_by_actor_id = {}
    adapter._last_applied_control = None
    adapter._yaw_sign = 1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}

    adapter._reset_episode_clock()
    frame = adapter._collect_runtime_frame()

    assert frame.sim_time_ns == 0
    assert frame.objects[0].kinematic.time_ns == 0
    assert frame.extras["carla_frame"] == 120
    assert frame.extras["carla_time_ns"] == 12_500_000_000
    assert frame.extras["episode_frame"] == 0

    world.frame = 122
    world.elapsed_seconds = 12.6
    frame = adapter._collect_runtime_frame()

    assert frame.sim_time_ns == 100_000_000
    assert frame.objects[0].kinematic.time_ns == 100_000_000
    assert frame.extras["episode_frame"] == 2


def test_collect_objects_keeps_ego_first_and_sorts_other_actors_by_xy() -> None:
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
    adapter._last_object_index_by_actor_id = {}
    adapter._episode_start_carla_time_ns = 0
    adapter._yaw_sign = 1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}

    objects, _, _, _ = adapter._collect_objects()

    assert [obj.kinematic.x for obj in objects] == [10.0, 1.0, 2.0, 2.0]
    assert [obj.kinematic.y for obj in objects] == [10.0, 5.0, 1.0, 3.0]
    assert adapter._last_object_index_by_actor_id == {1: 0, 2: 1, 3: 2, 4: 3}


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
    adapter._last_object_index_by_actor_id = {1: 0, 2: 1}

    collisions = adapter._collect_collision_infos()

    assert len(collisions) == 1
    assert collisions[0].details["carla_frame"] == 122
    assert collisions[0].details["episode_frame"] == 2
    assert collisions[0].details["carla_timestamp_seconds"] == 12.6
    assert collisions[0].details["timestamp_seconds"] == pytest.approx(0.1)


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
    adapter._sync = False
    ego_vehicle = _FakeKinematicActor(11)
    adapter._ego_vehicle = ego_vehicle
    adapter._clear_collision_events = lambda: calls.append("clear_collision_events")
    adapter._ensure_world = lambda scenario_pack, generate_opendrive_world=True: calls.append(
        ("ensure_world", generate_opendrive_world)
    )
    adapter._clear_dynamic_actors = lambda: calls.append("clear_dynamic_actors")
    adapter._apply_world_settings = lambda: calls.append("apply_world_settings")
    adapter._client = SimpleNamespace(start_recorder=lambda path: calls.append("start_recorder"))
    adapter._start_scenario_runner = lambda scenario_pack, params: calls.append(
        "start_scenario_runner"
    )
    adapter._setup_collision_sensor = lambda: calls.append("setup_collision_sensor")
    adapter._tick_scenario_runner_module = lambda: calls.append("tick_scenario_runner")
    adapter._collect_collision_infos = lambda: []
    original_collect_runtime_frame = adapter._collect_runtime_frame

    def collect_runtime_frame(speed_overrides=None):
        calls.append(("collect_runtime_frame", speed_overrides))
        return original_collect_runtime_frame(speed_overrides=speed_overrides)

    adapter._collect_runtime_frame = collect_runtime_frame
    adapter._finalize = lambda: calls.append("finalize")
    adapter._sr_scenario = SimpleNamespace(
        config=SimpleNamespace(
            ego_vehicles=[SimpleNamespace(rolename="ego", speed=5.0)],
            other_actors=[],
        ),
        ego_vehicles=[SimpleNamespace(id=11, attributes={"role_name": "ego"})],
        other_actors=[],
    )
    adapter._world = _FakeRuntimeWorld([ego_vehicle], frame=10, elapsed_seconds=0.1)
    adapter._objects_by_id = {}
    adapter._prev_yaw_rate = {}
    adapter._last_object_index_by_actor_id = {}
    adapter._last_applied_control = None
    adapter._yaw_sign = 1.0
    adapter._yaw_offset_deg = 0.0
    adapter.config = {}

    request = SimpleNamespace(output_dir="run", scenario_pack=object(), params={})

    response = adapter.reset(request)

    assert ("ensure_world", False) in calls
    assert "apply_world_settings" not in calls
    assert calls.index("start_scenario_runner") < calls.index("start_recorder")
    assert calls.index("start_recorder") < calls.index("setup_collision_sensor")
    assert "tick_scenario_runner" not in calls
    assert ("collect_runtime_frame", {11: 5.0}) in calls
    assert ego_vehicle.set_velocity_calls == [SimpleNamespace(x=5.0, y=0.0, z=0.0)]
    adapter._world.frame = 11
    adapter._world.elapsed_seconds = 0.3
    ego_vehicle.forward_speed = 8.0
    step_frame = adapter._collect_runtime_frame()
    assert step_frame.objects[0].kinematic.acceleration == pytest.approx(15.0)
    assert step_frame.objects[0].kinematic.speed == pytest.approx(8.0)
    assert adapter._initial_speed_acceleration_pending is False

    adapter._world.frame = 12
    adapter._world.elapsed_seconds = 0.35
    ego_vehicle.forward_speed = 9.0
    ego_vehicle.forward_accel = 0.7
    next_frame = adapter._collect_runtime_frame()
    assert next_frame.objects[0].kinematic.acceleration == pytest.approx(0.7)
    assert isinstance(response.frame, RuntimeFrameData)


def test_ensure_world_can_skip_opendrive_generation() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    world = _FakeSettingsWorld()
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._server_version = "test"
    adapter._client = _FakeClient(world)

    adapter._ensure_world(SimpleNamespace(), generate_opendrive_world=False)

    assert adapter._world is world
    assert adapter._client.generated is False


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
    adapter._ensure_connected = lambda: False

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

    def tick(self):
        self._calls.append("world_tick")

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


class _FakeKinematicActor:
    def __init__(self, actor_id, x=0.0, y=0.0, z=0.0):
        self.id = actor_id
        self.type_id = "vehicle.test"
        self.x = x
        self.y = y
        self.z = z
        self.forward_speed = 1.0
        self.forward_accel = 0.0
        self.bounding_box = SimpleNamespace(extent=SimpleNamespace(x=2.0, y=1.0, z=0.75))
        self.set_velocity_calls = []

    def get_velocity(self):
        return SimpleNamespace(x=self.forward_speed, y=0.0, z=0.0)

    def get_acceleration(self):
        return SimpleNamespace(x=self.forward_accel, y=0.0, z=0.0)

    def get_angular_velocity(self):
        return SimpleNamespace(z=0.0)

    def get_transform(self):
        return SimpleNamespace(
            location=SimpleNamespace(x=self.x, y=self.y, z=self.z),
            rotation=SimpleNamespace(yaw=0.0),
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


def test_first_post_reset_control_suppresses_brake_once(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle()
    adapter._world = _FakeWorld([])
    adapter._sync = True
    adapter._tick_scenario_runner_module = lambda: None
    adapter._collect_runtime_frame = lambda: RuntimeFrameData()
    adapter._yaw_sign = 1.0
    adapter._last_applied_control = None
    adapter._initial_velocity_brake_suppression_pending = True

    ctrl = ControlCommand(
        mode=ControlMode.THROTTLE_STEER_BREAK,
        payload={"throttle": 0.0, "brake": 0.3, "steer": 0.2},
    )
    adapter.step(StepRequest(ctrl_cmd=ctrl))

    assert adapter._ego_vehicle.applied_control.brake == 0.0
    assert adapter._ego_vehicle.applied_control.steer == 0.2
    assert adapter._initial_velocity_brake_suppression_pending is False

    adapter.step(StepRequest(ctrl_cmd=ctrl))

    assert adapter._ego_vehicle.applied_control.brake == 0.3


def test_ackermann_defaults_to_vehicle_control_backend(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle(forward_speed=4.0)
    adapter._scenario_runner_tm_port = 8000
    adapter._yaw_sign = 1.0
    adapter._max_steer_rad = 0.5
    adapter._last_applied_control = None
    adapter.config = {"ackermann_speed_kp": 0.5}

    adapter._apply_ctrl(
        ControlCommand(
            mode=ControlMode.ACKERMANN,
            payload={"speed": 0.0, "steer": 0.25},
        )
    )

    assert adapter._ego_vehicle.applied_control.throttle == 0.0
    assert adapter._ego_vehicle.applied_control.brake == 1.0
    assert adapter._ego_vehicle.applied_control.steer == 0.5
    assert adapter._last_applied_control["backend"] == "vehicle_control"
    assert adapter._last_applied_control["current_forward_speed"] == 4.0
    assert adapter._last_applied_control["decelerating"] is True


def test_ackermann_vehicle_control_backend_uses_launch_throttle_from_stop(
    monkeypatch,
) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle(forward_speed=0.0)
    adapter._scenario_runner_tm_port = 8000
    adapter._yaw_sign = 1.0
    adapter._max_steer_rad = None
    adapter._last_applied_control = None
    adapter.config = {
        "ackermann_speed_kp": 0.1,
        "ackermann_min_throttle": 0.25,
        "ackermann_launch_throttle": 0.5,
    }

    adapter._apply_ctrl(
        ControlCommand(
            mode=ControlMode.ACKERMANN,
            payload={"speed": 1.0, "steer": 0.0},
        )
    )

    assert adapter._ego_vehicle.applied_control.throttle == 0.5
    assert adapter._ego_vehicle.applied_control.brake == 0.0
    assert adapter._last_applied_control["current_forward_speed"] == 0.0


def test_ackermann_vehicle_control_backend_uses_brake_gain(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle(forward_speed=4.0)
    adapter._scenario_runner_tm_port = 8000
    adapter._yaw_sign = 1.0
    adapter._max_steer_rad = None
    adapter._last_applied_control = None
    adapter.config = {
        "ackermann_brake_kp": 0.25,
        "ackermann_min_brake": 0.15,
        "ackermann_max_brake": 0.6,
    }

    adapter._apply_ctrl(
        ControlCommand(
            mode=ControlMode.ACKERMANN,
            payload={"speed": 2.0, "steer": 0.0},
        )
    )

    assert adapter._ego_vehicle.applied_control.throttle == 0.0
    assert adapter._ego_vehicle.applied_control.brake == 0.5
    assert adapter._last_applied_control["decelerating"] is True


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
    adapter._yaw_sign = 1.0
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
    adapter._yaw_sign = 1.0
    adapter._max_steer_rad = None
    adapter._last_applied_control = None
    adapter._native_ackermann_settings_actor_id = None
    adapter._native_ackermann_settings_payload = None
    adapter.config = {"ackermann_use_native_control": True}

    adapter._apply_ctrl(
        ControlCommand(
            mode=ControlMode.ACKERMANN,
            payload={"speed": 2.0, "steer": 0.0},
        )
    )

    assert adapter._ego_vehicle.applied_control is None
    assert adapter._ego_vehicle.applied_ackermann_control.speed == 2.0


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


def test_kinematic_deadbands_default_to_disabled() -> None:
    from carla_wrapper import simulation

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter.config = {}

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
    adapter._yaw_sign = 1.0
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
