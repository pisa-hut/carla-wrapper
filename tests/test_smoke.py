"""Smoke tests for the pisa-api simulator-friendly contract."""

from types import SimpleNamespace

import pytest
from pisa_api.simulator import ControlCommand, ControlMode, RuntimeFrameData, StepRequest


def test_public_imports_use_pisa_api_simulator_contract() -> None:
    from pisa_api.simulator import RuntimeFrameData as PisaRuntimeFrameData

    from carla_wrapper.simulation import CarlaAdapter

    assert CarlaAdapter.init.__annotations__["request"].__name__ == "InitRequest"
    assert CarlaAdapter.reset.__annotations__["request"].__name__ == "ResetRequest"
    assert CarlaAdapter.step.__annotations__["request"].__name__ == "StepRequest"
    assert PisaRuntimeFrameData.__name__ == "RuntimeFrameData"
    assert CarlaAdapter is not None


def test_step_applies_external_control_after_scenario_runner_tick() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._world = _FakeWorld(calls)
    adapter._sync = True
    adapter._tick_scenario_runner_module = lambda: calls.append("scenario_runner")
    adapter._apply_ctrl = lambda ctrl: calls.append(("control", ctrl))
    adapter._collect_runtime_frame = lambda: RuntimeFrameData()

    ctrl = ControlCommand(
        mode=ControlMode.THROTTLE_STEER_BREAK,
        payload={"throttle": 0.0, "brake": 1.0, "steer": 0.0},
    )

    adapter.step(StepRequest(ctrl_cmd=ctrl))

    assert calls == ["scenario_runner", ("control", ctrl), "world_tick"]


def test_disable_scenario_runner_ego_control_removes_only_ego(monkeypatch) -> None:
    from carla_wrapper import simulation

    ego_controller = _FakeController()
    target_controller = _FakeController()
    blackboard = _FakeBlackboard(
        {
            1: ego_controller,
            2: target_controller,
        }
    )
    monkeypatch.setattr(
        simulation,
        "py_trees",
        SimpleNamespace(blackboard=SimpleNamespace(Blackboard=lambda: blackboard)),
    )
    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )

    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle()
    adapter._disable_sr_ego_control = True
    adapter._last_applied_control = None

    adapter._disable_scenario_runner_ego_control()

    assert blackboard.actor_dict == {2: target_controller}
    assert ego_controller.reset_calls == 1
    assert target_controller.reset_calls == 0
    assert adapter._ego_vehicle.applied_control.throttle == 0.0
    assert adapter._ego_vehicle.applied_control.brake == 0.0
    assert adapter._ego_vehicle.applied_control.steer == 0.0
    assert adapter._last_applied_control["mode"] == "CLEAR_EGO_CONTROL"


def test_scenario_runner_ego_control_is_disabled_after_configured_ticks(monkeypatch) -> None:
    from carla_wrapper import simulation

    calls = []
    ego_controller = _FakeController()
    blackboard = _FakeBlackboard({1: ego_controller})
    monkeypatch.setattr(
        simulation,
        "py_trees",
        SimpleNamespace(
            blackboard=SimpleNamespace(Blackboard=lambda: blackboard),
            common=SimpleNamespace(Status=SimpleNamespace(RUNNING="RUNNING")),
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
    adapter._disable_sr_ego_control = True
    adapter._sr_ego_control_ticks = 0
    adapter._sr_ego_control_ticks_before_disable = 1
    adapter._quit_flag = False

    adapter._tick_scenario_runner_module()

    assert calls == ["game_time", "data_provider", "tree_tick"]
    assert blackboard.actor_dict == {}
    assert ego_controller.reset_calls == 1


def test_finalize_destroys_collision_sensor_before_scenario_runner_cleanup() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._client = SimpleNamespace(stop_recorder=lambda: calls.append("stop_recorder"))
    adapter._destroy_spawned_actors = lambda: calls.append("destroy_spawned_actors")
    adapter._stop_scenario_runner_module = lambda: calls.append("stop_scenario_runner")

    adapter._finalize()

    assert calls == [
        "stop_recorder",
        "destroy_spawned_actors",
        "stop_scenario_runner",
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

    adapter._finalize()

    assert calls == [
        "destroy_spawned_actors",
        "stop_scenario_runner",
    ]
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
    adapter._start_scenario_runner = lambda scenario_pack, params: (_ for _ in ()).throw(
        RuntimeError("scenario failed")
    )
    adapter._finalize = lambda: calls.append("finalize")

    request = SimpleNamespace(output_dir="run", scenario_pack=object(), params={})

    with pytest.raises(RuntimeError, match="scenario failed"):
        adapter.reset(request)

    assert calls[-1] == "finalize"
    assert calls[1][0] == "ensure_world"


def test_open_scenario_reset_delegates_world_loading_to_scenario_runner(tmp_path) -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._finalized = True
    adapter._output_base = tmp_path
    adapter.scenario = SimpleNamespace(format="open_scenario1")
    adapter._sync = False
    adapter._ego_vehicle = object()
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
    adapter._collect_runtime_frame = lambda: RuntimeFrameData()
    adapter._finalize = lambda: calls.append("finalize")

    request = SimpleNamespace(output_dir="run", scenario_pack=object(), params={})

    adapter.reset(request)

    assert ("ensure_world", False) in calls
    assert "apply_world_settings" not in calls


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

    with pytest.raises(RuntimeError, match="ScenarioPack is required"):
        adapter._ensure_world(None)


def test_ensure_world_stops_when_reconnect_fails() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._server_version = None
    adapter._client = None
    adapter._ensure_connected = lambda: False

    with pytest.raises(RuntimeError, match="Failed to connect to CARLA"):
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

    with pytest.raises(RuntimeError, match="OpenSCENARIO file not found"):
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
            calls.append(("request_new_actor", FakeDataProvider.current_world))
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
    adapter._client = SimpleNamespace(get_trafficmanager=lambda port: _FakeTrafficManager())
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
    assert ("request_new_actor", new_world) in calls
    assert ("open_scenario", new_world, True) in calls
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


def test_lifecycle_clear_dynamic_actors_forces_async_and_returns_count() -> None:
    from carla_wrapper.lifecycle import clear_dynamic_actors

    vehicle = _FakeActor(actor_id=1, type_id="vehicle.tesla.model3")
    walker = _FakeActor(actor_id=2, type_id="walker.pedestrian.0001")
    sensor = _FakeActor(actor_id=3, type_id="sensor.other.collision")
    traffic_light = _FakeActor(actor_id=4, type_id="traffic.traffic_light")
    world = _FakeSettingsWorld(
        [vehicle, walker, sensor, traffic_light],
        synchronous_mode=True,
        fixed_delta_seconds=0.05,
    )
    traffic_manager = _FakeTrafficManager()
    client = _FakeClient(world, traffic_manager=traffic_manager)

    destroyed_count = clear_dynamic_actors(world, client=client, traffic_manager_port=8000)

    assert destroyed_count == 3
    assert world.settings.synchronous_mode is False
    assert world.settings.fixed_delta_seconds is None
    assert world.applied_settings is world.settings
    assert traffic_manager.sync_calls == [False]
    assert vehicle.destroy_calls == 1
    assert walker.destroy_calls == 1
    assert sensor.destroy_calls == 1
    assert traffic_light.destroy_calls == 0


def test_lifecycle_destroy_actor_treats_false_return_as_failure() -> None:
    from carla_wrapper.lifecycle import destroy_actor

    actor = _DestroyFalseActor(actor_id=1)

    assert destroy_actor(actor) is False
    assert actor.destroy_calls == 1


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


def test_start_scenario_runner_does_not_require_legacy_path() -> None:
    from carla_wrapper.simulation import CarlaAdapter

    calls = []
    adapter = CarlaAdapter.__new__(CarlaAdapter)
    adapter._start_scenario_runner_module = lambda sps, params: calls.append((sps, params))

    adapter._start_scenario_runner("scenario_pack", {"k": "v"})

    assert calls == [("scenario_pack", {"k": "v"})]


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
    def __init__(self, calls):
        self.status = "RUNNING"
        self._calls = calls

    def tick_once(self):
        self._calls.append("tree_tick")


class _FakeBlackboard:
    def __init__(self, actor_dict):
        self.actor_dict = actor_dict

    @property
    def ActorsWithController(self):
        return self.actor_dict

    def set(self, name, value, overwrite=False):
        assert name == "ActorsWithController"
        assert overwrite is True
        self.actor_dict = value


class _FakeController:
    def __init__(self):
        self.reset_calls = 0

    def reset(self):
        self.reset_calls += 1


class _FakeTrafficManager:
    def __init__(self):
        self.sync_calls = []
        self.seed_calls = []

    def set_synchronous_mode(self, enabled):
        self.sync_calls.append(enabled)

    def set_random_device_seed(self, seed):
        self.seed_calls.append(seed)


class _FakeSettingsWorld:
    def __init__(self, actors=None, synchronous_mode=False, fixed_delta_seconds=None):
        self.settings = SimpleNamespace(
            synchronous_mode=synchronous_mode,
            no_rendering_mode=False,
            fixed_delta_seconds=fixed_delta_seconds,
        )
        self.applied_settings = None
        self.actors = list(actors or [])

    def get_settings(self):
        return self.settings

    def apply_settings(self, settings):
        self.applied_settings = settings

    def get_actors(self):
        return list(self.actors)


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


def test_vehicle_control_disables_autopilot_and_brake_overrides_throttle(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle()
    adapter._scenario_runner_tm_port = 8000
    adapter._yaw_sign = 1.0
    adapter._last_applied_control = None
    adapter._external_control_prepared_actor_id = None

    adapter._apply_ctrl(
        ControlCommand(
            mode=ControlMode.THROTTLE_STEER_BREAK,
            payload={"throttle": 1.0, "brake": 1.0, "steer": 0.25},
        )
    )

    assert adapter._ego_vehicle.autopilot_calls == [(False, 8000)]
    assert adapter._ego_vehicle.applied_control.throttle == 0.0
    assert adapter._ego_vehicle.applied_control.brake == 1.0
    assert adapter._ego_vehicle.applied_control.steer == 0.25


def test_external_control_preparation_runs_once_per_actor(monkeypatch) -> None:
    from carla_wrapper import simulation

    monkeypatch.setattr(
        simulation,
        "carla",
        SimpleNamespace(VehicleControl=_FakeVehicleControl),
    )
    adapter = simulation.CarlaAdapter.__new__(simulation.CarlaAdapter)
    adapter._ego_vehicle = _FakeVehicle()
    adapter._scenario_runner_tm_port = 8000
    adapter._yaw_sign = 1.0
    adapter._last_applied_control = None
    adapter._external_control_prepared_actor_id = None

    ctrl = ControlCommand(
        mode=ControlMode.THROTTLE_STEER_BREAK,
        payload={"throttle": 0.2, "brake": 0.0, "steer": 0.0},
    )
    adapter._apply_ctrl(ctrl)
    adapter._apply_ctrl(ctrl)

    assert adapter._ego_vehicle.autopilot_calls == [(False, 8000)]
    assert adapter._ego_vehicle.simulate_physics_calls == [True]


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
    adapter._external_control_prepared_actor_id = None
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


def test_ackermann_vehicle_control_backend_uses_min_throttle_from_stop(monkeypatch) -> None:
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
    adapter._external_control_prepared_actor_id = None
    adapter.config = {"ackermann_speed_kp": 0.1, "ackermann_min_throttle": 0.25}

    adapter._apply_ctrl(
        ControlCommand(
            mode=ControlMode.ACKERMANN,
            payload={"speed": 1.0, "steer": 0.0},
        )
    )

    assert adapter._ego_vehicle.applied_control.throttle == 0.25
    assert adapter._ego_vehicle.applied_control.brake == 0.0


class _FakeVehicle:
    def __init__(self, forward_speed=0.0):
        self.id = 1
        self.applied_control = None
        self.autopilot_calls = []
        self.simulate_physics_calls = []
        self.forward_speed = forward_speed

    def apply_control(self, control):
        self.applied_control = control

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
