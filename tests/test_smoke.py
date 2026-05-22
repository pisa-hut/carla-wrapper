"""Smoke tests for the pisa-api simulator-friendly contract."""

from types import SimpleNamespace

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
    adapter._destroy_collision_sensor = lambda: calls.append("destroy_collision_sensor")
    adapter._restore_world_settings = lambda: calls.append("restore_world_settings")
    adapter._stop_scenario_runner_module = lambda: calls.append("stop_scenario_runner")
    adapter._clear_spawned_actor_tracking = lambda: calls.append("clear_tracking")

    adapter._finalize()

    assert calls == [
        "stop_recorder",
        "destroy_collision_sensor",
        "restore_world_settings",
        "stop_scenario_runner",
        "clear_tracking",
    ]
    assert adapter._finalized is True


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
        return SimpleNamespace(
            get_forward_vector=lambda: SimpleNamespace(x=1.0, y=0.0, z=0.0)
        )


class _FakeVehicleControl:
    def __init__(self, throttle=0.0, steer=0.0, brake=0.0):
        self.throttle = throttle
        self.steer = steer
        self.brake = brake
