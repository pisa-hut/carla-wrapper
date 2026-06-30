import hashlib
import logging
import math
import os
import subprocess
import time
import weakref
from collections import deque
from pathlib import Path
from threading import Lock

from pisa_api.simulator import (
    ActorRefData,
    ActorRole,
    CollisionInfoData,
    ControlCommand,
    ControlMode,
    InitRequest,
    InvalidSimulatorRequest,
    ObjectKinematicData,
    ObjectStateData,
    ResetRequest,
    ResetResponse,
    RoadObjectType,
    RuntimeFrameData,
    ScenarioPackData,
    ShapeCenterPoseData,
    ShapeData,
    ShapeDimensionData,
    ShapeType,
    ShouldQuitResponse,
    SimulatorEgoData,
    SimulatorObjectData,
    SimulatorPreconditionFailed,
    SimulatorTimeout,
    SimulatorUnavailable,
    StepRequest,
    StepResponse,
)

from .lifecycle import (
    clear_dynamic_actors,
    destroy_actor,
    force_async_world_for_cleanup,
    is_dynamic_actor,
)

try:
    import carla
    import py_trees
    from srunner.scenarioconfigs.openscenario_configuration import (
        OpenScenarioConfiguration,
    )
    from srunner.scenariomanager.carla_data_provider import CarlaDataProvider
    from srunner.scenariomanager.timer import GameTime
    from srunner.scenarios.open_scenario import OpenScenario
    from srunner.scenarios.route_scenario import RouteScenario
    from srunner.tools.route_parser import RouteParser
except ImportError as exc:
    carla = None
    py_trees = None
    OpenScenarioConfiguration = None
    CarlaDataProvider = None
    GameTime = None
    OpenScenario = None
    RouteScenario = None
    RouteParser = None
    _CARLA_IMPORT_ERROR = exc
else:
    _CARLA_IMPORT_ERROR = None

logger = logging.getLogger(__name__)


DEFAULT_CONFIG = {
    "synchronous_mode": True,
    "no_rendering_mode": True,
    "record": False,
    "open_scenario_map_loader": "scenario_runner",
    "yaw_sign": -1.0,
    "yaw_offset_deg": 0.0,
    "carla_connect_timeout_seconds": 40,
    "retry_interval_seconds": 2,
    "scenario_runner_tm_seed": 0,
    "kinematic_speed_deadband_mps": 0.02,
    "kinematic_acceleration_deadband_mps2": 0.15,
    "kinematic_yaw_rate_deadband_radps": 0.003,
    "kinematic_yaw_acceleration_deadband_radps2": 0.1,
    "ackermann_use_native_control": False,
    "ackermann_native_speed_kp": 0.10,
    "ackermann_native_speed_ki": 0.0,
    "ackermann_native_speed_kd": 0.10,
    "ackermann_native_accel_kp": 0.01,
    "ackermann_native_accel_ki": 0.0,
    "ackermann_native_accel_kd": 0.0,
    "ackermann_speed_kp": 0.5,
    "ackermann_min_throttle": 0.2,
    "ackermann_max_throttle": 0.75,
    "ackermann_stop_speed_threshold": 0.25,
    "ackermann_launch_speed_threshold": 0.1,
    "ackermann_launch_target_threshold": 0.25,
    "ackermann_launch_throttle": 0.45,
    "ackermann_brake_kp": 0.6,
    "ackermann_min_brake": 0.15,
    "ackermann_max_brake": 1.0,
    "ackermann_accel_default": 1.5,
    "ackermann_decel_default": 4.0,
    "ackermann_jerk_default": 0.0,
    "ackermann_brake_jerk_default": 8.0,
}


_OPEN_SCENARIO_MAP_LOADERS = {"scenario_runner", "wrapper"}


class _WorldLoadingDisabledClient:
    """Forward CARLA client calls while preventing ScenarioRunner world replacement."""

    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        return getattr(self._client, name)

    def _world_loading_disabled(self, *_args, **_kwargs):
        raise InvalidSimulatorRequest(
            "ScenarioRunner tried to replace a wrapper-generated world; ensure the "
            "OpenSCENARIO LogicFile uses the same OpenDRIVE map as ScenarioPack map_name"
        )

    generate_opendrive_world = _world_loading_disabled
    load_world = _world_loading_disabled
    reload_world = _world_loading_disabled


_VEHICLE_TYPE_BY_BLUEPRINT_ID = {
    "vehicle.audi.a2": RoadObjectType.CAR,
    "vehicle.audi.etron": RoadObjectType.CAR,
    "vehicle.audi.tt": RoadObjectType.CAR,
    "vehicle.bmw.grandtourer": RoadObjectType.CAR,
    "vehicle.chevrolet.impala": RoadObjectType.CAR,
    "vehicle.citroen.c3": RoadObjectType.CAR,
    "vehicle.dodge.charger_2020": RoadObjectType.CAR,
    "vehicle.dodge.charger_police": RoadObjectType.CAR,
    "vehicle.dodge.charger_police_2020": RoadObjectType.CAR,
    "vehicle.ford.crown": RoadObjectType.CAR,
    "vehicle.ford.mustang": RoadObjectType.CAR,
    "vehicle.jeep.wrangler_rubicon": RoadObjectType.CAR,
    "vehicle.lincoln.mkz_2017": RoadObjectType.CAR,
    "vehicle.lincoln.mkz_2020": RoadObjectType.CAR,
    "vehicle.mercedes.coupe": RoadObjectType.CAR,
    "vehicle.mercedes.coupe_2020": RoadObjectType.CAR,
    "vehicle.micro.microlino": RoadObjectType.CAR,
    "vehicle.mini.cooper_s": RoadObjectType.CAR,
    "vehicle.mini.cooper_s_2021": RoadObjectType.CAR,
    "vehicle.nissan.micra": RoadObjectType.CAR,
    "vehicle.nissan.patrol": RoadObjectType.CAR,
    "vehicle.nissan.patrol_2021": RoadObjectType.CAR,
    "vehicle.seat.leon": RoadObjectType.CAR,
    "vehicle.tesla.model3": RoadObjectType.CAR,
    "vehicle.toyota.prius": RoadObjectType.CAR,
    "vehicle.carlamotors.carlacola": RoadObjectType.TRUCK,
    "vehicle.carlamotors.european_hgv": RoadObjectType.TRUCK,
    "vehicle.carlamotors.firetruck": RoadObjectType.TRUCK,
    "vehicle.tesla.cybertruck": RoadObjectType.TRUCK,
    "vehicle.ford.ambulance": RoadObjectType.VAN,
    "vehicle.mercedes.sprinter": RoadObjectType.VAN,
    "vehicle.volkswagen.t2": RoadObjectType.VAN,
    "vehicle.volkswagen.t2_2021": RoadObjectType.VAN,
    "vehicle.mitsubishi.fusorosa": RoadObjectType.BUS,
    "vehicle.harley-davidson.low_rider": RoadObjectType.MOTORCYCLE,
    "vehicle.kawasaki.ninja": RoadObjectType.MOTORCYCLE,
    "vehicle.vespa.zx125": RoadObjectType.MOTORCYCLE,
    "vehicle.yamaha.yzf": RoadObjectType.MOTORCYCLE,
    "vehicle.bh.crossbike": RoadObjectType.BICYCLE,
    "vehicle.diamondback.century": RoadObjectType.BICYCLE,
    "vehicle.gazelle.omafiets": RoadObjectType.BICYCLE,
}


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


class CarlaAdapter:
    def __init__(self):
        if _CARLA_IMPORT_ERROR is not None:
            raise RuntimeError(
                "CARLA Python API and ScenarioRunner must be installed to run CarlaAdapter"
            ) from _CARLA_IMPORT_ERROR

        self._client = None
        self._server_version = None
        self._world = None
        self.config = None
        self._output_base = None
        self._output_dir = None
        self._finalized = True
        self._objects_by_id = {}
        self._prev_yaw_rate = {}
        self._active_actor_ids: set[int] = set()
        self._retired_actor_ids: set[int] = set()
        self._actor_metadata_by_id: dict[int, tuple[str | None, ActorRole]] = {}
        self._xosc_entity_names: set[str] = set()
        self._collision_sensor = None
        self._collision_events = deque()
        self._collision_lock = Lock()
        self._last_applied_control: dict | None = None
        self._quit_flag = False
        self._spawned_actor_ids: set[int] = set()
        self._sr_scenario = None
        self._sr_tree = None
        self._sr_running = False
        self._sr_ego_vehicles: list = []
        self._sr_last_tick_timestamp = None
        self._traffic_manager = None
        self._traffic_manager_sync_enabled = False
        self._pre_scenario_actor_ids: set[int] | None = None
        self._episode_start_carla_time_ns: int | None = None
        self._episode_start_carla_frame: int | None = None
        self._initial_speed_acceleration_pending = False
        self._initial_velocity_brake_suppression_pending = False
        self._initial_speed_overrides: dict[int, float] = {}
        self._quit_msg = ""

        self._server_log_path = "/mnt/output/carla_server"
        os.makedirs(self._server_log_path, exist_ok=True)
        with (
            open(f"{self._server_log_path}/stdout.log", "w") as out,
            open(f"{self._server_log_path}/stderr.log", "w") as err,
        ):
            subprocess.Popen(
                ["/app/carla_server.sh"],
                stdout=out,
                stderr=err,
            )
        logger.info("CARLA service launched.")

    def should_quit(self) -> ShouldQuitResponse:
        msg = getattr(self, "_quit_msg", "") if self._quit_flag else ""
        if self._quit_flag and not msg:
            msg = "CARLA simulator requested quit"
        return ShouldQuitResponse(should_quit=self._quit_flag, msg=msg)

    def init(self, request: InitRequest) -> None:
        self._output_base = request.output_dir
        self.config = request.config
        self.scenario = request.scenario

        if not self._finalized:
            self._finalize()

        if not self._ensure_connected():
            raise SimulatorTimeout("Timed out connecting to CARLA")

        self._fixed_delta_seconds = request.dt

        self._sync = bool(self._config_value("synchronous_mode"))
        self._no_rendering = bool(self._config_value("no_rendering_mode"))
        self._record = bool(self._config_value("record"))
        self._yaw_sign = float(self._config_value("yaw_sign"))
        self._yaw_offset_deg = float(self._config_value("yaw_offset_deg"))
        self._open_scenario_map_loader = (
            str(self._config_value("open_scenario_map_loader")).strip().lower()
        )
        if self._open_scenario_map_loader not in _OPEN_SCENARIO_MAP_LOADERS:
            choices = ", ".join(sorted(_OPEN_SCENARIO_MAP_LOADERS))
            raise InvalidSimulatorRequest(
                f"Invalid open_scenario_map_loader '{self._open_scenario_map_loader}'; "
                f"expected one of: {choices}"
            )

        self._max_steer_rad: float | None = None
        self._last_applied_control = None
        self._native_ackermann_settings_actor_id = None
        self._native_ackermann_settings_payload = None
        self._quit_flag = False
        self._quit_msg = ""

        self._spawned_actor_ids: set[int] = set()

        self._scenario_runner_tm_port = int(os.environ.get("CARLA_TM_PORT", 8000))
        self._scenario_runner_tm_seed = int(self._config_value("scenario_runner_tm_seed"))

        self._sr_scenario = None
        self._sr_tree = None
        self._sr_running = False
        self._sr_ego_vehicles: list = []
        self._sr_last_tick_timestamp = None
        self._traffic_manager = None
        self._traffic_manager_sync_enabled = False
        self._pre_scenario_actor_ids = None
        self._wrapper_loaded_opendrive_digest = None
        self._prepare_reused_server_state()

        return None

    def reset(self, request: ResetRequest) -> ResetResponse:
        if not self._finalized:
            self._finalize()

        self._finalized = False
        success = False

        try:
            self._output_dir = self._output_base / request.output_dir
            self._time_ns = 0
            self._episode_start_carla_time_ns = None
            self._episode_start_carla_frame = None
            self._quit_flag = False
            self._quit_msg = ""
            self._clear_collision_events()
            self._pre_scenario_actor_ids = None
            self._sr_last_tick_timestamp = None
            self._clear_post_reset_initialization_state()
            self._reset_episode_identity()

            scenario_format = self.scenario.format
            is_open_scenario = scenario_format == "open_scenario1"
            use_scenario_runner_world_loading = is_open_scenario and (
                getattr(self, "_open_scenario_map_loader", "scenario_runner") == "scenario_runner"
            )
            self._ensure_world(
                request.scenario_pack,
                generate_opendrive_world=not use_scenario_runner_world_loading,
            )
            self._clear_dynamic_actors()
            if not use_scenario_runner_world_loading:
                self._apply_world_settings()

            logger.info("Starting ScenarioRunner...")
            self._start_scenario_runner(request.scenario_pack, request.params)
            if self._record:
                p = str((self._output_dir / "carla_recording.log").resolve())
                self._client.start_recorder(p)

            if self._ego_vehicle is None:
                raise SimulatorPreconditionFailed(
                    "Ego vehicle not found after starting ScenarioRunner"
                )

            self._reset_episode_clock()
            self._setup_collision_sensor()
            if is_open_scenario:
                speed_overrides = self._open_scenario_initial_speed_overrides()
                self._apply_ego_initial_speed(speed_overrides)
                response = ResetResponse(
                    frame=self._collect_runtime_frame(speed_overrides=speed_overrides)
                )
                self._initial_speed_overrides = speed_overrides
                self._initial_speed_acceleration_pending = bool(speed_overrides)
                ego_id = getattr(self._ego_vehicle, "id", None)
                self._initial_velocity_brake_suppression_pending = ego_id in speed_overrides
            else:
                self._tick_scenario_runner_module()
                if self._sync:
                    self._world.tick()
                response = ResetResponse(frame=self._collect_runtime_frame())
                self._clear_post_reset_initialization_state()
            success = True
            return response
        finally:
            if not success:
                logger.error("Failed to reset CARLA simulation; finalizing partial state")
                self._finalize()

    def step(self, request: StepRequest) -> StepResponse:
        if self._world is None:
            raise SimulatorUnavailable("CARLA world is not available")

        self._tick_scenario_runner_module()
        self._apply_ctrl(request.ctrl_cmd)
        if self._sync:
            self._world.tick()
        else:
            self._world.wait_for_tick()
        self._initial_velocity_brake_suppression_pending = False
        return StepResponse(frame=self._collect_runtime_frame())

    def stop(self) -> None:
        self._finalize()

        self._ego_vehicle = None
        self._world = None
        self._client = None
        self._server_version = None
        logger.info("CARLA simulator stopped.")

    def _finalize(self):
        if self._client is not None and getattr(self, "_record", False):
            try:
                self._client.stop_recorder()
            except Exception:
                logger.exception("Failed to stop CARLA recorder")

        self._destroy_spawned_actors()
        self._stop_scenario_runner_module()
        self._clear_dynamic_actors()

        self._finalized = True
        logger.info("CARLA service finalized.")

    def _connect(self, timeout: float):
        if self._server_version is not None:
            return
        logger.info("Connecting to CARLA...")
        self._client = carla.Client(
            os.environ.get("CARLA_HOST", "localhost"),
            int(os.environ.get("CARLA_PORT", 2000)),
        )
        try:
            self._client.set_timeout(timeout)
            self._server_version = self._client.get_server_version()
        finally:
            self._client.set_timeout(float(os.environ.get("CARLA_TIMEOUT", 30.0)))

        logger.info("Connected to CARLA")

    def _to_carla_yaw(self, yaw_rad: float) -> float:
        return self._yaw_sign * math.degrees(yaw_rad) + self._yaw_offset_deg

    def _config_value(self, key: str):
        return (self.config or {}).get(key, DEFAULT_CONFIG[key])

    def _from_carla_yaw(self, yaw_deg: float) -> float:
        return math.radians((yaw_deg - self._yaw_offset_deg) * self._yaw_sign)

    def _ensure_connected(self) -> bool:
        timeout = self._config_value("carla_connect_timeout_seconds")
        retry_interval = self._config_value("retry_interval_seconds")

        end_time = time.time() + timeout

        while self._server_version is None:
            try:
                self._connect(2)
                return True

            except Exception:
                remaining = end_time - time.time()

                if remaining <= 0:
                    logger.error("Failed to connect to CARLA: connection timeout.")
                    return False

                logger.error(f"Failed to connect to CARLA, retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)

        return True

    def _ensure_world(
        self,
        sps: ScenarioPackData | None,
        generate_opendrive_world: bool = True,
    ) -> None:
        if sps is None:
            raise InvalidSimulatorRequest("ScenarioPack is required to prepare CARLA world")

        if self._server_version is None and not self._ensure_connected():
            raise SimulatorTimeout("Timed out connecting to CARLA before loading world")
        if self._client is None:
            raise SimulatorUnavailable("CARLA client is not available")

        if not generate_opendrive_world:
            world = self._client.get_world()
            if world is None:
                raise SimulatorUnavailable("CARLA world is not available")
            self._world = world
            return

        opendrive_name = sps.map_name
        if not opendrive_name:
            raise InvalidSimulatorRequest(
                "ScenarioPack map_name is required to generate CARLA world"
            )
        opendrive_path = Path(f"/mnt/map/xodr/{opendrive_name}.xodr").resolve()

        world = None
        if hasattr(self._client, "generate_opendrive_world"):
            if not opendrive_path.exists():
                raise InvalidSimulatorRequest("OpenDRIVE path not found for CARLA world generation")

            with open(opendrive_path, encoding="utf-8") as f:
                opendrive_str = f.read()
            opendrive_digest = self._opendrive_digest(opendrive_str)
            world = self._matching_wrapper_generated_world(opendrive_digest)
            if world is not None:
                logger.info(
                    "Reusing wrapper-generated OpenDRIVE world for map '%s'",
                    opendrive_name,
                )
                self._world = world
                return
            # OpenDRIVE world generation can take minutes — bump the
            # client timeout, but guarantee it gets restored even if
            # generation raises (otherwise every subsequent CARLA call
            # on this client inherits the inflated 300s timeout).
            default_timeout = float(os.environ.get("CARLA_TIMEOUT", 30.0))
            self._client.set_timeout(300.0)
            try:
                logger.info("Generating CARLA world from OpenDRIVE: %s", opendrive_path)
                try:
                    world = self._client.generate_opendrive_world(
                        opendrive_str,
                        carla.OpendriveGenerationParameters(
                            vertex_distance=2.0,
                            # max_road_length=500.0,
                            wall_height=0.0,
                            additional_width=5.6,
                            smooth_junctions=True,
                            enable_mesh_visibility=True,
                        ),
                    )
                except Exception as exc:
                    raise InvalidSimulatorRequest(
                        f"Failed to generate CARLA world from OpenDRIVE map: {opendrive_name}"
                    ) from exc
                logger.info("Generated CARLA world from OpenDRIVE: %s", opendrive_path)
                self._wrapper_loaded_opendrive_digest = opendrive_digest
            finally:
                self._client.set_timeout(default_timeout)
        else:
            raise InvalidSimulatorRequest("Cannot determine CARLA world to load")

        if world is None:
            world = self._client.get_world()

        self._world = world

    @staticmethod
    def _opendrive_digest(opendrive: str) -> str:
        start = opendrive.find("<OpenDRIVE")
        if start < 0:
            raise InvalidSimulatorRequest("OpenDRIVE content does not contain an OpenDRIVE root")
        payload = opendrive[start:].encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _matching_wrapper_generated_world(self, expected_digest: str):
        if getattr(self, "_wrapper_loaded_opendrive_digest", None) != expected_digest:
            return None
        if self._client is None:
            return None

        try:
            world = self._client.get_world()
            carla_map = world.get_map() if world is not None else None
            map_name = carla_map.name.split("/")[-1] if carla_map is not None else None
            if map_name != "OpenDriveMap":
                return None
            if self._opendrive_digest(carla_map.to_opendrive()) != expected_digest:
                return None
            return world
        except Exception:
            logger.exception("Failed to verify the current wrapper-generated OpenDRIVE world")
            return None

    def _sync_world_from_scenario_runner(self) -> None:
        if self._client is None:
            raise SimulatorUnavailable("CARLA client is not available")

        world = None
        try:
            world = CarlaDataProvider.get_world()
        except Exception:
            logger.exception("Failed to read CARLA world from CarlaDataProvider")

        if world is None:
            world = self._client.get_world()
        if world is None:
            raise SimulatorUnavailable("CARLA world is not available after ScenarioRunner setup")

        self._world = world
        CarlaDataProvider.set_world(world)

    def _load_and_wait_for_scenario_runner_world(self, config=None) -> None:
        if self._client is None:
            raise SimulatorUnavailable("CARLA client is not available")

        try:
            world = self._client.get_world()
        except Exception:
            logger.exception("Failed to get CARLA world after ScenarioRunner configuration")
            world = None

        if world is None:
            try:
                world = CarlaDataProvider.get_world()
            except Exception:
                logger.exception("Failed to read CARLA world from CarlaDataProvider")

        if world is None:
            raise SimulatorUnavailable("CARLA world is not available after ScenarioRunner setup")

        self._world = world
        self._apply_world_settings()
        CarlaDataProvider.set_client(self._client)
        CarlaDataProvider.set_world(world)

        if self._sync:
            world.tick()
        else:
            world.wait_for_tick()
        self._sr_last_tick_timestamp = None

        town = getattr(config, "town", None)
        get_map = getattr(CarlaDataProvider, "get_map", None)
        if town and get_map is not None:
            map_name = get_map().name.split("/")[-1]
            town_name = Path(str(town)).name
            if map_name not in (str(town), town_name, "OpenDriveMap"):
                raise InvalidSimulatorRequest(
                    f"CARLA server uses map '{map_name}', but scenario requires '{town}'"
                )

    def _apply_world_settings(self) -> None:
        if self._world is None:
            return
        settings = self._world.get_settings()
        settings.synchronous_mode = self._sync
        logger.info("Synchronous mode = %s", settings.synchronous_mode)
        settings.no_rendering_mode = self._no_rendering
        logger.info("No rendering mode = %s", settings.no_rendering_mode)
        if self._fixed_delta_seconds is not None:
            logger.info("Setting fixed_delta_seconds = %s", self._fixed_delta_seconds)
            settings.fixed_delta_seconds = float(self._fixed_delta_seconds)
        self._world.apply_settings(settings)

    def _force_async_world_for_cleanup(self) -> None:
        force_async_world_for_cleanup(
            self._world,
            client=getattr(self, "_client", None),
            traffic_manager_port=getattr(self, "_scenario_runner_tm_port", 8000),
            log=logger,
        )

    def _prepare_reused_server_state(self) -> None:
        if self._client is None:
            return

        try:
            self._world = self._client.get_world()
        except Exception:
            logger.exception("Failed to get CARLA world while preparing reused server")
            return

        self._clear_dynamic_actors()

    def _destroy_spawned_actors(self) -> None:
        self._destroy_collision_sensor()

        spawned_actor_ids = getattr(self, "_spawned_actor_ids", set())
        world = getattr(self, "_world", None)
        if world is not None:
            for actor_id in list(spawned_actor_ids):
                try:
                    actor = world.get_actor(actor_id)
                except Exception:
                    logger.exception("Failed to get spawned actor %s for cleanup", actor_id)
                    continue
                destroy_actor(actor, log=logger, label="spawned actor")

        spawned_actor_ids.clear()
        self._objects_by_id.clear()
        self._prev_yaw_rate.clear()
        getattr(self, "_active_actor_ids", set()).clear()
        getattr(self, "_retired_actor_ids", set()).clear()
        getattr(self, "_actor_metadata_by_id", {}).clear()
        getattr(self, "_xosc_entity_names", set()).clear()
        self._clear_collision_events()

    def _is_dynamic_actor(self, actor) -> bool:
        return is_dynamic_actor(actor)

    def _clear_dynamic_actors(self) -> None:
        logger.info("Clearing dynamic actors from CARLA world")
        clear_dynamic_actors(
            self._world,
            client=getattr(self, "_client", None),
            traffic_manager_port=getattr(self, "_scenario_runner_tm_port", 8000),
            log=logger,
        )

    def _snapshot_existing_actors(self) -> None:
        world = getattr(self, "_world", None)
        if world is None:
            self._pre_scenario_actor_ids = None
            return

        try:
            self._pre_scenario_actor_ids = {actor.id for actor in world.get_actors()}
        except Exception:
            logger.exception("Failed to snapshot CARLA actors before scenario start")
            self._pre_scenario_actor_ids = None

    def _destroy_new_scenario_actors(self) -> None:
        world = getattr(self, "_world", None)
        pre_scenario_actor_ids = getattr(self, "_pre_scenario_actor_ids", None)
        if world is None or pre_scenario_actor_ids is None:
            return

        self._force_async_world_for_cleanup()

        try:
            actors = world.get_actors()
        except Exception:
            logger.exception("Failed to list CARLA actors for partial scenario cleanup")
            return

        for actor in actors:
            actor_id = getattr(actor, "id", None)
            if actor_id in pre_scenario_actor_ids:
                continue
            destroy_actor(actor, log=logger, label="new scenario actor")

        self._pre_scenario_actor_ids = None

    def _start_scenario_runner(self, sps: ScenarioPackData, params: dict | None) -> None:
        self._start_scenario_runner_module(sps, params)

    def _prepare_open_scenario_config(
        self,
        sps: ScenarioPackData,
        params: dict | None,
    ):
        openscenario_params: dict[str, str] = {}
        if params:
            openscenario_params = {str(k): str(v) for k, v in params.items()}
        logger.info(f"parameters: {openscenario_params}")
        xosc_name = sps.name
        if not xosc_name:
            raise InvalidSimulatorRequest("ScenarioPack name is required for open_scenario1")
        xosc_path = Path(f"/mnt/scenario/{xosc_name}.xosc").resolve()
        if not xosc_path.exists():
            raise InvalidSimulatorRequest(f"OpenSCENARIO file not found: {xosc_path}")

        config_client = self._client
        if getattr(self, "_open_scenario_map_loader", "scenario_runner") == "wrapper":
            config_client = _WorldLoadingDisabledClient(self._client)
        config = OpenScenarioConfiguration(str(xosc_path), config_client, openscenario_params)
        configured_actors = list(getattr(config, "ego_vehicles", [])) + list(
            getattr(config, "other_actors", [])
        )
        entity_names = [
            str(actor.rolename)
            for actor in configured_actors
            if getattr(actor, "rolename", None) is not None
        ]
        if len(entity_names) != len(set(entity_names)):
            raise InvalidSimulatorRequest("OpenSCENARIO ScenarioObject names must be unique")
        self._xosc_entity_names = set(entity_names)
        self._load_and_wait_for_scenario_runner_world(config)
        CarlaDataProvider.set_world(self._world)
        return config, xosc_path

    def _build_open_scenario(self, config, xosc_path: Path, sps: ScenarioPackData):
        ego_vehicles = []
        for ego_config in config.ego_vehicles:
            actor = CarlaDataProvider.request_new_actor(
                ego_config.model,
                ego_config.transform,
                ego_config.rolename,
                random_location=ego_config.random_location,
                color=ego_config.color,
                actor_category=ego_config.category,
            )
            if actor is None:
                raise SimulatorPreconditionFailed(
                    f"Failed to spawn ego vehicle '{ego_config.rolename}'"
                )
            ego_vehicles.append(actor)
            self._sr_ego_vehicles = ego_vehicles
        logger.debug("Spawned %s ego vehicles for scenario", len(ego_vehicles))
        if not ego_vehicles:
            raise InvalidSimulatorRequest("OpenSCENARIO did not define any ego vehicles")

        scenario = OpenScenario(
            world=self._world,
            ego_vehicles=ego_vehicles,
            config=config,
            config_file=str(xosc_path),
            timeout=sps.timeout_ns / 1e9 if sps.timeout_ns > 0 else 10000.0,
        )

        self._sr_ego_vehicles = ego_vehicles
        self._ego_vehicle = ego_vehicles[0]
        return scenario

    def _start_scenario_runner_module(
        self,
        sps: ScenarioPackData,
        params: dict | None,
    ) -> None:
        if self._client is None or self._world is None:
            raise SimulatorUnavailable("CARLA client/world not available")

        self._sr_last_tick_timestamp = None

        CarlaDataProvider.set_client(self._client)

        match self.scenario.format:
            case "open_scenario1":
                config, xosc_path = self._prepare_open_scenario_config(sps, params)

            case "carla_lb_route":
                route_name = self.scenario.name
                if self.scenario.path is None:
                    raise InvalidSimulatorRequest("Scenario path is required for carla_lb_route")
                xml_path = str(self.scenario.path / f"{route_name}.xml")
                config = RouteParser.parse_routes_file(xml_path, 0)
                logger.info("Parsed route scenario config: %s", config)
                config = config[0]

            case _:
                raise InvalidSimulatorRequest(
                    f"Unsupported scenario format: {self.scenario.format}"
                )

        CarlaDataProvider.set_traffic_manager_port(self._scenario_runner_tm_port)
        tm = self._client.get_trafficmanager(self._scenario_runner_tm_port)
        self._traffic_manager = tm
        self._traffic_manager_sync_enabled = False
        tm.set_random_device_seed(self._scenario_runner_tm_seed)
        if self._sync:
            tm.set_synchronous_mode(True)
            self._traffic_manager_sync_enabled = True

        self._snapshot_existing_actors()

        match self.scenario.format:
            case "open_scenario1":
                scenario = self._build_open_scenario(config, xosc_path, sps)

            case "carla_lb_route":
                scenario = RouteScenario(world=self._world, config=config)
                self._ego_vehicle = scenario.ego_vehicles[0]
                if self._ego_vehicle is None:
                    raise SimulatorPreconditionFailed("Failed to spawn route ego vehicle")

            case _:
                raise InvalidSimulatorRequest(
                    f"Unsupported scenario format: {self.scenario.format}"
                )

        self._sr_scenario = scenario
        self._sr_tree = scenario.scenario_tree

        GameTime.restart()
        self._sr_running = True

    def _stop_scenario_runner_module(self) -> None:
        self._restore_traffic_manager_settings()

        if self._sr_scenario is None and self._sr_tree is None and not self._sr_ego_vehicles:
            logger.info("ScenarioRunner not running, no need to stop")
        else:
            try:
                if self._sr_tree is not None:
                    self._sr_tree.stop(py_trees.common.Status.INVALID)
                    logger.info("Scenario tree stopped successfully")
            except Exception:
                logger.exception("Failed to stop scenario tree")

            try:
                if self._sr_scenario is not None:
                    self._sr_scenario.terminate()
                    logger.info("Scenario terminated successfully")
            except Exception:
                logger.exception("Failed to terminate scenario")

            try:
                if self._sr_scenario is not None:
                    self._sr_scenario.remove_all_actors()
                    logger.info("Scenario actors removed successfully")
            except Exception:
                logger.exception("Failed to remove ScenarioRunner actors")

        if self._sr_scenario is None:
            for actor in self._sr_ego_vehicles:
                destroy_actor(actor, log=logger, label="partially spawned ego actor")
            self._destroy_new_scenario_actors()

        # Clean up data provider
        try:
            CarlaDataProvider.cleanup()
            logger.info("CarlaDataProvider cleaned up successfully")
        except Exception:
            logger.exception("Failed to cleanup CarlaDataProvider")

        try:
            py_trees.blackboard.Blackboard._Blackboard__shared_state.clear()
            logger.info("Py_trees blackboard cleared")
        except Exception:
            logger.exception("Failed to clear blackboard")
        self._sr_scenario = None
        self._sr_tree = None
        self._sr_running = False
        self._sr_ego_vehicles = []
        self._sr_last_tick_timestamp = None
        logger.info("ScenarioRunner cleanup complete")

    def _restore_traffic_manager_settings(self) -> None:
        if self._traffic_manager is None:
            return

        if self._traffic_manager_sync_enabled:
            try:
                self._traffic_manager.set_synchronous_mode(False)
                logger.info("Restored TrafficManager synchronous mode")
            except Exception:
                logger.exception("Failed to restore TrafficManager synchronous mode")

        self._traffic_manager = None
        self._traffic_manager_sync_enabled = False

    def _tick_scenario_runner_module(self) -> None:
        if self._world is None:
            return
        if self._sr_scenario is None:
            return

        snapshot = self._world.get_snapshot()
        timestamp = snapshot.timestamp

        if self._sr_tree is None or not self._sr_running:
            return

        timestamp_key = (
            getattr(timestamp, "frame", None),
            getattr(timestamp, "elapsed_seconds", None),
        )
        if timestamp_key != (None, None):
            if timestamp_key == getattr(self, "_sr_last_tick_timestamp", None):
                return
            self._sr_last_tick_timestamp = timestamp_key

        GameTime.on_carla_tick(timestamp)
        CarlaDataProvider.on_carla_tick()
        self._sr_tree.tick_once()
        if (
            self._sr_tree.status == py_trees.common.Status.FAILURE
            or self._sr_tree.status == py_trees.common.Status.INVALID
        ):
            self._sr_running = False
            self._quit_flag = True
            logger.error("ScenarioRunner tree failed with status: %s", self._sr_tree.status)
            raise SimulatorPreconditionFailed(
                f"ScenarioRunner tree failed with status: {self._sr_tree.status}"
            )
        if self._sr_tree.status != py_trees.common.Status.RUNNING:
            self._sr_running = False
            self._quit_flag = True
            self._quit_msg = f"ScenarioRunner finished with status {self._sr_tree.status}"

    def _actor_type(self, actor) -> RoadObjectType:
        type_id = getattr(actor, "type_id", "").lower()
        if type_id.startswith("walker.pedestrian"):
            return RoadObjectType.PEDESTRIAN
        if type_id.startswith("vehicle."):
            known_type = _VEHICLE_TYPE_BY_BLUEPRINT_ID.get(type_id)
            if known_type is not None:
                return known_type

            # Fallback for custom vehicle blueprints outside CARLA's catalogue.
            if "trailer" in type_id:
                return RoadObjectType.TRAILER
            if "bus" in type_id:
                return RoadObjectType.BUS
            if "truck" in type_id or "hgv" in type_id:
                return RoadObjectType.TRUCK
            if "ambulance" in type_id or "sprinter" in type_id or "van" in type_id:
                return RoadObjectType.VAN
            if (
                "motorcycle" in type_id
                or "motorbike" in type_id
                or "low_rider" in type_id
                or "ninja" in type_id
                or "vespa" in type_id
                or "yzf" in type_id
            ):
                return RoadObjectType.MOTORCYCLE
            if (
                "bicycle" in type_id
                or "bike" in type_id
                or "crossbike" in type_id
                or "diamondback" in type_id
                or "gazelle" in type_id
            ):
                return RoadObjectType.BICYCLE
            return RoadObjectType.CAR
        return RoadObjectType.UNKNOWN

    def _shape_from_actor(self, actor) -> ShapeData:
        try:
            bb = actor.bounding_box
            dims = ShapeDimensionData(
                x=float(bb.extent.x * 2.0),
                y=float(bb.extent.y * 2.0),
                z=float(bb.extent.z * 2.0),
            )
            location = getattr(bb, "location", None)
            rotation = getattr(bb, "rotation", None)
            center = ShapeCenterPoseData(
                x=float(getattr(location, "x", 0.0)),
                y=float(getattr(location, "y", 0.0)) * self._yaw_sign,
                z=float(getattr(location, "z", 0.0)),
                roll=math.radians(float(getattr(rotation, "roll", 0.0))),
                pitch=math.radians(float(getattr(rotation, "pitch", 0.0))),
                yaw=math.radians(float(getattr(rotation, "yaw", 0.0))) * self._yaw_sign,
            )
            return ShapeData(
                type=ShapeType.BOUNDING_BOX,
                dimensions=dims,
                center=center,
                reference_point="carla_actor_origin",
            )
        except Exception as exc:
            raise SimulatorPreconditionFailed(
                f"Failed to read bounding box for CARLA actor {getattr(actor, 'id', 'unknown')}"
            ) from exc

    def _reset_episode_identity(self) -> None:
        self._objects_by_id = {}
        self._prev_yaw_rate = {}
        self._active_actor_ids = set()
        self._retired_actor_ids = set()
        self._actor_metadata_by_id = {}
        self._xosc_entity_names = set()

    def _entity_name_from_actor(self, actor) -> str | None:
        role_name = getattr(actor, "attributes", {}).get("role_name")
        if role_name is None:
            return None
        role_name = str(role_name)
        if role_name not in getattr(self, "_xosc_entity_names", set()):
            return None
        return role_name

    def _actor_role(self, actor_id: int) -> ActorRole:
        ego_id = getattr(getattr(self, "_ego_vehicle", None), "id", None)
        return ActorRole.EGO if actor_id == ego_id else ActorRole.AGENT

    def _register_current_actor_ids(self, actors: list) -> None:
        current_ids = {int(actor.id) for actor in actors}
        active_ids = getattr(self, "_active_actor_ids", set())
        retired_ids = getattr(self, "_retired_actor_ids", set())

        reused_ids = (current_ids - active_ids) & retired_ids
        if reused_ids:
            reused = ", ".join(str(actor_id) for actor_id in sorted(reused_ids))
            raise SimulatorPreconditionFailed(
                f"CARLA reused retired actor ID(s) within one reset episode: {reused}"
            )

        retired_ids.update(active_ids - current_ids)
        self._active_actor_ids = current_ids
        self._retired_actor_ids = retired_ids

    def _actor_ref(
        self, actor_id: int | None, entity_name: str | None = None
    ) -> ActorRefData | None:
        if actor_id is None:
            return None
        actor_id = int(actor_id)
        stored_name, stored_role = getattr(self, "_actor_metadata_by_id", {}).get(
            actor_id,
            (None, self._actor_role(actor_id)),
        )
        return ActorRefData(
            tracking_id=actor_id,
            entity_name=entity_name if entity_name is not None else stored_name,
            role=stored_role,
        )

    def _get_forward_speed(self, actor) -> float:
        vel = actor.get_velocity()
        fwd = actor.get_transform().get_forward_vector()
        return float(vel.x * fwd.x + vel.y * fwd.y + vel.z * fwd.z)

    def _get_forward_accel(self, actor) -> float:
        acc = actor.get_acceleration()
        fwd = actor.get_transform().get_forward_vector()
        return float(acc.x * fwd.x + acc.y * fwd.y + acc.z * fwd.z)

    def _deadband_value(self, value: float, config_key: str) -> float:
        threshold = abs(float(self._config_value(config_key)))
        if threshold > 0.0 and abs(value) <= threshold:
            return 0.0
        return value

    def _apply_kinematic_deadbands(
        self,
        *,
        speed: float,
        acceleration: float,
        yaw_rate: float,
        yaw_acceleration: float,
    ) -> tuple[float, float, float, float]:
        return (
            self._deadband_value(speed, "kinematic_speed_deadband_mps"),
            self._deadband_value(acceleration, "kinematic_acceleration_deadband_mps2"),
            self._deadband_value(yaw_rate, "kinematic_yaw_rate_deadband_radps"),
            self._deadband_value(
                yaw_acceleration,
                "kinematic_yaw_acceleration_deadband_radps2",
            ),
        )

    def _ackermann_steer_to_vehicle_control(self, steer_rad: float) -> float:
        if self._max_steer_rad:
            return _clamp(steer_rad / self._max_steer_rad, -1.0, 1.0)
        return _clamp(steer_rad, -1.0, 1.0)

    def _ackermann_current_speed(self) -> float:
        current_speed = self._get_forward_speed(self._ego_vehicle)
        stop_threshold = float(self._config_value("ackermann_stop_speed_threshold"))
        if abs(current_speed) < stop_threshold:
            return 0.0
        return current_speed

    def _ackermann_speed_to_vehicle_control(
        self, target_speed: float
    ) -> tuple[float, float, float]:
        current_speed = self._ackermann_current_speed()
        stop_threshold = float(self._config_value("ackermann_stop_speed_threshold"))
        speed_error = target_speed - current_speed
        if target_speed <= stop_threshold and current_speed > stop_threshold:
            return 0.0, 1.0, current_speed

        if speed_error > 0.0:
            kp = float(self._config_value("ackermann_speed_kp"))
            min_throttle = float(self._config_value("ackermann_min_throttle"))
            max_throttle = float(self._config_value("ackermann_max_throttle"))
            launch_speed_threshold = float(self._config_value("ackermann_launch_speed_threshold"))
            launch_target_threshold = float(self._config_value("ackermann_launch_target_threshold"))
            launch_throttle = float(self._config_value("ackermann_launch_throttle"))

            throttle = speed_error * kp
            if (
                target_speed >= launch_target_threshold
                and abs(current_speed) <= launch_speed_threshold
            ):
                throttle = max(throttle, launch_throttle)
            elif target_speed > stop_threshold:
                throttle = max(throttle, min_throttle)
            return _clamp(throttle, 0.0, max_throttle), 0.0, current_speed

        if speed_error < 0.0:
            brake_kp = float(self._config_value("ackermann_brake_kp"))
            min_brake = float(self._config_value("ackermann_min_brake"))
            max_brake = float(self._config_value("ackermann_max_brake"))

            brake = max(-speed_error * brake_kp, min_brake)
            return 0.0, _clamp(brake, 0.0, max_brake), current_speed

        return 0.0, 0.0, current_speed

    def _ackermann_controller_settings_payload(self) -> dict[str, float]:
        return {
            "speed_kp": float(self._config_value("ackermann_native_speed_kp")),
            "speed_ki": float(self._config_value("ackermann_native_speed_ki")),
            "speed_kd": float(self._config_value("ackermann_native_speed_kd")),
            "accel_kp": float(self._config_value("ackermann_native_accel_kp")),
            "accel_ki": float(self._config_value("ackermann_native_accel_ki")),
            "accel_kd": float(self._config_value("ackermann_native_accel_kd")),
        }

    def _apply_native_ackermann_controller_settings(self) -> dict[str, float]:
        settings_payload = self._ackermann_controller_settings_payload()
        actor_id = getattr(self._ego_vehicle, "id", None)
        settings_key = tuple(settings_payload.items())
        if (
            actor_id == self._native_ackermann_settings_actor_id
            and settings_key == self._native_ackermann_settings_payload
        ):
            return settings_payload

        if not hasattr(self._ego_vehicle, "apply_ackermann_controller_settings"):
            logger.warning("Ego vehicle does not support Ackermann controller settings")
            return settings_payload
        if not hasattr(carla, "AckermannControllerSettings"):
            logger.warning("CARLA API does not provide AckermannControllerSettings")
            return settings_payload

        settings = carla.AckermannControllerSettings(**settings_payload)
        self._ego_vehicle.apply_ackermann_controller_settings(settings)
        self._native_ackermann_settings_actor_id = actor_id
        self._native_ackermann_settings_payload = settings_key
        return settings_payload

    def _collect_runtime_frame(
        self,
        speed_overrides: dict[int, float] | None = None,
    ) -> RuntimeFrameData:
        clear_initial_acceleration_state = speed_overrides is None and getattr(
            self, "_initial_speed_acceleration_pending", False
        )
        ego, agents, sim_time_ns, carla_frame, carla_time_ns = self._collect_objects(
            speed_overrides=speed_overrides
        )
        if clear_initial_acceleration_state:
            self._initial_speed_acceleration_pending = False
            self._initial_speed_overrides = {}
        collisions = self._collect_collision_infos()

        extras_payload = {
            "carla_frame": carla_frame,
            "carla_time_ns": carla_time_ns,
            "episode_frame": self._episode_frame_from_carla_frame(carla_frame),
            "collision_count": len(collisions),
            "untracked_collision_count": sum(
                1 for collision in collisions if collision.actor_b is None
            ),
        }
        if self._ego_vehicle is not None:
            extras_payload["ego_actor_id"] = int(self._ego_vehicle.id)
        if self._last_applied_control is not None:
            extras_payload["last_applied_control"] = dict(self._last_applied_control)

        return RuntimeFrameData(
            sim_time_ns=sim_time_ns,
            ego=ego,
            agents=agents,
            collision=collisions,
            extras=extras_payload,
        )

    def _reset_episode_clock(self) -> None:
        if getattr(self, "_world", None) is None:
            self._episode_start_carla_time_ns = 0
            self._episode_start_carla_frame = 0
            return

        snapshot = self._world.get_snapshot()
        self._episode_start_carla_time_ns = int(
            getattr(snapshot.timestamp, "elapsed_seconds", 0.0) * 1e9
        )
        self._episode_start_carla_frame = int(getattr(snapshot, "frame", 0))
        self._time_ns = 0

    def _episode_time_from_carla_time_ns(self, carla_time_ns: int) -> int:
        if getattr(self, "_episode_start_carla_time_ns", None) is None:
            self._episode_start_carla_time_ns = carla_time_ns
        return max(0, carla_time_ns - self._episode_start_carla_time_ns)

    def _episode_frame_from_carla_frame(self, carla_frame: int) -> int:
        if getattr(self, "_episode_start_carla_frame", None) is None:
            self._episode_start_carla_frame = carla_frame
        return max(0, carla_frame - self._episode_start_carla_frame)

    def _collect_objects(
        self,
        speed_overrides: dict[int, float] | None = None,
    ) -> tuple[SimulatorEgoData, dict[int, SimulatorObjectData], int, int, int]:
        if self._world is None:
            raise SimulatorUnavailable("CARLA world is not available")
        if self._ego_vehicle is None:
            raise SimulatorPreconditionFailed("CARLA ego vehicle is not available")

        snapshot = self._world.get_snapshot()
        carla_time_ns = int(snapshot.timestamp.elapsed_seconds * 1e9)
        sim_time_ns = self._episode_time_from_carla_time_ns(carla_time_ns)
        carla_frame = int(snapshot.frame)

        world_actors = self._world.get_actors()
        actors_by_id = {
            int(actor.id): actor
            for actor in world_actors
            if getattr(actor, "type_id", "").startswith(("vehicle.", "walker.pedestrian."))
            or self._entity_name_from_actor(actor) is not None
        }
        actors_by_id[int(self._ego_vehicle.id)] = self._ego_vehicle
        actors = list(actors_by_id.values())
        self._register_current_actor_ids(actors)

        actor_ids = {int(actor.id) for actor in actors}
        for stale_id in list(self._objects_by_id.keys()):
            if stale_id not in actor_ids:
                self._objects_by_id.pop(stale_id, None)
                self._prev_yaw_rate.pop(stale_id, None)

        def upsert(actor):
            transform = actor.get_transform()
            ang = actor.get_angular_velocity()

            yaw = self._from_carla_yaw(float(transform.rotation.yaw))
            collected_speed = self._get_forward_speed(actor)
            speed = collected_speed
            if speed_overrides and actor.id in speed_overrides:
                speed = speed_overrides[actor.id]
            accel = self._get_forward_accel(actor)
            yaw_rate = math.radians(float(ang.z)) * self._yaw_sign

            prev_rate = self._prev_yaw_rate.get(actor.id, yaw_rate)
            dt_s = max(0.0, (sim_time_ns - self._time_ns) / 1e9)
            if (
                getattr(self, "_initial_speed_acceleration_pending", False)
                and actor.id in getattr(self, "_initial_speed_overrides", {})
                and dt_s > 0.0
            ):
                init_speed = self._initial_speed_overrides[actor.id]
                accel = (collected_speed - init_speed) / dt_s
            yaw_acc = (yaw_rate - prev_rate) / dt_s if dt_s > 0 else 0.0
            self._prev_yaw_rate[actor.id] = yaw_rate
            speed, accel, yaw_rate, yaw_acc = self._apply_kinematic_deadbands(
                speed=speed,
                acceleration=accel,
                yaw_rate=yaw_rate,
                yaw_acceleration=yaw_acc,
            )

            kin = ObjectKinematicData(
                time_ns=sim_time_ns,
                x=float(transform.location.x),
                y=float(transform.location.y) * self._yaw_sign,
                z=float(transform.location.z),
                yaw=float(yaw),
                speed=float(speed),
                acceleration=float(accel),
                yaw_rate=float(yaw_rate),
                yaw_acceleration=float(yaw_acc),
            )
            previous = self._objects_by_id.get(actor.id)
            state = ObjectStateData(
                type=previous.state.type if previous is not None else self._actor_type(actor),
                kinematic=kin,
                shape=previous.state.shape
                if previous is not None
                else self._shape_from_actor(actor),
            )
            entity_name = self._entity_name_from_actor(actor)
            obj = SimulatorObjectData(state=state, entity_name=entity_name)
            self._objects_by_id[actor.id] = obj
            metadata = getattr(self, "_actor_metadata_by_id", {})
            metadata[int(actor.id)] = (
                entity_name,
                self._actor_role(int(actor.id)),
            )
            self._actor_metadata_by_id = metadata

            return obj

        ego_id = int(self._ego_vehicle.id)
        ego = SimulatorEgoData(tracking_id=ego_id, object=upsert(self._ego_vehicle))
        agents = {int(actor.id): upsert(actor) for actor in actors if int(actor.id) != ego_id}
        self._time_ns = sim_time_ns

        logger.debug("Collected ego and %s agents at time %s ns", len(agents), sim_time_ns)

        return ego, agents, sim_time_ns, carla_frame, carla_time_ns

    def _open_scenario_initial_speed_overrides(self) -> dict[int, float]:
        scenario = getattr(self, "_sr_scenario", None)
        if scenario is None:
            return {}

        config = getattr(scenario, "config", None)
        if config is None:
            return {}

        speed_by_role: dict[str, float] = {}
        for actor_config in getattr(config, "ego_vehicles", []) + getattr(
            config, "other_actors", []
        ):
            role_name = getattr(actor_config, "rolename", None)
            if role_name is None:
                continue
            speed_by_role[str(role_name)] = float(getattr(actor_config, "speed", 0) or 0)

        actor_ids_by_role: dict[str, int] = {}
        for actor in getattr(scenario, "ego_vehicles", []) + getattr(scenario, "other_actors", []):
            role_name = getattr(actor, "attributes", {}).get("role_name")
            if role_name is not None:
                actor_ids_by_role[str(role_name)] = int(actor.id)

        return {
            actor_ids_by_role[role_name]: speed
            for role_name, speed in speed_by_role.items()
            if role_name in actor_ids_by_role
        }

    def _apply_ego_initial_speed(self, speed_overrides: dict[int, float]) -> None:
        if self._ego_vehicle is None or carla is None:
            return

        speed = speed_overrides.get(int(self._ego_vehicle.id))
        if speed is None:
            return

        try:
            forward = self._ego_vehicle.get_transform().get_forward_vector()
            velocity = carla.Vector3D(
                x=float(forward.x) * float(speed),
                y=float(forward.y) * float(speed),
                z=float(forward.z) * float(speed),
            )
            if hasattr(self._ego_vehicle, "set_velocity"):
                self._ego_vehicle.set_velocity(velocity)
            elif hasattr(self._ego_vehicle, "set_target_velocity"):
                self._ego_vehicle.set_target_velocity(velocity)
        except Exception:
            logger.exception("Failed to seed initial speed for ego actor %s", self._ego_vehicle.id)

    def _clear_post_reset_initialization_state(self) -> None:
        self._initial_speed_acceleration_pending = False
        self._initial_velocity_brake_suppression_pending = False
        self._initial_speed_overrides = {}

    def _setup_collision_sensor(self) -> None:
        self._destroy_collision_sensor()
        self._clear_collision_events()

        if self._world is None or self._ego_vehicle is None:
            return

        try:
            bp = self._world.get_blueprint_library().find("sensor.other.collision")
            sensor = self._world.spawn_actor(bp, carla.Transform(), attach_to=self._ego_vehicle)
            self._collision_sensor = sensor
            self._spawned_actor_ids.add(sensor.id)

            weak_self = weakref.ref(self)
            sensor.listen(lambda event: CarlaAdapter._on_collision_event(weak_self, event))
        except Exception:
            logger.exception("Failed to attach collision sensor to ego vehicle")
            self._collision_sensor = None

    def _destroy_collision_sensor(self) -> None:
        sensor = self._collision_sensor
        self._collision_sensor = None
        if sensor is None:
            return

        try:
            sensor.stop()
        except Exception:
            logger.exception("Failed to stop collision sensor")

        try:
            self._spawned_actor_ids.discard(sensor.id)
        except Exception:
            logger.exception("Failed to untrack collision sensor")
        destroy_actor(sensor, log=logger, label="collision sensor")

    def _clear_collision_events(self) -> None:
        with self._collision_lock:
            self._collision_events.clear()

    @staticmethod
    def _on_collision_event(weak_self, event) -> None:
        self = weak_self()
        if self is None:
            return

        impulse = getattr(event, "normal_impulse", None)
        other_actor = getattr(event, "other_actor", None)
        actor = getattr(event, "actor", None)

        collision_event = {
            "frame": int(getattr(event, "frame", 0)),
            "timestamp": float(getattr(event, "timestamp", 0.0)),
            "actor_id": int(actor.id) if actor is not None else None,
            "other_actor_id": int(other_actor.id) if other_actor is not None else None,
            "actor_entity_name": self._entity_name_from_actor(actor) if actor is not None else None,
            "other_actor_entity_name": (
                self._entity_name_from_actor(other_actor) if other_actor is not None else None
            ),
            "other_actor_type_id": getattr(other_actor, "type_id", ""),
            "other_actor_semantic_tags": list(getattr(other_actor, "semantic_tags", [])),
            "normal_impulse": {
                "x": float(getattr(impulse, "x", 0.0)),
                "y": float(getattr(impulse, "y", 0.0)),
                "z": float(getattr(impulse, "z", 0.0)),
            },
        }
        with self._collision_lock:
            self._collision_events.append(collision_event)

    def _collect_collision_infos(self) -> list[CollisionInfoData]:
        with self._collision_lock:
            events = list(self._collision_events)
            self._collision_events.clear()

        collisions: list[CollisionInfoData] = []
        for event in events:
            carla_timestamp_seconds = event["timestamp"]
            episode_time_ns = self._episode_time_from_carla_time_ns(
                int(carla_timestamp_seconds * 1e9)
            )
            episode_timestamp_seconds = episode_time_ns / 1e9

            impulse = event["normal_impulse"]
            details_payload = {
                "carla_frame": event["frame"],
                "carla_timestamp_seconds": carla_timestamp_seconds,
                "episode_frame": self._episode_frame_from_carla_frame(event["frame"]),
                "timestamp_seconds": episode_timestamp_seconds,
                "other_actor_type_id": event["other_actor_type_id"],
                "other_actor_semantic_tags": event["other_actor_semantic_tags"],
                "normal_impulse": {
                    "x": impulse["x"],
                    "y": impulse["y"],
                    "z": impulse["z"],
                    "magnitude": math.sqrt(
                        impulse["x"] ** 2 + impulse["y"] ** 2 + impulse["z"] ** 2
                    ),
                },
            }
            if event["actor_id"] is not None:
                details_payload["actor_a_carla_id"] = event["actor_id"]
            if event["other_actor_id"] is not None:
                details_payload["actor_b_carla_id"] = event["other_actor_id"]

            collisions.append(
                CollisionInfoData(
                    occurred=True,
                    actor_a=self._actor_ref(event["actor_id"], event.get("actor_entity_name")),
                    actor_b=self._actor_ref(
                        event["other_actor_id"], event.get("other_actor_entity_name")
                    ),
                    details=details_payload,
                )
            )

        return collisions

    def _apply_ctrl(self, ctrl: ControlCommand | None) -> None:
        if self._ego_vehicle is None:
            return
        if ctrl is None or ctrl.mode == ControlMode.NONE:
            return

        payload = ctrl.payload

        if ctrl.mode == ControlMode.THROTTLE_STEER_BREAK:
            throttle = _clamp(float(payload.get("throttle", 0.0)), 0.0, 1.0)
            brake = _clamp(float(payload.get("brake", 0.0)), 0.0, 1.0)
            if getattr(self, "_initial_velocity_brake_suppression_pending", False):
                brake = 0.0
            steer = _clamp(float(payload.get("steer", 0.0)), -1.0, 1.0)
            if brake > 0.0:
                throttle = 0.0

            control = carla.VehicleControl(
                throttle=throttle, steer=steer * self._yaw_sign, brake=brake
            )
            self._ego_vehicle.apply_control(control)
            self._last_applied_control = {
                "mode": ctrl.mode.name,
                "throttle": throttle,
                "brake": brake,
                "steer": steer * self._yaw_sign,
            }
            return

        elif ctrl.mode == ControlMode.THROTTLE_STEER:
            if "throttle" in payload or "brake" in payload:
                throttle = float(payload.get("throttle", 0.0))
                brake = float(payload.get("brake", 0.0))
                steer = float(payload.get("steer", payload.get("wheel", 0.0)))
                throttle = _clamp(throttle, 0.0, 1.0)
                brake = _clamp(brake, 0.0, 1.0)
                steer = _clamp(steer, -1.0, 1.0)
            else:
                pedal = float(payload.get("pedal", 0.0))
                wheel = float(payload.get("wheel", 0.0))
                if pedal >= 0:
                    throttle = _clamp(abs(pedal), 0.0, 1.0)
                    brake = 0.0
                else:
                    throttle = 0.0
                    brake = _clamp(abs(pedal), 0.0, 1.0)
                steer = _clamp(wheel, -1.0, 1.0)

            if getattr(self, "_initial_velocity_brake_suppression_pending", False):
                brake = 0.0
            if brake > 0.0:
                throttle = 0.0
            control = carla.VehicleControl(
                throttle=throttle, steer=steer * self._yaw_sign, brake=brake
            )
            self._ego_vehicle.apply_control(control)
            self._last_applied_control = {
                "mode": ctrl.mode.name,
                "throttle": throttle,
                "brake": brake,
                "steer": steer * self._yaw_sign,
            }
            return

        elif ctrl.mode == ControlMode.ACKERMANN:
            steer_speed = float(payload.get("steer_speed", 0.0))

            steer = float(payload.get("steer", 0.0)) * self._yaw_sign
            speed = (
                float(payload["speed"])
                if "speed" in payload
                else self._get_forward_speed(self._ego_vehicle)
            )
            target_speed = max(speed, 0.0)
            current_forward_speed = self._ackermann_current_speed()
            stop_threshold = float(self._config_value("ackermann_stop_speed_threshold"))
            decelerating = target_speed < current_forward_speed - stop_threshold

            acceleration = payload.get("acceleration", None)
            if acceleration is None:
                if decelerating:
                    acceleration = -float(self._config_value("ackermann_decel_default"))
                else:
                    acceleration = float(self._config_value("ackermann_accel_default"))
            else:
                acceleration = float(acceleration)
            jerk = payload.get("jerk", None)
            if jerk is None:
                if decelerating:
                    jerk = float(self._config_value("ackermann_brake_jerk_default"))
                else:
                    jerk = float(self._config_value("ackermann_jerk_default"))
            else:
                jerk = float(jerk)

            if self._max_steer_rad:
                steer = _clamp(steer, -self._max_steer_rad, self._max_steer_rad)
            if bool(self._config_value("ackermann_use_native_control")):
                controller_settings = self._apply_native_ackermann_controller_settings()
                control = carla.VehicleAckermannControl(
                    steer=steer,
                    steer_speed=steer_speed,
                    speed=target_speed,
                    acceleration=acceleration,
                    jerk=jerk,
                )

                self._ego_vehicle.apply_ackermann_control(control)
                applied_payload = {
                    "mode": ctrl.mode.name,
                    "backend": "native_ackermann",
                    "steer": steer,
                    "steer_speed": steer_speed,
                    "target_speed": target_speed,
                    "current_forward_speed": current_forward_speed,
                    "acceleration": acceleration,
                    "jerk": jerk,
                    "decelerating": decelerating,
                    "controller_settings": controller_settings,
                }
            else:
                throttle, brake, current_forward_speed = self._ackermann_speed_to_vehicle_control(
                    target_speed
                )
                vehicle_steer = self._ackermann_steer_to_vehicle_control(steer)
                control = carla.VehicleControl(
                    throttle=throttle,
                    steer=vehicle_steer,
                    brake=brake,
                )
                self._ego_vehicle.apply_control(control)
                applied_payload = {
                    "mode": ctrl.mode.name,
                    "backend": "vehicle_control",
                    "throttle": throttle,
                    "brake": brake,
                    "steer": vehicle_steer,
                    "target_speed": target_speed,
                    "current_forward_speed": current_forward_speed,
                    "acceleration": acceleration,
                    "jerk": jerk,
                    "decelerating": decelerating,
                }

            self._last_applied_control = {
                **applied_payload,
                "ackermann_steer": steer,
                "ackermann_steer_speed": steer_speed,
            }
            return

        elif ctrl.mode == ControlMode.POSITION:
            transform = self._ego_vehicle.get_transform()
            x = float(payload.get("x", transform.location.x))
            y = float(payload["y"]) * self._yaw_sign if "y" in payload else transform.location.y
            z = float(payload.get("z", transform.location.z))
            h = float(payload.get("h", self._from_carla_yaw(transform.rotation.yaw)))
            yaw_deg = self._to_carla_yaw(h)

            loc = carla.Location(x=x, y=y, z=z)
            rot = carla.Rotation(
                pitch=transform.rotation.pitch,
                yaw=yaw_deg,
                roll=transform.rotation.roll,
            )
            self._ego_vehicle.set_transform(carla.Transform(loc, rot))
            return

        logger.warning("Unsupported control mode: %s", ctrl.mode)


CarlaSimulation = CarlaAdapter
