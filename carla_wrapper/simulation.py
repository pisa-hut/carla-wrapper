import logging
import math
import os
import subprocess
import time
import weakref
from collections import deque
from pathlib import Path
from threading import Lock

import carla
import py_trees
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
from pisa_api.collision_pb2 import CollisionInfo
from pisa_api.control_pb2 import CtrlCmd, CtrlMode
from pisa_api.object_pb2 import (
    ObjectKinematic,
    ObjectState,
    RoadObjectType,
    Shape,
    ShapeType,
)
from pisa_api.runtime_frame_pb2 import RuntimeFrame
from pisa_api.scenario_pb2 import ScenarioPack
from srunner.scenarioconfigs.openscenario_configuration import (
    OpenScenarioConfiguration,
)
from srunner.scenariomanager.carla_data_provider import CarlaDataProvider
from srunner.scenariomanager.timer import GameTime
from srunner.scenarios.open_scenario import OpenScenario
from srunner.scenarios.route_scenario import RouteScenario
from srunner.tools.route_parser import RouteParser

logger = logging.getLogger(__name__)


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


class CarlaSimulation:
    def __init__(self):
        self._client = None
        self._server_version = None
        self._world = None
        self.config = None
        self._original_settings = None
        self._output_base = None
        self._output_dir = None
        self._finalized = True
        self._objects_by_id = {}
        self._prev_yaw_rate = {}
        self._collision_sensor = None
        self._collision_events = deque()
        self._collision_lock = Lock()
        self._last_object_index_by_actor_id: dict[int, int] = {}

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

        while self._server_version is None:
            try:
                self._connect()
            except Exception:
                logger.exception("Failed to connect to CARLA, retrying in 2 seconds...")
                time.sleep(2)
                continue
            break
        print("CARLA service initialized")

    @property
    def should_quit(self) -> bool:
        return self._quit_flag

    def initialize(self, request) -> None:
        self._output_base = Path(request.output_dir.path)
        self.config = MessageToDict(request.config.config)
        self.scenario = request.scenario
        self._connect()

        self._fixed_delta_seconds = request.dt

        self._sync = bool(self.config.get("synchronous_mode", True))
        self._no_rendering = bool(self.config.get("no_rendering_mode", False))
        self._yaw_sign = float(self.config.get("yaw_sign", 1.0))
        self._yaw_offset_deg = float(self.config.get("yaw_offset_deg", 0.0))

        self._max_steer_rad: float | None = None
        self._quit_flag = False

        self._spawned_actor_ids: set[int] = set()
        self._scenario_runner_path = self.config.get("scenario_runner_path", None)
        self._ego_role_name = self.config.get("ego_role_name", "hero")

        self._scenario_runner_tm_port = int(os.environ.get("CARLA_TM_PORT", 8000))
        self._scenario_runner_tm_seed = int(self.config.get("scenario_runner_tm_seed", 0))

        self._sr_scenario = None
        self._sr_tree = None
        self._sr_running = False
        self._sr_ego_vehicles: list = []

    def reset(self, request) -> RuntimeFrame:
        if not self._finalized:
            self._finalize()

        self._output_dir = self._output_base / Path(request.output_dir.path)
        self._time_ns = 0
        self._quit_flag = False
        self._clear_collision_events()

        self._ensure_world(request.scenario_pack)
        self._apply_world_settings()

        p = str((self._output_dir / "carla_recording.log").resolve())
        self._client.start_recorder(p)
        logger.info("Starting ScenarioRunner...")
        self._start_scenario_runner(request.scenario_pack, request.params)

        if self._ego_vehicle is None:
            logger.warning("Ego vehicle not found after starting ScenarioRunner")

        self._setup_collision_sensor()
        if self._sync:
            self._world.tick()
        frame = self._collect_runtime_frame()

        self._finalized = False

        return frame

    def step(self, request) -> RuntimeFrame | None:
        if self._world is None:
            return None

        self._apply_ctrl(request.ctrl_cmd)
        self._tick_scenario_runner_module()
        if self._sync:
            self._world.tick()
        else:
            self._world.wait_for_tick()
        frame = self._collect_runtime_frame()
        return frame

    def stop(self) -> None:
        self._finalize()
        try:
            self._destroy_spawned_actors()
        finally:
            self._stop_scenario_runner_module()
            if self._world is not None and self._original_settings is not None:
                try:
                    self._world.apply_settings(self._original_settings)
                except Exception:
                    logger.exception("Failed to restore CARLA world settings")

        self._ego_vehicle = None
        self._world = None
        self._client = None
        # Clear `_server_version` too — it's the reconnection guard in
        # `_connect()`, so a follow-up `Init` after this Stop would
        # short-circuit the connect and leave `_client` as None.
        self._server_version = None
        logger.info("CARLA simulator stopped.")

    def _finalize(self):
        try:
            if self._client is not None:
                self._client.stop_recorder()
            self._destroy_spawned_actors()
            self._stop_scenario_runner_module()
        except Exception:
            logger.exception("Error during CARLA finalization")

        self._finalized = True
        logger.info("CARLA service finalized.")

    def _connect(self):
        if self._server_version is not None:
            return
        print("Connecting to CARLA...")
        self._client = carla.Client(
            os.environ.get("CARLA_HOST", "localhost"),
            int(os.environ.get("CARLA_PORT", 2000)),
        )
        try:
            self._client.set_timeout(2.0)
            self._server_version = self._client.get_server_version()
        finally:
            self._client.set_timeout(float(os.environ.get("CARLA_TIMEOUT", 10.0)))

        print("Connected to CARLA")

    def _to_carla_yaw(self, yaw_rad: float) -> float:
        return self._yaw_sign * math.degrees(yaw_rad) + self._yaw_offset_deg

    def _from_carla_yaw(self, yaw_deg: float) -> float:
        return math.radians((yaw_deg - self._yaw_offset_deg) * self._yaw_sign)

    def _ensure_world(self, sps: ScenarioPack | None) -> None:
        if self._client is None:
            self._connect()

        opendrive_name = sps.map_name
        opendrive_path = Path(f"/mnt/map/xodr/{opendrive_name}.xodr").resolve()

        world = None
        if hasattr(self._client, "generate_opendrive_world"):
            if not opendrive_path.exists():
                raise RuntimeError("OpenDRIVE path not found for CARLA world generation")

            with open(opendrive_path, encoding="utf-8") as f:
                opendrive_str = f.read()
            # OpenDRIVE world generation can take minutes — bump the
            # client timeout, but guarantee it gets restored even if
            # generation raises (otherwise every subsequent CARLA call
            # on this client inherits the inflated 300s timeout).
            default_timeout = float(os.environ.get("CARLA_TIMEOUT", 10.0))
            self._client.set_timeout(300.0)
            try:
                logger.info("Generating CARLA world from OpenDRIVE: %s", opendrive_path)
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
                logger.info("Generated CARLA world from OpenDRIVE: %s", opendrive_path)
            finally:
                self._client.set_timeout(default_timeout)
        else:
            raise RuntimeError("Cannot determine CARLA world to load")

        if world is None:
            world = self._client.get_world()

        self._world = world
        if self._original_settings is None:
            self._original_settings = world.get_settings()

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

    def _destroy_spawned_actors(self) -> None:
        self._destroy_collision_sensor()

        if self._world is not None:
            for actor_id in list(self._spawned_actor_ids):
                actor = self._world.get_actor(actor_id)
                if actor is not None:
                    try:
                        actor.destroy()
                    except Exception:
                        logger.exception("Failed to destroy actor %s", actor_id)

        self._spawned_actor_ids.clear()
        self._objects_by_id.clear()
        self._prev_yaw_rate.clear()
        self._last_object_index_by_actor_id.clear()
        self._clear_collision_events()

    def _start_scenario_runner(self, sps: ScenarioPack, params: dict | None) -> None:
        if self._scenario_runner_path is None:
            raise RuntimeError("scenario_runner_path is required when use_scenario_runner")

        self._start_scenario_runner_module(sps, params)

    def _start_scenario_runner_module(self, sps: ScenarioPack, params: dict | None) -> None:
        if self._client is None or self._world is None:
            raise RuntimeError("CARLA client/world not available")

        CarlaDataProvider.set_client(self._client)
        CarlaDataProvider.set_world(self._world)
        CarlaDataProvider.set_traffic_manager_port(self._scenario_runner_tm_port)
        tm = self._client.get_trafficmanager(self._scenario_runner_tm_port)
        tm.set_random_device_seed(self._scenario_runner_tm_seed)
        if self._sync:
            tm.set_synchronous_mode(True)

        match self.scenario.format:
            case "open_scenario1":
                openscenario_params: dict[str, str] = {}
                if params:
                    openscenario_params = {str(k): str(v) for k, v in params.items()}
                xosc_name = sps.name
                xosc_path = Path(f"/mnt/scenario/{xosc_name}.xosc").resolve()
                if xosc_path is None:
                    raise RuntimeError("ScenarioPack has no xosc scenario to run")

                config = OpenScenarioConfiguration(
                    str(xosc_path), self._client, openscenario_params
                )

            case "carla_lb_route":
                route_name = self.scenario.name
                xml_path = os.path.join(self.scenario.path.path, f"{route_name}.xml")
                config = RouteParser.parse_routes_file(xml_path, 0)
                logger.info("Parsed route scenario config: %s", config)
                config = config[0]

            case _:
                raise RuntimeError(f"Unsupported scenario format: {self.scenario.format}")

        match self.scenario.format:
            case "open_scenario1":
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
                        raise RuntimeError(f"Failed to spawn ego vehicle '{ego_config.rolename}'")
                    ego_vehicles.append(actor)
                logger.debug(f"Spawned {len(ego_vehicles)} ego vehicles for scenario")

                scenario = OpenScenario(
                    world=self._world,
                    ego_vehicles=ego_vehicles,
                    config=config,
                    config_file=str(xosc_path),
                    timeout=sps.timeout_ns / 1e9 if sps.timeout_ns > 0 else 10000.0,
                )

                self._sr_ego_vehicles = ego_vehicles
                self._ego_vehicle = ego_vehicles[0]

            case "carla_lb_route":
                scenario = RouteScenario(world=self._world, config=config)
                self._ego_vehicle = scenario.ego_vehicles[0]

            case _:
                raise RuntimeError(f"Unsupported scenario format: {self.scenario.format}")

        self._sr_scenario = scenario
        self._sr_tree = scenario.scenario_tree

        GameTime.restart()
        self._sr_running = True

    def _stop_scenario_runner_module(self) -> None:
        if self._sr_scenario is None and self._sr_tree is None and not self._sr_ego_vehicles:
            logger.info("ScenarioRunner not running, no need to stop")
            return

        # Explicitly destroy ego vehicles
        for ego in self._sr_ego_vehicles:
            if ego is not None and self._world is not None:
                try:
                    if ego in self._world.get_actors():
                        ego.destroy()
                except Exception:
                    logger.exception(
                        "Failed to destroy ego vehicle %s",
                        ego.id if hasattr(ego, "id") else "unknown",
                    )

        # Terminate scenario
        try:
            if self._sr_scenario is not None:
                self._sr_scenario.remove_all_actors()
                self._sr_scenario.terminate()
                logger.info("Scenario terminated successfully")
        except Exception:
            logger.exception("Failed to terminate scenario")

        try:
            if self._sr_tree is not None:
                self._sr_tree.stop(py_trees.common.Status.INVALID)
                logger.info("Scenario tree stopped successfully")
        except Exception:
            logger.exception("Failed to stop scenario tree")

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
        logger.info("ScenarioRunner cleanup complete")

    def _tick_scenario_runner_module(self) -> None:
        if self._world is None:
            return
        if self._sr_scenario is None:
            return

        snapshot = self._world.get_snapshot()
        timestamp = snapshot.timestamp

        if self._sr_tree is None or not self._sr_running:
            return

        GameTime.on_carla_tick(timestamp)
        CarlaDataProvider.on_carla_tick()
        self._sr_tree.tick_once()
        if self._sr_tree.status != py_trees.common.Status.RUNNING:
            self._sr_running = False
            self._quit_flag = True

    def _actor_type(self, actor) -> RoadObjectType:
        type_id = actor.type_id.lower()
        if type_id.startswith("walker.pedestrian"):
            return RoadObjectType.PEDESTRIAN
        if type_id.startswith("vehicle."):
            if "bus" in type_id:
                return RoadObjectType.BUS
            if "truck" in type_id:
                return RoadObjectType.TRUCK
            if "trailer" in type_id:
                return RoadObjectType.TRAILER
            if "motorcycle" in type_id or "motorbike" in type_id:
                return RoadObjectType.MOTORCYCLE
            if "bicycle" in type_id or "bike" in type_id or "diamondback" in type_id:
                return RoadObjectType.BICYCLE
            if "van" in type_id:
                return RoadObjectType.VAN
            return RoadObjectType.CAR
        return RoadObjectType.UNKNOWN

    def _shape_from_actor(self, actor) -> Shape:
        try:
            bb = actor.bounding_box
            dims = Shape.Dimension(
                x=float(bb.extent.x * 2.0),
                y=float(bb.extent.y * 2.0),
                z=float(bb.extent.z * 2.0),
            )
            return Shape(type=ShapeType.BOUNDING_BOX, dimensions=dims)
        except Exception:
            return Shape(
                type=ShapeType.BOUNDING_BOX,
                dimensions=Shape.Dimension(x=0.0, y=0.0, z=0.0),
            )

    def _get_forward_speed(self, actor) -> float:
        vel = actor.get_velocity()
        fwd = actor.get_transform().get_forward_vector()
        return float(vel.x * fwd.x + vel.y * fwd.y + vel.z * fwd.z)

    def _get_forward_accel(self, actor) -> float:
        acc = actor.get_acceleration()
        fwd = actor.get_transform().get_forward_vector()
        return float(acc.x * fwd.x + acc.y * fwd.y + acc.z * fwd.z)

    def _collect_runtime_frame(self) -> RuntimeFrame:
        objects, sim_time_ns, carla_frame = self._collect_objects()
        collisions = self._collect_collision_infos()

        extras_payload = {
            "carla_frame": carla_frame,
            "object_index_by_actor_id": {
                str(actor_id): index
                for actor_id, index in self._last_object_index_by_actor_id.items()
            },
            "collision_count": len(collisions),
            "untracked_collision_count": sum(
                1 for collision in collisions if not collision.HasField("actor_b")
            ),
        }
        if self._ego_vehicle is not None:
            extras_payload["ego_actor_id"] = int(self._ego_vehicle.id)

        extras = Struct()
        extras.update(extras_payload)

        return RuntimeFrame(
            sim_time_ns=sim_time_ns,
            objects=objects,
            collision=collisions,
            extras=extras,
        )

    def _collect_objects(self) -> tuple[list[ObjectState], int, int]:
        if self._world is None:
            return [], 0, 0

        snapshot = self._world.get_snapshot()
        sim_time_ns = int(snapshot.timestamp.elapsed_seconds * 1e9)
        carla_frame = int(snapshot.frame)

        actors = []
        actors.extend(self._world.get_actors().filter("vehicle.*"))
        actors.extend(self._world.get_actors().filter("walker.pedestrian.*"))

        actor_ids = {a.id for a in actors}
        for stale_id in list(self._objects_by_id.keys()):
            if stale_id not in actor_ids:
                self._objects_by_id.pop(stale_id, None)
                self._prev_yaw_rate.pop(stale_id, None)

        objects: list[ObjectState] = []
        ordered_actors = []

        def upsert(actor):
            transform = actor.get_transform()
            ang = actor.get_angular_velocity()

            yaw = self._from_carla_yaw(float(transform.rotation.yaw))
            speed = self._get_forward_speed(actor)
            accel = self._get_forward_accel(actor)
            yaw_rate = math.radians(float(ang.z)) * self._yaw_sign

            prev_rate = self._prev_yaw_rate.get(actor.id, yaw_rate)
            dt_s = 0.0
            if self._time_ns > 0 and sim_time_ns > self._time_ns:
                dt_s = (sim_time_ns - self._time_ns) / 1e9
            yaw_acc = (yaw_rate - prev_rate) / dt_s if dt_s > 0 else 0.0
            self._prev_yaw_rate[actor.id] = yaw_rate

            kin = ObjectKinematic(
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
            obj = self._objects_by_id.get(actor.id)
            if obj is None:
                obj = ObjectState(
                    type=self._actor_type(actor),
                    kinematic=kin,
                    shape=self._shape_from_actor(actor),
                )
                self._objects_by_id[actor.id] = obj
            else:
                obj.kinematic.CopyFrom(kin)

            return obj

        if self._ego_vehicle is not None:
            objects.append(upsert(self._ego_vehicle))
            ordered_actors.append(self._ego_vehicle)

        for actor in actors:
            if self._ego_vehicle is not None and actor.id == self._ego_vehicle.id:
                continue
            objects.append(upsert(actor))
            ordered_actors.append(actor)

        self._last_object_index_by_actor_id = {
            actor.id: index for index, actor in enumerate(ordered_actors)
        }
        self._time_ns = sim_time_ns

        logger.debug("Collected %s objects at time %s ns", len(objects), sim_time_ns)

        return objects, sim_time_ns, carla_frame

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
            sensor.listen(lambda event: CarlaSimulation._on_collision_event(weak_self, event))
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
            sensor.destroy()
        except Exception:
            logger.exception("Failed to destroy collision sensor")

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

    def _collect_collision_infos(self) -> list[CollisionInfo]:
        with self._collision_lock:
            events = list(self._collision_events)
            self._collision_events.clear()

        collisions: list[CollisionInfo] = []
        ego_actor_id = self._ego_vehicle.id if self._ego_vehicle is not None else None
        for event in events:
            actor_a_index = self._last_object_index_by_actor_id.get(event["actor_id"])
            if actor_a_index is None and event["actor_id"] == ego_actor_id:
                actor_a_index = 0
            actor_b_index = self._last_object_index_by_actor_id.get(event["other_actor_id"])

            impulse = event["normal_impulse"]
            details_payload = {
                "carla_frame": event["frame"],
                "timestamp_seconds": event["timestamp"],
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

            details = Struct()
            details.update(details_payload)

            collision = CollisionInfo(occurred=True, details=details)
            if actor_a_index is not None:
                collision.actor_a = actor_a_index
            if actor_b_index is not None:
                collision.actor_b = actor_b_index
            collisions.append(collision)

        return collisions

    def _apply_ctrl(self, ctrl: CtrlCmd) -> None:
        if self._ego_vehicle is None:
            return
        if ctrl is None or ctrl.mode == CtrlMode.NONE:
            return

        payload = MessageToDict(ctrl.payload)

        if ctrl.mode == CtrlMode.THROTTLE_STEER:
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

            control = carla.VehicleControl(
                throttle=throttle, steer=steer * self._yaw_sign, brake=brake
            )
            self._ego_vehicle.apply_control(control)
            return

        elif ctrl.mode == CtrlMode.ACKERMANN:
            steer_speed = float(payload.get("steer_speed", 0.0))

            steer = float(payload.get("steer", 0.0)) * self._yaw_sign
            speed = float(payload.get("speed", self._get_forward_speed(self._ego_vehicle)))
            acceleration = payload.get("acceleration", None)
            if acceleration is None:
                acceleration = float(self.config.get("ackermann_accel_default", 1.5))
            else:
                acceleration = float(acceleration)
            jerk = payload.get("jerk", None)
            if jerk is None:
                jerk = float(self.config.get("ackermann_jerk_default", 0.0))
            else:
                jerk = float(jerk)

            if self._max_steer_rad:
                steer = _clamp(steer / self._max_steer_rad, -1.0, 1.0)
            else:
                steer = _clamp(steer, -1.0, 1.0)

            control = carla.VehicleAckermannControl(
                steer=steer,
                steer_speed=steer_speed,
                speed=speed,
                acceleration=acceleration,
                jerk=jerk,
            )
            self._ego_vehicle.apply_ackermann_control(control)
            return

        elif ctrl.mode == CtrlMode.POSITION:
            transform = self._ego_vehicle.get_transform()
            x = float(payload.get("x", transform.location.x))
            y = float(payload.get("y", transform.location.y))
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
