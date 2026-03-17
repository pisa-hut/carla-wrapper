import grpc
from concurrent import futures
import time
from typing import Optional
from pathlib import Path
import os
import math
from google.protobuf.json_format import MessageToDict
from pprint import pprint

import carla

from pisa_api import sim_server_pb2, sim_server_pb2_grpc
from pisa_api.pong_pb2 import Pong
from pisa_api.empty_pb2 import Empty
from pisa_api.scenario_pb2 import ScenarioPack
from pisa_api.object_pb2 import (
    ObjectState,
    ObjectKinematic,
    Shape,
    ShapeType,
    RoadObjectType,
)
from pisa_api.control_pb2 import CtrlCmd, CtrlMode

from srunner.scenarioconfigs.openscenario_configuration import (
    OpenScenarioConfiguration,
)
from srunner.scenariomanager.carla_data_provider import CarlaDataProvider
from srunner.scenariomanager.timer import GameTime
from srunner.scenarios.open_scenario import OpenScenario
from srunner.scenariomanager.carla_data_provider import CarlaDataProvider
from srunner.scenariomanager.timer import GameTime
import py_trees

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


class CarlaService(sim_server_pb2_grpc.SimServerServicer):
    def __init__(self):
        self._client = None
        self._world = None
        self.config = None
        self._original_settings = None

        self._objects_by_id = {}
        self._prev_yaw_rate = {}

        while self._world is None:
            try:
                self._connect()
            except Exception:
                logger.exception("Failed to connect to CARLA, retrying in 2 seconds...")
                time.sleep(2)
                continue
            break
        print("CARLA service initialized")

    def Ping(self, request, context):
        logger.info(f"Received ping from client: {context.peer()}")
        return Pong(msg="CARLA alive")

    def Init(self, request, context):
        self.config = request.config.config
        self.config = MessageToDict(request.config.config)
        pprint(self.config)
        self._connect()

        # self._client.set_timeout(float(self.config.get("timeout", 2.0)))

        self._fixed_delta_seconds = request.dt

        self._sync = bool(self.config.get("synchronous_mode", True))
        self._no_rendering = bool(self.config.get("no_rendering_mode", False))
        self._yaw_sign = float(self.config.get("yaw_sign", 1.0))
        self._yaw_offset_deg = float(self.config.get("yaw_offset_deg", 0.0))
        self._spawn_z_offset = float(self.config.get("spawn_z_offset", 3.0))

        self._ego_bp_id = self.config.get("ego_vehicle_bp", "vehicle.tesla.model3")
        self._max_steer_rad: Optional[float] = None
        self._quit_flag = False

        self._spawned_actor_ids: set[int] = set()
        self._scenario_runner_path = self.config.get("scenario_runner_path", None)
        self._ego_role_name = self.config.get("ego_role_name", "hero")
        self._scenario_runner_tm_port = int(
            self.config.get("scenario_runner_tm_port", 8000)
        )
        self._scenario_runner_tm_seed = int(
            self.config.get("scenario_runner_tm_seed", 0)
        )

        self._sr_scenario = None
        self._sr_tree = None
        self._sr_running = False
        self._sr_ego_vehicles: list = []

        return sim_server_pb2.SimServerMessages.InitResponse(
            success=True, msg="CARLA initialized"
        )

    def Reset(self, request, context):
        self._output_dir = request.output_dir
        self._time_ns = 0
        self._quit_flag = False

        # self._ensure_world(request.scenario_pack)
        self._apply_world_settings()
        self._destroy_spawned_actors()
        self._stop_scenario_runner_module()

        logger.info("Starting ScenarioRunner...")
        self._start_scenario_runner(request.scenario_pack, request.params)

        if self._ego_vehicle is None:
            logger.warning("Ego vehicle not found after waiting, spawning ego...")
            self._spawn_ego(request.scenario_pack)

        if self._sync:
            self._world.tick()
        objects = self._collect_objects()

        return sim_server_pb2.SimServerMessages.ResetResponse(objects=objects)

    def Step(self, request, context):
        if self._world is None:
            return sim_server_pb2.SimServerMessages.StepResponse()

        dt_s = 0.0
        if self._time_ns > 0:
            dt_s = (request.timestamp_ns - self._time_ns) / 1e9

        self._apply_ctrl(request.ctrl_cmd, dt_s)
        self._tick_scenario_runner_module()
        if self._sync:
            self._world.tick()
        else:
            self._world.wait_for_tick()
        objects = self._collect_objects()
        return sim_server_pb2.SimServerMessages.StepResponse(objects=objects)

    def Stop(self, request, context):
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
        logger.info("CARLA simulator stopped.")
        return Empty()

    def ShouldQuit(self, request, context):
        return sim_server_pb2.SimServerMessages.ShouldQuitResponse(
            should_quit=self._quit_flag
        )

    def _connect(self):
        if self._world is None:
            print("Connecting to CARLA...")
            self._client = carla.Client(
                os.environ.get("CARLA_HOST", "localhost"),
                int(os.environ.get("CARLA_PORT", 2000)),
            )
            self._client.set_timeout(float(os.environ.get("CARLA_TIMEOUT", 10.0)))
            self._world = self._client.get_world()
            setting = self._world.get_settings()
            setting.no_rendering_mode = True
            setting.synchronous_mode = True
            self._world.apply_settings(setting)
            print("Connected to CARLA")
        print(f"Carla version: {self._client.get_server_version()}")

    def _to_carla_yaw(self, yaw_rad: float) -> float:
        return self._yaw_sign * math.degrees(yaw_rad) + self._yaw_offset_deg

    def _from_carla_yaw(self, yaw_deg: float) -> float:
        return math.radians((yaw_deg - self._yaw_offset_deg) * self._yaw_sign)

    def _ensure_world(self, sps: Optional[ScenarioPack]) -> None:
        if self._client is None:
            self._connect()

        carla_map_name = None
        opendrive_name = sps.map_name
        opendrive_path = Path(f"/mnt/map/xodr/{opendrive_name}.xodr").resolve()

        world = None
        if carla_map_name:
            world = self._client.load_world(carla_map_name, reset_settings=False)
        elif opendrive_path and hasattr(self._client, "generate_opendrive_world"):
            opendrive_path = Path(opendrive_path)
            if not opendrive_path.exists():
                raise RuntimeError(
                    "OpenDRIVE path not found for CARLA world generation"
                )

            # read opendrive file
            with open(opendrive_path, "r", encoding="utf-8") as f:
                opendrive_str = f.read()
            world = self._client.generate_opendrive_world(
                opendrive_str,
                carla.OpendriveGenerationParameters(
                    vertex_distance=2.0,
                    max_road_length=3000.0,
                    wall_height=10.0,
                    additional_width=0.6,
                    smooth_junctions=True,
                    enable_mesh_visibility=True,
                ),
            )
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
        if self._world is None:
            return
        if not self._spawned_actor_ids:
            return
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

    def _spawn_ego(self, sps: ScenarioPack):
        input("Press Enter to spawn ego vehicle...")
        if self._world is None:
            raise RuntimeError("CARLA world not available")

        bp_lib = self._world.get_blueprint_library()
        try:
            ego_bp = bp_lib.find(self._ego_bp_id)
        except Exception:
            candidates = bp_lib.filter("vehicle.*")
            if not candidates:
                raise RuntimeError("No vehicle blueprints available in CARLA")
            ego_bp = candidates[0]

        if ego_bp.has_attribute("role_name"):
            ego_bp.set_attribute("role_name", self._ego_role_name)

        pos = sps.ego.spawn.position
        loc = carla.Location(
            x=float(pos.x),
            y=float(pos.y) * self._yaw_sign,
            z=float(pos.z) + self._spawn_z_offset,
        )
        rot = carla.Rotation(
            pitch=math.degrees(float(pos.p)),
            yaw=self._to_carla_yaw(float(pos.h)),
            roll=math.degrees(float(pos.r)),
        )
        transform = carla.Transform(loc, rot)

        ego = self._world.try_spawn_actor(ego_bp, transform)
        if ego is None:
            logger.warning("Initial spawn failed, trying spawn points...")
            spawn_points = self._world.get_map().get_spawn_points()
            if not spawn_points:
                raise RuntimeError("Failed to spawn ego vehicle (no spawn points)")
            ego = self._world.try_spawn_actor(ego_bp, spawn_points[0])
            if ego is None:
                raise RuntimeError("Failed to spawn ego vehicle")

        self._ego_vehicle = ego
        self._spawned_actor_ids.add(ego.id)

        try:
            phys = ego.get_physics_control()
            max_steer = max([w.max_steer_angle for w in phys.wheels])
            self._max_steer_rad = math.radians(max_steer)
        except Exception:
            self._max_steer_rad = None

    def _start_scenario_runner(self, sps: ScenarioPack, params: Optional[dict]) -> None:
        if self._scenario_runner_path is None:
            raise RuntimeError(
                "scenario_runner_path is required when use_scenario_runner"
            )

        sr_path = Path(self._scenario_runner_path)
        if sr_path.is_dir():
            sr_script = sr_path / "scenario_runner.py"
        else:
            sr_script = sr_path

        self._start_scenario_runner_module(sps, params, sr_script)

    def _start_scenario_runner_module(
        self, sps: ScenarioPack, params: Optional[dict], sr_script: Path
    ) -> None:
        if self._client is None or self._world is None:
            raise RuntimeError("CARLA client/world not available")

        CarlaDataProvider.set_client(self._client)
        CarlaDataProvider.set_world(self._world)
        CarlaDataProvider.set_traffic_manager_port(self._scenario_runner_tm_port)
        tm = self._client.get_trafficmanager(self._scenario_runner_tm_port)
        tm.set_random_device_seed(self._scenario_runner_tm_seed)
        if self._sync:
            tm.set_synchronous_mode(True)

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
                raise RuntimeError(
                    f"Failed to spawn ego vehicle '{ego_config.rolename}'"
                )
            ego_vehicles.append(actor)

        scenario = OpenScenario(
            world=self._world,
            ego_vehicles=ego_vehicles,
            config=config,
            config_file=str(xosc_path),
            timeout=sps.timeout_ns / 1e9 if sps.timeout_ns > 0 else 10000.0,
        )

        self._sr_scenario = scenario
        self._sr_tree = scenario.scenario_tree

        self._sr_ego_vehicles = ego_vehicles
        self._ego_vehicle = ego_vehicles[0] if ego_vehicles else None

        GameTime.restart()
        self._sr_running = True

    def _stop_scenario_runner_module(self) -> None:
        if (
            self._sr_scenario is None
            and self._sr_tree is None
            and not self._sr_ego_vehicles
        ):
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

    def _find_ego_vehicle(self):
        if self._world is None:
            return None
        actors = self._world.get_actors().filter("vehicle.*")
        for actor in actors:
            role = actor.attributes.get("role_name", "")
            if role == self._ego_role_name:
                return actor
        return None

    def _wait_for_ego(self):
        if self._world is None:
            return None
        deadline = time.time() + self._wait_for_ego_sec
        while time.time() < deadline:
            ego = self._find_ego_vehicle()
            if ego is not None:
                return ego
            try:
                self._world.wait_for_tick()
            except Exception:
                time.sleep(0.1)
        logger.warning("Ego vehicle not found within %.1f sec", self._wait_for_ego_sec)
        return None

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

    def _collect_objects(self) -> list[ObjectState]:
        if self._world is None:
            return []

        snapshot = self._world.get_snapshot()
        sim_time_ns = int(snapshot.timestamp.elapsed_seconds * 1e9)

        actors = []
        actors.extend(self._world.get_actors().filter("vehicle.*"))
        actors.extend(self._world.get_actors().filter("walker.pedestrian.*"))

        actor_ids = {a.id for a in actors}
        for stale_id in list(self._objects_by_id.keys()):
            if stale_id not in actor_ids:
                self._objects_by_id.pop(stale_id, None)
                self._prev_yaw_rate.pop(stale_id, None)

        objects: list[ObjectState] = []

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

        for actor in actors:
            if self._ego_vehicle is not None and actor.id == self._ego_vehicle.id:
                continue
            objects.append(upsert(actor))

        self._time_ns = sim_time_ns

        return objects

    def _apply_ctrl(self, ctrl: CtrlCmd, dt_s: float) -> None:
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
            steer = float(payload.get("steer", 0.0)) * self._yaw_sign
            speed = float(
                payload.get("speed", self._get_forward_speed(self._ego_vehicle))
            )
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

            cur_speed = self._get_forward_speed(self._ego_vehicle)
            kp = float(self.config.get("speed_kp", 1))
            kb = float(self.config.get("brake_kp", kp))
            speed_err = speed - cur_speed
            throttle = _clamp(speed_err * kp, 0.0, 1.0)
            brake = _clamp(-speed_err * kb, 0.0, 1.0)
            control = carla.VehicleControl(throttle=throttle, steer=steer, brake=brake)
            self._ego_vehicle.apply_control(control)
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


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))

    sim_server_pb2_grpc.add_SimServerServicer_to_server(CarlaService(), server)

    PORT = os.environ.get("PORT", "50051")

    server.add_insecure_port(f"[::]:{PORT}")
    server.start()

    print(f"gRPC server running on port {PORT}")

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("Shutting down")
        server.stop(0)


if __name__ == "__main__":
    serve()
