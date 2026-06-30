# Carla Wrapper

CARLA simulator wrapper for the PISA simulator API. The container starts a CARLA
server process, connects through the CARLA Python API, prepares the CARLA world,
then runs a ScenarioRunner scenario for each reset.

## Runtime Contract

The wrapper expects these paths to be mounted in the container:

- `/mnt/map/xodr`: OpenDRIVE maps for wrapper-loaded maps. When the wrapper
  loads a map directly, `ScenarioPackData.map_name` is resolved as
  `/mnt/map/xodr/<map_name>.xodr`.
- `/mnt/scenario`: OpenSCENARIO files. `open_scenario1` resolves
  `ScenarioPackData.name` as `/mnt/scenario/<name>.xosc`.
- `/mnt/output`: Runtime output. CARLA server logs are written to
  `/mnt/output/carla_server`, and each reset starts a CARLA recorder under the
  request output directory.

## Supported Scenario Formats

- `open_scenario1`: Runs a ScenarioRunner `OpenScenario` from a `.xosc` file.
  The OpenDRIVE world is loaded by ScenarioRunner from the `.xosc`
  `RoadNetwork/LogicFile`, and the wrapper syncs to the resulting CARLA world.
- `carla_lb_route`: Runs a ScenarioRunner `RouteScenario` from the route XML
  found at `<scenario.path>/<scenario.name>.xml`.

## Configuration

Common config keys accepted by `InitRequest.config`:

- `synchronous_mode`: Enables CARLA synchronous mode. Defaults to `true`.
- `no_rendering_mode`: Enables CARLA no-rendering mode. Defaults to `true`.
- `record`: Records each run to `carla_recording.log`. Defaults to `false`.
- `open_scenario_map_loader`: Selects who prepares the map for `open_scenario1`.
  `scenario_runner` (default) preserves ScenarioRunner's native loading flow.
  `wrapper` generates `/mnt/map/xodr/<ScenarioPackData.map_name>.xodr` first with
  `wall_height=0.0`; ScenarioRunner may verify the map but is prevented from
  replacing the generated world. The OpenSCENARIO `LogicFile` must reference the
  same OpenDRIVE content. Because `map_name` is only available on `ResetRequest`,
  the first reset after each `init()` generates the map. Later resets reuse it
  only when the current CARLA map is `OpenDriveMap` and its OpenDRIVE digest
  still matches; otherwise the wrapper regenerates the requested map.
- `yaw_sign`: Coordinate yaw/sign convention multiplier. Defaults to `-1.0`.
- `yaw_offset_deg`: Coordinate yaw offset in degrees. Defaults to `0.0`.
- `carla_connect_timeout_seconds`: Total CARLA connection retry window.
  Defaults to `40`.
- `retry_interval_seconds`: Delay between CARLA connection attempts. Defaults
  to `2`.
- `scenario_runner_tm_seed`: TrafficManager random seed. Defaults to `0`.
- `kinematic_speed_deadband_mps`,
  `kinematic_acceleration_deadband_mps2`,
  `kinematic_yaw_rate_deadband_radps`,
  `kinematic_yaw_acceleration_deadband_radps2`: Clamp near-zero output
  kinematic values to `0.0` before publishing runtime objects. Defaults are
  `0.02`, `0.15`, `0.003`, and `0.1`, respectively.
- `ackermann_use_native_control`: Uses CARLA native Ackermann control when
  enabled. Defaults to `false`.
- `ackermann_native_speed_kp`, `ackermann_native_speed_ki`,
  `ackermann_native_speed_kd`, `ackermann_native_accel_kp`,
  `ackermann_native_accel_ki`, `ackermann_native_accel_kd`,
  `ackermann_speed_kp`, `ackermann_min_throttle`,
  `ackermann_max_throttle`, `ackermann_stop_speed_threshold`,
  `ackermann_launch_speed_threshold`, `ackermann_launch_target_threshold`,
  `ackermann_launch_throttle`, `ackermann_brake_kp`,
  `ackermann_min_brake`, `ackermann_max_brake`,
  `ackermann_accel_default`, `ackermann_decel_default`,
  `ackermann_jerk_default`, `ackermann_brake_jerk_default`: Parameters for
  Ackermann native and fallback vehicle-control behavior.

ScenarioRunner is loaded through the container `PYTHONPATH`.

## Ports

The wrapper uses these environment variables:

- `CARLA_HOST`: CARLA server host. Defaults to `localhost`.
- `CARLA_PORT`: CARLA RPC port and launched server port. Defaults to `2000`.
- `CARLA_TM_PORT`: TrafficManager port. Defaults to `8000`.
- `CARLA_TIMEOUT`: Default CARLA client RPC timeout in seconds. Defaults to
  `30` in the container.

Only one CARLA server can bind a given `CARLA_PORT` at a time. A port conflict
will prevent the launched CARLA server from starting correctly.

## Lifecycle

During `init()`, the wrapper prepares a reused CARLA server by forcing the
current world and TrafficManager out of synchronous mode and removing leftover
dynamic actors. This keeps local development runs from inheriting vehicles or
sync settings from an interrupted previous run.

Each `reset()` finalizes any previous run before creating a new world and
scenario. After the world is prepared, reset removes dynamic runtime actors
such as vehicles, walkers, walker controllers, and sensors before starting the
next scenario. The wrapper owns the CARLA server state during a run, so it does
not restore the previous world settings between resets; it reapplies the desired
settings for each scenario instead.

If reset fails partway through, the wrapper finalizes the partial state before
re-raising the error. Finalization stops the CARLA recorder, destroys
wrapper-spawned sensors/actors, stops ScenarioRunner, cleans ScenarioRunner
global state, and restores TrafficManager synchronous mode when the wrapper
enabled it.
