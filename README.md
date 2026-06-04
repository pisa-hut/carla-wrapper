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
- `yaw_sign`: Coordinate yaw/sign convention multiplier. Defaults to `-1.0`.
- `yaw_offset_deg`: Coordinate yaw offset in degrees. Defaults to `0.0`.
- `carla_connect_timeout_seconds`: Total CARLA connection retry window.
  Defaults to `40`.
- `retry_interval_seconds`: Delay between CARLA connection attempts. Defaults
  to `2`.
- `disable_scenario_runner_ego_control`: Removes ScenarioRunner's ego
  controller before external control is applied. Defaults to `true`.
- `scenario_runner_tm_seed`: TrafficManager random seed. Defaults to `0`.
- `kinematic_speed_deadband_mps`: Clamp near-zero speed values to `0.0`.
  Defaults to `0.02`.
- `kinematic_acceleration_deadband_mps2`: Clamp near-zero longitudinal
  acceleration values to `0.0`. Defaults to `0.15`.
- `kinematic_yaw_rate_deadband_radps`: Clamp near-zero yaw-rate values to
  `0.0`. Defaults to `0.003`.
- `kinematic_yaw_acceleration_deadband_radps2`: Clamp near-zero yaw
  acceleration values to `0.0`. Defaults to `0.1`.
- `ackermann_use_native_control`: Uses CARLA native Ackermann control when
  enabled. Defaults to `false`.
- `ackermann_native_speed_kp`: Native Ackermann speed PID proportional gain.
  Defaults to `0.10`.
- `ackermann_native_speed_ki`: Native Ackermann speed PID integral gain.
  Defaults to `0.0`.
- `ackermann_native_speed_kd`: Native Ackermann speed PID derivative gain.
  Defaults to `0.10`.
- `ackermann_native_accel_kp`: Native Ackermann acceleration PID proportional
  gain. Defaults to `0.01`.
- `ackermann_native_accel_ki`: Native Ackermann acceleration PID integral gain.
  Defaults to `0.0`.
- `ackermann_native_accel_kd`: Native Ackermann acceleration PID derivative
  gain. Defaults to `0.0`.
- `ackermann_speed_kp`: Fallback throttle proportional gain. Defaults to
  `0.5`.
- `ackermann_min_throttle`: Fallback minimum non-zero throttle. Defaults to
  `0.2`.
- `ackermann_max_throttle`: Fallback maximum throttle. Defaults to `0.75`.
- `ackermann_stop_speed_threshold`: Speed treated as stopped, in m/s. Defaults
  to `0.25`.
- `ackermann_launch_speed_threshold`: Current speed below which launch throttle
  can be used, in m/s. Defaults to `0.1`.
- `ackermann_launch_target_threshold`: Target speed required for launch
  throttle, in m/s. Defaults to `0.25`.
- `ackermann_launch_throttle`: Fallback launch throttle. Defaults to `0.45`.
- `ackermann_brake_kp`: Fallback brake proportional gain. Defaults to `0.6`.
- `ackermann_min_brake`: Fallback minimum non-zero brake. Defaults to `0.15`.
- `ackermann_max_brake`: Fallback maximum brake. Defaults to `1.0`.
- `ackermann_accel_default`: Default Ackermann acceleration when omitted.
  Defaults to `1.5`.
- `ackermann_decel_default`: Default Ackermann deceleration magnitude when
  omitted while slowing. Defaults to `4.0`.
- `ackermann_jerk_default`: Default Ackermann jerk when omitted. Defaults to
  `0.0`.
- `ackermann_brake_jerk_default`: Default Ackermann jerk when omitted while
  slowing. Defaults to `8.0`.

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
