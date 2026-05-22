# Carla Wrapper

CARLA simulator wrapper for the PISA simulator API. The container starts a CARLA
server process, connects through the CARLA Python API, loads an OpenDRIVE map,
then runs a ScenarioRunner scenario for each reset.

## Runtime Contract

The wrapper expects these paths to be mounted in the container:

- `/mnt/map/xodr`: OpenDRIVE maps. `ScenarioPackData.map_name` is resolved as
  `/mnt/map/xodr/<map_name>.xodr`.
- `/mnt/scenario`: OpenSCENARIO files. `open_scenario1` resolves
  `ScenarioPackData.name` as `/mnt/scenario/<name>.xosc`.
- `/mnt/output`: Runtime output. CARLA server logs are written to
  `/mnt/output/carla_server`, and each reset starts a CARLA recorder under the
  request output directory.

## Supported Scenario Formats

- `open_scenario1`: Runs a ScenarioRunner `OpenScenario` from a `.xosc` file.
- `carla_lb_route`: Runs a ScenarioRunner `RouteScenario` from the route XML
  found at `<scenario.path>/<scenario.name>.xml`.

## Configuration

Common config keys accepted by `InitRequest.config`:

- `synchronous_mode`: Enables CARLA synchronous mode. Defaults to `true`.
- `no_rendering_mode`: Enables CARLA no-rendering mode. Defaults to `false`.
- `yaw_sign`: Coordinate yaw/sign convention multiplier. Defaults to `1.0`.
- `yaw_offset_deg`: Coordinate yaw offset in degrees. Defaults to `0.0`.
- `carla_connect_timeout_seconds`: Total CARLA connection retry window.
  Defaults to `10`.
- `retry_interval_seconds`: Delay between CARLA connection attempts. Defaults
  to `2`.
- `disable_scenario_runner_ego_control`: Removes ScenarioRunner's ego
  controller before external control is applied. Defaults to `true`.
- `scenario_runner_ego_control_ticks_before_disable`: Number of ScenarioRunner
  ticks before disabling its ego controller. Defaults to `1`.
- `scenario_runner_tm_seed`: TrafficManager random seed. Defaults to `0`.
- `ackermann_use_native_control`: Uses CARLA native Ackermann control when
  enabled. Defaults to `false`.
- `ackermann_speed_kp`, `ackermann_min_throttle`,
  `ackermann_accel_default`, `ackermann_jerk_default`: Parameters for the
  fallback vehicle-control Ackermann backend.

The legacy `scenario_runner_path` key is accepted for compatibility but is not
required. ScenarioRunner is loaded through the container `PYTHONPATH`.

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

Each `reset()` finalizes any previous run before creating a new world and
scenario. If reset fails partway through, the wrapper finalizes the partial
state before re-raising the error. Finalization stops the CARLA recorder,
destroys wrapper-spawned sensors/actors, restores world settings, stops
ScenarioRunner, cleans ScenarioRunner global state, and restores TrafficManager
synchronous mode when the wrapper enabled it.
