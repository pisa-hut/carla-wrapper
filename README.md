# Carla Wrapper

CARLA simulator wrapper for the PISA simulator API. The container starts a CARLA
server process, connects through the CARLA Python API, prepares the CARLA world,
then runs a ScenarioRunner scenario for each reset.

## Runtime Contract

Ping identifies this artifact with `name="carla-wrapper"`; its `version` is the
installed wrapper distribution/build version. A successful Init identifies the
simulator component with `name="carla"` and returns metadata that is written to
the execution manifest. Wrapper and runner deployments must use compatible
versions of the newer `pisa-api` Ping/Init contract.

Init metadata contains CARLA `client_version` (when available), `server_version`,
`host`, RPC `port`, `traffic_manager_port`, and a nested `config` allowlist with
the effective values for `synchronous_mode`, `no_rendering_mode`, `record`,
`open_scenario_map_loader`, `yaw_sign`, `yaw_offset_deg`,
`scenario_runner_tm_seed`, `ackermann_use_native_control`, all kinematic output
deadbands, and all Ackermann controller gains, limits, launch thresholds, and
acceleration/deceleration/jerk defaults. These are normalized effective values,
including defaults, because they can affect test results. Connection timeout and
retry settings, common request fields such as `dt`, and output paths are omitted.
Map identity is not reported by Init because maps are loaded during Reset. Never
place secrets in these settings: metadata is persisted in the execution manifest.

The normative units, coordinate frames, identity, timestamp, shape, collision,
and control semantics are defined by sim-core's
`runner/docs/data-contracts/README.md`. This wrapper runs CARLA synchronously
and exposes the canonical right-handed PISA frame. It rejects asynchronous
execution and non-canonical `yaw_sign`/`yaw_offset_deg` values.

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

- `synchronous_mode`: Must be `true` so absolute PISA timestamps map to fixed
  CARLA ticks.
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
- `yaw_sign`: Must be `-1.0` for CARLA-left-handed to PISA-right-handed
  conversion.
- `yaw_offset_deg`: Must be `0.0`; heading-only offsets are not a valid
  world-frame transform.
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
- `ackermann_use_native_control`: Selects CARLA native Ackermann control when
  `true`, or the wrapper's VehicleControl speed-feedback backend when `false`.
  Defaults to `false` for the established driving behavior.
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
  native and fallback Ackermann behavior. Omitted acceleration and jerk use
  the defaults documented in `config_example.yaml`.

ScenarioRunner is loaded through the container `PYTHONPATH`.

## Canonical Controls

The wrapper accepts only the sim-core canonical action modes:

- `THROTTLE_STEER_BREAK` requires exactly `throttle`, `brake`, and `steer`.
  Values outside `[0,1]`, `[0,1]`, and `[-1,1]`, respectively, are rejected.
  Positive PISA steer means left and is sign-converted for CARLA. Brake has
  priority and forces applied throttle to zero.
- `ACKERMANN` requires `steer` in radians and non-negative `speed` in m/s.
  Optional fields are non-negative `steer_speed` in rad/s, signed
  `acceleration` in m/s², and signed `jerk` in m/s³.
- `NONE` is an empty-payload no-op. All legacy/reserved control modes are
  rejected explicitly.

Unknown, missing, non-numeric, non-finite, or out-of-range action fields are
reported as invalid simulator requests rather than clipped or reinterpreted.

## Collision Details

`CollisionInfo.actor_a/b` carry authoritative CARLA tracking IDs and semantic
metadata. The optional details mapping contains:

- `carla_frame` and `carla_timestamp_seconds`: native event diagnostics;
- `episode_frame` and `timestamp_seconds`: reset-relative diagnostics;
- `other_actor_type_id` and `other_actor_semantic_tags`: CARLA classification;
- `normal_impulse`: finite CARLA impulse components and magnitude in CARLA's
  SI impulse unit (`kg·m/s`);
- `actor_a_carla_id`/`actor_b_carla_id`: duplicate native IDs for diagnostics
  only, never a separate identity namespace.

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
