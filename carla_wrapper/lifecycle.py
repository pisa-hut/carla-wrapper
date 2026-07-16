import logging
from typing import Any

logger = logging.getLogger(__name__)

RUNTIME_ACTOR_PREFIXES = (
    "vehicle.",
    "walker.",
    "controller.ai.walker",
    "sensor.",
)


def is_dynamic_actor(actor: Any) -> bool:
    type_id = getattr(actor, "type_id", "")
    return any(type_id.startswith(prefix) for prefix in RUNTIME_ACTOR_PREFIXES)


def force_async_world_for_cleanup(
    world: Any,
    *,
    client: Any = None,
    traffic_manager_port: int = 8000,
    keep_synchronous: bool = False,
    log: logging.Logger = logger,
) -> None:
    if world is None:
        return

    try:
        settings = world.get_settings()
        changed = False
        if keep_synchronous and not getattr(settings, "synchronous_mode", False):
            settings.synchronous_mode = True
            changed = True
        elif not keep_synchronous and getattr(settings, "synchronous_mode", False):
            settings.synchronous_mode = False
            changed = True
        if not keep_synchronous and getattr(settings, "fixed_delta_seconds", None) is not None:
            settings.fixed_delta_seconds = None
            changed = True
        # An asynchronous world advances without client ticks. If rendering is
        # left enabled between episodes, an otherwise idle CARLA server renders
        # as fast as possible and can saturate the GPU. The next reset restores
        # the configured rendering mode before the scenario starts.
        if not getattr(settings, "no_rendering_mode", False):
            settings.no_rendering_mode = True
            changed = True
        if changed:
            world.apply_settings(settings)
            mode = "synchronous" if keep_synchronous else "asynchronous"
            log.info("Prepared CARLA world in %s no-rendering mode for cleanup", mode)
    except Exception:
        log.exception("Failed to configure CARLA world for cleanup")

    if client is None:
        return

    try:
        client.get_trafficmanager(traffic_manager_port).set_synchronous_mode(keep_synchronous)
    except Exception:
        mode = "synchronous" if keep_synchronous else "asynchronous"
        log.exception("Failed to set TrafficManager %s mode for cleanup", mode)


def clear_dynamic_actors(
    world: Any,
    *,
    client: Any = None,
    traffic_manager_port: int = 8000,
    keep_synchronous: bool = False,
    destroy_actor_command: Any = None,
    log: logging.Logger = logger,
) -> int:
    if world is None:
        return 0

    force_async_world_for_cleanup(
        world,
        client=client,
        traffic_manager_port=traffic_manager_port,
        keep_synchronous=keep_synchronous,
        log=log,
    )

    try:
        actors = sorted(
            world.get_actors(),
            key=lambda actor: getattr(actor, "id", -1),
        )
    except Exception:
        log.exception("Failed to list CARLA actors for reset cleanup")
        return 0

    dynamic_actors = [actor for actor in actors if is_dynamic_actor(actor)]
    for actor in dynamic_actors:
        if getattr(actor, "type_id", "").startswith("sensor.") and getattr(
            actor, "is_listening", True
        ):
            try:
                actor.stop()
            except Exception:
                log.exception(
                    "Failed to stop dynamic sensor %s before batch cleanup",
                    getattr(actor, "id", "<unknown>"),
                )

    destroyed_count = 0
    can_batch = (
        client is not None
        and destroy_actor_command is not None
        and hasattr(client, "apply_batch_sync")
    )
    if can_batch and dynamic_actors:
        commands = [destroy_actor_command(actor.id) for actor in dynamic_actors]
        fallback_actors = []
        try:
            responses = client.apply_batch_sync(commands, False)
        except Exception:
            log.exception("Failed to batch-destroy dynamic CARLA actors")
            fallback_actors = dynamic_actors
        else:
            for index, actor in enumerate(dynamic_actors):
                if index >= len(responses):
                    fallback_actors.append(actor)
                    continue
                response = responses[index]
                has_error = getattr(response, "has_error", None)
                failed = (
                    has_error() if callable(has_error) else bool(getattr(response, "error", None))
                )
                if failed:
                    error = getattr(response, "error", "unknown CARLA error")
                    log.warning(
                        "CARLA failed to batch-destroy dynamic actor %s: %s",
                        getattr(actor, "id", "<unknown>"),
                        error,
                    )
                    fallback_actors.append(actor)
                else:
                    destroyed_count += 1
        for actor in fallback_actors:
            if destroy_actor(actor, log=log, label="dynamic actor fallback"):
                destroyed_count += 1
    else:
        for actor in dynamic_actors:
            if destroy_actor(actor, log=log, label="dynamic actor"):
                destroyed_count += 1

    if destroyed_count:
        log.info("Destroyed %s dynamic CARLA actors before reset", destroyed_count)
    return destroyed_count


def destroy_actor(actor: Any, *, log: logging.Logger = logger, label: str = "actor") -> bool:
    if actor is None:
        return False

    actor_id = getattr(actor, "id", "<unknown>")
    try:
        result = actor.destroy()
        if result is False:
            log.warning("CARLA reported failure while destroying %s %s", label, actor_id)
            return False
        return True
    except Exception:
        log.exception("Failed to destroy %s %s", label, actor_id)
        return False
