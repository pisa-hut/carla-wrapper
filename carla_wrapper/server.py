import logging
import sys
from pathlib import Path

import grpc
from pisa_api import sim_server_pb2
from pisa_api.empty_pb2 import Empty
from pisa_api.wrapper import BaseSimServer, serve_sim, setup_logging

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from carla_wrapper.simulation import CarlaSimulation

setup_logging()
logger = logging.getLogger(__name__)

SUPPORTED_SCENARIO_FORMATS = {"open_scenario1", "carla_lb_route"}


def _set_grpc_error(context, code, details: str) -> None:
    if context is None:
        return
    context.set_code(code)
    context.set_details(details)


def _peer(context) -> str:
    if context is None:
        return "unknown"
    try:
        return context.peer()
    except Exception:
        return "unknown"


class CarlaService(BaseSimServer):
    _name = "CARLA"

    def __init__(self):
        self._simulation = CarlaSimulation()
        self._initialized = False
        self._reset = False

    def Init(self, request, context):
        logger.debug("Received Init request from client: %s", _peer(context))

        scenario_format = request.scenario.format
        if scenario_format not in SUPPORTED_SCENARIO_FORMATS:
            msg = f"Unsupported scenario format: {scenario_format}"
            logger.error(msg)
            return sim_server_pb2.SimServerMessages.InitResponse(success=False, msg=msg)

        try:
            self._simulation.initialize(request)
        except Exception:
            logger.exception("Failed to initialize CARLA")
            return sim_server_pb2.SimServerMessages.InitResponse(
                success=False,
                msg="Failed to initialize CARLA",
            )

        self._initialized = True
        self._reset = False
        return sim_server_pb2.SimServerMessages.InitResponse(
            success=True,
            msg="CARLA initialized",
        )

    def Reset(self, request, context):
        logger.debug("Received Reset request from client: %s", _peer(context))
        if not self._initialized:
            msg = "CARLA simulation not initialized. Call Init first."
            logger.error(msg)
            _set_grpc_error(context, grpc.StatusCode.FAILED_PRECONDITION, msg)
            return sim_server_pb2.SimServerMessages.ResetResponse()

        try:
            frame = self._simulation.reset(request)
        except RuntimeError as exc:
            msg = f"Failed to reset CARLA: {exc}"
            logger.error(msg)
            _set_grpc_error(context, grpc.StatusCode.FAILED_PRECONDITION, msg)
            return sim_server_pb2.SimServerMessages.ResetResponse()
        except Exception:
            msg = "Unexpected error while resetting CARLA"
            logger.exception(msg)
            _set_grpc_error(context, grpc.StatusCode.INTERNAL, msg)
            return sim_server_pb2.SimServerMessages.ResetResponse()

        self._reset = True
        return sim_server_pb2.SimServerMessages.ResetResponse(frame=frame)

    def Step(self, request, context):
        logger.debug("Received Step request with timestamp_ns=%s", request.timestamp_ns)
        if not self._initialized or not self._reset:
            msg = "CARLA simulation not ready. Call Init and Reset first."
            logger.error(msg)
            _set_grpc_error(context, grpc.StatusCode.FAILED_PRECONDITION, msg)
            return sim_server_pb2.SimServerMessages.StepResponse()

        try:
            frame = self._simulation.step(request)
        except RuntimeError as exc:
            msg = f"Failed to step CARLA: {exc}"
            logger.error(msg)
            _set_grpc_error(context, grpc.StatusCode.FAILED_PRECONDITION, msg)
            return sim_server_pb2.SimServerMessages.StepResponse()
        except Exception:
            msg = "Unexpected error while stepping CARLA"
            logger.exception(msg)
            _set_grpc_error(context, grpc.StatusCode.INTERNAL, msg)
            return sim_server_pb2.SimServerMessages.StepResponse()

        if frame is None:
            return sim_server_pb2.SimServerMessages.StepResponse()
        return sim_server_pb2.SimServerMessages.StepResponse(frame=frame)

    def Stop(self, request, context):
        logger.debug("Received Stop request from client: %s", _peer(context))
        if not self._initialized:
            msg = "CARLA simulation not initialized. Call Init first."
            logger.error(msg)
            _set_grpc_error(context, grpc.StatusCode.FAILED_PRECONDITION, msg)
            return Empty()

        try:
            self._simulation.stop()
        except Exception:
            msg = "Failed to stop CARLA"
            logger.exception(msg)
            _set_grpc_error(context, grpc.StatusCode.INTERNAL, msg)
            return Empty()

        self._initialized = False
        self._reset = False
        return Empty()

    def ShouldQuit(self, request, context):
        logger.debug("Received ShouldQuit request from client: %s", _peer(context))
        if not self._initialized:
            return sim_server_pb2.SimServerMessages.ShouldQuitResponse(should_quit=False)

        return sim_server_pb2.SimServerMessages.ShouldQuitResponse(
            should_quit=self._simulation.should_quit,
        )


if __name__ == "__main__":
    serve_sim(CarlaService(), name="CARLA")
