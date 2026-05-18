from pisa_api.simulator import serve_simulator
from pisa_api.wrapper import setup_logging

from .simulation import CarlaAdapter

SUPPORTED_SCENARIO_FORMATS = {"open_scenario1", "carla_lb_route"}

setup_logging()


if __name__ == "__main__":
    serve_simulator(
        CarlaAdapter(),
        name="Carla",
        scenario_formats=SUPPORTED_SCENARIO_FORMATS,
    )
