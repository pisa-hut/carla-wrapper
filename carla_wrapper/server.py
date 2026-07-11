from pisa_api.simulator import serve_simulator
from pisa_api.wrapper import setup_logging

from .simulation import CarlaAdapter
from .version import wrapper_version

SUPPORTED_SCENARIO_FORMATS = {"open_scenario1", "carla_lb_route"}

setup_logging()


def main() -> None:
    serve_simulator(
        CarlaAdapter(),
        name="carla-wrapper",
        version=wrapper_version(),
        scenario_formats=SUPPORTED_SCENARIO_FORMATS,
    )


if __name__ == "__main__":
    main()
