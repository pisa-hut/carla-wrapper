"""Smoke tests for the pisa-api simulator-friendly contract."""


def test_public_imports_use_pisa_api_simulator_contract() -> None:
    from pisa_api.simulator import RuntimeFrameData as PisaRuntimeFrameData

    from carla_wrapper.simulation import CarlaAdapter

    assert CarlaAdapter.init.__annotations__["request"].__name__ == "InitRequest"
    assert CarlaAdapter.reset.__annotations__["request"].__name__ == "ResetRequest"
    assert CarlaAdapter.step.__annotations__["request"].__name__ == "StepRequest"
    assert PisaRuntimeFrameData.__name__ == "RuntimeFrameData"
    assert CarlaAdapter is not None
