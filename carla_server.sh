#!/bin/bash

pushd /opt/carla
CMD=(
    "./CarlaUE4.sh"
    "-RenderOffScreen"
    "-nosound"
    "-nullrhi"
    "-carla-port=${CARLA_PORT:-2000}"
    "-carla-rpc-timeout=${CARLA_TIMEOUT:-30}"
    "-carla-tm-port=${CARLA_TM_PORT:-8000}"
)
echo "Running command: ${CMD[*]}"
"${CMD[@]}"
popd
