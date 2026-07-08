#!/bin/bash

cd /opt/carla || exit 1
CMD=(
    "./CarlaUE4.sh"
    "-RenderOffScreen"
    "-nosound"
    "-carla-port=${CARLA_PORT:-2000}"
    "-carla-rpc-timeout=${CARLA_TIMEOUT:-30}"
    "-carla-tm-port=${CARLA_TM_PORT:-8000}"
)
echo "Running command: ${CMD[*]}"
exec "${CMD[@]}"
