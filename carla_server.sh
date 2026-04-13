#!/bin/bash

pushd /opt/carla
CMD="./CarlaUE4.sh -RenderOffScreen -carla-port=${CARLA_PORT:-2000}"
echo "Running command: $CMD"
eval $CMD
popd
