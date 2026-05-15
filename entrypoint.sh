#!/bin/bash

pushd /app
uv run python -m carla_wrapper.server
popd
