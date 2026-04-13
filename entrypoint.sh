#!/bin/bash

pushd /app
uv run carla_wrapper/server.py
popd
