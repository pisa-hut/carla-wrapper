#!/bin/bash

cd /app || exit 1
exec /app/.venv/bin/python -X faulthandler -m carla_wrapper.server
