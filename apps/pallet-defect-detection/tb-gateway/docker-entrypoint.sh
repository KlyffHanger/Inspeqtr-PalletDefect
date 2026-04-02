#!/bin/sh
set -eu

TEMPLATES_DIR="${TEMPLATES_DIR:-/config-templates}"
CONFIG_DIR="${CONFIG_DIR:-/thingsboard_gateway/config}"

mkdir -p "$CONFIG_DIR"

# Seed the writable config volume with the config we version in git.
# The gateway writes runtime files next to it (e.g., connected_devices.json).
if [ -f "$TEMPLATES_DIR/tb_gateway.json" ]; then
  cp -f "$TEMPLATES_DIR/tb_gateway.json" "$CONFIG_DIR/tb_gateway.json"
fi
if [ -f "$TEMPLATES_DIR/mqtt.json" ]; then
  cp -f "$TEMPLATES_DIR/mqtt.json" "$CONFIG_DIR/mqtt.json"
fi

exec /start-gateway.sh

