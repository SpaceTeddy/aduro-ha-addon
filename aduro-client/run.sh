#!/usr/bin/env bash
set -euo pipefail

OPTS_FILE="/data/options.json"

# --- MQTT ---
export MQTT_SERVER_IP=$(jq -r '.mqtt_server_ip' "$OPTS_FILE")
export MQTT_SERVER_PORT=$(jq -r '.mqtt_server_port' "$OPTS_FILE")
export MQTT_USERNAME=$(jq -r '.mqtt_username' "$OPTS_FILE")
export MQTT_PASSWORD=$(jq -r '.mqtt_password' "$OPTS_FILE")
export MQTT_BASE_PATH=$(jq -r '.mqtt_base_path' "$OPTS_FILE")

# --- Stove ---
export STOVE_SERIAL=$(jq -r '.stove_serial' "$OPTS_FILE")
export STOVE_PIN=$(jq -r '.stove_pin' "$OPTS_FILE")

# --- Mode & Intervals ---
export MODE=$(jq -r '.mode' "$OPTS_FILE")
export STOVE_TEMP_ENTITY=$(jq -r '.stove_temp_entity' "$OPTS_FILE")
export HOT_THRESHOLD=$(jq -r '.hot_threshold' "$OPTS_FILE")
export INTERVAL_HOT_SECONDS=$(jq -r '.interval_hot_seconds' "$OPTS_FILE")
export INTERVAL_COLD_SECONDS=$(jq -r '.interval_cold_seconds' "$OPTS_FILE")
export REDISCOVER_EVERY_N_CYCLES=$(jq -r '.rediscover_every_n_cycles' "$OPTS_FILE")

# --- Address selection ---
export STOVE_HOST_OVERRIDE=$(jq -r '.stove_host_override' "$OPTS_FILE")
export CLOUD_FALLBACK_HOST=$(jq -r '.cloud_fallback_host' "$OPTS_FILE")

# --- Logging & Debug ---
export LOG_LEVEL=$(jq -r '.log_level' "$OPTS_FILE")
export PUBLISH_DEBUG=$(jq -r '.publish_debug' "$OPTS_FILE")

# --- TLS ---
export MQTT_TLS=$(jq -r '.mqtt_tls' "$OPTS_FILE")
export MQTT_TLS_INSECURE=$(jq -r '.mqtt_tls_insecure' "$OPTS_FILE")
export MQTT_CA_CERT=$(jq -r '.mqtt_ca_cert' "$OPTS_FILE")

# --- Command Bridge ---
export ENABLE_COMMANDS=$(jq -r '.enable_commands' "$OPTS_FILE")
export ALLOWED_FUNCTION_IDS=$(jq -r '.allowed_function_ids' "$OPTS_FILE")
export COMMAND_QOS=$(jq -r '.command_qos' "$OPTS_FILE")

echo "[RUN] starting Aduro client..."
exec python3 /app/main.py
