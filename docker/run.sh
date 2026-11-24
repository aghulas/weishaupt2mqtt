#!/usr/bin/env bash
set -euo pipefail

OPTIONS_FILE="/data/options.json"
OPT_ARGS=()

if [ -f "${OPTIONS_FILE}" ]; then
    OPT_ARGS=(--options "${OPTIONS_FILE}")
    EBUSD_EXTERNAL="${EBUSD_EXTERNAL:-$(jq -r '.ebusd_external // false' "${OPTIONS_FILE}")}"
    EBUS_DEVICE="${EBUS_DEVICE:-$(jq -r '.ebus_device // ""' "${OPTIONS_FILE}")}"
    EBUSD_ADDRESS="${EBUSD_ADDRESS:-$(jq -r '.ebusd_address // "31"' "${OPTIONS_FILE}")}"
    LOG_LEVEL="${LOG_LEVEL:-$(jq -r '.log_level // "INFO"' "${OPTIONS_FILE}")}"
    export WEISHAUPT_STATE_FILE="${WEISHAUPT_STATE_FILE:-/data/state.json}"
else
    EBUSD_EXTERNAL="${EBUSD_EXTERNAL:-false}"
    EBUS_DEVICE="${EBUS_DEVICE:-}"
    EBUSD_ADDRESS="${EBUSD_ADDRESS:-31}"
    LOG_LEVEL="${LOG_LEVEL:-INFO}"
fi

EBUSD_PID=""
cleanup() {
    if [ -n "${EBUSD_PID}" ] && kill -0 "${EBUSD_PID}" 2>/dev/null; then
        kill "${EBUSD_PID}" 2>/dev/null || true
        wait "${EBUSD_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

if [ -n "${SUPERVISOR_TOKEN:-}" ]; then
    echo "Supervisor token detected, checking for MQTT service..."
    MQTT_SERVICE_JSON=$(curl -sSL -H "Authorization: Bearer ${SUPERVISOR_TOKEN}" http://supervisor/services/mqtt 2>/dev/null || true)
    if [ "$(echo "${MQTT_SERVICE_JSON}" | jq -r '.result // empty' 2>/dev/null)" = "ok" ]; then
        echo "Supervisor MQTT service available."
    fi
fi

if [ "${EBUSD_EXTERNAL}" != "true" ]; then
    if [ -z "${EBUS_DEVICE}" ]; then
        echo "ERROR: EBUS_DEVICE is required unless EBUSD_EXTERNAL=true" >&2
        exit 1
    fi
    # Start internal ebusd bound to loopback only, with native ebusd MQTT disabled.
    /usr/bin/ebusd \
        --foreground \
        --device="${EBUS_DEVICE}" \
        --address="${EBUSD_ADDRESS}" \
        --scanconfig \
        --configpath=/etc/weishaupt/config \
        --localhost \
        --port=8888 \
        --loglevel=notice &
    EBUSD_PID=$!

    echo "Waiting for internal ebusd on 127.0.0.1:8888..."
    for _ in $(seq 1 30); do
        if nc -z 127.0.0.1 8888 2>/dev/null; then
            break
        fi
        if ! kill -0 "${EBUSD_PID}" 2>/dev/null; then
            echo "ERROR: internal ebusd exited unexpectedly" >&2
            exit 1
        fi
        sleep 1
    done
    # Give ebusd time to complete initial broadcast scan of all slave devices
    sleep 25
fi

exec python3 -m weishaupt2mqtt --log-level "${LOG_LEVEL}" "${OPT_ARGS[@]}"
