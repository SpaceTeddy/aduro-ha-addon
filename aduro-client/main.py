#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, socket, ssl, logging, traceback, threading
from typing import Optional, Tuple, Callable, Any
from queue import Queue, Empty
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

import requests
import paho.mqtt.client as mqtt

# ========================
# HA Card
# ========================
ADURO_DASHBOARD_YAML_TMPL = r"""\
type: vertical-stack
cards:
  - type: custom:apexcharts-card
    experimental:
      color_threshold: true
    apex_config:
      chart:
        zoom:
          enabled: true
          type: x
          autoScaleYaxis: true
          zoomedArea:
            fill:
              opacity: 0.4
            stroke:
              opacity: 0.4
              width: 1
      legend:
        show: true
        floating: true
        offsetY: 25
      annotations:
        yaxis:
          - y: 300
            borderColor: "#00FF00"
            borderWidth: 3
            label:
              text: 300
              style:
                color: "#FFFF00"
                background: "#000000"
          - y: 120
            borderColor: "#00FF00"
            borderWidth: 3
            label:
              text: 120
              style:
                color: "#FFFF00"
                background: "#000000"
    header:
      show: true
      title: Aduro H2
      show_states: true
      colorize_states: true
    graph_span: 24h
    span:
      start: day
    now:
      show: true
    yaxis:
      - id: first
        min: 0
        max: 350
        decimals: 0
        apex_config:
          tickAmount: 4
    series:
      - entity: sensor.aduro_h2_smoke_temperature
        yaxis_id: first
        unit: " °C"
        show:
          extremas: true
          header_color_threshold: true
        type: line
        color_threshold:
          - value: 0
            color: "#000000"
          - value: 90
            color: "#00ff00"
          - value: 160
            color: "#ffa500"
          - value: 300
            color: "#ff0000"
        stroke_width: 2
        curve: smooth
        extend_to: now
        color: red
  - type: vertical-stack
    cards:
      - type: heading
        heading: Control
        heading_style: title
      - type: grid
        columns: 2
        square: false
        cards:
          - type: entity
            entity: binary_sensor.aduro_h2_{{SERIAL}}_aduro_h2_running
            name: Running
            icon: mdi:fire
          - type: entity
            entity: sensor.aduro_h2_state_name
            name: State
            icon: mdi:state-machine
      - type: grid
        columns: 2
        square: false
        cards:
          - type: entity
            entity: select.aduro_h2_{{SERIAL}}_aduro_h2_operation_mode
            name: Mode
            icon: mdi:power-settings
          - type: entity
            entity: number.aduro_h2_{{SERIAL}}_aduro_h2_heatlevel
            name: Heatlevel
            icon: mdi:fire
      - type: grid
        columns: 1
        square: false
        cards:
          - type: entity
            entity: number.aduro_h2_{{SERIAL}}_aduro_h2_target_temperature
            name: Target Temperature in Room Temp mode
            icon: mdi:temperature-celsius
      - type: grid
        columns: 6
        square: false
        cards:
          - type: button
            name: Fetch data
            icon: mdi:download
            tap_action:
              action: call-service
              service: button.press
              target:
                entity_id: button.aduro_h2_{{SERIAL}}_aduro_h2_fetch_all
          - type: button
            entity: button.aduro_h2_{{SERIAL}}_aduro_h2_start
            name: Start
            icon: mdi:play
            tap_action:
              action: call-service
              service: button.press
              target:
                entity_id: button.aduro_h2_{{SERIAL}}_aduro_h2_start
          - type: button
            entity: button.aduro_h2_{{SERIAL}}_aduro_h2_stop
            name: Stop
            icon: mdi:stop
            tap_action:
              action: call-service
              service: button.press
              target:
                entity_id: button.aduro_h2_{{SERIAL}}_aduro_h2_stop
          - type: button
            entity: button.aduro_h2_{{SERIAL}}_aduro_h2_force_auger
            name: Auger
            icon: mdi:snake
            tap_action:
              action: call-service
              service: button.press
              target:
                entity_id: button.aduro_h2_{{SERIAL}}_aduro_h2_force_auger
          - type: button
            entity: button.aduro_h2_{{SERIAL}}_aduro_h2_reset_alarm
            name: Reset
            icon: mdi:alarm-off
            tap_action:
              action: call-service
              service: button.press
              target:
                entity_id: button.aduro_h2_{{SERIAL}}_aduro_h2_reset_alarm
  - type: vertical-stack
    cards:
      - type: heading
        heading: Sensors
        heading_style: title
      - type: entities
        title: Temperature & Power
        show_header_toggle: false
        entities:
          - sensor.aduro_h2_smoke_temperature
          - sensor.aduro_h2_shaft_temperature
          - sensor.aduro_h2_room_temperature
          - sensor.aduro_h2_room_target_temperature
          - sensor.aduro_h2_stove_heatlevel
          - sensor.aduro_h2_power
          - sensor.aduro_h2_oxygen
      - type: entities
        title: Status
        show_header_toggle: false
        entities:
          - sensor.aduro_h2_state_name
          - sensor.aduro_h2_substate_name
          - sensor.aduro_h2_substate_seconds
          - sensor.aduro_h2_alarm
      - type: entities
        title: Consumption
        show_header_toggle: false
        entities:
          - sensor.aduro_h2_consumption_day
          - sensor.aduro_h2_consumption_yesterday
          - sensor.aduro_h2_consumption_month
          - sensor.aduro_h2_consumption_year
      - type: entities
        title: Discovery / Meta
        show_header_toggle: false
        entities:
          - sensor.aduro_h2_stove_serial
          - sensor.aduro_h2_stove_ip
          - sensor.aduro_h2_{{SERIAL}}_aduro_h2_firmware
          - sensor.aduro_h2_{{SERIAL}}_aduro_h2_nbe_type
          - sensor.aduro_h2_{{SERIAL}}_aduro_h2_sw_build
          - sensor.aduro_h2_{{SERIAL}}_aduro_h2_sw_version
"""

# =========================
# HA SERIAL helper
# =========================
def _get_stove_serial_for_dashboard() -> str:
    # 1. STOVE_SERIAL env
    serial = (os.getenv("STOVE_SERIAL") or "").strip()
    if serial:
        return serial

    # 2. try Discovery
    try:
        _, _, serial_from_disc, _ = get_discovery_data()
        serial_from_disc = (serial_from_disc or "").strip()
        if serial_from_disc:
            return serial_from_disc
    except Exception:
        pass

    # 3. Fallback
    return "unknown"

def install_dashboard():
    cfg = os.getenv("CONFIG_PATH", "/config")
    out_file = os.path.join(cfg, "dashboards", "aduro_h2_dashboard.yaml")
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    serial = _get_stove_serial_for_dashboard()
    yaml_content = ADURO_DASHBOARD_YAML_TMPL.replace("{{SERIAL}}", serial)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write(yaml_content)

    logging.info(f"[ASSETS] wrote Lovelace dashboard to {out_file} (serial={serial})")

# =========================
# Logging & Debug-Publishes
# =========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
PUBLISH_DEBUG = os.getenv("PUBLISH_DEBUG", "false").lower() == "true"

def dbg_publish(client: mqtt.Client, base: str, topic: str, payload_obj: dict):
    if not PUBLISH_DEBUG:
        return
    try:
        client.publish(base + "debug/" + topic, json.dumps(payload_obj), qos=0, retain=False)
    except Exception:
        logging.debug("debug publish failed", exc_info=True)

# =====================
# Supervisor / HA State
# =====================
SUPERVISOR_API = "http://supervisor/core/api"
SUPERVISOR_TOKEN = os.getenv("SUPERVISOR_TOKEN")

def ha_get_state(entity_id: str, default="unknown"):
    if not SUPERVISOR_TOKEN:
        return default
    try:
        r = requests.get(
            f"{SUPERVISOR_API}/states/{entity_id}",
            headers={"Authorization": f"Bearer {SUPERVISOR_TOKEN}"},
            timeout=5,
        )
        if r.status_code == 200:
            return r.json().get("state", default)
        return default
    except Exception as e:
        logging.warning(f"[HA] get_state error for {entity_id}: {e}")
        return default

# =========
# MQTT-Client
# =========
COMMAND_Q: "Queue[tuple[str, bytes]]" = Queue()

def on_connect(client, userdata, flags, rc, properties=None):
    logging.info(f"[MQTT] Connected (rc={rc})")
    try:
        base = userdata.get("MQTT_BASE_PATH", "")
        client.publish(base + "availability", "online", qos=0, retain=True)
        if userdata.get("DISCOVERY_ENABLED", True):
            publish_mqtt_discovery(client, userdata)
    except Exception:
        logging.debug("on_connect post-actions failed", exc_info=True)

def on_disconnect(client, userdata, rc=0, properties=None):
    logging.info(f"[MQTT] Disconnected (rc={rc})")
    try:
        base = (userdata or {}).get("MQTT_BASE_PATH", "")
        client.publish(base + "availability", "offline", qos=0, retain=True)
        client.loop_stop()
    except Exception:
        pass

def on_message(client, userdata, msg):
    logging.debug(f"[MQTT] RX {msg.topic} {msg.payload!r}")
    base = (userdata or {}).get("MQTT_BASE_PATH", "")
    if not msg.topic.startswith(base + "cmd/"):
        return
    suffix = msg.topic[len(base):]
    if suffix not in ("cmd/set", "cmd/get", "cmd/raw"):
        return
    COMMAND_Q.put((msg.topic, msg.payload))

def mqtt_connect_with_retry(client: mqtt.Client, host: str, port: int, tls_cfg: dict, max_attempts: int = 8) -> bool:
    if tls_cfg.get("enabled", False):
        if tls_cfg.get("cafile"):
            client.tls_set(
                ca_certs=tls_cfg["cafile"],
                certfile=None,
                keyfile=None,
                cert_reqs=ssl.CERT_REQUIRED,
            )
        else:
            client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.tls_insecure_set(bool(tls_cfg.get("insecure", False)))

    backoff = 2
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"[MQTT] Connecting to {host}:{port} (attempt {attempt}/{max_attempts}) ...")
            client.connect(host, port, keepalive=60)
            client.loop_start()
            logging.info("[MQTT] Connected.")
            return True
        except (ConnectionRefusedError, socket.gaierror, TimeoutError, OSError) as e:
            logging.warning(f"[MQTT] {type(e).__name__}: {e} — retry in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    logging.error("[MQTT] Failed to connect after retries.")
    return False

def publish(client, topic, payload):
    client.publish(topic, payload, qos=0, retain=False)
    time.sleep(0.01)

# =================
# pyduro-Utilities
# =================
def _safe_float(x) -> Optional[float]:
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def _resolve_to_ip(host_or_ip: str) -> Optional[str]:
    try:
        return socket.gethostbyname(host_or_ip.strip())
    except Exception as e:
        logging.warning(f"[RESOLVE] cannot resolve '{host_or_ip}': {e}")
        return None

# --- pyduro: Lock + Retry gegen EADDRINUSE ---
PYDURO_LOCK = threading.Lock()

def _errno(ex):
    try:
        return getattr(ex, "errno", None) or (ex.__cause__ and getattr(ex.__cause__, "errno", None))
    except Exception:
        return None

def _pyduro_call(fn: Callable[[], Any], *, retries: int = 5, base_sleep: float = 0.15):
    """Serialisiert pyduro-Calls und retryt EADDRINUSE mit Backoff."""
    for attempt in range(1, retries + 1):
        with PYDURO_LOCK:
            try:
                return fn()
            except OSError as e:
                if (_errno(e) == 98) or ("Address already in use" in str(e)):
                    sleep = min(base_sleep * attempt, 1.0)
                    logging.warning(f"[pyduro] EADDRINUSE, retry {attempt}/{retries} in {sleep:.2f}s")
                    time.sleep(sleep)
                    continue
                raise
    raise OSError(98, "Address already in use (after retries)")

# -------- pyduro Wrappers ----------
def get_discovery_data(aduro_cloud_backup_address="apprelay20.stokercloud.dk"):
    from pyduro.actions import discover
    import json as _json
    try:
        response = _pyduro_call(lambda: discover.run())
        if response is None:
            raise RuntimeError("discover.run() returned None")
        payload = response.parse_payload()
        data = _json.loads(_json.dumps(payload))
        serial = data.get("Serial", " ")
        ip = data.get("IP", "no connection")
        if "0.0.0.0" in ip:
            ip = aduro_cloud_backup_address
        discovery_json = {
            "DISCOVERY": {
                "StoveSerial": serial,
                "StoveIP": ip,
                "NBE_Type": data.get("Type", " "),
                "StoveSWVersion": data.get("Ver", " "),
                "StoveSWBuild": data.get("Build", " "),
                "StoveLanguage": data.get("Lang", " "),
            }
        }
        return 0, ip, serial, _json.dumps(discovery_json)
    except Exception as e:
        logging.warning(f"[pyduro] discovery error: {e}")
        discovery_json = {
            "DISCOVERY": {
                "StoveSerial": " ",
                "StoveIP": "no connection",
                "NBE_Type": " ",
                "StoveSWVersion": " ",
                "StoveSWBuild": " ",
                "StoveLanguage": " ",
            }
        }
        return -1, "no connection", " ", _json.dumps(discovery_json)

def get_status(ip, serial, pin) -> Tuple[int, str]:
    from pyduro.actions import STATUS_PARAMS, raw
    import json as _json
    try:
        resp = _pyduro_call(lambda: raw.run(burner_address=str(ip), serial=str(serial), pin_code=str(pin), function_id=11, payload="*"))
        status_list = resp.parse_payload().split(",")
        i = 0
        for key in list(STATUS_PARAMS.keys()):
            if i < len(status_list):
                STATUS_PARAMS[key] = status_list[i]
                i += 1
            else:
                break
        return 0, _json.dumps({"STATUS": STATUS_PARAMS})
    except Exception as e:
        logging.error(f"[pyduro] status error: {e}")
        return -1, ""

def get_network_data(ip, serial, pin) -> Tuple[int, str]:
    from pyduro.actions import raw
    import json as _json
    try:
        resp = _pyduro_call(lambda: raw.run(burner_address=str(ip), serial=str(serial), pin_code=str(pin), function_id=1, payload="wifi.router"))
        data = resp.payload.split(",")
        def safe(i, d=""):
            return data[i] if 0 <= i < len(data) else d
        stove_ssid = safe(0)[7:] if len(safe(0)) >= 7 else ""
        payload = {
            "NETWORK": {
                "RouterSSID": stove_ssid,
                "StoveIP": safe(4),
                "RouterIP": safe(5),
                "StoveRSSI": safe(6),
                "StoveMAC": safe(9),
            }
        }
        return 0, _json.dumps(payload)
    except Exception as e:
        logging.error(f"[pyduro] network error: {e}")
        return -1, ""

def get_operating_data(ip, serial, pin) -> Tuple[int, str]:
    from pyduro.actions import raw
    import json as _json
    try:
        # 001* = Operating-Block (Function 11 liest verschiedene Blöcke)
        resp = _pyduro_call(lambda: raw.run(
            burner_address=str(ip),
            serial=str(serial),
            pin_code=str(pin),
            function_id=11,
            payload="001*"
        ))
        data = resp.payload.split(",")

        def safe(i, d=""):
            return data[i] if 0 <= i < len(data) else d

        power_kw   = safe(31)
        power_pct  = safe(36)
        shaft_temp = safe(35)
        smoke_temp = safe(37)
        rawdt      = safe(94)

        date_stove = time_stove = ""
        if len(rawdt) >= 10:
            date_stove = rawdt[0:5] + "/" + str(20) + rawdt[6:8]
            time_stove = rawdt[9:]

        payload = {
            "OPERATING_DATA": {
                "Power_kw": power_kw,
                "Power_pct": power_pct,
                "SmokeTemp": smoke_temp,
                "ShaftTemp": shaft_temp,
                "TimeStove": time_stove,
                "DateStove": date_stove,
                "State": safe(6),
                "OperatingTimeAuger": safe(119),
                "OperatingTimeStove": safe(121),
                "OperatingTimeIgnition": safe(120),
            }
        }
        return 0, _json.dumps(payload)
    except Exception as e:
        logging.error(f"[pyduro] operating error: {e}")
        return -1, ""

def get_consumption_data(ip, serial, pin) -> Tuple[int, str]:
    from pyduro.actions import raw
    import json as _json
    from datetime import date, timedelta
    try:
        resp = _pyduro_call(lambda: raw.run(burner_address=str(ip), serial=str(serial), pin_code=str(pin), function_id=6, payload="total_days"))
        days = resp.payload.split(",")
        days[0] = days[0][11:]
        today = date.today().day
        yesterday = (date.today() - timedelta(1)).day
        if not (1 <= today <= len(days)) or not (1 <= yesterday <= len(days)):
            return -1, ""
        cons_today = days[today - 1]
        cons_yday = days[yesterday - 1]

        resp = _pyduro_call(lambda: raw.run(burner_address=str(ip), serial=str(serial), pin_code=str(pin), function_id=6, payload="total_months"))
        months = resp.payload.split(",")
        months[0] = months[0][13:]
        month_idx = date.today().month
        if not (1 <= month_idx <= len(months)):
            return -1, ""
        cons_month = months[month_idx - 1]

        resp = _pyduro_call(lambda: raw.run(burner_address=str(ip), serial=str(serial), pin_code=str(pin), function_id=6, payload="total_years"))
        years = resp.payload.split(",")
        years[0] = years[0][12:]
        year = date.today().year
        offset = year - (year - (len(years) - 1))
        if not (0 <= offset < len(years)):
            return -1, ""

        payload_obj = {
            "CONSUMPTION_DATA": {
                "Day": cons_today,
                "Yesterday": cons_yday,
                "Month": cons_month,
                "Year": years[offset],
            }
        }
        return 0, _json.dumps(payload_obj)
    except Exception as e:
        logging.error(f"[pyduro] consumption error: {e}")
        return -1, ""

def should_query_consumption(status_json_str: str, operating_json_str: str) -> bool:
    return True

# ==========================
# MQTT Auto-Discovery Support
# ==========================
def _topic(path: str, base: str) -> str:
    return base + path.strip("/")

def _uid_safe(s: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)

def _default_serial(userdata: dict) -> str:
    hn = socket.gethostname()
    base = userdata.get("MQTT_BASE_PATH", "aduro_h2/").strip("/").replace("/", "_")
    return f"{hn}_{base}"

def publish_mqtt_discovery(client: mqtt.Client, userdata: dict):
    """Retained Discovery-Configs (mit node_id) – nur status/discovery/consumption_data."""
    discovery_prefix = userdata.get("DISCOVERY_PREFIX", "homeassistant").strip("/")
    base = userdata.get("MQTT_BASE_PATH", "aduro_h2/")

    # --- Serial/IDs stabil bestimmen ---
    serial = (os.getenv("STOVE_SERIAL") or "").strip()
    sw_ver = "unknown"
    model = "Aduro/NBE"
    manufacturer = "Aduro (via pyduro)"
    if not serial:
        try:
            _, _, serial_from_disc, disc_payload = get_discovery_data()
            serial = (serial_from_disc or "").strip()
            if disc_payload:
                d = json.loads(disc_payload)
                sw_ver = f"{d['DISCOVERY'].get('StoveSWVersion','')} ({d['DISCOVERY'].get('StoveSWBuild','')})".strip() or sw_ver
                model = d["DISCOVERY"].get("NBE_Type", model) or model
        except Exception:
            pass
    if not serial:
        serial = _default_serial(userdata)

    device_id = f"aduro_h2_{serial}"
    node_id = device_id
    device_name = os.getenv("DEVICE_NAME") or f"Aduro H2 ({serial})"

    device = {
        "identifiers": [device_id],
        "manufacturer": manufacturer,
        "model": model,
        "name": device_name,
        "sw_version": sw_ver,
    }

    availability = [{
        "topic": _topic("availability", base),
        "payload_available": "online",
        "payload_not_available": "offline",
    }]

    def pub_cfg(component: str, node_id: str, object_id: str, cfg: dict):
        config_topic = f"{discovery_prefix}/{component}/{_uid_safe(node_id)}/{_uid_safe(object_id)}/config"
        payload = json.dumps(cfg, separators=(",", ":"), ensure_ascii=False)
        client.publish(config_topic, payload, qos=0, retain=True)
        logging.info(f"[DISCOVERY] {config_topic} <- {payload}")

    # --------- Sensor-Configs (DEINE 23 YAML-Sensoren 1:1) ----------
    sensors = [
        # STATUS-basierte Sensoren
        {
            "component": "sensor",
            "object_id": "smoketemperature",
            "unique_id": "sensor.aduro_smoketemperature",
            "name": "Smoke Temperature",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['smoke_temp']) | float | round(1)}}",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
        },
        {
            "component": "sensor",
            "object_id": "shafttemperature",
            "unique_id": "sensor.aduro_shafttemperature",
            "name": "Shaft Temperature",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['shaft_temp']) | float | round(1)}}",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
        },
        {
            "component": "sensor",
            "object_id": "boiler_temp",
            "unique_id": "sensor.aduro_boiler_temp",
            "name": "Room Temperature",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['boiler_temp']) | float | round(1)}}",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
        },
        {
            "component": "sensor",
            "object_id": "boiler_ref",
            "unique_id": "sensor.aduro_boiler_ref",
            "name": "Room Target Temperature",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['boiler_ref']) | float | round(1)}}",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
        },
        {
            "component": "sensor",
            "object_id": "dhw_temp",
            "unique_id": "sensor.aduro_dhw_temp",
            "name": "DHW Temperature",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['dhw_temp']) | float | round(1)}}",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
        },
        {
            "component": "sensor",
            "object_id": "power_kw",
            "unique_id": "sensor.aduro_power_kw",
            "name": "Power kW",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['power_kw']|float) | round(1)}}",
            "unit_of_measurement": "kW",
            "state_class": "measurement",
        },
        {
            "component": "sensor",
            "object_id": "power_w",
            "unique_id": "sensor.aduro_power_w",
            "name": "Power W",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['power_kw']|float * 1000) | round(1)}}",
            "unit_of_measurement": "W",
            "state_class": "measurement",
        },
        {
            "component": "sensor",
            "object_id": "operation_mode",
            "unique_id": "sensor.aduro_operation_mode",
            "name": "Operation Mode",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['operation_mode']|float) | round(0)}}",
        },
        {
            "component": "sensor",
            "object_id": "operation_mode_name",
            "unique_id": "sensor.aduro_operation_mode_name",
            "name": "Operation Mode Name",
            "state_topic": _topic("status", base),
            "value_template":
                "{%   if (value_json['STATUS']['operation_mode']|float) | round(0) == 0 %}heatlevel"
                "{% elif (value_json['STATUS']['operation_mode']|float) | round(0) == 1 %}room teperature"
                "{% elif (value_json['STATUS']['operation_mode']|float) | round(0) == 2 %}timer"
                "{% else %}{{(value_json['STATUS']['operation_mode']|float) | round(0)}}{% endif %}",
        },
        {
            "component": "sensor",
            "object_id": "state",
            "unique_id": "sensor.aduro_state",
            "name": "State",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['state']|float) | round(0)}}",
        },
        {
            "component": "sensor",
            "object_id": "state_name",
            "unique_id": "sensor.aduro_state_name",
            "name": "State Name",
            "state_topic": _topic("status", base),
            "value_template":
                "{%   if (value_json['STATUS']['state']|float) | round(0) == 0 %}wait"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 2 %}ignition"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 5 %}normal power"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 6 %}room temperature reached"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 9 %}wood burning"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 11 %}dropshaft hot"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 13 %}ignition failed"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 14 %}stopped by Button"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 15 %}bad smoke sensor"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 17 %}bad dropshaft sensor"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 18 %}check burner yellow"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 19 %}bad external auger output"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 20 %}no fire"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 23 %}stopped by timer"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 24 %}air damper closed"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 28 %}door open"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 32 %}heating up"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 33 %}co sensor defect"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 34 %}check burner red"
                "{% elif (value_json['STATUS']['state']|float) | round(0) == 35 %}no power consumption for fan"
                "{% else %}{{(value_json['STATUS']['state']|float) | round(0)}}{% endif %}",
        },
        {
            "component": "sensor",
            "object_id": "substate",
            "unique_id": "sensor.aduro_substate",
            "name": "Substate",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['substate']|float) | round(0)}}",
        },
        {
            "component": "sensor",
            "object_id": "substate_name",
            "unique_id": "sensor.aduro_substate_name",
            "name": "Substate Name",
            "state_topic": _topic("status", base),
            "value_template":
                "{%   if (value_json['STATUS']['substate']|float) | round(0) == 0 %}normal"
                "{% elif (value_json['STATUS']['substate']|float) | round(0) == 1 %}wait"
                "{% elif (value_json['STATUS']['substate']|float) | round(0) == 2 %}start-up feeding"
                "{% elif (value_json['STATUS']['substate']|float) | round(0) == 4 %}piezo lighter on"
                "{% elif (value_json['STATUS']['substate']|float) | round(0) == 6 %}cleaning"
                "{% else %}{{(value_json['STATUS']['substate']|float) | round(0)}}{% endif %}",
        },
        {
            "component": "sensor",
            "object_id": "substate_sec",
            "unique_id": "sensor.aduro_substate_sec",
            "name": "Substate remaining time",
            "state_topic": _topic("status", base),
            "unit_of_measurement": "sec",
            "value_template":
                "{%   if (value_json['STATUS']['substate_sec']|float) | round(0) == 1 %}0"
                "{% else %}{{(value_json['STATUS']['substate_sec']|float) | round(0)}}{% endif %}",
        },
        {
            "component": "sensor",
            "object_id": "off_on_alarm",
            "unique_id": "sensor.aduro_off_on_alarm",
            "name": "last Alarm",
            "state_topic": _topic("status", base),
            "value_template":
                "{%   if (value_json['STATUS']['off_on_alarm']|float) | round(0) == 0 %}OK"
                "{% elif (value_json['STATUS']['off_on_alarm']|float) | round(0) == 1 %}stopped by user"
                "{% elif (value_json['STATUS']['off_on_alarm']|float) | round(0) == 2 %}stopped unexpected"
                "{% else %}{{(value_json['STATUS']['off_on_alarm']|float) | round(0)}}{% endif %}",
        },
        {
            "component": "sensor",
            "object_id": "oxygen",
            "unique_id": "sensor.aduro_oxygen",
            "name": "Oxygen",
            "state_topic": _topic("status", base),
            "value_template": "{{(value_json['STATUS']['oxygen']|float) | round(1)}}",
            "unit_of_measurement": "ppm",
        },

        # CONSUMPTION-basierte Sensoren
        {
            "component": "sensor",
            "object_id": "consumption_day",
            "unique_id": "sensor.aduro_consumption_day",
            "name": "Consumption Day",
            "state_topic": _topic("consumption_data", base),
            "value_template": "{{ (((value_json.CONSUMPTION_DATA.Day | float(0)) * 1000) | round(0) / 1000) | float }}",
            "unit_of_measurement": "kg",
            "state_class": "measurement",
        },
        {
            "component": "sensor",
            "object_id": "consumption_yesterday",
            "unique_id": "sensor.aduro_consumption_yesterday",
            "name": "Consumption Yesterday",
            "state_topic": _topic("consumption_data", base),
            "value_template": "{{ (((value_json.CONSUMPTION_DATA.Yesterday | float(0)) * 1000) | round(0) / 1000) | float }}",
            "unit_of_measurement": "kg",
            "state_class": "measurement",
        },
        {
            "component": "sensor",
            "object_id": "consumption_month",
            "unique_id": "sensor.aduro_consumption_month",
            "name": "Consumption Month",
            "state_topic": _topic("consumption_data", base),
            "value_template": "{{ (((value_json.CONSUMPTION_DATA.Month | float(0)) * 1000) | round(0) / 1000) | float }}",
            "unit_of_measurement": "kg",
            "state_class": "measurement",
        },
        {
            "component": "sensor",
            "object_id": "consumption_year",
            "unique_id": "sensor.aduro_consumption_year",
            "name": "Consumption Year",
            "state_topic": _topic("consumption_data", base),
            "value_template": "{{ (((value_json.CONSUMPTION_DATA.Year | float(0)) * 1000) | round(0) / 1000) | float }}",
            "unit_of_measurement": "kg",
            "state_class": "measurement",
        },

        # DISCOVERY-basierte Sensoren
        {
            "component": "sensor",
            "object_id": "stove_serial",
            "unique_id": "sensor.aduro_stove_serial",
            "name": "Stove Serial",
            "state_topic": _topic("discovery", base),
            "value_template": "{{(value_json.DISCOVERY.StoveSerial)}}",
        },
        {
            "component": "sensor",
            "object_id": "stove_ip",
            "unique_id": "sensor.aduro_stove_ip",
            "name": "Stove IP",
            "state_topic": _topic("discovery", base),
            "value_template": "{{(value_json.DISCOVERY.StoveIP)}}",
        },
        {
            "component": "sensor",
            "object_id": "stove_heatlevel",
            "unique_id": "sensor.aduro_stove_heatlevel",
            "name": "Stove Heatlevel",
            "state_topic": _topic("status", base),
            "value_template":
                "{%   if (value_json['STATUS']['regulation.fixed_power']|float) |float | round(0) == 10 %}low"
                "{% elif (value_json['STATUS']['regulation.fixed_power']|float) |float | round(0) == 50 %}middle"
                "{% elif (value_json['STATUS']['regulation.fixed_power']|float) |float | round(0) == 100 %}high"
                "{% else %}{{(value_json['STATUS']['regulation.fixed_power']|float) |float | round(0)}}{% endif %}",
        },
    ]

    # --------- DISCOVERY Diagnostics (SW Version, Build, Type, Firmware) ----------
    diag_sensors = [
        {
            "component": "sensor",
            "object_id": "sw_version",
            "unique_id": "sensor.aduro_sw_version",
            "name": "SW Version",
            "state_topic": _topic("discovery", base),
            "value_template": "{{ value_json.DISCOVERY.StoveSWVersion }}",
            "icon": "mdi:numeric",
            "entity_category": "diagnostic",
        },
        {
            "component": "sensor",
            "object_id": "sw_build",
            "unique_id": "sensor.aduro_sw_build",
            "name": "SW Build",
            "state_topic": _topic("discovery", base),
            "value_template": "{{ value_json.DISCOVERY.StoveSWBuild }}",
            "icon": "mdi:tools",
            "entity_category": "diagnostic",
        },
        {
            "component": "sensor",
            "object_id": "nbe_type",
            "unique_id": "sensor.aduro_nbe_type",
            "name": "NBE Type",
            "state_topic": _topic("discovery", base),
            "value_template": "{{ value_json.DISCOVERY.NBE_Type }}",
            "icon": "mdi:cog",
            "entity_category": "diagnostic",
        },
        {
            "component": "sensor",
            "object_id": "firmware",
            "unique_id": "sensor.aduro_firmware",
            "name": "Firmware",
            "state_topic": _topic("discovery", base),
            "value_template": "{{ value_json.DISCOVERY.StoveSWVersion ~ ' (' ~ value_json.DISCOVERY.StoveSWBuild ~ ')' }}",
            "icon": "mdi:chip",
            "entity_category": "diagnostic",
            "json_attributes_topic": _topic("discovery", base),
            "json_attributes_template": "{{ value_json.DISCOVERY | tojson }}",
        },
    ]

    for s in diag_sensors:
        cfg = {
            "name": s["name"],
            "unique_id": s["unique_id"],
            "state_topic": s["state_topic"],
            "value_template": s["value_template"],
            "availability": availability,
            "device": device,
        }
        for k in ("icon","entity_category","json_attributes_topic","json_attributes_template"):
            if k in s: cfg[k] = s[k]
        pub_cfg(s["component"], node_id, s["object_id"], cfg)

    # --------- Number (Heatlevel 1–3 via Slider) ----------
    heatlevel_number_cfg = {
        "name": "Aduro H2 Heatlevel",
        "unique_id": "number.heatlevel_slider",
        "availability": availability,
        "device": device,

        # Schieberegler
        "min": 1, "max": 3, "step": 1, "mode": "slider",

        # State aus STATUS -> regulation.fixed_power (10/50/100) -> 1/2/3
        "state_topic": _topic("status", base),
        "value_template": (
            "{% set p = (value_json['STATUS']['regulation.fixed_power']|float)|round(0) %}"
            "{% if p == 10 %}1{% elif p == 50 %}2{% elif p == 100 %}3{% else %}{{ 1 if p < 30 else (2 if p < 75 else 3) }}{% endif %}"
        ),

        # Befehle gehen an cmd/set als JSON, das dein Handler versteht
        "command_topic": _topic("cmd/set", base),
        "command_template": (
            "{% set lvl = value|int %}"
            "{% set pct = 10 if lvl == 1 else (50 if lvl == 2 else 100) %}"
            "{\"type\":\"set\",\"path\":\"regulation.fixed_power\",\"value\":\"{{ pct }}\"}"
        ),

        # Optional: hübsches Icon
        "icon": "mdi:fire"
    }
    pub_cfg("number", node_id, "heatlevel", heatlevel_number_cfg)

    # ---- Number: Setpoint Temperature (Solltemperatur) ----
    BOILER_TEMP_PATH = os.getenv("BOILER_TEMP_PATH", "boiler.temp")

    set_temp_number_cfg = {
        "name": "Aduro H2 Target Temperature",
        "unique_id": "number.target_temperature",
        "availability": availability,
        "device": device,
        "icon": "mdi:thermostat",
        "unit_of_measurement": "°C",
        "device_class": "temperature",

        # Slider-Eigenschaften
        "min": 10,
        "max": 30,
        "step": 0.5,
        "mode": "slider",

        # Status wird aus STATUS.boiler_ref gelesen
        "state_topic": _topic("status", base),
        "value_template": "{{ (value_json['STATUS']['boiler_ref']|float) | round(1) }}",

        # Beim Verstellen sendet HA JSON an dein cmd/set
        "command_topic": _topic("cmd/set", base),
        "command_template": (
            "{\"type\":\"set\",\"path\":\"" + BOILER_TEMP_PATH + "\","
            "\"value\":\"{{ value | round(1) }}\"}"
        )
    }
    pub_cfg("number", node_id, "target_temperature", set_temp_number_cfg)

    # Publish alle Sensoren als Discovery
    for s in sensors:
        cfg = {
            "name": s["name"],
            "unique_id": s["unique_id"],        # exakt wie in deiner YAML
            "state_topic": s["state_topic"],
            "value_template": s["value_template"],
            "availability": availability,
            "device": device,
        }
        for k in ("unit_of_measurement", "device_class", "state_class", "icon"):
            if k in s:
                cfg[k] = s[k]
        pub_cfg(s["component"], node_id, s["object_id"], cfg)

    # ---- Button: fetch all data ----
    fetch_all_btn = {
        "name": "Aduro H2 Fetch ALL",
        "unique_id": "button.fetch_all",
        "availability": availability,
        "device": device,
        "command_topic": _topic("cmd/set", base),
        "payload_press": "{\"type\":\"set\",\"path\":\"__refresh__\",\"value\":\"all\"}",
        "icon": "mdi:database-sync"
    }
    pub_cfg("button", node_id, "fetch_all", fetch_all_btn)

    # ---- Button: Reset Alarm ----
    RESET_ALARM_PATH  = os.getenv("RESET_ALARM_PATH", "misc.reset_alarm")
    RESET_ALARM_VALUE = os.getenv("RESET_ALARM_VALUE", "1")

    reset_alarm_btn = {
        "name": "Aduro H2 Reset Alarm",
        "unique_id": "button.reset_alarm",
        "availability": availability,
        "device": device,
        "command_topic": _topic("cmd/set", base),
        "payload_press": "{\"type\":\"set\",\"path\":\"" + RESET_ALARM_PATH + "\",\"value\":\"" + RESET_ALARM_VALUE + "\"}",
        "icon": "mdi:alarm-off"
    }
    pub_cfg("button", node_id, "reset_alarm", reset_alarm_btn)
    
    # ---- Button: Start ----
    STOVE_SWITCH_PATH_ON  = os.getenv("STOVE_SWITCH_PATH_ON", "misc.start")
    STOVE_SWITCH_PATH_OFF = os.getenv("STOVE_SWITCH_PATH_OFF", "misc.stop")
    STOVE_VALUE           = os.getenv("STOVE_ON_VALUE", "1")

    stove_start_btn = {
        "name": "Aduro H2 Start",
        "unique_id": "button.stove_start",
        "availability": availability,
        "device": device,
        "command_topic": _topic("cmd/set", base),
        # Button sendet beim Drücken genau diesen Payload:
        "payload_press": "{\"type\":\"set\",\"path\":\"" + STOVE_SWITCH_PATH_ON + "\",\"value\":\"" + STOVE_VALUE + "\"}",
        "icon": "mdi:play"
    }
    pub_cfg("button", node_id, "stove_start", stove_start_btn)

    # ---- Button: Stop ----
    stove_stop_btn = {
        "name": "Aduro H2 Stop",
        "unique_id": "button.stove_stop",
        "availability": availability,
        "device": device,
        "command_topic": _topic("cmd/set", base),
        "payload_press": "{\"type\":\"set\",\"path\":\"" + STOVE_SWITCH_PATH_OFF + "\",\"value\":\"" + STOVE_VALUE + "\"}",
        "icon": "mdi:stop"
    }
    pub_cfg("button", node_id, "stove_stop", stove_stop_btn)

    # ---- Binary Sensor: Running ----
    running_bin = {
        "name": "Aduro H2 Running",
        "unique_id": "binary_sensor.running",
        "availability": availability,
        "device": device,
        "state_topic": _topic("status", base),
        # liefert ON/OFF (Default-Payloads für binary_sensor)
        "value_template": (
            "{% set s = (value_json['STATUS']['state']|int) %}"
            "{% if s in [5,6,9,11,32] %}ON{% else %}OFF{% endif %}"
        ),
        "device_class": "power",
        "icon": "mdi:fire"
    }
    pub_cfg("binary_sensor", node_id, "running", running_bin)


    # ---- Button: Force Auger ----
    AUGER_FORCE_PATH   = os.getenv("AUGER_FORCE_PATH", "auger.forced_run")
    AUGER_FORCE_VALUE  = os.getenv("AUGER_FORCE_VALUE", "1")

    force_auger_btn = {
        "name": "Aduro H2 Force Auger",
        "unique_id": "button.force_auger",
        "availability": availability,
        "device": device,
        "command_topic": _topic("cmd/set", base),
        "payload_press": "{\"type\":\"set\",\"path\":\"" + AUGER_FORCE_PATH + "\",\"value\":\"" + AUGER_FORCE_VALUE + "\"}",
        "icon": "mdi:progress-wrench"
    }
    pub_cfg("button", node_id, "force_auger", force_auger_btn)

    # ---- Button: Clean Stove ----
    CLEAN_STOVE_PATH   = os.getenv("CLEAN_STOVE_PATH", "maintenance.clean_stove")
    CLEAN_STOVE_VALUE  = os.getenv("CLEAN_STOVE_VALUE", "1")

    clean_stove_btn = {
        "name": "Aduro H2 Clean Stove",
        "unique_id": "button.clean_stove",
        "availability": availability,
        "device": device,
        "command_topic": _topic("cmd/set", base),
        "payload_press": "{\"type\":\"set\",\"path\":\"" + CLEAN_STOVE_PATH + "\",\"value\":\"" + CLEAN_STOVE_VALUE + "\"}",
        "icon": "mdi:broom"
    }
    pub_cfg("button", node_id, "clean_stove", clean_stove_btn)

    # ---- Select: Operation Mode (heatlevel / room temperature / timer) ----
    # Mapping: 0=heatlevel, 1=room temperature, 2=timer
    MODE_SWITCH_PATH   = os.getenv("MODE_SWITCH_PATH", "regulation.operation_mode")
    
    mode_select_cfg = {
        "name": "Aduro H2 Operation Mode",
        "unique_id": "select.operation_mode",
        "availability": availability,
        "device": device,
        "options": ["heatlevel", "room temperature", "timer"],
        "state_topic": _topic("status", base),
        "value_template": (
            "{% set m = (value_json['STATUS']['operation_mode']|int) %}"
            "{% if m == 0 %}heatlevel{% elif m == 1 %}room temperature{% elif m == 2 %}timer{% else %}unknown{% endif %}"
        ),
        "command_topic": _topic("cmd/set", base),
        "command_template": (
            "{% set v = 0 if value == 'heatlevel' else (1 if value == 'room temperature' else 2) %}"
            "{\"type\":\"set\",\"path\":\"" + MODE_SWITCH_PATH + "\",\"value\":\"{{ v }}\"}"
        ),
        "icon": "mdi:tune"
    }
    pub_cfg("select", node_id, "operation_mode", mode_select_cfg)

# ================
# Adressauswahl & Zyklus
# ================
def _select_ip(client, base_topic, stove_override, cloud_fallback) -> Optional[Tuple[str, str]]:
    if stove_override:
        ip = _resolve_to_ip(stove_override)
        if ip:
            logging.info(f"[ADDR] using stove_host_override -> {stove_override} (resolved {ip})")
            dbg_publish(client, base_topic, "addr/selected", {"ip": ip, "source": "override"})
            return ip, "override"
    sensor_ip = ha_get_state("sensor.aduro_h2_stove_ip", default="no connection")
    if sensor_ip not in ("unknown", "no connection", "", "0.0.0.0"):
        ip = _resolve_to_ip(sensor_ip)
        if ip:
            logging.info(f"[ADDR] using HA sensor -> {sensor_ip} (resolved {ip})")
            dbg_publish(client, base_topic, "addr/selected", {"ip": ip, "source": "ha"})
            return ip, "ha"
    res, disc_ip, serial_found, disc_payload = get_discovery_data()
    if disc_ip not in ("no connection", "", "0.0.0.0"):
        ip = _resolve_to_ip(disc_ip) or _resolve_to_ip(cloud_fallback)
        if ip:
            logging.info(f"[ADDR] using discovery -> {disc_ip} (resolved {ip})")
            try:
                d = json.loads(disc_payload)
                d["DISCOVERY"]["StoveIP"] = ip
                d["DISCOVERY"]["StoveSerial"] = serial_found
                publish(client, base_topic + "discovery", json.dumps(d))
            except Exception:
                publish(client, base_topic + "discovery", str(disc_payload))
            dbg_publish(client, base_topic, "addr/selected", {"ip": ip, "source": "discovery"})
            return ip, "discovery"
    ip = _resolve_to_ip(cloud_fallback)
    if ip:
        logging.info(f"[ADDR] using cloud fallback -> {cloud_fallback} (resolved {ip})")
        publish(client, base_topic + "discovery", json.dumps({
            "DISCOVERY": {"StoveSerial": " ", "StoveIP": ip, "NBE_Type": " ", "StoveSWVersion": " ", "StoveSWBuild": " ", "StoveLanguage": " "}
        }))
        dbg_publish(client, base_topic, "addr/selected", {"ip": ip, "source": "cloud"})
        return ip, "cloud"
    return None

def run_cycle(client, base_topic, mode, serial, pin, cached_ip=None, stove_override=None, cloud_fallback="apprelay20.stokercloud.dk") -> Optional[str]:
    ip = cached_ip
    if not ip:
        selected = _select_ip(client, base_topic, stove_override, cloud_fallback)
        if selected:
            ip, _ = selected
        else:
            logging.warning("[ADDR] no usable address found")
            dbg_publish(client, base_topic, "errors/addr", {"msg": "no usable address"})
            return cached_ip

    # STATUS
    if mode in ("status", "all"):
        t0 = time.perf_counter()
        r, p = get_status(ip, serial, pin)
        dt = int((time.perf_counter() - t0) * 1000)
        if r != -1 and p:
            publish(client, base_topic + "status", p)
            logging.info(f"[STATUS] ok in {dt} ms via {ip}")
            dbg_publish(client, base_topic, "timings/status_ms", {"ms": dt, "ip": ip})
        else:
            logging.warning(f"[STATUS] failed in {dt} ms via {ip}")
            dbg_publish(client, base_topic, "errors/status", {"ms": dt, "ip": ip})

    # OPERATING
    if mode in ("operating", "all"):
        t0 = time.perf_counter()
        r, p = get_operating_data(ip, serial, pin)
        dt = int((time.perf_counter() - t0) * 1000)
        if r != -1 and p:
            publish(client, base_topic + "operating_data", p)
            logging.info(f"[OPER] ok in {dt} ms via {ip}")
            dbg_publish(client, base_topic, "timings/operating_ms", {"ms": dt, "ip": ip})
        else:
            logging.warning(f"[OPER] failed in {dt} ms via {ip}")
            dbg_publish(client, base_topic, "errors/operating", {"ms": dt, "ip": ip})
            
    # NETWORK (nur fürs interne Debug/Discovery; keine HA-Sensoren hieraus)
    if mode in ("network", "all"):
        t0 = time.perf_counter()
        r, p = get_network_data(ip, serial, pin)
        dt = int((time.perf_counter() - t0) * 1000)
        if r != -1 and p:
            publish(client, base_topic + "network", p)
            logging.info(f"[NET] ok in {dt} ms via {ip}")
            dbg_publish(client, base_topic, "timings/network_ms", {"ms": dt, "ip": ip})
        else:
            logging.warning(f"[NET] failed in {dt} ms via {ip}")
            dbg_publish(client, base_topic, "errors/network", {"ms": dt, "ip": ip})

    # CONSUMPTION
    if mode in ("consumption", "all"):
        t0 = time.perf_counter()
        r, p = get_consumption_data(ip, serial, pin)
        dt = int((time.perf_counter() - t0) * 1000)
        if r != -1 and p:
            publish(client, base_topic + "consumption_data", p)
            logging.info(f"[CONS] ok in {dt} ms via {ip}")
            dbg_publish(client, base_topic, "timings/consumption_ms", {"ms": dt, "ip": ip})
        else:
            logging.warning(f"[CONS] failed in {dt} ms via {ip}")
            dbg_publish(client, base_topic, "errors/consumption", {"ms": dt, "ip": ip})

    return ip

# ==========================
# Ausführung mit Timeout (soft)
# ==========================
PYDURO_TIMEOUT_MS = max(0, int(os.getenv("PYDURO_TIMEOUT_MS", "5000")))
_EXECUTOR = ThreadPoolExecutor(max_workers=4)

def _call_with_timeout(fn: Callable[[], Any]) -> Any:
    if PYDURO_TIMEOUT_MS <= 0:
        return fn()
    future = _EXECUTOR.submit(fn)
    return future.result(timeout=PYDURO_TIMEOUT_MS / 1000.0)

# ==========================
# MQTT Command Bridge (RAW/SET/GET)
# ==========================
def resolve_stove_ip_for_commands(stove_override: str, cloud_fallback: str, cached_ip: Optional[str]) -> Optional[Tuple[str, str]]:
    if stove_override:
        ip = _resolve_to_ip(stove_override)
        if ip: return ip, "override"
    sensor_ip = ha_get_state("sensor.aduro_h2_stove_ip", default="no connection")
    if sensor_ip not in ("unknown", "no connection", "", "0.0.0.0"):
        ip = _resolve_to_ip(sensor_ip)
        if ip: return ip, "ha"
    if cached_ip:
        return cached_ip, "cached"
    res, disc_ip, serial_found, disc_payload = get_discovery_data()
    if disc_ip not in ("no connection", "", "0.0.0.0"):
        ip = _resolve_to_ip(disc_ip) or _resolve_to_ip(cloud_fallback)
        if ip: return ip, "discovery"
    ip = _resolve_to_ip(cloud_fallback)
    if ip: return ip, "cloud"
    return None

METRICS_ENABLED = os.getenv("METRICS_ENABLED", "false").lower() == "true"
METRICS_EVERY = max(1, int(os.getenv("METRICS_EVERY", "20")))
_metrics_durations = deque(maxlen=200)

def _metrics_push_and_maybe_publish(client: mqtt.Client, base: str, elapsed_ms: int):
    if not METRICS_ENABLED:
        return
    _metrics_durations.append(elapsed_ms)
    if len(_metrics_durations) % METRICS_EVERY == 0:
        arr = list(_metrics_durations)
        arr_sorted = sorted(arr)
        p50 = arr_sorted[len(arr_sorted)//2]
        p95 = arr_sorted[int(len(arr_sorted)*0.95)-1 if len(arr_sorted)>1 else 0]
        pub = {"count": len(arr), "avg_ms": sum(arr)//len(arr), "p50_ms": p50, "p95_ms": p95}
        try:
            client.publish(base + "cmd/metrics", json.dumps(pub), qos=0, retain=False)
        except Exception:
            logging.debug("metrics publish failed", exc_info=True)

def handle_command_message(client: mqtt.Client, base_topic: str, payload_bytes: bytes, stove_ip: str, serial: str, pin: str, allowed_ids: set[int], fast_ack: bool):
    def _ack(ok: bool, correlation_id: Optional[str], **extra):
        payload = {"ok": ok, "type": "ack", "correlation_id": correlation_id}
        payload.update(extra)
        client.publish(base_topic + "cmd/ack", json.dumps(payload), qos=0, retain=False)

    def _resp(ok: bool, correlation_id: Optional[str], **extra):
        payload = {"ok": ok, "type": "resp", "correlation_id": correlation_id}
        payload.update(extra)
        client.publish(base_topic + "cmd/resp", json.dumps(payload), qos=0, retain=False)

    def _publish_full_refresh():
        try:
            r, p = get_status(stove_ip, serial, pin)
            if r != -1 and p:
                client.publish(base_topic + "status", p)
            r, p = get_operating_data(stove_ip, serial, pin)
            if r != -1 and p:
                client.publish(base_topic + "operating_data", p)
            r, p = get_network_data(stove_ip, serial, pin)
            if r != -1 and p:
                client.publish(base_topic + "network", p)
            r, p = get_consumption_data(stove_ip, serial, pin)
            if r != -1 and p:
                client.publish(base_topic + "consumption_data", p)
            logging.info("[REFRESH] full refresh published (status+operating+network+consumption)")
        except Exception as e:
            logging.warning(f"[REFRESH] full refresh failed: {e}")

    try:
        data = json.loads(payload_bytes.decode("utf-8"))
        if not data or not isinstance(data, dict) or "type" not in data:
            logging.warning(f"[CMD] invalid payload, ignoring: {data}")
            return
    except Exception as e:
        _ack(False, None, err=f"invalid json: {e}")
        return
    
    c_id = data.get("correlation_id")
    ctype = str(data.get("type", "")).lower().strip()

    # --- SPECIAL: full refresh via pseudo-path ---
    if ctype == "set" and str(data.get("path", "")) == "__refresh__":
        try:
            if fast_ack:
                _ack(True, c_id, accepted=True)
            _publish_full_refresh()
            if not fast_ack:
                _ack(True, c_id)
            _resp(True, c_id, result={"refresh": "all"})
        except Exception as e:
            if not fast_ack:
                _ack(False, c_id, err=str(e))
            _resp(False, c_id, err=str(e))
        return

    if ctype not in ("raw", "set", "get"):
        _ack(False, c_id, err=f"unsupported type '{ctype}'")
        return

    if ctype in ("set", "get"):
        path = str(data.get("path", "")).strip()
        if not path:
            _ack(False, c_id, err="missing 'path'")
            return

    if ctype == "raw":
        try:
            fid = int(data.get("function_id"))
        except Exception:
            _ack(False, c_id, err="missing/invalid 'function_id'")
            return
        if fid not in allowed_ids:
            _ack(False, c_id, err=f"function_id {fid} not allowed")
            return

    t0_cmd = time.perf_counter()
    if fast_ack:
        _ack(True, c_id, accepted=True)

    try:
        if ctype == "raw":
            def _do_raw():
                from pyduro.actions import raw
                raw_payload = str(data.get("payload", ""))
                r = raw.run(burner_address=str(stove_ip), serial=str(serial), pin_code=str(pin),
                            function_id=int(data["function_id"]), payload=raw_payload)
                try:
                    parsed = r.parse_payload()
                except Exception:
                    parsed = None
                return {"function_id": int(data["function_id"]), "raw_payload": raw_payload,
                        "response_payload": getattr(r, "payload", None), "response_parsed": parsed}
            result = _call_with_timeout(lambda: _pyduro_call(_do_raw))

        elif ctype == "set":
            def _do_set():
                from pyduro.actions import set as pyset
                path = str(data.get("path", "")).strip()
                value = str(data.get("value", ""))
                r = pyset.run(burner_address=str(stove_ip), serial=str(serial), pin_code=str(pin),
                              path=path, value=value)
                try:
                    parsed = r.parse_payload()
                except Exception:
                    parsed = None
                return {"path": path, "value": value,
                        "response_payload": getattr(r, "payload", None), "response_parsed": parsed}
            result = _call_with_timeout(lambda: _pyduro_call(_do_set))

        else:
            def _do_get():
                from pyduro.actions import get as pyget
                path = str(data.get("path", "")).strip()
                r = pyget.run(burner_address=str(stove_ip), serial=str(serial), pin_code=str(pin),
                              path=path)
                try:
                    parsed = r.parse_payload()
                except Exception:
                    parsed = None
                return {"path": path, "response_payload": getattr(r, "payload", None), "response_parsed": parsed}
            result = _call_with_timeout(lambda: _pyduro_call(_do_get))

        dt_ms = int((time.perf_counter() - t0_cmd) * 1000)
        if not fast_ack:
            _ack(True, c_id)
        _resp(True, c_id, result={"elapsed_ms": dt_ms, "ip": stove_ip, **result})
        # Sofortiger Refresh nach erfolgreichem Kommando
        try:
            # _publish_full_refresh() # full refresh commented out to reduce load
            r, p = get_status(stove_ip, serial, pin)
            if r != -1 and p:
                client.publish(base_topic + "status", p)
            r, p = get_operating_data(stove_ip, serial, pin)
            if r != -1 and p:
                client.publish(base_topic + "operating_data", p)
            logging.info("[REFRESH] immediate status+operating published after command")
        except Exception as e:
            logging.warning(f"[REFRESH] failed: {e}")
    except FuturesTimeout:
        if not fast_ack:
            _ack(False, c_id, err=f"timeout after {PYDURO_TIMEOUT_MS}ms")
        _resp(False, c_id, err=f"timeout after {PYDURO_TIMEOUT_MS}ms")
    except Exception as e:
        if not fast_ack:
            _ack(False, c_id, err=str(e))
        _resp(False, c_id, err=str(e), trace=traceback.format_exc()[:800])

# =====
# MAIN
# =====
CURRENT_IP: Optional[str] = None

def command_worker(client: mqtt.Client, base: str, serial: str, pin: str, allowed_ids: set[int],
                   stove_override: str, cloud_fallback: str, fast_ack: bool):
    global CURRENT_IP
    logging.info("[CMD] worker started")
    while True:
        try:
            topic, payload = COMMAND_Q.get(timeout=0.2)
        except Empty:
            continue
        target = resolve_stove_ip_for_commands(
            stove_override=stove_override,
            cloud_fallback=cloud_fallback,
            cached_ip=CURRENT_IP,
        )
        if not target:
            client.publish(base + "cmd/ack", json.dumps({"ok": False, "type": "ack", "err": "no stove address available"}), qos=0, retain=False)
            continue
        stove_ip, src = target
        logging.info(f"[CMD] processing ({src} -> {stove_ip}) on {topic}")
        try:
            handle_command_message(client, base, payload, stove_ip, serial, pin, allowed_ids, fast_ack)
        except Exception as e:
            logging.error(f"[CMD] handler error: {e}")
            client.publish(base + "cmd/ack", json.dumps({"ok": False, "type": "ack", "err": str(e)}), qos=0, retain=False)

def main():
    global CURRENT_IP
    MQTT_SERVER_IP   = os.getenv("MQTT_SERVER_IP", "127.0.0.1")
    MQTT_SERVER_PORT = int(os.getenv("MQTT_SERVER_PORT", "1883"))
    MQTT_BASE_PATH   = os.getenv("MQTT_BASE_PATH", "aduro_h2").rstrip("/") + "/"
    MQTT_USERNAME    = os.getenv("MQTT_USERNAME") or None
    MQTT_PASSWORD    = os.getenv("MQTT_PASSWORD") or None

    STOVE_SERIAL     = os.getenv("STOVE_SERIAL") or ""
    STOVE_PIN        = os.getenv("STOVE_PIN") or ""
    MODE             = os.getenv("MODE", "all")

    # >>> Temperaturabhängiges Intervall (wie ursprünglich)
    STOVE_TEMP_ENTITY = os.getenv("STOVE_TEMP_ENTITY", "sensor.aduro_stove_temperature")
    HOT_THRESHOLD     = _safe_float(os.getenv("HOT_THRESHOLD", "100")) or 100.0
    INTERVAL_HOT      = int(os.getenv("INTERVAL_HOT_SECONDS", "15"))
    INTERVAL_COLD     = int(os.getenv("INTERVAL_COLD_SECONDS", "60"))
    # <<<

    REDISCOVER_EVERY = int(os.getenv("REDISCOVER_EVERY_N_CYCLES", "20"))

    STOVE_HOST_OVERRIDE = os.getenv("STOVE_HOST_OVERRIDE", "").strip()
    CLOUD_FALLBACK_HOST = os.getenv("CLOUD_FALLBACK_HOST", "apprelay20.stokercloud.dk").strip()

    MQTT_TLS = (os.getenv("MQTT_TLS", "false").lower() == "true")
    MQTT_TLS_INSECURE = (os.getenv("MQTT_TLS_INSECURE", "false").lower() == "true")
    MQTT_CA_CERT = os.getenv("MQTT_CA_CERT", "")

    ENABLE_COMMANDS = os.getenv("ENABLE_COMMANDS", "true").lower() == "true"
    ALLOWED_FUNCTION_IDS = os.getenv("ALLOWED_FUNCTION_IDS", "2,5,6,10,11")
    try:
        ALLOWED_IDS = {int(x.strip()) for x in ALLOWED_FUNCTION_IDS.split(",") if x.strip()}
    except Exception:
        ALLOWED_IDS = set()
    COMMAND_QOS = int(os.getenv("COMMAND_QOS", "0"))
    FAST_ACK = os.getenv("FAST_ACK", "true").lower() == "true"

    MQTT_PROTOCOL = os.getenv("MQTT_PROTOCOL", "v311").lower()   # "v311" | "v5"
    MQTT_NO_LOCAL = int(os.getenv("MQTT_NO_LOCAL", "0"))

    MQTT_DISCOVERY_ENABLED = os.getenv("MQTT_DISCOVERY_ENABLED", "true").lower() == "true"
    MQTT_DISCOVERY_PREFIX  = os.getenv("MQTT_DISCOVERY_PREFIX", "homeassistant").strip("/")

    logging.info(f"[BOOT] Mode={MODE} base={MQTT_BASE_PATH} hot={INTERVAL_HOT}s cold={INTERVAL_COLD}s threshold={HOT_THRESHOLD}°C")
    logging.info(f"[BOOT] MQTT target {MQTT_SERVER_IP}:{MQTT_SERVER_PORT} TLS={MQTT_TLS} user={'set' if MQTT_USERNAME else 'none'}")
    logging.info(f"[BOOT] ENABLE_COMMANDS={ENABLE_COMMANDS} FAST_ACK={FAST_ACK} PYDURO_TIMEOUT_MS={PYDURO_TIMEOUT_MS}")
    if STOVE_HOST_OVERRIDE:
        logging.info(f"[BOOT] stove_host_override={STOVE_HOST_OVERRIDE}")
    logging.info(f"[BOOT] cloud_fallback={CLOUD_FALLBACK_HOST} allowed_fids={sorted(list(ALLOWED_IDS))}")
    logging.info(f"[BOOT] MQTT_DISCOVERY enabled={MQTT_DISCOVERY_ENABLED} prefix={MQTT_DISCOVERY_PREFIX}")

    protocol = mqtt.MQTTv5 if MQTT_PROTOCOL == "v5" else mqtt.MQTTv311
    userdata = {
        "MQTT_BASE_PATH": MQTT_BASE_PATH,
        "DISCOVERY_ENABLED": MQTT_DISCOVERY_ENABLED,
        "DISCOVERY_PREFIX": MQTT_DISCOVERY_PREFIX,
    }
    client = mqtt.Client(userdata=userdata, protocol=protocol)

    client.will_set(MQTT_BASE_PATH + "availability", "offline", qos=0, retain=True)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    tls_cfg = {"enabled": MQTT_TLS, "insecure": MQTT_TLS_INSECURE, "cafile": MQTT_CA_CERT.strip() or None}
    if not mqtt_connect_with_retry(client, MQTT_SERVER_IP, MQTT_SERVER_PORT, tls_cfg):
        logging.error("[BOOT] exiting because MQTT not reachable")
        return

    if ENABLE_COMMANDS:
        topic = MQTT_BASE_PATH + "cmd/#"
        try:
            if protocol == mqtt.MQTTv5 and MQTT_NO_LOCAL:
                from paho.mqtt.subscribeoptions import SubscribeOptions
                opts = SubscribeOptions(qos=COMMAND_QOS, noLocal=True)
                client.subscribe(topic, options=opts)
                logging.info(f"[CMD] subscribed {topic} (qos={COMMAND_QOS}, noLocal=True)")
            else:
                client.subscribe(topic, qos=COMMAND_QOS)
                logging.info(f"[CMD] subscribed {topic} (qos={COMMAND_QOS})")
        except Exception:
            client.subscribe(topic, qos=COMMAND_QOS)
            logging.info(f"[CMD] subscribed {topic} (qos={COMMAND_QOS})")

        t = threading.Thread(
            target=command_worker,
            args=(client, MQTT_BASE_PATH, STOVE_SERIAL, STOVE_PIN, ALLOWED_IDS, STOVE_HOST_OVERRIDE, CLOUD_FALLBACK_HOST, FAST_ACK),
            daemon=True,
        )
        t.start()

    CURRENT_IP = None
    cycle = 0
    try:
        while True:
            cycle += 1

            # alle N Zyklen IP neu wählen
            if REDISCOVER_EVERY > 0 and cycle % REDISCOVER_EVERY == 0:
                CURRENT_IP = None

            CURRENT_IP = run_cycle(
                client=client,
                base_topic=MQTT_BASE_PATH,
                mode=MODE,
                serial=STOVE_SERIAL,
                pin=STOVE_PIN,
                cached_ip=CURRENT_IP,
                stove_override=STOVE_HOST_OVERRIDE,
                cloud_fallback=CLOUD_FALLBACK_HOST,
            ) or CURRENT_IP

            # >>> temperaturabhängiges Intervall
            t_state = ha_get_state(STOVE_TEMP_ENTITY, default="unknown")
            temp = _safe_float(t_state) if t_state is not None else None
            if temp is None:
                temp = -9999.0
            interval = INTERVAL_HOT if temp >= HOT_THRESHOLD else INTERVAL_COLD
            logging.debug(f"[SLEEP] next poll in {interval}s (temp_entity={STOVE_TEMP_ENTITY}, temp={temp})")
            time.sleep(interval)
            # <<<
    except KeyboardInterrupt:
        pass
    finally:
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass
        logging.info("[BOOT] stopped")

if __name__ == "__main__":
    main()
