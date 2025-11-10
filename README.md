# Aduro MQTT Bridge Add-on

## Overview
The **Aduro MQTT Bridge** is a Home Assistant add-on that connects directly to an Aduro / NBE pellet stove through the [pyduro](https://pypi.org/project/pyduro/) library.  
It reads operational data, publishes it to MQTT, and allows full two-way control (start/stop, heat level, forced auger, alarm reset, etc.) through structured MQTT messages.

This add-on replaces Python-script integrations with a containerized MQTT bridge that works in all Home Assistant installations (Core, OS, Supervised, or Container).

---

## Features
- Periodic data polling: `status`, `operating_data`, `network`, `consumption_data`
- Automatic discovery of stove IP with cloud fallback
- Bidirectional MQTT bridge for control and telemetry
- LAN or cloud connection (use LAN for write commands)
- Host network mode for local UDP/MDNS discovery
- Lightweight Python 3 container using pyduro 3.2.1 and paho-mqtt 1.6.1
- Structured JSON responses with timing and debug information

---

## MQTT Topics

### Published by the Add-on
| Topic | Description | Example |
|-------|--------------|----------|
| `<base>/discovery` | Stove serial, IP, type, version | `{"DISCOVERY":{"StoveSerial":"11111","StoveIP":"192.168.0.55"}}` |
| `<base>/status` | Stove status parameters | `{"STATUS":{"HeatLevel":"2","State":"Running"}}` |
| `<base>/operating_data` | Temperatures, power, runtime | `{"OPERATING_DATA":{"Power_pct":"50","SmokeTemp":"145"}}` |
| `<base>/network` | Network information | `{"NETWORK":{"RouterSSID":"MyWiFi","StoveRSSI":"-64"}}` |
| `<base>/consumption_data` | Consumption totals | `{"CONSUMPTION_DATA":{"Day":"1.3","Month":"32.5"}}` |
| `<base>/cmd/ack` | Command acknowledgment | `{"ok":true,"type":"set","correlation_id":"force_auger_173058"}` |
| `<base>/cmd/resp` | Command response with details | includes `elapsed_ms`, `response_payload`, `response_parsed` |
| `<base>/debug/*` | Optional debug metrics | Timings, selected IP, errors |

### Accepted Command Topics
| Topic | Purpose | JSON Schema |
|-------|----------|-------------|
| `<base>/cmd/raw` | Direct function-ID call | `{"type":"raw","function_id":6,"payload":"total_days","correlation_id":"id"}` |
| `<base>/cmd/set` | Parameter write | `{"type":"set","path":"misc.start","value":"1","correlation_id":"id"}` |
| `<base>/cmd/get` | Parameter read | `{"type":"get","path":"regulation.fixed_power","correlation_id":"id"}` |

---

## Control Paths
| Action | Path | Value |
|--------|------|-------|
| Start stove | `misc.start` | `1` |
| Stop stove | `misc.stop` | `1` |
| Force auger on/off | `auger.forced_run` | `1` / `0` |
| Reset alarm | `misc.reset_alarm` | `1` |
| Heat level 1 | `regulation.fixed_power` | `10` |
| Heat level 2 | `regulation.fixed_power` | `50` |
| Heat level 3 | `regulation.fixed_power` | `100` |

---


## Example Home Assistant Scripts

### Stove Start
```yaml
aduro_start:
  alias: Aduro – Start
  sequence:
    - service: mqtt.publish
      data:
        topic: aduro_h2/cmd/set
        payload_template: >
          {{ {
            "type": "set",
            "path": "misc.start",
            "value": "1",
            "correlation_id": "start_" ~ now().timestamp()|int
          } | tojson }}
```
### Stove Stop
```yaml
aduro_stop:
  alias: Aduro – Stop
  sequence:
    - service: mqtt.publish
      data:
        topic: aduro_h2/cmd/set
        payload_template: >
          {{ {
            "type": "set",
            "path": "misc.start",
            "value": "0",
            "correlation_id": "stop_" ~ now().timestamp()|int
          } | tojson }}
```
### Heatlevel (parametrized)
```yaml
aduro_heatlevel:
  alias: Aduro – Heatlevel (param)
  fields:
    level:
      name: Level
      selector:
        select:
          options: ["1","2","3"]
  sequence:
    - variables:
        map: {"1":"10","2":"50","3":"100"}
        fp: "{{ map[level] }}"
    - service: mqtt.publish
      data:
        topic: aduro_h2/cmd/set
        payload_template: >
          {{ {
            "type": "set",
            "path": "regulation.fixed_power",
            "value": fp,
            "correlation_id": "hl_" ~ level ~ "_" ~ now().timestamp()|int
          } | tojson }}
```
### Alarm Reset
```yaml
aduro_alarm_reset:
  alias: Aduro – Alarm Reset
  sequence:
    - service: mqtt.publish
      data:
        topic: aduro_h2/cmd/set
        payload: '{"type":"set","path":"misc.reset_alarm","value":"1","correlation_id":"alarm_reset"}'

### Example Dashboard Configuration
```yaml
type: entities
title: Aduro Control
entities:
  - entity: input_boolean.aduro_stove_power
    name: Power
  - entity: input_select.aduro_heatlevel
    name: Heatlevel
  - entity: input_boolean.aduro_force_auger
    name: Force Auger
  - type: button
    name: Alarm Reset
    icon: mdi:alarm-off
    action_name: Reset
    tap_action:
      action: call-service
      service: script.turn_on
      target:
        entity_id: script.aduro_alarm_reset
```
Heatlevel selection and power/auger toggles are handled by small automations that publish the respective MQTT commands.

# Aduro MQTT Bridge – Installation and Usage Guide

## 1. Installation

### 1.1 Add the Repository
1. Open **Settings → Add-ons → Add-on Store** in Home Assistant.  
2. Click the three-dot menu in the top-right → **Repositories**.  
3. Add: https://github.com/spaceteddy/aduro-ha-addon

4. Click **Add** → **Close**.

### 1.2 Install the Add-on
1. In the Add-on Store, search for **Aduro Client**.  
2. Click **Install**.

### 1.3 Configure
Open the add-on configuration panel and set the options:

| Key | Example | Description |
|-----|----------|-------------|
| `mqtt_server_ip` | `core-mosquitto` | Address of your MQTT broker. |
| `mqtt_server_port` | `1883` | MQTT port. |
| `mqtt_username` / `mqtt_password` | — | Broker credentials (optional). |
| `stove_serial` | `serialnr` | Stove serial number. |
| `stove_pin` | `123456789` | Stove PIN code. |
| `stove_host_override` | `192.168.0.100` | LAN IP of the stove (recommended). |
| `enable_commands` | `true` | Enable the MQTT command bridge. |
| `log_level` | `DEBUG` | Log verbosity (`INFO` / `DEBUG` / `ERROR`). |
| `host_network` | `true` | Required for UDP/MDNS discovery. |

> Use the LAN IP for all write commands (`set`); cloud connections are often read-only.

### 1.4 Start the Add-on
- Click **Start**.  
- Open the **Log** tab and verify that you see:
  [MQTT] Connected
  [STATUS] ok ...
  [CMD] subscribed aduro_h2/cmd/#

### 1.5 Verify MQTT Connectivity
Use **Developer Tools → Services → mqtt.publish** to test:

```yaml
topic: aduro_h2/cmd/set
payload: >-
{"type":"set","path":"misc.start","value":"1","correlation_id":"start_test"}
```
You should receive ok:true on aduro_h2/cmd/ack and aduro_h2/cmd/resp.

## License

MIT License © 2025 spaceteddy
