# Aduro Client — Home Assistant Add-on

[![Build & Publish](https://img.shields.io/github/actions/workflow/status/spaceteddy/aduro-ha-addon/build.yml?branch=main)](https://github.com/spaceteddy/aduro-ha-addon/actions)
[![GHCR Image](https://img.shields.io/badge/GHCR-ghcr.io%2Fspaceteddy%2Faduro--client-blue)](https://ghcr.io/spaceteddy/aduro-client)
![Version](https://img.shields.io/badge/version-0.6.2-informational)

This Add-on read data from Aduro Stove (via `pyduro`).

## Command API (MQTT Bridge)

**Topics (publish from HA → Add-on)**

- `<base>/cmd/raw`  
  Payload: `{"type":"raw","function_id":<int>,"payload":"<string>","correlation_id":"<id>"}`  
  *RAW passt direkt an `pyduro.raw(...)` durch. Erlaubte IDs siehe `allowed_function_ids`.*

- `<base>/cmd/set`  
  Payload: `{"type":"set","path":"<pyduro_path>","value":"<val>","correlation_id":"<id>"}`  
  → ruft `pyduro.set.run(...)`.

- `<base>/cmd/get`  
  Payload: `{"type":"get","path":"<pyduro_path>","correlation_id":"<id>"}`  
  → ruft `pyduro.get.run(...)`.

**Responses (Add-on → HA)**

- `<base>/cmd/ack` – `{"ok": true|false, "type":"raw|set|get", "correlation_id":"<id>", ...}`
- `<base>/cmd/resp` – `{"ok": true|false, "type":"...", "correlation_id":"<id>", "result": {...}}`

**Beispiel Set:**
```yaml
service: mqtt.publish
data:
  topic: aduro_h2/cmd/set
  payload: >
    {"type":"set","path":"user.temperature.target","value":"75","correlation_id":"set_{{ now().timestamp() | int }}"}
