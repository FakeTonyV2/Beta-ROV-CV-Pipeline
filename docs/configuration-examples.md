# Configuration examples

The following checked-in files are parsed and statically validated by the
configuration-contract test suite:

| Scenario | Valid source |
|---|---|
| Single-camera onboard V4L2 | `tests/fixtures/config/valid/single_camera.yaml` |
| Generic V4L2 using provisioning fallback | `tests/fixtures/config/valid/fallback_camera.yaml` |
| Surface-host processing | `config/development.yaml` |
| Disabled optional task | `tests/fixtures/config/valid/fallback_camera.yaml` (`gate_detection.enabled: false`) |
| More than eight cameras | Constructed from `single_camera.yaml` by the contract test with ten stable mappings and unique stream indexes. |

`single_camera.yaml` is not a special camera mode: it is the smallest complete
mission fixture. It makes the required fields and the normal `by_id` link easy
to see. `fallback_camera.yaml` proves the separate project-provisioned link
contract and disabled-task behavior. The invalid fixtures deliberately exercise
duplicate-key, malformed-document, and non-mapping-root YAML failures without a
real camera. They are regression inputs, not deployment configurations.

## Variations

Use one host configuration per execution target. A surface task belongs in a
surface-host configuration, where both `device.execution_target` and the
enabled task's `execution_target` are `surface_laptop`; do not add a surface
task to the Pi host file.

An OAK-D camera retains the same stable path contract and changes only its
adapter. Exact OAK-D capability is hardware-aware validation:

```yaml
cameras:
  front_camera:
    adapter: oakd
    device_path: /dev/v4l/by-id/usb-luxonis-oakd
    device_path_kind: by_id
```

Camera transport has one source of truth. Explicit port overrides are not
supported:

```yaml
camera_limits:
  maximum_configured: 16
  maximum_active: 12
cameras:
  bottom_camera:
    # Copy every required camera field from an existing camera.
    device_path: /dev/purdue-rov-cv/bottom_camera
    device_path_kind: fallback
    stream_index: 1  # RTP 5002, RTCP 5003, RTP PT 97
```

For a model fallback, use an ONNX artifact with `runtime: onnxruntime`.
Artifact existence, checksum verification, and provider availability remain
hardware-aware checks. Run `rov-cv config validate <file>` for static
validation; append `--probe-hardware` only once a hardware-probe implementation
is supplied.
