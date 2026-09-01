# Error-code contract

`CommandResponse.error_code` remains a string. `purdue_rov_cv.wire.errors.ErrorCode`
is the canonical source for its safe use; no protobuf enum is introduced. “N/A” in
the status column means the code is **not valid in `CommandResponse.error_code`**:
it belongs to diagnostics, events, logs, data-plane drops, or client/process lifecycle behavior.

| Error code | Emitter | Trigger | Command/result status | State effect | Exit code | Recovery | Required test |
|---|---|---|---|---|---|---|---|
| CONFIG_INVALID | module | startup config invalid | N/A diagnostic | ERROR | 78 | fix config/restart | test_config_invalid |
| RESTART_REQUIRED | module | immutable setting changed | REJECTED | DEGRADED | 0 | restart module | test_restart_required |
| CAMERA_NOT_FOUND | camera service | device missing | FAILED | ERROR | 75 | restore device/retry | test_camera_not_found |
| CAMERA_MODE_UNSUPPORTED | camera service | unsupported format | FAILED | ERROR | 0 | select supported mode | test_camera_mode_unsupported |
| CAMERA_FRAME_TIMEOUT | camera service | frame deadline elapsed | N/A diagnostic | DEGRADED | 0 | restart capture | test_camera_frame_timeout |
| SHARED_MEMORY_INVALID | camera/module | shared-memory invalid | N/A diagnostic | ERROR | 75 | restart services | test_shared_memory_invalid |
| MODEL_NOT_FOUND | module | artifact absent | FAILED | ERROR | 78 | deploy model | test_model_not_found |
| MODEL_HASH_MISMATCH | module | checksum differs | FAILED | ERROR | 78 | replace artifact | test_model_hash_mismatch |
| MODEL_LOAD_FAILED | module | loader error | FAILED | ERROR | 78 | inspect artifact/runtime | test_model_load_failed |
| RUNTIME_UNAVAILABLE | module | runtime missing | FAILED | ERROR | 78 | install runtime | test_runtime_unavailable |
| TARGET_INCOMPATIBLE | module | artifact incompatible | FAILED | ERROR | 78 | deploy compatible artifact | test_target_incompatible |
| BROKER_UNAVAILABLE | publisher/subscriber | broker unreachable | N/A diagnostic | DEGRADED | 0 | reconnect/backoff | test_broker_unavailable |
| CONTROL_ROUTER_UNAVAILABLE | control client | router unreachable | N/A client outcome | unchanged | 0 | retry command | test_control_router_unavailable |
| TARGET_UNAVAILABLE | control router | target unregistered | REJECTED | unchanged | 0 | start target/retry | test_target_unavailable |
| TARGET_SEND_TIMEOUT | control router | send deadline elapsed | REJECTED | unchanged | 0 | retry command | test_target_send_timeout |
| COMMAND_ACK_TIMEOUT | control client | no acknowledgement | N/A client outcome | unchanged | 0 | query status | test_command_ack_timeout |
| COMMAND_COMPLETION_TIMEOUT | control client | completion deadline elapsed | N/A client outcome | unchanged | 0 | query status | test_command_completion_timeout |
| COMMAND_OUTCOME_UNKNOWN | control client | status indeterminate | OUTCOME_UNKNOWN | unchanged | 0 | query status/diagnostics | test_command_outcome_unknown |
| DUPLICATE_COMMAND_ID | target module | UUID already seen | REJECTED | unchanged | 0 | use cached/new UUID | test_duplicate_command_id |
| INVALID_COMMAND | target module | bad request | REJECTED | unchanged | 0 | correct request | test_invalid_command |
| INVALID_STATE_TRANSITION | target module | invalid current state | REJECTED | unchanged | 0 | wait/retry | test_invalid_state_transition |
| MODULE_BUSY | target module | exclusive work active | REJECTED | unchanged | 0 | retry later | test_module_busy |
| PROCESSING_FAILURE | module | processing exception | N/A diagnostic | DEGRADED | 0 | inspect/restart | test_processing_failure |
| PROCESSING_WATCHDOG_EXCEEDED | module | deadline exceeded | N/A diagnostic | DEGRADED | 75 | reduce load/restart | test_processing_watchdog_exceeded |
| UNKNOWN_PAYLOAD_TYPE | receiver | registry miss | N/A data-plane drop | unchanged | 0 | upgrade receiver | test_unknown_payload_type |
| UNSUPPORTED_SCHEMA_VERSION | receiver | unsupported version | N/A data-plane drop | unchanged | 0 | upgrade component | test_unsupported_schema_version |
| INVALID_ENVELOPE | receiver | invalid envelope/payload | N/A data-plane drop | unchanged | 0 | inspect publisher/logs | test_invalid_envelope |
| MESSAGE_TOO_LARGE | publisher/receiver | limit exceeded | N/A data-plane drop | unchanged | 0 | reduce payload | test_message_too_large |
| CLOCK_UNSYNCHRONIZED | clock monitor | invalid offset | N/A diagnostic | DEGRADED | 0 | restore time sync | test_clock_unsynchronized |
| VIDEO_STREAM_LOST | video receiver | stream absent | N/A diagnostic | DEGRADED | 0 | restart stream | test_video_stream_lost |
| FRAME_INDEX_MISS | video receiver | unmatched frame index | N/A diagnostic | unchanged | 0 | continue/record metric | test_frame_index_miss |
| RECORDER_QUEUE_FULL | recorder | queue full | FAILED | DEGRADED | 0 | drain/reduce rate | test_recorder_queue_full |
| DISK_SPACE_LOW | recorder | free space low | FAILED | DEGRADED | 0 | free space/retry | test_disk_space_low |
| INTERNAL_ERROR | any component | unexpected failure | FAILED | ERROR | 70 | inspect/restart | test_internal_error |

The Python contract metadata mirrors this table and is tested for exhaustive
coverage of every `ErrorCode`. Individual component phases must add the named
operational tests when they implement the corresponding emitter behavior.
