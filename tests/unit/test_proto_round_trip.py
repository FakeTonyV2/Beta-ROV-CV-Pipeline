"""Round-trip coverage for every protobuf message in the CV schema."""

from google.protobuf.descriptor import FieldDescriptor

from purdue_rov.cv.v1 import bounding_box_pb2
from purdue_rov.cv.v1 import classification_pb2
from purdue_rov.cv.v1 import control_pb2
from purdue_rov.cv.v1 import debug_snapshot_pb2
from purdue_rov.cv.v1 import diagnostics_pb2
from purdue_rov.cv.v1 import envelope_pb2
from purdue_rov.cv.v1 import frame_index_pb2
from purdue_rov.cv.v1 import module_state_pb2
from purdue_rov.cv.v1 import target_pose_pb2


def _sample_scalar(field: FieldDescriptor):
    if field.cpp_type == FieldDescriptor.CPPTYPE_BOOL:
        return True
    if field.cpp_type == FieldDescriptor.CPPTYPE_STRING:
        return b"session-id" if field.type == FieldDescriptor.TYPE_BYTES else "sample"
    if field.cpp_type == FieldDescriptor.CPPTYPE_ENUM:
        return next(value.number for value in field.enum_type.values if value.number)
    if field.cpp_type in (
        FieldDescriptor.CPPTYPE_INT32,
        FieldDescriptor.CPPTYPE_INT64,
        FieldDescriptor.CPPTYPE_UINT32,
        FieldDescriptor.CPPTYPE_UINT64,
    ):
        return 7
    if field.cpp_type in (FieldDescriptor.CPPTYPE_FLOAT, FieldDescriptor.CPPTYPE_DOUBLE):
        return 0.75
    raise AssertionError(f"Unhandled protobuf field type: {field.full_name}")


def _populate(message) -> None:
    # Struct's map field is the only map in this schema.
    if message.DESCRIPTOR.full_name == "google.protobuf.Struct":
        message.fields["enabled"].bool_value = True
        return

    populated_oneofs = set()
    for field in message.DESCRIPTOR.fields:
        if field.containing_oneof:
            if field.containing_oneof.name in populated_oneofs:
                continue
            populated_oneofs.add(field.containing_oneof.name)

        if field.is_repeated:
            values = getattr(message, field.name)
            if field.message_type is not None:
                child = values.add()
                _populate(child)
            else:
                values.append(_sample_scalar(field))
        elif field.message_type is not None:
            _populate(getattr(message, field.name))
        else:
            setattr(message, field.name, _sample_scalar(field))


def _assert_fields_equal(expected, actual) -> None:
    assert expected.DESCRIPTOR.full_name == actual.DESCRIPTOR.full_name
    for oneof in expected.DESCRIPTOR.oneofs:
        assert expected.WhichOneof(oneof.name) == actual.WhichOneof(oneof.name)

    for field in expected.DESCRIPTOR.fields:
        expected_value = getattr(expected, field.name)
        actual_value = getattr(actual, field.name)
        if field.is_repeated:
            assert list(expected_value) == list(actual_value), field.full_name
        elif field.message_type is not None:
            assert expected.HasField(field.name) == actual.HasField(field.name), field.full_name
            if expected.HasField(field.name):
                _assert_fields_equal(expected_value, actual_value)
        else:
            assert expected_value == actual_value, field.full_name


def _assert_round_trip(message_type) -> None:
    original = message_type()
    _populate(original)
    restored = message_type.FromString(original.SerializeToString())
    _assert_fields_equal(original, restored)


def test_bounding_box_result_round_trip():
    _assert_round_trip(bounding_box_pb2.BoundingBoxResult)


def test_detection_round_trip():
    _assert_round_trip(bounding_box_pb2.Detection)


def test_classification_result_round_trip():
    _assert_round_trip(classification_pb2.ClassificationResult)


def test_class_score_round_trip():
    _assert_round_trip(classification_pb2.ClassScore)


def test_command_request_round_trip():
    _assert_round_trip(control_pb2.CommandRequest)


def test_get_status_round_trip():
    _assert_round_trip(control_pb2.GetStatus)


def test_start_round_trip():
    _assert_round_trip(control_pb2.Start)


def test_stop_round_trip():
    _assert_round_trip(control_pb2.Stop)


def test_set_mode_round_trip():
    _assert_round_trip(control_pb2.SetMode)


def test_set_dynamic_config_round_trip():
    _assert_round_trip(control_pb2.SetDynamicConfig)


def test_request_debug_snapshot_round_trip():
    _assert_round_trip(control_pb2.RequestDebugSnapshot)


def test_start_recording_round_trip():
    _assert_round_trip(control_pb2.StartRecording)


def test_stop_recording_round_trip():
    _assert_round_trip(control_pb2.StopRecording)


def test_reset_round_trip():
    _assert_round_trip(control_pb2.Reset)


def test_get_command_status_round_trip():
    _assert_round_trip(control_pb2.GetCommandStatus)


def test_command_response_round_trip():
    _assert_round_trip(control_pb2.CommandResponse)


def test_debug_snapshot_round_trip():
    _assert_round_trip(debug_snapshot_pb2.DebugSnapshot)


def test_diagnostic_status_round_trip():
    _assert_round_trip(diagnostics_pb2.DiagnosticStatus)


def test_camera_metrics_round_trip():
    _assert_round_trip(diagnostics_pb2.CameraMetrics)


def test_module_metrics_round_trip():
    _assert_round_trip(diagnostics_pb2.ModuleMetrics)


def test_messaging_metrics_round_trip():
    _assert_round_trip(diagnostics_pb2.MessagingMetrics)


def test_video_metrics_round_trip():
    _assert_round_trip(diagnostics_pb2.VideoMetrics)


def test_system_metrics_round_trip():
    _assert_round_trip(diagnostics_pb2.SystemMetrics)


def test_message_envelope_round_trip():
    _assert_round_trip(envelope_pb2.MessageEnvelope)


def test_frame_index_round_trip():
    _assert_round_trip(frame_index_pb2.FrameIndex)


def test_module_state_round_trip():
    _assert_round_trip(module_state_pb2.ModuleState)


def test_target_pose_result_round_trip():
    _assert_round_trip(target_pose_pb2.TargetPoseResult)
