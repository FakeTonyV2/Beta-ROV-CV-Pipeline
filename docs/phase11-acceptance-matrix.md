# Authoritative v1 acceptance matrix

This matrix starts conservative. Results change only with the named evidence;
normal CI cannot satisfy a physical or extended HIL row.

| ID | Criterion | Owner phase | Implementation | Verification | Environment | Evidence/artifact | Result | Notes |
|---:|---|---:|---|---|---|---|---|---|
| 1 | Camera continuously streams video to surface | 7/10/11 | Camera RTP sender and production receiver | Physical end-to-end observation | Pi + surface + camera + tether | camera-hub and receiver artifact | UNVERIFIED | Requires real camera/RTP. |
| 2 | Same camera supplies onboard CV frames | 5/6/10 | Camera shared memory and module frame source | Physical camera → shared memory → module | Pi + camera | camera-hub artifact | UNVERIFIED | Requires real shared-memory flow. |
| 3 | Module publishes valid protobuf result | 3/5 | Envelope builder, payload registry, publisher | Decode production broker result | Pi | HIL subscriber artifact | UNVERIFIED | Automated coverage is not physical evidence. |
| 4 | Two independent subscribers receive results | 4/11 | Broker PUB/SUB | Two OS subscriber processes | Pi/surface | process-isolation artifact | UNVERIFIED | Two callbacks in one process do not qualify. |
| 5 | Slow consumer has no unbounded backlog | 7.5 | HWM/bounded queues | Privileged slow-consumer acceptance | Linux transport environment | Phase 7.5 report | UNVERIFIED | Reexecute for final revision. |
| 6 | Surface START/STOP controls module | 4/5/9 | `rov-cv operator` production control path | START and STOP on deployed task | Surface + Pi | control HIL artifact | UNVERIFIED | No harness substitution. |
| 7 | Timeout has no unsafe automatic resend | 4/9 | Outcome-unknown client semantics | Timeout test and attempt count | CI + deployed control | command-timeout artifact | UNVERIFIED | Unit regression exists; deployed evidence pending. |
| 8 | Camera disconnect isolates unrelated components | 6/10/11 | Per-camera processes/reconnect | Explicit opt-in disconnect | Pi + cameras | fault artifact | UNVERIFIED | Disruptive hardware action required. |
| 9 | Camera reconnects by stable identity | 10/11 | Phase 10 resolver/rebuild | Disconnect/reconnect physical camera | Pi + camera | fault artifact | UNVERIFIED | Must reacquire exact identity. |
| 10 | Module crash does not stop video | 5/7/11 | Process isolation/systemd | Kill deployed module, observe video | Pi + surface | fault artifact | UNVERIFIED | Record systemd restart. |
| 11 | Video/FrameIndex correlate | 7/11 | Exact source/session/frame identity | Production receiver statistics | Pi + surface + camera | camera-hub artifact | UNVERIFIED | Representative exact identities required. |
| 12 | Real clock loss invalidates latency only | 9/11 | Canonical clock monitor/system health | Stop/restore project chrony service | Pi + surface | clock-loss artifact | UNVERIFIED | Must invalidate ≤15 s and recover after valid checks. |
| 13 | Structured MCAP and encoded MKV read back | 8/11 | Production recorder | Record and parse both formats | Pi + surface + camera | recording artifact | UNVERIFIED | Real production path required. |
| 14 | Production recording replays | 8/11 | Structured/video replay | Replay recorded session | Surface | replay artifact | UNVERIFIED | Both structured and video. |
| 15 | Invalid physical camera mode fails preflight | 2/10 | Canonical hardware probe | Unsupported tuple through real probe | Pi + camera | preflight artifact | UNVERIFIED | No direct failed-result injection. |
| 16 | Invalid model hash fails preflight | 2/5/9 | SHA-256 artifact validation | Corrupt/copy artifact safely | Pi | preflight artifact | UNVERIFIED | Restore artifact afterward. |
| 17 | Unsynchronized clock fails preflight | 9/11 | Production clock evaluator | Real invalid chrony state | Pi + surface | clock/preflight artifacts | UNVERIFIED | Runtime still continues. |
| 18 | Mandatory automated verification passes | 1–11 | CI/test suites | Exact final command matrix | Linux CI/reference env | JUnit/coverage/static outputs | UNVERIFIED | Final post-change suite pending in checked-in matrix. |
| 19 | Full deployment is stable for 60 minutes | 11 | Stability harness | Actual 3600-second two-host run | Pi + surface + cameras + tether | stability artifacts | UNVERIFIED | Short smoke never qualifies. |
| 20 | Documented module workflow works without bypass | 3/5/8/10/11 | SDK/config workflow | Audit or execute all 16 steps | Developer + CI + Pi | workflow evidence | UNVERIFIED | Echo audit plus hardware preflight required. |
