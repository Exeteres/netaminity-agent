# Reliability

[Netaminity](https://github.com/exeteres/netaminity) runs both agent roles with `--reliable --health-addr 0.0.0.0:8080`. Reliable mode recovers network and peer failures inside the agent process. Kubernetes handles local process failures reported by `/live`.

## Recovery Ownership

Failures are handled by the component capable of repairing them:

| Failure | Owner | Response |
| --- | --- | --- |
| Control EOF, reset, timeout, or peer replacement | Agent | Mark unready and reconnect in-process. |
| Temporary authentication failure | Agent | Remain live and retry with bounded backoff. |
| Backend unavailable | Agent | Mark unready and continue checking the backend. |
| Active Proxy pod becomes non-viable | Operator | Route the rendezvous Service to a viable Proxy pod. |
| Agent exits, deadlocks, or loses its critical supervisor | Kubernetes | Apply the container liveness policy. |
| Workload configuration changes | Operator and Kubernetes | Replace pods through the Deployment. |

## Health Endpoints

The health server exposes:

- `/live`: whether the process and its critical supervisor are running and making internal progress.
- `/ready`: whether this agent can currently carry application traffic.
- `/status`: diagnostic JSON containing the role, control-session state, reported backend state, and tunnel-probe state.

The target is ready when its control connection is authenticated and it can establish a TCP connection to the configured target endpoint. The active proxy is ready when those conditions are true and an end-to-end tunnel probe has succeeded.

The following external conditions make an agent unready but must not make it unlive:

- No control connection.
- Peer deletion or replacement.
- Connection refusal, timeout, EOF, or reset.
- Authentication failure.
- Backend unavailability.
- A failed end-to-end tunnel probe.
- An operator-driven switch to another Proxy pod.

`/live` fails only for a local internal failure, such as an unexpectedly terminated reconnect supervisor or an event loop that no longer makes progress. A healthy HTTP listener alone is not sufficient if the critical supervisor has failed.

## Session Supervisor

The Target owns a long-running session supervisor. It repeatedly establishes TCP, authenticates, runs the control session, and returns to reconnecting whenever that session ends:

```text
disconnected
    -> connect
    -> authenticate
    -> run control session
    -> connection lost
    -> mark unready
    -> reconnect
```

Each failed session must release its listeners, pending connection requests, and per-session state before another session becomes active. There must be at most one active reliable Target session per process.

The Proxy remains listening after an individual connection or authentication failure. A failed Target session does not terminate the Proxy process. Once the operator routes a Target to a different viable Proxy, that Proxy accepts and authenticates the new session.

## Reconnect Policy

Reconnect should be fast enough for pod replacement and transient network interruption while remaining bounded during a prolonged outage.

| Attempt | Delay before attempt |
| --- | ---: |
| First | Immediate |
| Second | 100 milliseconds |
| Third | 250 milliseconds |
| Fourth | 500 milliseconds |
| Fifth | 1 second |
| Later | 2 seconds maximum |

Each delay uses approximately 20 percent positive or negative jitter so many agents do not reconnect in lockstep. Authentication failures use the same bounded retry path and never terminate the process.

Connection and handshake work is bounded independently:

| Operation | Timeout |
| --- | ---: |
| TCP connect | 2 seconds |
| Authentication and protocol handshake | 2 seconds |
| Complete connection attempt | 3 seconds maximum |

Connection refusal normally fails immediately. A blackholed endpoint can consume the complete three-second attempt timeout, followed by at most two seconds of backoff, producing one attempt approximately every five seconds during a sustained outage.

The reconnect backoff resets after a control session remains healthy for 10 seconds. It does not reset immediately after TCP connection or authentication, which prevents a peer that repeatedly accepts and drops sessions from creating a tight loop.

## Failure Detection

Fast retries only help after the agent detects that the established session is no longer usable.

- EOF, reset, broken pipe, and protocol errors end the session immediately.
- The control heartbeat runs every 2 seconds while the session is otherwise idle.
- Two consecutive missed or invalid heartbeat responses declare the session lost.
- A heartbeat response timeout is bounded so a silent connection is detected in approximately 4 to 6 seconds.
- Backend DNS and connection latency do not block heartbeat responses.

Once the session is declared lost, the Target marks itself unready and begins the reconnect sequence immediately. The Proxy clears that session's readiness and tunnel-verification state while continuing to accept a replacement session.

## Backend And Tunnel Checks

The Target checks its configured backend every five seconds in a background task using the same host, port, and bounded connection timeout as application forwarding. Backend DNS and connection latency never block control-heartbeat responses.

When the Target reports the backend healthy, the Proxy connects to its own consumer port and accepts that connection through the normal listener path with a unique connection ID. The Target opens a data connection to the Proxy, connects to the backend, and only then accepts the probe. A successful accept verifies:

1. The Proxy consumer port is bound and accepting TCP connections.
2. The Proxy control session can issue work.
3. The Target event loop is processing control messages.
4. The Target can open a new data connection to the Proxy.
5. The Target can connect to its backend.
6. The Proxy can correlate and complete the data handshake.

Synthetic probes establish and immediately close a TCP connection to the backend. They do not send application bytes.

A backend failure or tunnel-probe failure changes readiness and diagnostics but not liveness. Repeated tunnel-probe failure ends the current session and triggers in-process reconnection after the configured threshold.

## Kubernetes Probes

Netaminity configures both generated containers with:

- A startup probe against `/live` every second, allowing 20 failures.
- A liveness probe against `/live` every five seconds, allowing three failures.
- A readiness probe against `/ready` every three seconds.

The health listener is pod-local and is not exposed by the consumer or rendezvous Services.

Kubernetes liveness operates independently of session recovery. Control EOF, heartbeat failure, authentication failure, backend state, and tunnel-probe results do not change `/live`.

## Timing Objectives

The agent-level objectives are:

| Failure | Detection | Recovery behavior |
| --- | --- | --- |
| Explicit control EOF, reset, or protocol error | Immediate when observed | Mark unready and reconnect immediately. If the peer is available, recovery is normally below 1 second. |
| Peer becomes available during an outage | At the next attempt | Reconnect within 2 seconds when connects fail quickly. |
| Silent established connection failure | Approximately 4 to 6 seconds | End the session and enter immediate reconnect. |
| Blackholed endpoint | At most 3 seconds per attempt | Continue retrying with at most 2 seconds between attempts. |
| Temporary authentication mismatch | One failed handshake | Remain live and retry, capped at a 2-second delay. |
| Backend becomes unavailable | One backend check and timeout, at most 8 seconds | Mark both sides unready as state propagates and retain the control session. |
| Backend recovers | One backend check, heartbeat, and tunnel probe | Restore readiness and traffic without replacing either pod. |
| End-to-end probe repeatedly fails while backend is healthy | Configured probe threshold | Clear readiness, close the current session, and establish a new session. |
| Critical internal supervisor fails | Internal health observation, then Kubernetes liveness threshold | Fail `/live` and let Kubernetes apply the container liveness policy. |

These objectives describe agent and control-session recovery. DNS resolver behavior, operator endpoint selection, node failure detection, and a peer or backend that remains unavailable can extend full application recovery.

## Scope

- `/live` reports local process health, not remote reachability or application availability.
- `/ready` reports whether the agent can currently carry application traffic.
- Session retry uses the reconnect policy defined above.
- Reliable mode converges automatically on a new usable session after session loss; existing application connections are not preserved.
- Reliable mode supports one Target session per Proxy process, which is how Netaminity generates Proxy and Target Deployments.
- Standard bore behavior remains available when `--reliable` is omitted.
