# Reliability

Netaminity runs both agent roles with `--reliable --health-addr 0.0.0.0:8080`. Reliable mode distinguishes an unavailable target dependency from a broken tunnel so Kubernetes only restarts agents when restarting can repair the path.

## Health Endpoints

The health server exposes:

- `/live`: process and tunnel-integrity liveness.
- `/ready`: whether this agent can currently carry application traffic.
- `/status`: diagnostic JSON containing the role, control-session state, reported backend state, and tunnel-probe state.

The target is ready when its control connection is active and it can establish a TCP connection to the configured target endpoint. The proxy is ready when those conditions are true and an end-to-end tunnel probe has succeeded.

An unavailable target endpoint makes both agents unready but leaves `/live` healthy. Restarting either agent cannot repair an unavailable application dependency.

## Reliability Protocol

The target checks its configured backend every five seconds in a background task using the same host, port, and three-second connection timeout as application forwarding. Backend DNS and connection latency never block control-heartbeat responses.

Every five seconds the proxy sends a health check with a random nonce. The target responds immediately with the same nonce and its cached backend state. The proxy ignores stale nonce responses and allows three consecutive missing or invalid responses before failing liveness. One delayed response therefore affects readiness but does not restart either agent.

When the target reports the backend healthy, the proxy connects to its own consumer port and accepts that connection through the normal listener path with a unique connection ID. The target opens a data connection to the proxy, connects to the backend, and only then accepts the probe. A successful accept therefore verifies:

1. The proxy consumer port is bound and accepting TCP connections.
2. The proxy control session can issue work.
3. The target event loop is processing control messages.
4. The target can open a new data connection to the proxy.
5. The target can connect to its backend.
6. The proxy can correlate and complete the data handshake.

Synthetic probes establish and immediately close a TCP connection to the backend. They do not send application bytes.

## Restart Policy

Backend failures reset the tunnel-probe failure counter and never fail liveness. Three consecutive control-heartbeat failures fail proxy liveness and trigger the same coordinated restart path. If control health is good and the target reports the backend healthy but three consecutive end-to-end probes fail, the proxy:

1. Marks `/live` unhealthy.
2. Sends a restart request to the target when possible.
3. Terminates the failed control-session handler while `/live` remains unhealthy.

The target marks `/live` unhealthy and exits with an error when it receives a restart request or loses its reliable control connection. This causes both Kubernetes Deployments to restart even when only one side detects the integrity failure. If the restart request cannot be delivered, control-session loss provides the fallback signal to the target.

Reliable mode is intended for one target session per proxy process, which is how Netaminity generates Proxy and Target Deployments. Standard bore behavior remains available when `--reliable` is omitted.

## Kubernetes Probes

Netaminity configures both generated containers with:

- A startup probe against `/live` every second, allowing 20 failures.
- A liveness probe against `/live` every five seconds, allowing three failures.
- A readiness probe against `/ready` every three seconds.

The health listener is pod-local and is not exposed by the consumer or rendezvous Services.

## Timing And Recovery SLOs

The fixed timings are intentionally conservative enough to tolerate short scheduler, runtime, DNS, and network stalls without making a broken tunnel remain ready for long:

| Mechanism                  | Timing                                                                                    | Rationale                                                                                       |
| -------------------------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Backend TCP check          | Every 5 seconds, 3-second connect timeout                                                 | Detect dependency changes promptly without continuously opening connections to the application. |
| Control health check       | Every 5 seconds, 3-second response timeout                                                | Detect an unresponsive control loop independently of backend latency.                           |
| Control failure threshold  | 3 consecutive failures                                                                    | Tolerate one or two transient delays; fail after a sustained control outage.                    |
| End-to-end tunnel probe    | After each successful control check while backend is healthy, 3-second completion timeout | Verify the real consumer listener and Target-initiated data path.                               |
| Tunnel failure threshold   | 3 consecutive failures                                                                    | Avoid coordinated restarts for isolated connection loss.                                        |
| Kubernetes readiness probe | Every 3 seconds, 1 failure                                                                | Remove unusable proxy pods from Service endpoints quickly.                                      |
| Kubernetes liveness probe  | Every 5 seconds, 3 failures                                                               | Give the agent time to coordinate restart before kubelet forces the local restart.              |
| Kubernetes startup probe   | Every second, 20 failures                                                                 | Allow up to 20 seconds for process and health-listener startup before liveness begins.          |

Expected agent-level recovery objectives are below. Bounds are worst-case phase alignment and include configured network timeouts and Kubernetes probe periods, but exclude image pulls, pod scheduling, container startup, DNS resolver retries outside the TCP connect future, and application-specific warm-up.

| Failure                                                   | Readiness response                                                                                                                                                                                           | Restart response                                                                                                                                                                       | Expected recovery behavior                                                                                      |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Target backend becomes unavailable                        | Target detects within 8 seconds; proxy observes within another 5 seconds. Kubernetes reflects unready within another 3 seconds, so target is expected unready within 11 seconds and proxy within 16 seconds. | No restart.                                                                                                                                                                            | Both remain live and return ready automatically after the backend recovers.                                     |
| Target backend recovers                                   | Target normally becomes ready within 8 seconds. Proxy performs the next control check and end-to-end probe, then Kubernetes observes readiness; worst-case objective is 16 seconds.                          | No restart.                                                                                                                                                                            | Traffic resumes without replacing either pod.                                                                   |
| One or two delayed control responses                      | Proxy becomes unready on the first miss and logs the failure count.                                                                                                                                          | No restart.                                                                                                                                                                            | A later matching response logs recovery, resets the counter, and permits tunnel verification.                   |
| Sustained unresponsive control session                    | Three checks fail within at most 18 seconds.                                                                                                                                                                 | Target receives the coordinated restart request immediately when possible. Proxy kubelet restart begins within another 15 seconds, for a detection-to-restart objective of 33 seconds. | Both agents establish a fresh control session after container startup.                                          |
| Definitive control EOF or TCP error                       | Detected immediately when read, or at the next control read within 5 seconds.                                                                                                                                | Target exits immediately on its own control loss. Proxy marks liveness failed and kubelet restarts it within up to 15 additional seconds.                                              | Both sides converge on a fresh session; no reconnect state is retained in-process.                              |
| Backend reported healthy but end-to-end tunnel path fails | Three probes fail within at most 18 seconds.                                                                                                                                                                 | Coordinated Target restart is requested immediately; Proxy restart begins within another 15 seconds, for a worst-case objective of 33 seconds.                                         | Both agents restart because the contradictory state indicates tunnel corruption rather than dependency failure. |

The objectives describe control-plane recovery, not application availability. Kubernetes restart backoff, node outages, unavailable images, and a backend that remains unhealthy can extend or prevent full service recovery.
