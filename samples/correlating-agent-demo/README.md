# Correlating Agent Demo

This isolated Spring Boot application exercises the correlating-agent implementation in the current Spring Integration checkout. It provides a local browser dashboard, an HTTP API, JSON run history, and an external topology that launches the handler and agent in separate JVMs connected through plaintext loopback gRPC.

Correlating-agent processing is opt-in. The demo enables it explicitly for the in-process topology and implicitly by configuring an agent channel for the external topology. It also demonstrates an optional opaque `text/plain` projection; the handler retains and processes the original message locally, so payloads and correlation keys do not need to implement Java `Serializable`.

The sample is a composite Gradle build. It substitutes `org.springframework.integration:spring-integration-core:7.2.0-SNAPSHOT` with `../../spring-integration-core`; it is not part of the framework publication, BOM, or distribution.

## Run the dashboard

From the repository root on Windows:

```powershell
.\gradlew.bat -p samples/correlating-agent-demo bootRun
```

Open <http://127.0.0.1:8080>. The dashboard can run the in-process suite, a fresh two-JVM external suite, or both. Start the persistent external environment before sending it custom external messages.

The built-in Embabel decision is deterministic. This sample does not require an LLM, API key, prompt, or planning runtime.

## Build and run the executable JAR

```powershell
.\gradlew.bat -p samples/correlating-agent-demo clean test bootJar
java -jar samples/correlating-agent-demo/build/libs/correlating-agent-demo.jar
```

The JAR launches child JVMs from that same executable. During `bootRun`, child JVMs use the development runtime classpath.

## Launch the services manually

Choose unused loopback ports, then start the agent first:

```powershell
java -jar samples/correlating-agent-demo/build/libs/correlating-agent-demo.jar `
  --demo.mode=agent `
  --demo.agent-grpc-port=9091 `
  --demo.dependency-grpc-port=9092
```

Start the handler in another terminal:

```powershell
java -jar samples/correlating-agent-demo/build/libs/correlating-agent-demo.jar `
  --demo.mode=handler `
  --server.port=8081 `
  --demo.agent-grpc-port=9091 `
  --demo.dependency-grpc-port=9092
```

The agent prints `READY agent grpc=<port>`. The handler prints `READY handler http=<port> dependency=<port>` and serves the shared control dashboard at <http://127.0.0.1:8081>. Stop either process with `Ctrl+C`; both release their gRPC servers and channels through Spring shutdown callbacks.

## HTTP API

The dashboard mode exposes:

- `GET /api/application`
- `POST /api/runs` with `{"topology":"IN_PROCESS|EXTERNAL|ALL"}`
- `GET /api/runs` and `GET /api/runs/{id}`
- `GET /api/environments/{IN_PROCESS|EXTERNAL}`
- `POST /api/environments/{topology}/{start|stop|reset}`
- `POST /api/environments/{topology}/lifecycle/{start|stop}`
- `POST /api/environments/{topology}/messages`

The handler mode exposes its loopback control API under `/api/internal`, including `status`, `message`, `scenario`, lifecycle, reset, and graceful shutdown operations.

Only one automatic suite may run at a time; a concurrent request receives HTTP 409. Message operations are serialized per stateful environment.

## History and timeouts

Completed reports are written atomically to `./data/correlating-agent-demo/runs`. The defaults retain 100 reports and ignore malformed history files with a warning. Common overrides are:

```powershell
--demo.history-directory=D:/temp/agent-runs
--demo.history-retention=25
--demo.startup-timeout=20s
--demo.process-timeout=60s
--demo.max-log-characters=262144
```

All HTTP and gRPC listeners bind to `127.0.0.1` by default. The sample deliberately uses plaintext transport and does not expose remote authentication or shutdown controls.
