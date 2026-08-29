/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.samples.agentdemo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.annotation.PreDestroy;
import tools.jackson.databind.ObjectMapper;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.integration.samples.agentdemo.DemoModels.EnvironmentStatus;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageCommand;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageResult;
import org.springframework.integration.samples.agentdemo.DemoModels.ScenarioResult;
import org.springframework.integration.samples.agentdemo.DemoModels.Topology;
import org.springframework.stereotype.Component;

/**
 * Starts and controls separate agent and handler JVMs.
 *
 * @author Mohamed Zayton
 */
@Component
@ConditionalOnProperty(name = "demo.mode", havingValue = "dashboard", matchIfMissing = true)
final class ExternalProcessOrchestrator {

	private static final Pattern AGENT_READY = Pattern.compile("READY agent grpc=(\\d+)");

	private static final Pattern HANDLER_READY =
			Pattern.compile("READY handler http=(\\d+) dependency=(\\d+)");

	private final DemoProperties properties;

	private final ObjectMapper objectMapper;

	private final HttpClient httpClient;

	private ProcessPair interactive;

	ExternalProcessOrchestrator(DemoProperties properties, ObjectMapper objectMapper) {
		this.properties = properties;
		this.objectMapper = objectMapper;
		this.httpClient = HttpClient.newBuilder()
				.connectTimeout(properties.getStartupTimeout())
				.build();
	}

	synchronized ExternalRun runSuite() {
		try (ProcessPair pair = startPair()) {
			ScenarioResult result = post(pair.handlerHttpPort(), "/api/internal/scenario", null,
					ScenarioResult.class);
			return new ExternalRun(result.checks(), pair.logs());
		}
	}

	synchronized EnvironmentStatus startInteractive() {
		if (this.interactive == null) {
			this.interactive = startPair();
		}
		return status();
	}

	synchronized EnvironmentStatus stopInteractive() {
		if (this.interactive != null) {
			this.interactive.close();
			this.interactive = null;
		}
		return unavailable("External environment is stopped");
	}

	synchronized EnvironmentStatus resetInteractive() {
		stopInteractive();
		return startInteractive();
	}

	synchronized EnvironmentStatus lifecycle(boolean start) {
		ProcessPair pair = requireInteractive();
		post(pair.handlerHttpPort(), start ? "/api/internal/lifecycle/start" : "/api/internal/lifecycle/stop",
				null, EnvironmentStatus.class);
		return status();
	}

	synchronized MessageResult send(MessageCommand command) {
		ProcessPair pair = requireInteractive();
		return post(pair.handlerHttpPort(), "/api/internal/message", command, MessageResult.class);
	}

	synchronized EnvironmentStatus status() {
		if (this.interactive == null || !this.interactive.isAlive()) {
			return unavailable("External environment is stopped");
		}
		try {
			return get(this.interactive.handlerHttpPort(), "/api/internal/status", EnvironmentStatus.class);
		}
		catch (RuntimeException ex) {
			return unavailable(ex.getMessage());
		}
	}

	@PreDestroy
	synchronized void close() {
		stopInteractive();
	}

	private ProcessPair startPair() {
		ChildJvm agent = null;
		ChildJvm handler = null;
		try {
			int dependencyPort = reservePort();
			agent = startChild(AGENT_READY,
					"--demo.mode=agent",
					"--spring.main.web-application-type=none",
					"--demo.agent-grpc-port=0",
					"--demo.dependency-grpc-port=" + dependencyPort,
					"--logging.level.root=WARN");
			ReadyMatch agentReady = agent.awaitReady();
			int agentPort = Integer.parseInt(agentReady.groups().get(0));
			handler = startChild(HANDLER_READY,
					"--demo.mode=handler",
					"--server.port=0",
					"--demo.agent-grpc-port=" + agentPort,
					"--demo.dependency-grpc-port=" + dependencyPort,
					"--logging.level.root=WARN");
			ReadyMatch handlerReady = handler.awaitReady();
			int handlerHttpPort = Integer.parseInt(handlerReady.groups().get(0));
			ProcessPair pair = new ProcessPair(agent, handler, handlerHttpPort);
			waitForHandler(pair);
			return pair;
		}
		catch (Exception ex) {
			if (handler != null) {
				handler.close();
			}
			if (agent != null) {
				agent.close();
			}
			throw new IllegalStateException("Failed to start external environment", ex);
		}
	}

	private ChildJvm startChild(Pattern readyPattern, String... arguments) throws IOException {
		List<String> command = javaCommand();
		command.addAll(List.of(arguments));
		return new ChildJvm(new ProcessBuilder(command).redirectErrorStream(true).start(), readyPattern,
				this.properties.getMaxLogCharacters(), this.properties.getStartupTimeout(),
				this.properties.getProcessTimeout());
	}

	private List<String> javaCommand() {
		String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
		List<String> command = new ArrayList<>();
		command.add(javaExecutable);
		String classPath = System.getProperty("java.class.path");
		if (!classPath.contains(File.pathSeparator)) {
			Path classPathJar = Path.of(classPath);
			if (Files.isRegularFile(classPathJar) && classPathJar.getFileName().toString().endsWith(".jar")) {
				command.add("-jar");
				command.add(classPathJar.toAbsolutePath().toString());
				return command;
			}
		}
		try {
			Path location = Path.of(CorrelatingAgentDemoApplication.class.getProtectionDomain()
					.getCodeSource().getLocation().toURI());
			if (Files.isRegularFile(location) && location.getFileName().toString().endsWith(".jar")) {
				command.add("-jar");
				command.add(location.toString());
				return command;
			}
		}
		catch (URISyntaxException ex) {
			// Fall back to the development classpath.
		}
		command.add("-cp");
		command.add(classPath);
		command.add(CorrelatingAgentDemoApplication.class.getName());
		return command;
	}

	private void waitForHandler(ProcessPair pair) {
		long deadline = System.nanoTime() + this.properties.getStartupTimeout().toNanos();
		RuntimeException failure = null;
		while (System.nanoTime() < deadline) {
			try {
				get(pair.handlerHttpPort(), "/api/internal/status", EnvironmentStatus.class);
				return;
			}
			catch (RuntimeException ex) {
				failure = ex;
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException interruptedException) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Interrupted waiting for handler", interruptedException);
				}
			}
		}
		throw new IllegalStateException("Handler did not become ready", failure);
	}

	private <T> T get(int port, String path, Class<T> responseType) {
		HttpRequest request = HttpRequest.newBuilder(uri(port, path))
				.timeout(this.properties.getStartupTimeout())
				.GET()
				.build();
		return exchange(request, responseType);
	}

	private <T> T post(int port, String path, Object body, Class<T> responseType) {
		String json = body == null ? "{}" : this.objectMapper.writeValueAsString(body);
		HttpRequest request = HttpRequest.newBuilder(uri(port, path))
				.timeout(this.properties.getProcessTimeout())
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(json))
				.build();
		return exchange(request, responseType);
	}

	private <T> T exchange(HttpRequest request, Class<T> responseType) {
		try {
			HttpResponse<String> response = this.httpClient.send(request,
					HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			if (response.statusCode() < 200 || response.statusCode() >= 300) {
				throw new IllegalStateException("Child request returned HTTP " + response.statusCode() + ": "
						+ response.body());
			}
			return this.objectMapper.readValue(response.body(), responseType);
		}
		catch (IOException ex) {
			throw new IllegalStateException("Child request failed", ex);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted calling child process", ex);
		}
	}

	private URI uri(int port, String path) {
		return URI.create("http://" + this.properties.getHost() + ':' + port + path);
	}

	private ProcessPair requireInteractive() {
		if (this.interactive == null || !this.interactive.isAlive()) {
			throw new IllegalStateException("External environment is not running");
		}
		return this.interactive;
	}

	private EnvironmentStatus unavailable(String detail) {
		return new EnvironmentStatus(Topology.EXTERNAL, false, false, false, Map.of(), detail);
	}

	private static int reservePort() throws IOException {
		try (ServerSocket socket = new ServerSocket()) {
			socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
			return socket.getLocalPort();
		}
	}

	record ExternalRun(List<DemoModels.ScenarioCheck> checks, String logs) {
	}

	private record ReadyMatch(List<String> groups) {
	}

	private static final class ProcessPair implements AutoCloseable {

		private final ChildJvm agent;

		private final ChildJvm handler;

		private final int handlerHttpPort;

		ProcessPair(ChildJvm agent, ChildJvm handler, int handlerHttpPort) {
			this.agent = agent;
			this.handler = handler;
			this.handlerHttpPort = handlerHttpPort;
		}

		int handlerHttpPort() {
			return this.handlerHttpPort;
		}

		boolean isAlive() {
			return this.agent.isAlive() && this.handler.isAlive();
		}

		String logs() {
			return "AGENT\n" + this.agent.logs() + "\nHANDLER\n" + this.handler.logs();
		}

		@Override
		public void close() {
			this.handler.close();
			this.agent.close();
		}

	}

	private static final class ChildJvm implements AutoCloseable {

		private final Process process;

		private final int maxLogCharacters;

		private final Duration processTimeout;

		private final StringBuilder logs = new StringBuilder();

		private final CompletableFuture<ReadyMatch> ready = new CompletableFuture<>();

		ChildJvm(Process process, Pattern readyPattern, int maxLogCharacters, Duration startupTimeout,
				Duration processTimeout) {

			this.process = process;
			this.maxLogCharacters = maxLogCharacters;
			this.processTimeout = processTimeout;
			Thread reader = new Thread(() -> readOutput(readyPattern), "correlating-agent-demo-child-output");
			reader.setDaemon(true);
			reader.start();
			this.ready.orTimeout(startupTimeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		ReadyMatch awaitReady() throws Exception {
			try {
				return this.ready.get();
			}
			catch (java.util.concurrent.ExecutionException ex) {
				if (ex.getCause() instanceof TimeoutException) {
					throw new IllegalStateException("Child readiness timed out:\n" + logs(), ex.getCause());
				}
				throw ex;
			}
		}

		boolean isAlive() {
			return this.process.isAlive();
		}

		String logs() {
			synchronized (this.logs) {
				return this.logs.toString();
			}
		}

		@Override
		public void close() {
			if (!this.process.isAlive()) {
				return;
			}
			this.process.destroy();
			try {
				if (!this.process.waitFor(this.processTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
					this.process.destroyForcibly();
					this.process.waitFor(5, TimeUnit.SECONDS);
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				this.process.destroyForcibly();
			}
		}

		private void readOutput(Pattern readyPattern) {
			try (BufferedReader reader = new BufferedReader(
					new InputStreamReader(this.process.getInputStream(), StandardCharsets.UTF_8))) {

				String line;
				while ((line = reader.readLine()) != null) {
					append(line);
					Matcher matcher = readyPattern.matcher(line);
					if (matcher.find()) {
						List<String> groups = new ArrayList<>();
						for (int index = 1; index <= matcher.groupCount(); index++) {
							groups.add(matcher.group(index));
						}
						this.ready.complete(new ReadyMatch(List.copyOf(groups)));
					}
				}
				if (!this.ready.isDone()) {
					this.ready.completeExceptionally(
							new IllegalStateException("Child exited before readiness:\n" + logs()));
				}
			}
			catch (IOException ex) {
				this.ready.completeExceptionally(ex);
			}
		}

		private void append(String line) {
			synchronized (this.logs) {
				this.logs.append(line).append(System.lineSeparator());
				if (this.logs.length() > this.maxLogCharacters) {
					this.logs.delete(0, this.logs.length() - this.maxLogCharacters);
				}
			}
		}

	}

}
