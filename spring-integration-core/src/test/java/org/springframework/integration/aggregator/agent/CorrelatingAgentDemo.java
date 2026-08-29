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

package org.springframework.integration.aggregator.agent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import org.springframework.integration.aggregator.AggregatingMessageHandler;
import org.springframework.integration.aggregator.DefaultAggregatingMessageGroupProcessor;
import org.springframework.integration.aggregator.agent.grpc.CorrelatingAgentPortGrpc;
import org.springframework.integration.aggregator.agent.grpc.HealthRequest;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;

/**
 * Console test applet for the correlating agent in both in-process and external modes.
 *
 * @author Mohamed Zayton
 *
 * @since 7.2
 */
public final class CorrelatingAgentDemo {

	private static final String HOST = InetAddress.getLoopbackAddress().getHostAddress();

	private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(15);

	private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(30);

	private static final PrintWriter OUTPUT = new PrintWriter(
			new OutputStreamWriter(new FileOutputStream(FileDescriptor.out), StandardCharsets.UTF_8), true);

	private static final PrintWriter ERROR = new PrintWriter(
			new OutputStreamWriter(new FileOutputStream(FileDescriptor.err), StandardCharsets.UTF_8), true);

	private CorrelatingAgentDemo() {
	}

	public static void main(String[] args) {
		try {
			if (args.length == 0) {
				runInProcessScenarios();
				runExternalProcessScenarios();
			}
			else if ("agent-server".equals(args[0]) && args.length == 2) {
				runAgentServer(Integer.parseInt(args[1]));
			}
			else if ("handler-client".equals(args[0]) && args.length == 3) {
				runExternalHandler(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
			}
			else {
				throw new IllegalArgumentException("Unsupported arguments: " + Arrays.toString(args));
			}
		}
		catch (Throwable ex) {
			ERROR.println("FAIL correlating agent demo: " + ex.getMessage());
			ex.printStackTrace(ERROR);
			System.exit(1);
		}
	}

	static void runInProcessScenarios() {
		try (Scenario scenario = new Scenario()) {
			scenario.start();
			scenario.verifyWaitReleaseDiscard();
			OUTPUT.println("PASS in-process: wait/release/discard");
			scenario.verifyTimeout();
			OUTPUT.println("PASS in-process: timeout");
			scenario.verifyStopStart();
			OUTPUT.println("PASS in-process: stop/start");
		}
	}

	static void runExternalProcessScenarios() throws Exception {
		int dependencyPort = reservePort();
		try (ChildProcess agent = startChild("agent", "READY agent ",
				"agent-server", Integer.toString(dependencyPort))) {

			int agentPort = agent.awaitReadyPort();
			try (ChildProcess handler = startChild("handler", "READY handler ",
					"handler-client", Integer.toString(dependencyPort), Integer.toString(agentPort))) {

				handler.awaitReadyPort();
				int exitCode = handler.awaitExit();
				verify(exitCode == 0, "External handler process failed:\n" + handler.diagnostics());
				OUTPUT.println("PASS external: health and aggregation");
				OUTPUT.println("PASS external: timeout");
				OUTPUT.println("PASS external: stop/start");
			}
			agent.requestStop();
			int exitCode = agent.awaitExit();
			verify(exitCode == 0, "External agent process failed:\n" + agent.diagnostics());
		}
	}

	private static void runAgentServer(int dependencyPort) throws Exception {
		ManagedChannel dependencyChannel = NettyChannelBuilder.forAddress(HOST, dependencyPort)
				.usePlaintext()
				.build();
		Server server = NettyServerBuilder.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
				.addService(new EmbabelCorrelatingAgentService(dependencyChannel))
				.build()
				.start();
		try {
			OUTPUT.println("READY agent " + server.getPort());
			OUTPUT.flush();
			new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)).readLine();
		}
		finally {
			shutdown(server);
			shutdown(dependencyChannel);
		}
	}

	private static void runExternalHandler(int dependencyPort, int agentPort) throws Exception {
		ManagedChannel agentChannel = NettyChannelBuilder.forAddress(HOST, agentPort)
				.usePlaintext()
				.build();
		Scenario scenario = new Scenario(agentChannel);
		Server dependencyServer = NettyServerBuilder
				.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), dependencyPort))
				.addService(scenario.dependencyPort())
				.build()
				.start();
		CorrelatingAgentPortGrpc.CorrelatingAgentPortBlockingStub agent =
				CorrelatingAgentPortGrpc.newBlockingStub(agentChannel);
		try (scenario) {
			OUTPUT.println("READY handler " + dependencyServer.getPort());
			OUTPUT.flush();
			verify(health(agent), "External agent did not report serving");
			scenario.start();
			scenario.verifyWaitReleaseDiscard();
			scenario.verifyTimeout();
			scenario.verifyStopStart(() -> health(agent));
		}
		finally {
			shutdown(dependencyServer);
			shutdown(agentChannel);
		}
	}

	private static boolean health(CorrelatingAgentPortGrpc.CorrelatingAgentPortBlockingStub agent) {
		return agent.withDeadlineAfter(STARTUP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
				.health(HealthRequest.getDefaultInstance())
				.getServing();
	}

	private static ChildProcess startChild(String name, String readyPrefix, String... args) throws IOException {
		String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
		List<String> command = new ArrayList<>();
		command.add(javaExecutable);
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add(CorrelatingAgentDemo.class.getName());
		command.addAll(Arrays.asList(args));
		Process process = new ProcessBuilder(command)
				.redirectErrorStream(true)
				.start();
		return new ChildProcess(name, readyPrefix, process);
	}

	private static int reservePort() throws IOException {
		try (ServerSocket socket = new ServerSocket()) {
			socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
			return socket.getLocalPort();
		}
	}

	private static void shutdown(Server server) throws InterruptedException {
		server.shutdownNow();
		server.awaitTermination(5, TimeUnit.SECONDS);
	}

	private static void shutdown(ManagedChannel channel) throws InterruptedException {
		channel.shutdownNow();
		channel.awaitTermination(5, TimeUnit.SECONDS);
	}

	private static void verify(boolean condition, String message) {
		if (!condition) {
			throw new IllegalStateException(message);
		}
	}

	private static final class Scenario implements AutoCloseable {

		private static final long RECEIVE_TIMEOUT = 5_000;

		private final SimpleMessageStore store = new SimpleMessageStore();

		private final QueueChannel replies = new QueueChannel();

		private final QueueChannel discards = new QueueChannel();

		private final AggregatingMessageHandler handler =
				new AggregatingMessageHandler(new DefaultAggregatingMessageGroupProcessor(), this.store);

		Scenario() {
			this.handler.setDiscardChannel(this.discards);
		}

		Scenario(Channel agentChannel) {
			this();
			this.handler.setCorrelatingAgentChannel(agentChannel);
		}

		void start() {
			this.handler.start();
		}

		void verifyWaitReleaseDiscard() {
			this.handler.handleMessage(message("one", "complete", 1, 2, this.replies));
			verify(this.replies.receive(0) == null, "The first message was released instead of waiting");
			this.handler.handleMessage(message("two", "complete", 2, 2, this.replies));
			Message<?> reply = this.replies.receive(RECEIVE_TIMEOUT);
			verify(reply != null, "The completed group did not produce a reply");
			verify(List.of("one", "two").equals(reply.getPayload()),
					"Unexpected aggregate payload: " + reply.getPayload());

			this.handler.handleMessage(message("late", "complete", 3, 2, this.replies));
			Message<?> discarded = this.discards.receive(RECEIVE_TIMEOUT);
			verify(discarded != null && "late".equals(discarded.getPayload()),
					"A message for the completed group was not discarded");
		}

		void verifyTimeout() {
			this.handler.handleMessage(message("partial", "timeout", 1, 2, this.replies));
			int expired = this.store.expireMessageGroups(-10_000);
			verify(expired > 0, "No message group was selected for expiration");
			Message<?> discarded = this.discards.receive(RECEIVE_TIMEOUT);
			verify(discarded != null && "partial".equals(discarded.getPayload()),
					"The incomplete expired group was not discarded");
			verify(this.store.getMessageGroup("timeout").size() == 0,
					"The expired message group was not removed");
		}

		void verifyStopStart() {
			verifyStopStart(() -> this.handler.isRunning());
		}

		void verifyStopStart(BooleanSupplier agentHealth) {
			this.handler.stop();
			verify(!this.handler.isRunning(), "The correlating handler did not stop");
			verify(!agentHealth.getAsBoolean(), "The correlating agent still reports serving after stop");
			boolean failedWhileStopped = false;
			try {
				this.handler.handleMessage(message("stopped", "restart", 1, 2, this.replies));
			}
			catch (MessageHandlingException ex) {
				failedWhileStopped = true;
			}
			verify(failedWhileStopped, "The stopped correlating agent accepted a message");

			this.handler.start();
			verify(this.handler.isRunning(), "The correlating handler did not restart");
			verify(agentHealth.getAsBoolean(), "The correlating agent did not report serving after restart");
			this.handler.handleMessage(message("three", "restart", 1, 2, this.replies));
			this.handler.handleMessage(message("four", "restart", 2, 2, this.replies));
			Message<?> reply = this.replies.receive(RECEIVE_TIMEOUT);
			verify(reply != null && List.of("three", "four").equals(reply.getPayload()),
					"The restarted correlating agent did not release a complete group");
		}

		BindableService dependencyPort() {
			return this.handler.getCorrelatingDependencyPort();
		}

		@Override
		public void close() {
			this.handler.destroy();
		}

		private static Message<String> message(String payload, String correlationId, int sequenceNumber,
				int sequenceSize, QueueChannel replyChannel) {

			return MessageBuilder.withPayload(payload)
					.setCorrelationId(correlationId)
					.setSequenceNumber(sequenceNumber)
					.setSequenceSize(sequenceSize)
					.setReplyChannel(replyChannel)
					.build();
		}

	}

	private static final class ChildProcess implements AutoCloseable {

		private final String name;

		private final Process process;

		private final BufferedWriter input;

		private final StringBuilder diagnostics = new StringBuilder();

		private final CompletableFuture<Integer> readyPort = new CompletableFuture<>();

		ChildProcess(String name, String readyPrefix, Process process) {
			this.name = name;
			this.process = process;
			this.input = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
			Thread outputReader = new Thread(() -> readOutput(readyPrefix), "correlating-agent-demo-" + name);
			outputReader.setDaemon(true);
			outputReader.start();
		}

		int awaitReadyPort() throws Exception {
			try {
				return this.readyPort.get(STARTUP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
			}
			catch (TimeoutException ex) {
				throw new IllegalStateException("Timed out waiting for " + this.name + ":\n" + diagnostics(), ex);
			}
		}

		int awaitExit() throws InterruptedException {
			if (!this.process.waitFor(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
				throw new IllegalStateException("Timed out waiting for " + this.name + ":\n" + diagnostics());
			}
			return this.process.exitValue();
		}

		void requestStop() throws IOException {
			this.input.write("stop");
			this.input.newLine();
			this.input.flush();
		}

		String diagnostics() {
			synchronized (this.diagnostics) {
				return this.diagnostics.toString();
			}
		}

		@Override
		public void close() {
			try {
				this.input.close();
			}
			catch (IOException ex) {
				// The child may already have closed its standard input.
			}
			if (this.process.isAlive()) {
				this.process.destroy();
				try {
					if (!this.process.waitFor(5, TimeUnit.SECONDS)) {
						this.process.destroyForcibly();
					}
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					this.process.destroyForcibly();
				}
			}
		}

		private void readOutput(String readyPrefix) {
			try (BufferedReader reader = new BufferedReader(
					new InputStreamReader(this.process.getInputStream(), StandardCharsets.UTF_8))) {

				String line;
				while ((line = reader.readLine()) != null) {
					synchronized (this.diagnostics) {
						this.diagnostics.append(line).append(System.lineSeparator());
					}
					if (line.startsWith(readyPrefix)) {
						this.readyPort.complete(Integer.parseInt(line.substring(readyPrefix.length())));
					}
				}
				if (!this.readyPort.isDone()) {
					this.readyPort.completeExceptionally(
							new IllegalStateException(this.name + " exited before reporting ready:\n" + diagnostics()));
				}
			}
			catch (IOException | RuntimeException ex) {
				this.readyPort.completeExceptionally(ex);
			}
		}

	}

}
