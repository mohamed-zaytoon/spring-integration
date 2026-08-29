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

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import org.springframework.integration.aggregator.AggregatingMessageHandler;
import org.springframework.integration.aggregator.DefaultAggregatingMessageGroupProcessor;
import org.springframework.integration.aggregator.agent.grpc.CorrelatingAgentPortGrpc;
import org.springframework.integration.aggregator.agent.grpc.HealthRequest;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.samples.agentdemo.DemoModels.CheckState;
import org.springframework.integration.samples.agentdemo.DemoModels.EnvironmentStatus;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageCommand;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageOutcome;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageResult;
import org.springframework.integration.samples.agentdemo.DemoModels.ScenarioCheck;
import org.springframework.integration.samples.agentdemo.DemoModels.Topology;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;

/**
 * Stateful aggregator environment shared by the dashboard and handler process.
 *
 * @author Mohamed Zayton
 */
final class CorrelatingEnvironment implements AutoCloseable {

	private static final long RECEIVE_TIMEOUT = 5_000;

	private final Topology topology;

	private final Duration deadline;

	private final SimpleMessageStore store = new SimpleMessageStore();

	private final QueueChannel discards = new QueueChannel();

	private final AggregatingMessageHandler handler =
			new AggregatingMessageHandler(new DefaultAggregatingMessageGroupProcessor(), this.store);

	private final ManagedChannel agentChannel;

	private final CorrelatingAgentPortGrpc.CorrelatingAgentPortBlockingStub agent;

	private CorrelatingEnvironment(Topology topology, Duration deadline, ManagedChannel agentChannel) {
		this.topology = topology;
		this.deadline = deadline;
		this.agentChannel = agentChannel;
		this.agent = agentChannel != null ? CorrelatingAgentPortGrpc.newBlockingStub(agentChannel) : null;
		this.handler.setDiscardChannel(this.discards);
		if (agentChannel != null) {
			this.handler.setCorrelatingAgentChannel(agentChannel);
		}
	}

	static CorrelatingEnvironment inProcess(Duration deadline) {
		return new CorrelatingEnvironment(Topology.IN_PROCESS, deadline, null);
	}

	static CorrelatingEnvironment external(String host, int port, Duration deadline) {
		ManagedChannel channel = NettyChannelBuilder.forAddress(host, port)
				.usePlaintext()
				.build();
		return new CorrelatingEnvironment(Topology.EXTERNAL, deadline, channel);
	}

	synchronized void start() {
		this.handler.start();
	}

	synchronized void stop() {
		this.handler.stop();
	}

	synchronized MessageResult send(MessageCommand command) {
		validate(command);
		QueueChannel replies = new QueueChannel();
		Message<String> message = MessageBuilder.withPayload(command.payload())
				.setCorrelationId(command.correlationId())
				.setSequenceNumber(command.sequenceNumber())
				.setSequenceSize(command.sequenceSize())
				.setReplyChannel(replies)
				.build();
		try {
			this.handler.handleMessage(message);
			Message<?> reply = replies.receive(0);
			if (reply != null) {
				return new MessageResult(MessageOutcome.RELEASED, reply.getPayload(), "The group was released");
			}
			Message<?> discarded = this.discards.receive(0);
			if (discarded != null) {
				return new MessageResult(MessageOutcome.DISCARDED, discarded.getPayload(),
						"The message was discarded");
			}
			return new MessageResult(MessageOutcome.WAITING, null, "The message is stored and waiting");
		}
		catch (MessageHandlingException ex) {
			return new MessageResult(MessageOutcome.FAILED, null, rootMessage(ex));
		}
	}

	synchronized List<ScenarioCheck> runSuite() {
		List<ScenarioCheck> checks = new ArrayList<>();
		String prefix = Long.toUnsignedString(System.nanoTime());
		checks.add(check("wait / release / discard", () -> verifyWaitReleaseDiscard(prefix)));
		checks.add(check("force expiration", () -> verifyTimeout(prefix)));
		checks.add(check("health / stop / start", () -> verifyLifecycle(prefix)));
		return List.copyOf(checks);
	}

	synchronized EnvironmentStatus status(Map<String, Integer> ports) {
		boolean serving = this.agent == null ? this.handler.isRunning() : agentHealth();
		return new EnvironmentStatus(this.topology, true, this.handler.isRunning(), serving,
				new LinkedHashMap<>(ports), serving ? "Ready" : "Stopped");
	}

	BindableService dependencyPort() {
		return this.handler.getCorrelatingDependencyPort();
	}

	boolean agentHealth() {
		if (this.agent == null) {
			return this.handler.isRunning();
		}
		try {
			return this.agent.withDeadlineAfter(this.deadline.toMillis(), TimeUnit.MILLISECONDS)
					.health(HealthRequest.getDefaultInstance())
					.getServing();
		}
		catch (RuntimeException ex) {
			return false;
		}
	}

	@Override
	public synchronized void close() {
		this.handler.destroy();
		if (this.agentChannel != null) {
			this.agentChannel.shutdownNow();
			try {
				this.agentChannel.awaitTermination(5, TimeUnit.SECONDS);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private void verifyWaitReleaseDiscard(String prefix) {
		String correlationId = prefix + "-complete";
		MessageResult first = send(new MessageCommand("one", correlationId, 1, 2));
		verify(first.outcome() == MessageOutcome.WAITING, "First message did not wait");
		MessageResult second = send(new MessageCommand("two", correlationId, 2, 2));
		verify(second.outcome() == MessageOutcome.RELEASED, "Complete group was not released");
		verify(List.of("one", "two").equals(second.payload()), "Unexpected aggregate: " + second.payload());
		MessageResult late = send(new MessageCommand("late", correlationId, 2, 2));
		verify(late.outcome() == MessageOutcome.DISCARDED, "Late message was not discarded");
	}

	private void verifyTimeout(String prefix) {
		String correlationId = prefix + "-timeout";
		MessageResult partial = send(new MessageCommand("partial", correlationId, 1, 2));
		verify(partial.outcome() == MessageOutcome.WAITING, "Partial group did not wait");
		int expired = this.store.expireMessageGroups(-10_000);
		verify(expired > 0, "No group was selected for expiration");
		Message<?> discarded = this.discards.receive(RECEIVE_TIMEOUT);
		verify(discarded != null && "partial".equals(discarded.getPayload()),
				"Expired group was not discarded");
		verify(this.store.getMessageGroup(correlationId).size() == 0, "Expired group was not removed");
	}

	private void verifyLifecycle(String prefix) {
		stop();
		verify(!this.handler.isRunning(), "Handler did not stop");
		verify(!agentHealth(), "Agent still reports serving after stop");
		MessageResult stopped = send(new MessageCommand("stopped", prefix + "-restart", 1, 2));
		verify(stopped.outcome() == MessageOutcome.FAILED, "Stopped agent accepted a message");
		start();
		verify(agentHealth(), "Agent did not report serving after restart");
		MessageResult first = send(new MessageCommand("three", prefix + "-restart", 1, 2));
		MessageResult second = send(new MessageCommand("four", prefix + "-restart", 2, 2));
		verify(first.outcome() == MessageOutcome.WAITING, "Restarted group did not wait");
		verify(second.outcome() == MessageOutcome.RELEASED, "Restarted group was not released");
	}

	private static ScenarioCheck check(String name, Runnable action) {
		long started = System.nanoTime();
		try {
			action.run();
			return new ScenarioCheck(name, CheckState.PASSED, "Completed successfully", elapsed(started));
		}
		catch (RuntimeException ex) {
			return new ScenarioCheck(name, CheckState.FAILED, rootMessage(ex), elapsed(started));
		}
	}

	private static void validate(MessageCommand command) {
		if (command == null || command.payload() == null || command.payload().isBlank()) {
			throw new IllegalArgumentException("payload must not be blank");
		}
		if (command.payload().length() > 4_096) {
			throw new IllegalArgumentException("payload must not exceed 4096 characters");
		}
		if (command.correlationId() == null || command.correlationId().isBlank()) {
			throw new IllegalArgumentException("correlationId must not be blank");
		}
		if (command.correlationId().length() > 256) {
			throw new IllegalArgumentException("correlationId must not exceed 256 characters");
		}
		if (command.sequenceNumber() < 1 || command.sequenceSize() < 1) {
			throw new IllegalArgumentException("sequenceNumber and sequenceSize must be positive");
		}
		if (command.sequenceNumber() > command.sequenceSize()) {
			throw new IllegalArgumentException("sequenceNumber must not exceed sequenceSize");
		}
		if (command.sequenceSize() > 10_000) {
			throw new IllegalArgumentException("sequenceSize must not exceed 10000");
		}
	}

	private static long elapsed(long started) {
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
	}

	private static void verify(boolean condition, String message) {
		if (!condition) {
			throw new IllegalStateException(message);
		}
	}

	private static String rootMessage(Throwable throwable) {
		Throwable candidate = throwable;
		while (candidate.getCause() != null && candidate.getCause() != candidate) {
			candidate = candidate.getCause();
		}
		return candidate.getMessage() != null ? candidate.getMessage() : candidate.getClass().getSimpleName();
	}

}
