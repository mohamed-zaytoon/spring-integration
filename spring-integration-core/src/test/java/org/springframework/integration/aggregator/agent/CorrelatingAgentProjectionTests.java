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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import org.springframework.integration.aggregator.AggregatingMessageHandler;
import org.springframework.integration.aggregator.DefaultAggregatingMessageGroupProcessor;
import org.springframework.integration.aggregator.agent.grpc.AgentMessageProjection;
import org.springframework.integration.aggregator.agent.grpc.CorrelatingAgentPortGrpc;
import org.springframework.integration.aggregator.agent.grpc.DecisionOutcome;
import org.springframework.integration.aggregator.agent.grpc.HandleMessageRequest;
import org.springframework.integration.aggregator.agent.grpc.HandleMessageResponse;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.store.MessageGroup;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Tests for opt-in correlating agents and opaque message projections.
 *
 * @author Mohamed Zayton
 *
 * @since 7.2
 */
class CorrelatingAgentProjectionTests {

	@Test
	void defaultHandlerDoesNotInvokeProjectionAdapter() {
		AggregatingMessageHandler handler =
				new AggregatingMessageHandler(new DefaultAggregatingMessageGroupProcessor());
		handler.setCorrelatingAgentProjectionAdapter(message -> {
			throw new AssertionError("The adapter must not be invoked unless agent processing is enabled");
		});
		QueueChannel replies = new QueueChannel();
		Object correlationKey = new Object();
		NonSerializablePayload one = new NonSerializablePayload("one", new Object());
		NonSerializablePayload two = new NonSerializablePayload("two", new Object());

		handler.handleMessage(message(one, correlationKey, 1, replies));
		handler.handleMessage(message(two, correlationKey, 2, replies));

		assertThat(replies.receive(0).getPayload()).isEqualTo(List.of(one, two));
	}

	@Test
	void sendsOpaqueProjectionAndAllowsItToBeOmitted() throws IOException {
		RecordingAgent agent = new RecordingAgent();
		try (AgentFixture fixture = new AgentFixture(agent)) {
			AggregatingMessageHandler handler = fixture.handler();
			handler.setCorrelatingAgentProjectionAdapter(message -> {
				if ("omit".equals(((NonSerializablePayload) message.getPayload()).name())) {
					return null;
				}
				return AgentMessageProjection.newBuilder()
						.setType("test-payload")
						.setContentType("application/octet-stream")
						.setSchemaVersion(3)
						.setData(ByteString.copyFromUtf8("projected"))
						.build();
			});

			handler.handleMessage(message(new NonSerializablePayload("project", new Object()), new Object(), 1,
					new QueueChannel()));
			handler.handleMessage(message(new NonSerializablePayload("omit", new Object()), new Object(), 1,
					new QueueChannel()));

			assertThat(agent.requests).hasSize(2);
			AgentMessageProjection projection = agent.requests.get(0).getProjection();
			assertThat(projection.getType()).isEqualTo("test-payload");
			assertThat(projection.getContentType()).isEqualTo("application/octet-stream");
			assertThat(projection.getSchemaVersion()).isEqualTo(3);
			assertThat(projection.getData().toStringUtf8()).isEqualTo("projected");
			assertThat(agent.requests.get(1).hasProjection()).isFalse();
		}
	}

	@Test
	void rejectsInvalidProjectionMetadata() throws IOException {
		try (AgentFixture fixture = new AgentFixture(new RecordingAgent())) {
			AggregatingMessageHandler handler = fixture.handler();
			handler.setCorrelatingAgentProjectionAdapter(message -> AgentMessageProjection.getDefaultInstance());

			assertThatExceptionOfType(MessageHandlingException.class)
					.isThrownBy(() -> handler.handleMessage(message(
							new NonSerializablePayload("test", new Object()), new Object(), 1, new QueueChannel())))
					.withMessageContaining("Correlating agent invocation failed")
					.withCauseInstanceOf(IllegalArgumentException.class)
					.withStackTraceContaining("A correlating agent projection must declare a type");
		}
	}

	@Test
	void reportsProjectionAdapterFailure() throws IOException {
		try (AgentFixture fixture = new AgentFixture(new RecordingAgent())) {
			AggregatingMessageHandler handler = fixture.handler();
			handler.setCorrelatingAgentProjectionAdapter(message -> {
				throw new IllegalStateException("projection failed");
			});

			assertThatExceptionOfType(MessageHandlingException.class)
					.isThrownBy(() -> handler.handleMessage(message(
							new NonSerializablePayload("test", new Object()), new Object(), 1, new QueueChannel())))
					.withMessageContaining("Correlating agent invocation failed")
					.withCauseInstanceOf(IllegalStateException.class)
					.withStackTraceContaining("projection failed");
		}
	}

	@Test
	void forceCompleteAcceptsNonSerializableGroupId() {
		SimpleMessageStore store = new SimpleMessageStore();
		TestAggregatingMessageHandler handler = new TestAggregatingMessageHandler(store);
		handler.setCorrelatingAgentEnabled(true);
		handler.setDiscardChannel(new QueueChannel());
		Object correlationKey = new Object();
		store.addMessageToGroup(correlationKey, message(
				new NonSerializablePayload("test", new Object()), correlationKey, 1, new QueueChannel()));

		try {
			handler.force(store.getMessageGroup(correlationKey));

			assertThat(store.getMessageGroup(correlationKey).size()).isZero();
		}
		finally {
			handler.destroy();
		}
	}

	private static Message<NonSerializablePayload> message(NonSerializablePayload payload, Object correlationKey,
			int sequenceNumber, QueueChannel replies) {

		return MessageBuilder.withPayload(payload)
				.setCorrelationId(correlationKey)
				.setSequenceNumber(sequenceNumber)
				.setSequenceSize(2)
				.setReplyChannel(replies)
				.build();
	}

	private record NonSerializablePayload(String name, Object resource) {
	}

	private static final class RecordingAgent extends CorrelatingAgentPortGrpc.CorrelatingAgentPortImplBase {

		private final List<HandleMessageRequest> requests = new ArrayList<>();

		@Override
		public void handleMessage(HandleMessageRequest request,
				StreamObserver<HandleMessageResponse> responseObserver) {

			this.requests.add(request);
			responseObserver.onNext(HandleMessageResponse.newBuilder()
					.setOutcome(DecisionOutcome.WAITING)
					.setAttempts(1)
					.build());
			responseObserver.onCompleted();
		}

	}

	private static final class TestAggregatingMessageHandler extends AggregatingMessageHandler {

		TestAggregatingMessageHandler(SimpleMessageStore store) {
			super(new DefaultAggregatingMessageGroupProcessor(), store);
		}

		void force(MessageGroup group) {
			forceComplete(group);
		}

	}

	private static final class AgentFixture implements AutoCloseable {

		private final Server server;

		private final ManagedChannel channel;

		private final AggregatingMessageHandler handler =
				new AggregatingMessageHandler(new DefaultAggregatingMessageGroupProcessor());

		AgentFixture(RecordingAgent agent) throws IOException {
			String serverName = InProcessServerBuilder.generateName();
			this.server = InProcessServerBuilder.forName(serverName).directExecutor().addService(agent).build().start();
			this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
			this.handler.setCorrelatingAgentChannel(this.channel);
		}

		AggregatingMessageHandler handler() {
			return this.handler;
		}

		@Override
		public void close() {
			this.handler.destroy();
			this.channel.shutdownNow();
			this.server.shutdownNow();
		}

	}

}
