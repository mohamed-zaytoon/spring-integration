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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.integration.aggregator.agent.EmbabelCorrelatingAgentService;
import org.springframework.stereotype.Component;

/**
 * Headless gRPC server used by the external agent process.
 *
 * @author Mohamed Zayton
 */
@Component
@ConditionalOnProperty(name = "demo.mode", havingValue = "agent")
final class AgentModeServer implements ApplicationRunner {

	private final DemoProperties properties;

	private ManagedChannel dependencyChannel;

	private Server server;

	AgentModeServer(DemoProperties properties) {
		this.properties = properties;
	}

	@PostConstruct
	void start() throws Exception {
		this.dependencyChannel = NettyChannelBuilder
				.forAddress(this.properties.getDependencyHost(), this.properties.getDependencyGrpcPort())
				.usePlaintext()
				.build();
		this.server = NettyServerBuilder.forAddress(new InetSocketAddress(
				InetAddress.getByName(this.properties.getHost()), this.properties.getAgentGrpcPort()))
				.addService(new EmbabelCorrelatingAgentService(this.dependencyChannel))
				.build()
				.start();
		ReadinessMarker.emit("READY agent grpc=" + this.server.getPort());
	}

	@Override
	public void run(ApplicationArguments arguments) throws Exception {
		this.server.awaitTermination();
	}

	@PreDestroy
	void stop() {
		if (this.server != null) {
			this.server.shutdownNow();
			await(this.server);
		}
		if (this.dependencyChannel != null) {
			this.dependencyChannel.shutdownNow();
			await(this.dependencyChannel);
		}
	}

	private static void await(Server server) {
		try {
			server.awaitTermination(5, TimeUnit.SECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	private static void await(ManagedChannel channel) {
		try {
			channel.awaitTermination(5, TimeUnit.SECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

}
