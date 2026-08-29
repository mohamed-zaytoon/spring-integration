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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.integration.samples.agentdemo.DemoModels.EnvironmentStatus;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageCommand;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageResult;
import org.springframework.stereotype.Component;

/**
 * Long-running handler process with its dependency gRPC server.
 *
 * @author Mohamed Zayton
 */
@Component
@ConditionalOnProperty(name = "demo.mode", havingValue = "handler")
final class HandlerModeRuntime {

	private final DemoProperties properties;

	private CorrelatingEnvironment environment;

	private Server dependencyServer;

	HandlerModeRuntime(DemoProperties properties) {
		this.properties = properties;
	}

	@PostConstruct
	synchronized void initialize() throws Exception {
		startEnvironment();
	}

	@EventListener(ApplicationReadyEvent.class)
	void ready(ApplicationReadyEvent event) {
		int httpPort = ((WebServerApplicationContext) event.getApplicationContext()).getWebServer().getPort();
		ReadinessMarker.emit("READY handler http=" + httpPort + " dependency=" + this.dependencyServer.getPort());
	}

	synchronized MessageResult send(MessageCommand command) {
		return this.environment.send(command);
	}

	synchronized java.util.List<DemoModels.ScenarioCheck> runSuite() {
		return this.environment.runSuite();
	}

	synchronized void start() {
		this.environment.start();
	}

	synchronized void stop() {
		this.environment.stop();
	}

	synchronized void reset() throws Exception {
		closeEnvironment();
		startEnvironment();
	}

	synchronized EnvironmentStatus status() {
		return this.environment.status(Map.of(
				"agentGrpc", this.properties.getAgentGrpcPort(),
				"dependencyGrpc", this.dependencyServer.getPort()));
	}

	@PreDestroy
	synchronized void close() {
		closeEnvironment();
	}

	private void startEnvironment() throws Exception {
		this.environment = CorrelatingEnvironment.external(this.properties.getAgentHost(),
				this.properties.getAgentGrpcPort(), this.properties.getStartupTimeout());
		this.dependencyServer = NettyServerBuilder.forAddress(new InetSocketAddress(
				InetAddress.getByName(this.properties.getHost()), this.properties.getDependencyGrpcPort()))
				.addService(this.environment.dependencyPort())
				.build()
				.start();
		this.environment.start();
	}

	private void closeEnvironment() {
		if (this.dependencyServer != null) {
			this.dependencyServer.shutdownNow();
			try {
				this.dependencyServer.awaitTermination(5, TimeUnit.SECONDS);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			this.dependencyServer = null;
		}
		if (this.environment != null) {
			this.environment.close();
			this.environment = null;
		}
	}

}
