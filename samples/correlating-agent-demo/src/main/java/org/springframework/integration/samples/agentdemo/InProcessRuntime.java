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

import java.util.Map;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.integration.samples.agentdemo.DemoModels.EnvironmentStatus;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageCommand;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageResult;
import org.springframework.stereotype.Component;

/**
 * Persistent in-process environment owned by the dashboard.
 *
 * @author Mohamed Zayton
 */
@Component
@ConditionalOnProperty(name = "demo.mode", havingValue = "dashboard", matchIfMissing = true)
final class InProcessRuntime {

	private final DemoProperties properties;

	private CorrelatingEnvironment environment;

	InProcessRuntime(DemoProperties properties) {
		this.properties = properties;
	}

	@PostConstruct
	synchronized void initialize() {
		reset();
	}

	synchronized MessageResult send(MessageCommand command) {
		return this.environment.send(command);
	}

	synchronized void start() {
		this.environment.start();
	}

	synchronized void stop() {
		this.environment.stop();
	}

	synchronized void reset() {
		if (this.environment != null) {
			this.environment.close();
		}
		this.environment = CorrelatingEnvironment.inProcess(this.properties.getStartupTimeout());
		this.environment.start();
	}

	synchronized EnvironmentStatus status() {
		return this.environment.status(Map.of());
	}

	@PreDestroy
	synchronized void close() {
		if (this.environment != null) {
			this.environment.close();
		}
	}

}
