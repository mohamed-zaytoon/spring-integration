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

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.integration.samples.agentdemo.DemoModels.EnvironmentStatus;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageCommand;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageResult;
import org.springframework.integration.samples.agentdemo.DemoModels.ScenarioResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Loopback control API exposed by a handler process.
 *
 * @author Mohamed Zayton
 */
@RestController
@RequestMapping("/api/internal")
@ConditionalOnProperty(name = "demo.mode", havingValue = "handler")
final class HandlerApiController {

	private final HandlerModeRuntime runtime;

	private final ConfigurableApplicationContext applicationContext;

	HandlerApiController(HandlerModeRuntime runtime, ConfigurableApplicationContext applicationContext) {
		this.runtime = runtime;
		this.applicationContext = applicationContext;
	}

	@GetMapping("/status")
	EnvironmentStatus status() {
		return this.runtime.status();
	}

	@PostMapping("/message")
	MessageResult message(@RequestBody MessageCommand command) {
		return this.runtime.send(command);
	}

	@PostMapping("/scenario")
	ScenarioResult scenario() {
		return new ScenarioResult(this.runtime.runSuite());
	}

	@PostMapping("/lifecycle/start")
	EnvironmentStatus start() {
		this.runtime.start();
		return this.runtime.status();
	}

	@PostMapping("/lifecycle/stop")
	EnvironmentStatus stop() {
		this.runtime.stop();
		return this.runtime.status();
	}

	@PostMapping("/reset")
	EnvironmentStatus reset() throws Exception {
		this.runtime.reset();
		return this.runtime.status();
	}

	@PostMapping("/shutdown")
	ResponseEntity<Void> shutdown() {
		Thread shutdown = new Thread(this.applicationContext::close, "correlating-agent-demo-shutdown");
		shutdown.setDaemon(false);
		shutdown.start();
		return ResponseEntity.accepted().build();
	}

}
