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

import java.util.List;
import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.integration.samples.agentdemo.DemoModels.ApplicationInfo;
import org.springframework.integration.samples.agentdemo.DemoModels.EnvironmentStatus;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageCommand;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageResult;
import org.springframework.integration.samples.agentdemo.DemoModels.RunReport;
import org.springframework.integration.samples.agentdemo.DemoModels.RunRequest;
import org.springframework.integration.samples.agentdemo.DemoModels.Topology;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Public loopback API used by the dashboard.
 *
 * @author Mohamed Zayton
 */
@RestController
@RequestMapping("/api")
@ConditionalOnProperty(name = "demo.mode", havingValue = "dashboard", matchIfMissing = true)
final class DashboardApiController {

	private final InProcessRuntime inProcess;

	private final ExternalProcessOrchestrator external;

	private final RunCoordinator runs;

	DashboardApiController(InProcessRuntime inProcess, ExternalProcessOrchestrator external, RunCoordinator runs) {
		this.inProcess = inProcess;
		this.external = external;
		this.runs = runs;
	}

	@GetMapping("/application")
	ApplicationInfo application() {
		return new ApplicationInfo("dashboard", "Correlating Agent Lab", this.inProcess.status(),
				this.external.status(), this.runs.activeRunId(), Map.of(
						"runs", "/api/runs",
						"inProcess", "/api/environments/IN_PROCESS",
						"external", "/api/environments/EXTERNAL"));
	}

	@PostMapping("/runs")
	ResponseEntity<RunReport> run(@RequestBody RunRequest request) {
		return ResponseEntity.accepted().body(this.runs.start(request != null ? request.topology() : Topology.ALL));
	}

	@GetMapping("/runs")
	List<RunReport> runs() {
		return this.runs.recent();
	}

	@GetMapping("/runs/{id}")
	ResponseEntity<RunReport> run(@PathVariable String id) {
		return this.runs.find(id).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
	}

	@GetMapping("/environments/{topology}")
	EnvironmentStatus environment(@PathVariable Topology topology) {
		return status(topology);
	}

	@PostMapping("/environments/{topology}/start")
	EnvironmentStatus start(@PathVariable Topology topology) {
		if (topology == Topology.IN_PROCESS) {
			this.inProcess.start();
			return this.inProcess.status();
		}
		return externalOnly(topology, this.external::startInteractive);
	}

	@PostMapping("/environments/{topology}/stop")
	EnvironmentStatus stop(@PathVariable Topology topology) {
		if (topology == Topology.IN_PROCESS) {
			this.inProcess.stop();
			return this.inProcess.status();
		}
		return externalOnly(topology, this.external::stopInteractive);
	}

	@PostMapping("/environments/{topology}/reset")
	EnvironmentStatus reset(@PathVariable Topology topology) {
		if (topology == Topology.IN_PROCESS) {
			this.inProcess.reset();
			return this.inProcess.status();
		}
		return externalOnly(topology, this.external::resetInteractive);
	}

	@PostMapping("/environments/{topology}/messages")
	MessageResult message(@PathVariable Topology topology, @RequestBody MessageCommand command) {
		if (topology == Topology.IN_PROCESS) {
			return this.inProcess.send(command);
		}
		if (topology == Topology.EXTERNAL) {
			return this.external.send(command);
		}
		throw new IllegalArgumentException("ALL is not a message topology");
	}

	@PostMapping("/environments/{topology}/lifecycle/{action}")
	EnvironmentStatus lifecycle(@PathVariable Topology topology, @PathVariable String action) {
		if (!List.of("start", "stop").contains(action)) {
			throw new IllegalArgumentException("action must be start or stop");
		}
		boolean start = "start".equals(action);
		if (topology == Topology.IN_PROCESS) {
			if (start) {
				this.inProcess.start();
			}
			else {
				this.inProcess.stop();
			}
			return this.inProcess.status();
		}
		return externalOnly(topology, () -> this.external.lifecycle(start));
	}

	private EnvironmentStatus status(Topology topology) {
		if (topology == Topology.IN_PROCESS) {
			return this.inProcess.status();
		}
		return externalOnly(topology, this.external::status);
	}

	private static EnvironmentStatus externalOnly(Topology topology,
			java.util.function.Supplier<EnvironmentStatus> operation) {

		if (topology != Topology.EXTERNAL) {
			throw new IllegalArgumentException("ALL is not an environment topology");
		}
		return operation.get();
	}

}
