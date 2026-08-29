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

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * HTTP and persistence models used by the demo.
 *
 * @author Mohamed Zayton
 */
public final class DemoModels {

	private DemoModels() {
	}

	public enum Topology {
		IN_PROCESS,
		EXTERNAL,
		ALL
	}

	public enum RunState {
		RUNNING,
		PASSED,
		FAILED
	}

	public enum CheckState {
		PASSED,
		FAILED
	}

	public enum MessageOutcome {
		WAITING,
		RELEASED,
		DISCARDED,
		FAILED
	}

	public record RunRequest(Topology topology) {
	}

	public record MessageCommand(String payload, String correlationId, int sequenceNumber, int sequenceSize) {
	}

	public record ScenarioCheck(String name, CheckState state, String detail, long durationMillis) {
	}

	public record ScenarioResult(List<ScenarioCheck> checks) {
	}

	public record RunReport(String id, Topology topology, RunState state, Instant startedAt, Instant completedAt,
			long durationMillis, List<ScenarioCheck> checks, String failure, String logs) {
	}

	public record MessageResult(MessageOutcome outcome, Object payload, String detail) {
	}

	public record EnvironmentStatus(Topology topology, boolean available, boolean running, boolean agentServing,
			Map<String, Integer> ports, String detail) {
	}

	public record ApplicationInfo(String mode, String name, EnvironmentStatus inProcess,
			EnvironmentStatus external, String activeRunId, Map<String, String> endpoints) {
	}

}
