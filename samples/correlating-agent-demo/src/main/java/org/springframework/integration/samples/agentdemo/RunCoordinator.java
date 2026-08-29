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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.annotation.PreDestroy;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.integration.samples.agentdemo.DemoModels.CheckState;
import org.springframework.integration.samples.agentdemo.DemoModels.RunReport;
import org.springframework.integration.samples.agentdemo.DemoModels.RunState;
import org.springframework.integration.samples.agentdemo.DemoModels.ScenarioCheck;
import org.springframework.integration.samples.agentdemo.DemoModels.Topology;
import org.springframework.stereotype.Component;

/**
 * Runs one asynchronous scenario suite at a time.
 *
 * @author Mohamed Zayton
 */
@Component
@ConditionalOnProperty(name = "demo.mode", havingValue = "dashboard", matchIfMissing = true)
final class RunCoordinator {

	private final DemoProperties properties;

	private final RunHistoryRepository history;

	private final ExternalProcessOrchestrator external;

	private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
		Thread thread = new Thread(runnable, "correlating-agent-demo-runner");
		thread.setDaemon(true);
		return thread;
	});

	private final AtomicReference<RunReport> active = new AtomicReference<>();

	RunCoordinator(DemoProperties properties, RunHistoryRepository history,
			ExternalProcessOrchestrator external) {

		this.properties = properties;
		this.history = history;
		this.external = external;
	}

	RunReport start(Topology topology) {
		Topology selected = topology != null ? topology : Topology.ALL;
		RunReport report = new RunReport(UUID.randomUUID().toString(), selected, RunState.RUNNING,
				Instant.now(), null, 0, List.of(), null, "");
		if (!this.active.compareAndSet(null, report)) {
			throw new RunConflictException("Another scenario run is already active");
		}
		this.executor.execute(() -> execute(report));
		return report;
	}

	List<RunReport> recent() {
		List<RunReport> result = new ArrayList<>(this.history.recent());
		RunReport current = this.active.get();
		if (current != null) {
			result.add(current);
		}
		result.sort(Comparator.comparing(RunReport::startedAt).reversed());
		return List.copyOf(result);
	}

	Optional<RunReport> find(String id) {
		RunReport current = this.active.get();
		if (current != null && current.id().equals(id)) {
			return Optional.of(current);
		}
		return this.history.find(id);
	}

	String activeRunId() {
		RunReport current = this.active.get();
		return current != null ? current.id() : null;
	}

	@PreDestroy
	void close() {
		this.executor.shutdownNow();
	}

	private void execute(RunReport running) {
		List<ScenarioCheck> checks = new ArrayList<>();
		String logs = "";
		String failure = null;
		try {
			if (running.topology() == Topology.IN_PROCESS || running.topology() == Topology.ALL) {
				checks.addAll(prefix("in-process", runInProcess()));
			}
			if (running.topology() == Topology.EXTERNAL || running.topology() == Topology.ALL) {
				ExternalProcessOrchestrator.ExternalRun externalRun = this.external.runSuite();
				checks.addAll(prefix("external", externalRun.checks()));
				logs = externalRun.logs();
			}
		}
		catch (RuntimeException ex) {
			failure = rootMessage(ex);
		}
		Instant completedAt = Instant.now();
		boolean passed = failure == null && checks.stream().allMatch(check -> check.state() == CheckState.PASSED);
		RunReport completed = new RunReport(running.id(), running.topology(),
				passed ? RunState.PASSED : RunState.FAILED, running.startedAt(), completedAt,
				Duration.between(running.startedAt(), completedAt).toMillis(), List.copyOf(checks), failure, logs);
		try {
			this.history.save(completed);
		}
		finally {
			this.active.compareAndSet(running, null);
		}
	}

	private List<ScenarioCheck> runInProcess() {
		try (CorrelatingEnvironment environment =
				CorrelatingEnvironment.inProcess(this.properties.getStartupTimeout())) {

			environment.start();
			return environment.runSuite();
		}
	}

	private static List<ScenarioCheck> prefix(String topology, List<ScenarioCheck> checks) {
		return checks.stream()
				.map(check -> new ScenarioCheck(topology + " — " + check.name(), check.state(), check.detail(),
						check.durationMillis()))
				.toList();
	}

	private static String rootMessage(Throwable throwable) {
		Throwable candidate = throwable;
		while (candidate.getCause() != null && candidate.getCause() != candidate) {
			candidate = candidate.getCause();
		}
		return candidate.getMessage() != null ? candidate.getMessage() : candidate.getClass().getSimpleName();
	}

}
