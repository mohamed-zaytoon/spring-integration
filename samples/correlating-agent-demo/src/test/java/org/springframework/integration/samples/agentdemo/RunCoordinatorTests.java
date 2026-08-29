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

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.integration.samples.agentdemo.DemoModels.CheckState;
import org.springframework.integration.samples.agentdemo.DemoModels.RunState;
import org.springframework.integration.samples.agentdemo.DemoModels.ScenarioCheck;
import org.springframework.integration.samples.agentdemo.DemoModels.Topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Mohamed Zayton
 */
class RunCoordinatorTests {

	@TempDir
	Path temporaryDirectory;

	@Test
	void rejectsConcurrentRunsAndPublishesAsynchronousCompletion() throws Exception {
		DemoProperties properties = new DemoProperties();
		properties.setHistoryDirectory(this.temporaryDirectory);
		RunHistoryRepository history = new RunHistoryRepository(
				JsonMapper.builder().findAndAddModules().build(), properties);
		history.load();
		ExternalProcessOrchestrator external = mock(ExternalProcessOrchestrator.class);
		CountDownLatch release = new CountDownLatch(1);
		when(external.runSuite()).thenAnswer(invocation -> {
			release.await(10, TimeUnit.SECONDS);
			return new ExternalProcessOrchestrator.ExternalRun(
					List.of(new ScenarioCheck("external", CheckState.PASSED, "ok", 1)), "logs");
		});
		RunCoordinator coordinator = new RunCoordinator(properties, history, external);
		try {
			String id = coordinator.start(Topology.EXTERNAL).id();
			assertThatThrownBy(() -> coordinator.start(Topology.IN_PROCESS))
					.isInstanceOf(RunConflictException.class);
			release.countDown();

			awaitCompletion(coordinator, id);

			assertThat(coordinator.find(id)).hasValueSatisfying(report -> {
				assertThat(report.state()).isEqualTo(RunState.PASSED);
				assertThat(report.logs()).isEqualTo("logs");
			});
		}
		finally {
			release.countDown();
			coordinator.close();
		}
	}

	private static void awaitCompletion(RunCoordinator coordinator, String id) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		while (coordinator.find(id).orElseThrow().state() == RunState.RUNNING && System.nanoTime() < deadline) {
			Thread.sleep(20);
		}
		assertThat(coordinator.find(id).orElseThrow().state()).isNotEqualTo(RunState.RUNNING);
	}

}
