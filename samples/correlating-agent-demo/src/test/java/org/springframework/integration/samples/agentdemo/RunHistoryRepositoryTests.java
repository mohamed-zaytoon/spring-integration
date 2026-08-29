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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.integration.samples.agentdemo.DemoModels.RunReport;
import org.springframework.integration.samples.agentdemo.DemoModels.RunState;
import org.springframework.integration.samples.agentdemo.DemoModels.Topology;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mohamed Zayton
 */
class RunHistoryRepositoryTests {

	@TempDir
	Path temporaryDirectory;

	@Test
	void atomicallyPersistsReloadsAndPrunesReports() throws Exception {
		RunHistoryRepository repository = repository(2);
		repository.load();
		repository.save(report("first", "2026-01-01T00:00:00Z"));
		repository.save(report("second", "2026-01-01T00:00:01Z"));
		repository.save(report("third", "2026-01-01T00:00:02Z"));

		assertThat(repository.recent()).extracting(RunReport::id).containsExactly("third", "second");
		try (Stream<Path> paths = Files.list(this.temporaryDirectory)) {
			assertThat(paths)
					.extracting(path -> path.getFileName().toString())
					.containsExactlyInAnyOrder("second.json", "third.json");
		}
		try (Stream<Path> paths = Files.list(this.temporaryDirectory)) {
			assertThat(paths).noneMatch(path -> path.toString().endsWith(".tmp"));
		}

		RunHistoryRepository reloaded = repository(2);
		reloaded.load();
		assertThat(reloaded.recent()).extracting(RunReport::id).containsExactly("third", "second");
	}

	@Test
	void ignoresMalformedHistoryFile() throws Exception {
		Files.writeString(this.temporaryDirectory.resolve("broken.json"), "{not-json");
		RunHistoryRepository repository = repository(10);

		repository.load();

		assertThat(repository.recent()).isEmpty();
	}

	private RunHistoryRepository repository(int retention) {
		DemoProperties properties = new DemoProperties();
		properties.setHistoryDirectory(this.temporaryDirectory);
		properties.setHistoryRetention(retention);
		return new RunHistoryRepository(JsonMapper.builder().findAndAddModules().build(), properties);
	}

	private static RunReport report(String id, String startedAt) {
		Instant started = Instant.parse(startedAt);
		return new RunReport(id, Topology.IN_PROCESS, RunState.PASSED, started, started.plusMillis(10),
				10, List.of(), null, "");
	}

}
