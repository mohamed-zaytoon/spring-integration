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

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.integration.samples.agentdemo.DemoModels.RunReport;
import org.springframework.stereotype.Component;

/**
 * Atomic JSON-file history for completed scenario runs.
 *
 * @author Mohamed Zayton
 */
@Component
@ConditionalOnProperty(name = "demo.mode", havingValue = "dashboard", matchIfMissing = true)
final class RunHistoryRepository {

	private static final Logger LOGGER = LoggerFactory.getLogger(RunHistoryRepository.class);

	private final ObjectMapper objectMapper;

	private final Path directory;

	private final int retention;

	private final Map<String, RunReport> reports = new LinkedHashMap<>();

	RunHistoryRepository(ObjectMapper objectMapper, DemoProperties properties) {
		this.objectMapper = objectMapper;
		this.directory = properties.getHistoryDirectory().toAbsolutePath().normalize();
		this.retention = Math.max(1, properties.getHistoryRetention());
	}

	@PostConstruct
	synchronized void load() throws IOException {
		Files.createDirectories(this.directory);
		try (Stream<Path> paths = Files.list(this.directory)) {
			paths.filter(path -> path.getFileName().toString().endsWith(".json"))
					.sorted(Comparator.comparing(Path::getFileName))
					.forEach(this::loadOne);
		}
		prune();
	}

	synchronized void save(RunReport report) {
		this.reports.put(report.id(), report);
		Path target = this.directory.resolve(report.id() + ".json");
		Path temporary = this.directory.resolve(report.id() + ".tmp");
		try {
			this.objectMapper.writerWithDefaultPrettyPrinter().writeValue(temporary.toFile(), report);
			try {
				Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			}
			catch (AtomicMoveNotSupportedException ex) {
				Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
			}
			prune();
		}
		catch (IOException ex) {
			throw new IllegalStateException("Failed to persist run report " + report.id(), ex);
		}
		finally {
			try {
				Files.deleteIfExists(temporary);
			}
			catch (IOException ex) {
				LOGGER.debug("Could not remove temporary history file {}", temporary, ex);
			}
		}
	}

	synchronized List<RunReport> recent() {
		List<RunReport> result = new ArrayList<>(this.reports.values());
		result.sort(Comparator.comparing(RunReport::startedAt).reversed());
		return List.copyOf(result);
	}

	synchronized Optional<RunReport> find(String id) {
		return Optional.ofNullable(this.reports.get(id));
	}

	Path directory() {
		return this.directory;
	}

	private void loadOne(Path path) {
		try {
			RunReport report = this.objectMapper.readValue(path.toFile(), RunReport.class);
			this.reports.put(report.id(), report);
		}
		catch (Exception ex) {
			LOGGER.warn("Ignoring malformed run history file {}: {}", path, ex.getMessage());
		}
	}

	private void prune() throws IOException {
		List<RunReport> ordered = recent();
		for (int index = this.retention; index < ordered.size(); index++) {
			RunReport removed = ordered.get(index);
			this.reports.remove(removed.id());
			Files.deleteIfExists(this.directory.resolve(removed.id() + ".json"));
		}
	}

}
