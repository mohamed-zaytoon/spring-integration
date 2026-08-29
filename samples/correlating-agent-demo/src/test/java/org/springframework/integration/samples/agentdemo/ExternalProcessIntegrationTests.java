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

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.integration.samples.agentdemo.DemoModels.CheckState;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mohamed Zayton
 */
class ExternalProcessIntegrationTests {

	@Test
	void startsSeparateJvmsRunsScenarioCapturesDiagnosticsAndCleansUp() {
		DemoProperties properties = new DemoProperties();
		properties.setStartupTimeout(Duration.ofSeconds(30));
		properties.setProcessTimeout(Duration.ofSeconds(30));
		ExternalProcessOrchestrator orchestrator = new ExternalProcessOrchestrator(properties,
				JsonMapper.builder().findAndAddModules().build());
		try {
			ExternalProcessOrchestrator.ExternalRun run = orchestrator.runSuite();

			assertThat(run.checks()).hasSize(3).allMatch(check -> check.state() == CheckState.PASSED);
			assertThat(run.logs()).contains("READY agent grpc=", "READY handler http=");
			assertThat(orchestrator.status().available()).isFalse();
		}
		finally {
			orchestrator.close();
		}
	}

}
