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

import org.junit.jupiter.api.Test;

import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Mohamed Zayton
 */
class DashboardApiControllerTests {

	@Test
	void mapsConcurrentRunConflictToHttp409() throws Exception {
		RunCoordinator runs = mock(RunCoordinator.class);
		when(runs.start(DemoModels.Topology.ALL))
				.thenThrow(new RunConflictException("Another scenario run is already active"));
		DashboardApiController controller = new DashboardApiController(
				mock(InProcessRuntime.class), mock(ExternalProcessOrchestrator.class), runs);
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller)
				.setControllerAdvice(new DemoExceptionHandler())
				.build();

		mockMvc.perform(post("/api/runs")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"topology\":\"ALL\"}"))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.error").value("Another scenario run is already active"));
	}

}
