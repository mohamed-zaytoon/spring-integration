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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Mohamed Zayton
 */
@SpringBootTest(properties = "demo.history-directory=build/test-history/dashboard")
class DashboardApplicationTests {

	@Autowired
	WebApplicationContext applicationContext;

	MockMvc mockMvc;

	@BeforeEach
	void setUp() {
		this.mockMvc = MockMvcBuilders.webAppContextSetup(this.applicationContext).build();
	}

	@Test
	void servesDashboardAndApplicationStatus() throws Exception {
		this.mockMvc.perform(get("/index.html"))
				.andExpect(status().isOk())
				.andExpect(content().string(org.hamcrest.Matchers.containsString("Correlating Agent Lab")));
		this.mockMvc.perform(get("/api/application"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.mode").value("dashboard"))
				.andExpect(jsonPath("$.inProcess.agentServing").value(true));
	}

	@Test
	void rejectsInvalidCustomMessage() throws Exception {
		this.mockMvc.perform(post("/api/environments/IN_PROCESS/messages")
				.contentType(MediaType.APPLICATION_JSON)
				.content("""
						{"payload":"one","correlationId":"group","sequenceNumber":3,"sequenceSize":2}
						"""))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.error").value("sequenceNumber must not exceed sequenceSize"));
	}

}
