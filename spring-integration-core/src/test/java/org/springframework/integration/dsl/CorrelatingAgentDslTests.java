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

package org.springframework.integration.dsl;

import org.junit.jupiter.api.Test;

import org.springframework.integration.aggregator.AbstractCorrelatingMessageHandler;
import org.springframework.integration.aggregator.agent.CorrelatingAgentProjectionAdapter;
import org.springframework.integration.test.util.TestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for correlating-agent DSL configuration.
 *
 * @author Mohamed Zayton
 *
 * @since 7.2
 */
class CorrelatingAgentDslTests {

	@Test
	void configuresOptInAgentAndProjectionAdapter() {
		CorrelatingAgentProjectionAdapter projectionAdapter = message -> null;
		AggregatorSpec spec = new AggregatorSpec()
				.correlatingAgent()
				.correlatingAgentProjectionAdapter(projectionAdapter);
		AbstractCorrelatingMessageHandler handler = TestUtils.getPropertyValue(spec, "handler");

		assertThat(TestUtils.<Boolean>getPropertyValue(handler, "correlatingAgentEnabled")).isTrue();
		assertThat(TestUtils.<Object>getPropertyValue(handler, "correlatingAgentProjectionAdapter"))
				.isSameAs(projectionAdapter);
	}

}
