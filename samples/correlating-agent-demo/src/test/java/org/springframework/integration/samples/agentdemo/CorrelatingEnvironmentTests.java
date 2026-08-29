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

import org.springframework.integration.samples.agentdemo.DemoModels.CheckState;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageCommand;
import org.springframework.integration.samples.agentdemo.DemoModels.MessageOutcome;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * @author Mohamed Zayton
 */
class CorrelatingEnvironmentTests {

	@Test
	void exercisesAggregationExpirationAndLifecycle() {
		try (CorrelatingEnvironment environment = CorrelatingEnvironment.inProcess(Duration.ofSeconds(5))) {
			environment.start();

			assertThat(environment.runSuite())
					.hasSize(3)
					.allMatch(check -> check.state() == CheckState.PASSED);
		}
	}

	@Test
	void waitsThenReleasesOrderedPayloadsAndDiscardsCompletedGroupMessages() {
		try (CorrelatingEnvironment environment = CorrelatingEnvironment.inProcess(Duration.ofSeconds(5))) {
			environment.start();

			assertThat(environment.send(new MessageCommand("one", "custom", 1, 2)).outcome())
					.isEqualTo(MessageOutcome.WAITING);
			assertThat(environment.send(new MessageCommand("two", "custom", 2, 2)))
					.satisfies(result -> {
						assertThat(result.outcome()).isEqualTo(MessageOutcome.RELEASED);
						assertThat(result.payload()).isEqualTo(java.util.List.of("one", "two"));
					});
			assertThat(environment.send(new MessageCommand("late", "custom", 2, 2)).outcome())
					.isEqualTo(MessageOutcome.DISCARDED);
		}
	}

	@Test
	void validatesCustomMessageFields() {
		try (CorrelatingEnvironment environment = CorrelatingEnvironment.inProcess(Duration.ofSeconds(5))) {
			environment.start();

			assertThatIllegalArgumentException()
					.isThrownBy(() -> environment.send(new MessageCommand(" ", "group", 1, 2)))
					.withMessage("payload must not be blank");
			assertThatIllegalArgumentException()
					.isThrownBy(() -> environment.send(new MessageCommand("one", "group", 3, 2)))
					.withMessage("sequenceNumber must not exceed sequenceSize");
		}
	}

}
