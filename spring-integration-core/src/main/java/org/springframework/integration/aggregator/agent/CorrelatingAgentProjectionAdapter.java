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

package org.springframework.integration.aggregator.agent;

import org.jspecify.annotations.Nullable;

import org.springframework.integration.aggregator.agent.grpc.AgentMessageProjection;
import org.springframework.messaging.Message;

/**
 * Strategy for creating an optional, application-defined message projection for a
 * correlating agent. A projection is informational only; the handler always applies the
 * agent decision to the original locally retained message.
 *
 * @author Mohamed Zayton
 *
 * @since 7.2
 */
@FunctionalInterface
public interface CorrelatingAgentProjectionAdapter {

	/**
	 * Create a projection for the supplied message.
	 * @param message the original message
	 * @return the projection, or {@code null} to omit it
	 */
	@Nullable
	AgentMessageProjection project(Message<?> message);

}
