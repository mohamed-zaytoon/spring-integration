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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the standalone demo.
 *
 * @author Mohamed Zayton
 */
@ConfigurationProperties("demo")
public class DemoProperties {

	private Mode mode = Mode.DASHBOARD;

	private String host = "127.0.0.1";

	private String agentHost = "127.0.0.1";

	private String dependencyHost = "127.0.0.1";

	private int agentGrpcPort;

	private int dependencyGrpcPort;

	private Path historyDirectory = Path.of("data", "correlating-agent-demo", "runs");

	private int historyRetention = 100;

	private Duration startupTimeout = Duration.ofSeconds(15);

	private Duration processTimeout = Duration.ofSeconds(45);

	private int maxLogCharacters = 262_144;

	public Mode getMode() {
		return this.mode;
	}

	public void setMode(Mode mode) {
		this.mode = mode;
	}

	public String getHost() {
		return this.host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getAgentHost() {
		return this.agentHost;
	}

	public void setAgentHost(String agentHost) {
		this.agentHost = agentHost;
	}

	public String getDependencyHost() {
		return this.dependencyHost;
	}

	public void setDependencyHost(String dependencyHost) {
		this.dependencyHost = dependencyHost;
	}

	public int getAgentGrpcPort() {
		return this.agentGrpcPort;
	}

	public void setAgentGrpcPort(int agentGrpcPort) {
		this.agentGrpcPort = agentGrpcPort;
	}

	public int getDependencyGrpcPort() {
		return this.dependencyGrpcPort;
	}

	public void setDependencyGrpcPort(int dependencyGrpcPort) {
		this.dependencyGrpcPort = dependencyGrpcPort;
	}

	public Path getHistoryDirectory() {
		return this.historyDirectory;
	}

	public void setHistoryDirectory(Path historyDirectory) {
		this.historyDirectory = historyDirectory;
	}

	public int getHistoryRetention() {
		return this.historyRetention;
	}

	public void setHistoryRetention(int historyRetention) {
		this.historyRetention = historyRetention;
	}

	public Duration getStartupTimeout() {
		return this.startupTimeout;
	}

	public void setStartupTimeout(Duration startupTimeout) {
		this.startupTimeout = startupTimeout;
	}

	public Duration getProcessTimeout() {
		return this.processTimeout;
	}

	public void setProcessTimeout(Duration processTimeout) {
		this.processTimeout = processTimeout;
	}

	public int getMaxLogCharacters() {
		return this.maxLogCharacters;
	}

	public void setMaxLogCharacters(int maxLogCharacters) {
		this.maxLogCharacters = maxLogCharacters;
	}

	public enum Mode {

		DASHBOARD,

		AGENT,

		HANDLER

	}

}
