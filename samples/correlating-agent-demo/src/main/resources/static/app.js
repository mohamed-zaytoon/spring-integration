(() => {
	"use strict";

	const state = { mode: "dashboard", activeRunId: null, latestRun: null, busy: false };
	const $ = (selector) => document.querySelector(selector);
	const $$ = (selector) => [...document.querySelectorAll(selector)];

	async function request(path, options = {}) {
		const response = await fetch(path, {
			...options,
			headers: options.body ? { "Content-Type": "application/json", ...(options.headers || {}) } : options.headers
		});
		const text = await response.text();
		const body = text ? JSON.parse(text) : null;
		if (!response.ok) {
			throw new Error(body?.detail || body?.error || `Request failed with HTTP ${response.status}`);
		}
		return body;
	}

	function setStatus(prefix, status) {
		const pill = $(`#${prefix}-status`);
		const detail = $(`#${prefix}-detail`);
		if (!pill || !status) return;
		const healthy = status.available && status.running && status.agentServing;
		pill.textContent = healthy ? "Serving" : status.running ? "Degraded" : status.available ? "Stopped" : "Offline";
		pill.className = `status-pill ${healthy ? "online" : status.running ? "failed" : ""}`;
		const ports = Object.entries(status.ports || {}).map(([name, port]) => `${name} ${port}`).join(" · ");
		detail.textContent = [status.detail, ports].filter(Boolean).join(" · ");
	}

	async function loadApplication() {
		try {
			const app = await request("/api/application");
			state.mode = app.mode;
			state.activeRunId = app.activeRunId;
			document.body.classList.toggle("handler-mode", app.mode === "handler");
			if (app.mode === "handler") {
				$("#message-topology").value = "EXTERNAL";
				$('#message-topology option[value="IN_PROCESS"]').disabled = true;
				$('#external-card [data-action="start"]').textContent = "Start agent";
				$('#external-card [data-action="stop"]').textContent = "Stop agent";
			}
			$("#runtime-mode").textContent = app.mode === "handler" ? "Manual handler" : "Dashboard";
			$("#connection-status").textContent = app.mode === "handler"
				? "Connected to a manually launched handler process."
				: "HTTP and gRPC endpoints are bound to loopback.";
			setStatus("in-process", app.inProcess);
			setStatus("external", app.external);
			updateRunState();
		}
		catch (error) {
			$("#connection-status").textContent = `Unavailable: ${error.message}`;
		}
	}

	async function loadRuns() {
		if (state.mode !== "dashboard") return;
		try {
			const runs = await request("/api/runs");
			renderHistory(runs);
			if (state.activeRunId) {
				state.latestRun = await request(`/api/runs/${state.activeRunId}`);
			}
			else if (runs.length) {
				state.latestRun = runs[0];
			}
			renderRun(state.latestRun);
		}
		catch (error) {
			$("#run-summary").textContent = error.message;
		}
	}

	function updateRunState() {
		const running = Boolean(state.activeRunId);
		$("#active-run-badge").textContent = running ? "Running" : "Idle";
		$("#active-run-badge").className = `status-pill ${running ? "running" : ""}`;
		$("#run-progress").classList.toggle("active", running);
		$$('.run-suite').forEach((button) => button.disabled = running || state.busy);
	}

	function renderRun(run) {
		if (!run) return;
		const running = run.state === "RUNNING";
		$("#run-summary").textContent = running
			? `${label(run.topology)} suite is running…`
			: `${label(run.topology)} ${run.state.toLowerCase()} in ${formatDuration(run.durationMillis)}.`;
		const checks = run.checks || [];
		$("#check-list").innerHTML = checks.length ? checks.map((check) => `
			<div class="check ${check.state === "FAILED" ? "failed" : ""}">
				<span class="check-mark" aria-hidden="true">${check.state === "PASSED" ? "✓" : "!"}</span>
				<div><strong>${escapeHtml(check.name)}</strong><small>${escapeHtml(check.detail)}</small></div>
				<time>${formatDuration(check.durationMillis)}</time>
			</div>`).join("") : '<p class="empty">Checks are starting…</p>';
		$("#child-logs").textContent = run.logs || run.failure || "No child logs captured.";
		if (run.logs || run.failure) $("#logs-panel").open = run.state === "FAILED";
	}

	function renderHistory(runs) {
		$("#history-body").innerHTML = runs.length ? runs.map((run) => `
			<tr data-run-id="${escapeHtml(run.id)}" tabindex="0">
				<td>${escapeHtml(run.id.slice(0, 8))}</td><td>${label(run.topology)}</td>
				<td><span class="status-pill ${run.state.toLowerCase()}">${run.state}</span></td>
				<td>${new Date(run.startedAt).toLocaleString()}</td><td>${formatDuration(run.durationMillis)}</td>
			</tr>`).join("") : '<tr><td colspan="5">No persisted runs yet.</td></tr>';
		$$('[data-run-id]').forEach((row) => {
			const show = async () => { state.latestRun = await request(`/api/runs/${row.dataset.runId}`); renderRun(state.latestRun); $("#results-title").scrollIntoView({ behavior: "smooth" }); };
			row.addEventListener("click", show);
			row.addEventListener("keydown", (event) => { if (event.key === "Enter") show(); });
		});
	}

	async function runSuite(topology) {
		state.busy = true;
		updateRunState();
		try {
			const run = await request("/api/runs", { method: "POST", body: JSON.stringify({ topology }) });
			state.activeRunId = run.id;
			state.latestRun = run;
			renderRun(run);
		}
		catch (error) {
			$("#run-summary").textContent = error.message;
		}
		finally {
			state.busy = false;
			updateRunState();
		}
	}

	async function environmentAction(topology, action) {
		state.busy = true;
		disableControls(true);
		try {
			const agentAction = action.startsWith("agent-") ? action.substring(6) : null;
			const path = state.mode === "handler"
				? `/api/internal/${action === "reset" ? "reset" : `lifecycle/${action}`}`
				: agentAction
					? `/api/environments/${topology}/lifecycle/${agentAction}`
					: `/api/environments/${topology}/${action}`;
			const status = await request(path, { method: "POST", body: "{}" });
			setStatus(topology === "IN_PROCESS" ? "in-process" : "external", status);
		}
		catch (error) {
			showMessage(error.message, true);
		}
		finally {
			state.busy = false;
			disableControls(false);
			loadApplication();
		}
	}

	async function sendMessage(event) {
		event.preventDefault();
		const topology = $("#message-topology").value;
		const command = {
			payload: $("#payload").value,
			correlationId: $("#correlation-id").value,
			sequenceNumber: Number($("#sequence-number").value),
			sequenceSize: Number($("#sequence-size").value)
		};
		disableControls(true);
		try {
			const path = state.mode === "handler" ? "/api/internal/message" : `/api/environments/${topology}/messages`;
			const result = await request(path, { method: "POST", body: JSON.stringify(command) });
			const payload = result.payload == null ? "no released payload" : JSON.stringify(result.payload);
			showMessage(`${result.outcome}: ${result.detail} (${payload})`, result.outcome === "FAILED");
		}
		catch (error) {
			showMessage(error.message, true);
		}
		finally { disableControls(false); }
	}

	function showMessage(message, error) {
		$("#message-result").textContent = message;
		$("#message-result").classList.toggle("error", error);
	}

	function disableControls(disabled) {
		$$('button, input, select').forEach((control) => control.disabled = disabled);
		if (!disabled) updateRunState();
	}

	function formatDuration(milliseconds) {
		if (milliseconds == null) return "—";
		return milliseconds < 1000 ? `${milliseconds} ms` : `${(milliseconds / 1000).toFixed(1)} s`;
	}

	function label(value) { return (value || "").toLowerCase().replaceAll("_", " ").replace(/^./, (c) => c.toUpperCase()); }
	function escapeHtml(value) { const node = document.createElement("span"); node.textContent = value == null ? "" : String(value); return node.innerHTML; }

	$$('.run-suite').forEach((button) => button.addEventListener("click", () => runSuite(button.dataset.suite)));
	$$('.environment-controls button').forEach((button) => button.addEventListener("click", () => environmentAction(button.dataset.topology, button.dataset.action)));
	$("#message-form").addEventListener("submit", sendMessage);
	$("#refresh").addEventListener("click", async () => { await loadApplication(); await loadRuns(); });

	async function poll() {
		await loadApplication();
		await loadRuns();
		window.setTimeout(poll, state.activeRunId ? 900 : 3500);
	}
	poll();
})();
