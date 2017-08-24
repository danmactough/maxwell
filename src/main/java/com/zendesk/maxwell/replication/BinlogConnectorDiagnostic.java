package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.metrics.Diagnostic;
import com.zendesk.maxwell.metrics.DiagnosticResult;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class BinlogConnectorDiagnostic implements Diagnostic {

	private final MaxwellContext context;

	public BinlogConnectorDiagnostic(MaxwellContext context) {
			this.context = context;
	}

	@Override
	public String getName() {
		return "binlog-connector";
	}

	@Override
	public CompletableFuture<DiagnosticResult.Check> check() {
		return getLatency().thenApply(this::normalResult).exceptionally(this::exceptionResult);
	}

	@Override
	public boolean isMandatory() {
		return true;
	}

	@Override
	public Optional<String> getResource() {
		MaxwellMysqlConfig mysql = context.getConfig().maxwellMysql;
		return Optional.of(mysql.host + ":" + mysql.port);
	}

	public CompletableFuture<Long> getLatency() {
		HeartbeatObserver observer = new HeartbeatObserver(context.getHeartbeatNotifier(), Clock.systemUTC());
		try {
			context.heartbeat();
		} catch (Exception e) {
			observer.fail(e);
		}

		return observer.latency;
	}

	private DiagnosticResult.Check normalResult(Long latency) {
		Map<String, String> info = new HashMap<>();
		info.put("message", "Binlog replication lag is " + latency.toString() + "ms");
		return new DiagnosticResult.Check(this, true, Optional.of(info));
	}

	private DiagnosticResult.Check exceptionResult(Throwable e) {
		Map<String, String> info = new HashMap<>();
		info.put("error", e.getCause().toString());
		return new DiagnosticResult.Check(this, false, Optional.of(info));
	}

	static class HeartbeatObserver implements Observer {
		final CompletableFuture<Long> latency;
		private final HeartbeatNotifier notifier;
		private final Clock clock;

		HeartbeatObserver(HeartbeatNotifier notifier, Clock clock) {
			this.notifier = notifier;
			this.clock = clock;
			this.latency = new CompletableFuture<>();
			this.latency.whenComplete((value, exception) -> close());
			notifier.addObserver(this);
		}

		@Override
		public void update(Observable o, Object arg) {
			long heartbeatReadTime = clock.millis();
			long latestHeartbeat = (long) arg;
			close();
			latency.complete( heartbeatReadTime - latestHeartbeat);
		}

		void fail(Exception e) {
			latency.completeExceptionally(e);
			close();
		}

		private void close() {
			notifier.deleteObserver(this);
		}
	}
}
