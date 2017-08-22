package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.MaxwellContext;

import java.time.Clock;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.CompletableFuture;

public class HeartbeatDiagnostic {

	private final MaxwellContext context;

	public HeartbeatDiagnostic(MaxwellContext context) {
		this.context = context;
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

	static class HeartbeatObserver implements Observer {
		final CompletableFuture<Long> latency;
		private final HeartbeatNotifier notifier;
		private final Clock clock;

		HeartbeatObserver(HeartbeatNotifier notifier, Clock clock) {
			this.latency = new CompletableFuture<>();
			this.notifier = notifier;
			this.clock = clock;
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
