package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.MaxwellContext;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.CompletableFuture;

public class HeartbeatObserver implements Observer {

	private final MaxwellContext context;

	private long newHeartbeat;
	private CompletableFuture<Long> latency;

	public HeartbeatObserver(MaxwellContext context) {
		this.context = context;
	}

	@Override
	public void update(Observable o, Object arg) {
		if (newHeartbeat == 0) {
			return;
		}
		long heartbeatReadTime = System.currentTimeMillis();
		long latestHeartbeat = (long) arg;
		context.getHeartbeatNotifier().deleteObserver(this);
		latency.complete( heartbeatReadTime - latestHeartbeat);
		newHeartbeat = 0;
	}

	public CompletableFuture<Long> getLatency() {
		this.latency = new CompletableFuture<>();

		try {
			context.getHeartbeatNotifier().addObserver(this);
			newHeartbeat = context.heartbeat();
		} catch (Exception e) {
			latency.completeExceptionally(e);
		}

		return latency;
	}

}
