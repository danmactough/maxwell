package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.PositionStoreThread;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

public class KafkaProducerDiagnostic {

	private final MaxwellKafkaProducerWorker producer;
	private final MaxwellConfig config;
	private final PositionStoreThread positionStoreThread;

	public KafkaProducerDiagnostic(MaxwellKafkaProducerWorker producer, MaxwellConfig config, PositionStoreThread positionStoreThread) {
		this.producer = producer;
		this.config = config;
		this.positionStoreThread = positionStoreThread;
	}

	public CompletableFuture<Long> getLatency() {
		DiagnosticCallback callback = new DiagnosticCallback();
		try {
			RowMap rowMap = new RowMap("insert", config.databaseName, "dummy", System.currentTimeMillis(),
					new ArrayList<>(), positionStoreThread.getPosition());
			ProducerRecord<String, String> record = producer.makeProducerRecord(rowMap);
			producer.sendAsync(record, callback);
		} catch (Exception e) {
			callback.latency.completeExceptionally(e);
		}
		return callback.latency;
	}

	static class DiagnosticCallback implements Callback {
		final CompletableFuture<Long> latency;
		final long sendTime;

		DiagnosticCallback() {
			latency = new CompletableFuture<>();
			sendTime = System.currentTimeMillis();
		}

		@Override
		public void onCompletion(final RecordMetadata metadata, final Exception exception) {
			if (exception == null) {
				latency.complete(System.currentTimeMillis() - sendTime);
			} else {
				latency.completeExceptionally(exception);
			}
		}
	}
}
