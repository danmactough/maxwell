package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.PositionStoreThread;
import com.zendesk.maxwell.metrics.Diagnostic;
import com.zendesk.maxwell.metrics.DiagnosticResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class KafkaProducerDiagnostic implements Diagnostic {

	private final MaxwellKafkaProducerWorker producer;
	private final MaxwellConfig config;
	private final PositionStoreThread positionStoreThread;

	public KafkaProducerDiagnostic(MaxwellKafkaProducerWorker producer, MaxwellConfig config, PositionStoreThread positionStoreThread) {
		this.producer = producer;
		this.config = config;
		this.positionStoreThread = positionStoreThread;
	}

	@Override
	public String getName() {
		return "kafka-producer";
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
		return Optional.ofNullable(config.getKafkaProperties().getProperty("bootstrap.servers"));
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

	private DiagnosticResult.Check normalResult(Long latency) {
		Map<String, String> info = new HashMap<>();
		info.put("message", "Kafka producer acknowledgement lag is " + latency.toString() + "ms");
		return new DiagnosticResult.Check(this, true, Optional.of(info));
	}

	private DiagnosticResult.Check exceptionResult(Throwable e) {
		Map<String, String> info = new HashMap<>();
		info.put("error", e.getCause().toString());
		return new DiagnosticResult.Check(this, false, Optional.of(info));
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
