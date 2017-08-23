package com.zendesk.maxwell.metrics;

import java.util.concurrent.CompletableFuture;

public interface Diagnostic {

	String getName();

	boolean isMandatory();

	CompletableFuture<DiagnosticResult.Check> check();

	void close();

}
