package com.zendesk.maxwell.metrics;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface Diagnostic {

	String getName();

	boolean isMandatory();

	Optional<String> getResource();

	CompletableFuture<DiagnosticResult.Check> check();

}
