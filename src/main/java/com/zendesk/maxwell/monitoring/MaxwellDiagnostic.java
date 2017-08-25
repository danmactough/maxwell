package com.zendesk.maxwell.monitoring;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface MaxwellDiagnostic {

	String getName();

	boolean isMandatory();

	Optional<String> getResource();

	CompletableFuture<MaxwellDiagnosticResult.Check> check();

}
