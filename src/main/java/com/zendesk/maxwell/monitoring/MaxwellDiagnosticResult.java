package com.zendesk.maxwell.monitoring;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MaxwellDiagnosticResult {

	private final boolean success;
	private final boolean mandatoryFailed;
	private final List<Check> checks;

	public MaxwellDiagnosticResult(List<Check> checks) {
		success = checks.stream().allMatch(Check::isSuccess);
		mandatoryFailed = checks.stream().anyMatch(check -> !check.success && check.mandatory);
		this.checks = checks;
	}

	public boolean isSuccess() {
		return success;
	}

	public boolean isMandatoryFailed() {
		return mandatoryFailed;
	}

	public List<Check> getChecks() {
		return checks;
	}

	public static class Check {
		private final String name;
		private final boolean success;
		private final boolean mandatory;
		private final Optional<String> resource;
		private final Optional<Map<String, String>> info;

		public Check(MaxwellDiagnostic diagnostic, boolean success, Optional<Map<String, String>> info) {
			this.name = diagnostic.getName();
			this.success = success;
			this.mandatory = diagnostic.isMandatory();
			this.resource = diagnostic.getResource();
			this.info = info;
		}

		public String getName() {
			return name;
		}

		public boolean isSuccess() {
			return success;
		}

		public boolean isMandatory() {
			return mandatory;
		}

		public Optional<String> getResource() {
			return resource;
		}

		public Optional<Map<String, String>> getInfo() {
			return info;
		}
	}
}
