package com.zendesk.maxwell.metrics;

import java.util.List;
import java.util.Map;

public class DiagnosticResult {

	private final boolean success;
	private final boolean mandatoryFailed;
	private final List<Check> checks;

	public DiagnosticResult(List<Check> checks) {
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
		private final Map<String, String> info;

		public Check(String name, boolean success, boolean mandatory, Map<String, String> info) {
			this.name = name;
			this.success = success;
			this.mandatory = mandatory;
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

		public Map<String, String> getInfo() {
			return info;
		}
	}
}
