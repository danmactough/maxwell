package com.zendesk.maxwell.metrics;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class DiagnosticHealthCheckModule extends Module {

	final static Logger LOGGER = LoggerFactory.getLogger(DiagnosticHealthCheckModule.class);

	private static class DiagnosticHealthCheckResultSerializer extends StdSerializer<DiagnosticResult> {
		private DiagnosticHealthCheckResultSerializer() {
			super(DiagnosticResult.class);
		}

		@Override
		public void serialize(DiagnosticResult result,
													JsonGenerator json,
													SerializerProvider provider) throws IOException {
			json.writeStartObject();
			json.writeBooleanField("success", result.isSuccess());

			json.writeFieldName("checks");
			json.writeStartArray();
			result.getChecks().forEach(check -> serializeCheck(json, check));
			json.writeEndArray();

			json.writeEndObject();
		}

		private void serializeCheck(JsonGenerator json, DiagnosticResult.Check check) {
			try {
				json.writeStartObject();
				json.writeStringField("name", check.getName());
				json.writeBooleanField("success", check.isSuccess());
				json.writeBooleanField("mandatory", check.isMandatory());
				check.getInfo().forEach((k, v) -> {
					try {
						json.writeStringField(k, v);
					} catch (IOException e) {
						LOGGER.error("Could not serialize DiagnosticResult.Check info", e);
					}
				});
				json.writeEndObject();
			} catch (IOException e) {
				LOGGER.error("Could not serialize DiagnosticResult.Check", e);
			}
		}
	}

	@Override
	public String getModuleName() {
		return "diagnostic";
	}

	@Override
	public Version version() {
		return new Version(0, 1, 0, "", "com.zendesk", "maxwell");
	}

	@Override
	public void setupModule(SetupContext context) {
		context.addSerializers(new SimpleSerializers(Collections.singletonList(new DiagnosticHealthCheckResultSerializer())));
	}
}
