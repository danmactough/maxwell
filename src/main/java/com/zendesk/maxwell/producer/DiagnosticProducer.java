package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.monitoring.MaxwellDiagnostic;

public interface DiagnosticProducer {

	MaxwellDiagnostic getDiagnostic();

}
