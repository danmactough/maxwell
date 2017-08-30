package com.zendesk.maxwell.monitoring;

import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellTestWithIsolatedServer;
import com.zendesk.maxwell.MaxwellWithContext;
import com.zendesk.maxwell.replication.BinlogConnectorDiagnostic;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiagnosticMaxwellTest extends MaxwellTestWithIsolatedServer {

	@Test
	public void testNormalBinlogReplicationDiagnostic() throws Exception {
		// Given
		MaxwellDiagnosticContext.Config config = new MaxwellDiagnosticContext.Config();
		config.timeout = 5000;
		MaxwellContext maxwellContext = buildContext();

		DiagnosticHealthCheck healthCheck = getDiagnosticHealthCheck(config, maxwellContext);

		HttpServletRequest request = mock(HttpServletRequest.class);
		HttpServletResponse response = mock(HttpServletResponse.class);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		PrintWriter writer = new PrintWriter(outputStream);
		when(response.getWriter()).thenReturn(writer);

		final CountDownLatch latch = new CountDownLatch(1);
		Maxwell maxwell = new MaxwellWithContext(maxwellContext) {
			@Override
			protected void onReplicatorStart() {
				latch.countDown();
			}
		};
		new Thread(maxwell).start();
		latch.await();

		// When
		healthCheck.doGet(request, response);
		writer.flush();

		// Then
		String result = new String(outputStream.toByteArray());
		assertTrue(result.contains("\"name\":\"binlog-connector\",\"success\":true"));
		maxwell.terminate();
		writer.close();
	}

	@Test
	public void testBinlogReplicationDiagnosticTimeout() throws Exception {
		// Given
		MaxwellDiagnosticContext.Config config = new MaxwellDiagnosticContext.Config();
		config.timeout = 100;
		MaxwellContext maxwellContext = buildContext();

		DiagnosticHealthCheck healthCheck = getDiagnosticHealthCheck(config, maxwellContext);

		HttpServletRequest request = mock(HttpServletRequest.class);
		HttpServletResponse response = mock(HttpServletResponse.class);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		PrintWriter writer = new PrintWriter(outputStream);
		when(response.getWriter()).thenReturn(writer);

		// When
		healthCheck.doGet(request, response);
		writer.flush();

		// Then
		String result = new String(outputStream.toByteArray());
		assertTrue(result.contains("\"name\":\"binlog-connector\",\"success\":false"));
	}

	private DiagnosticHealthCheck getDiagnosticHealthCheck(MaxwellDiagnosticContext.Config config,
																												 MaxwellContext maxwellContext) throws ServletException {
		MaxwellDiagnosticContext diagnosticContext = new MaxwellDiagnosticContext(config,
				Collections.singletonList(new BinlogConnectorDiagnostic(maxwellContext)));
		DiagnosticHealthCheck healthCheck = new DiagnosticHealthCheck(diagnosticContext);
		healthCheck.init(healthCheck);
		return healthCheck;
	}
}
