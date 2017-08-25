package com.zendesk.maxwell.monitoring;

import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.util.StoppableTask;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.zendesk.maxwell.monitoring.MaxwellMetrics.reportingTypeHttp;

public class MaxwellHTTPServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHTTPServer.class);

	public static void startIfRequired(MaxwellContext context) {
		Optional<MaxwellMetrics.Registries> metricsRegistries = getMetricsRegistries(context.getConfig());
		Optional<MaxwellDiagnosticContext> diagnosticContext = getDiagnosticContext(context);
		if (metricsRegistries.isPresent() || diagnosticContext.isPresent()) {
			LOGGER.info("Metrics http server starting");
			int port = context.getConfig().monitoringHTTPPort;
			MaxwellHTTPServerWorker maxwellHTTPServerWorker = new MaxwellHTTPServerWorker(port, metricsRegistries, diagnosticContext);
			Thread thread = new Thread(maxwellHTTPServerWorker);

			context.addTask(maxwellHTTPServerWorker);
			thread.setUncaughtExceptionHandler((t, e) -> {
				e.printStackTrace();
				System.exit(1);
			});

			thread.setDaemon(true);
			thread.start();
			LOGGER.info("Metrics http server started on port " + port);
		}
	}

	private static Optional<MaxwellMetrics.Registries> getMetricsRegistries(MaxwellConfig config) {
		return Optional.ofNullable(config.metricsReportingType)
				.filter(type -> type.contains(reportingTypeHttp))
				.map(type -> new MaxwellMetrics.Registries(config.metricRegistry, config.healthCheckRegistry));
	}

	private static Optional<MaxwellDiagnosticContext> getDiagnosticContext(MaxwellContext context) {
		MaxwellDiagnosticContext.Config diagnosticConfig = context.getConfig().diagnosticConfig;
		if (diagnosticConfig.enable) {
			return Optional.of(new MaxwellDiagnosticContext(diagnosticConfig, context.getDiagnostics()));
		} else {
			return Optional.empty();
		}
	}
}

class MaxwellHTTPServerWorker implements StoppableTask, Runnable {

	private int port;
	private final Optional<MaxwellMetrics.Registries> metricsRegistries;
	private final Optional<MaxwellDiagnosticContext> diagnosticContext;
	private Server server;

	public MaxwellHTTPServerWorker(int port, Optional<MaxwellMetrics.Registries> metricsRegistries,
																 Optional<MaxwellDiagnosticContext> diagnosticContext) {
		this.port = port;
		this.metricsRegistries = metricsRegistries;
		this.diagnosticContext = diagnosticContext;
	}

	public void startServer() throws Exception {
		this.server = new Server(this.port);
		ServletContextHandler handler = new ServletContextHandler(this.server, "/");

		metricsRegistries.ifPresent(registries -> {
			// TODO: there is a way to wire these up automagically via the AdminServlet, but it escapes me right now
			handler.addServlet(new ServletHolder(new MetricsServlet(registries.metricRegistry)), "/metrics");
			handler.addServlet(new ServletHolder(new HealthCheckServlet(registries.healthCheckRegistry)), "/healthcheck");
			handler.addServlet(new ServletHolder(new PingServlet()), "/ping");
		});

		diagnosticContext.ifPresent(context -> {
			String path = context.config.path;
			if (!path.startsWith("/")) {
				path = "/" + path;
			}
			handler.addServlet(new ServletHolder(new DiagnosticHealthCheck(context)), path);
		});

		this.server.start();
		this.server.join();
	}

	@Override
	public void run() {
		try {
			startServer();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void requestStop() {
		try {
			this.server.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {
	}
}
