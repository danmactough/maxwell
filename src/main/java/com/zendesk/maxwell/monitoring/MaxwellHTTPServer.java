package com.zendesk.maxwell.monitoring;

import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.util.StoppableTask;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static com.zendesk.maxwell.monitoring.MaxwellMetrics.reportingTypeHttp;

public class MaxwellHTTPServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHTTPServer.class);

	public static void startIfRequired(MaxwellContext context) {
		MaxwellMetrics.Registries metricsRegistries = getMetricsRegistries(context.getConfig());
		MaxwellDiagnosticContext diagnosticContext = getDiagnosticContext(context);
		if (metricsRegistries != null || diagnosticContext != null) {
			LOGGER.info("Metrics http server starting");
			int port = context.getConfig().monitoringHTTPPort;
			String pathPrefix = context.getConfig().monitoringHTTPPathPrefix;
			MaxwellHTTPServerWorker maxwellHTTPServerWorker = new MaxwellHTTPServerWorker(port, pathPrefix,
					metricsRegistries, diagnosticContext);
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

	private static MaxwellMetrics.Registries getMetricsRegistries(MaxwellConfig config) {
		String reportingType = config.metricsReportingType;
		if (reportingType != null && reportingType.contains(reportingTypeHttp)) {
			return new MaxwellMetrics.Registries(config.metricRegistry, config.healthCheckRegistry);
		} else {
			return null;
		}
	}

	private static MaxwellDiagnosticContext getDiagnosticContext(MaxwellContext context) {
		MaxwellDiagnosticContext.Config diagnosticConfig = context.getConfig().diagnosticConfig;
		if (diagnosticConfig.enable) {
			return new MaxwellDiagnosticContext(diagnosticConfig, context.getDiagnostics());
		} else {
			return null;
		}
	}
}

class MaxwellHTTPServerWorker implements StoppableTask, Runnable {

	private int port;
	private final String pathPrefix;
	private final MaxwellMetrics.Registries metricsRegistries;
	private final MaxwellDiagnosticContext diagnosticContext;
	private Server server;

	public MaxwellHTTPServerWorker(int port, String pathPrefix, MaxwellMetrics.Registries metricsRegistries,
																 MaxwellDiagnosticContext diagnosticContext) {
		this.port = port;
		this.pathPrefix = pathPrefix;
		this.metricsRegistries = metricsRegistries;
		this.diagnosticContext = diagnosticContext;
	}

	public void startServer() throws Exception {
		this.server = new Server(this.port);
		ServletContextHandler handler = new ServletContextHandler(this.server, "/");

		if (metricsRegistries != null) {
			// TODO: there is a way to wire these up automagically via the AdminServlet, but it escapes me right now
			handler.addServlet(new ServletHolder(new MetricsServlet(metricsRegistries.metricRegistry)), genFullPath(pathPrefix, "/metrics"));
			handler.addServlet(new ServletHolder(new HealthCheckServlet(metricsRegistries.healthCheckRegistry)), genFullPath(pathPrefix, "/healthcheck"));
			handler.addServlet(new ServletHolder(new PingServlet()), genFullPath(pathPrefix, "/ping"));
		}

		if (diagnosticContext != null) {
			handler.addServlet(new ServletHolder(new DiagnosticHealthCheck(diagnosticContext)),
					genFullPath(pathPrefix, diagnosticContext.config.path));
		}

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

	private String genFullPath(String pathPrefix, String path) {
		String prefix = genPath(pathPrefix);
		if (prefix != null) {
			return prefix + genPath(path);
		} else {
			return genPath(path);
		}
	}

	private String genPath(String path) {
		if (StringUtils.isNotBlank(path)) {
			if (!path.startsWith("/")) {
				return "/" + path;
			}
			return path;
		} else {
			return null;
		}
	}
}
