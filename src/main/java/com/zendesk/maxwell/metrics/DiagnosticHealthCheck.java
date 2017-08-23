package com.zendesk.maxwell.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.MaxwellContext;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class DiagnosticHealthCheck extends HttpServlet {

	private static final String CONTENT_TYPE = "text/plain";
	private static final String CACHE_CONTROL = "Cache-Control";
	private static final String NO_CACHE = "must-revalidate,no-cache,no-store";
	private final MaxwellContext context;
	private transient ObjectMapper mapper;

	public DiagnosticHealthCheck(MaxwellContext context) {
		this.context = context;
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		this.mapper = new ObjectMapper().registerModule(new DiagnosticHealthCheckModule());
	}

	@Override
	protected void doGet(HttpServletRequest req,
											 HttpServletResponse resp) throws ServletException, IOException {
		resp.setHeader(CACHE_CONTROL, NO_CACHE);
		resp.setContentType(CONTENT_TYPE);
		Map<Diagnostic, CompletableFuture<DiagnosticResult.Check>> futureChecks = context.getDiagnostics().stream()
				.collect(Collectors.toMap(diagnostic -> diagnostic, Diagnostic::check));

		List<DiagnosticResult.Check> checks = futureChecks.entrySet().stream().map(future -> {
			try {
				return future.getValue().get(2, TimeUnit.SECONDS); // TODO use config
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				Diagnostic diagnostic = future.getKey();
				diagnostic.close();
				Map<String, String> info = new HashMap<>();
				info.put("message", "check did not return after 5"); // TODO use config
				return new DiagnosticResult.Check(diagnostic.getName(), false, diagnostic.isMandatory(), info);
			}
		}).collect(Collectors.toList());

		DiagnosticResult result = new DiagnosticResult(checks);

		if (result.isSuccess()) {
			resp.setStatus(HttpServletResponse.SC_OK);
		} else if (result.isMandatoryFailed()) {
			resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
		} else {
			resp.setStatus(299); // HTTP 299 Disappointed
		}

		try (OutputStream output = resp.getOutputStream()) {
			mapper.writer().writeValue(output, result);
		}
	}
}
