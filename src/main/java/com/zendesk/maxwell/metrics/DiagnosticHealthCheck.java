package com.zendesk.maxwell.metrics;

import com.zendesk.maxwell.MaxwellContext;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
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

	public DiagnosticHealthCheck(MaxwellContext context) {
		this.context = context;
	}

	@Override
	protected void doGet(HttpServletRequest req,
											 HttpServletResponse resp) throws ServletException, IOException {
		resp.setStatus(HttpServletResponse.SC_OK);
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

		try (PrintWriter writer = resp.getWriter()) {
			checks.forEach(check -> check.getInfo().forEach((key, value) -> writer.println(key + ": " + value)));
		}
	}
}
