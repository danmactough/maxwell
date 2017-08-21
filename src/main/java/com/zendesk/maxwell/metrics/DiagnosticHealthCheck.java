package com.zendesk.maxwell.metrics;

import com.zendesk.maxwell.MaxwellContext;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
		CompletableFuture<Long> latency = context.getHeartbeatObserver().getLatency();
		try (PrintWriter writer = resp.getWriter()) {
			writer.println(latency.get().toString());
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
}
