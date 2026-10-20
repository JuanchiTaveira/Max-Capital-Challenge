package com.maxcapital.challenge.demo;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
@Profile("evaluation-console")
public class ConsoleMutationInterceptor implements HandlerInterceptor {
  private final String origin;

  public ConsoleMutationInterceptor(@Value("${demo.origin}") String origin) {
    this.origin = origin;
  }

  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
      throws Exception {
    if (java.util.Set.of("GET", "HEAD", "OPTIONS").contains(request.getMethod())) return true;
    boolean json = false;
    try {
      json =
          request.getContentType() != null
              && MediaType.APPLICATION_JSON.isCompatibleWith(
                  MediaType.parseMediaType(request.getContentType()));
    } catch (IllegalArgumentException ignored) {
      /* Se rechaza el formato no interpretable. */
    }
    if (origin.equals(request.getHeader("Origin"))
        && "1".equals(request.getHeader("X-Demo-Console"))
        && json) return true;
    response.setStatus(403);
    response.setContentType("application/json;charset=UTF-8");
    response
        .getWriter()
        .write("{\"error\":\"La acción requiere JSON y el origen local de la consola\"}");
    return false;
  }
}
