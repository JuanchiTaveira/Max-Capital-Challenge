package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.maxcapital.challenge.demo.ConsoleKafka;
import com.maxcapital.challenge.demo.DockerAccess;
import com.maxcapital.challenge.demo.InstanceAccess;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import tools.jackson.databind.json.JsonMapper;

@ActiveProfiles("evaluation-console")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "spring.kafka.bootstrap-servers=127.0.0.1:1",
      "spring.datasource.url=jdbc:postgresql://127.0.0.1:1/missing?connectTimeout=1"
    })
class ConsoleOperationsIT {
  @MockitoBean DockerAccess docker;
  @MockitoBean ConsoleKafka kafka;
  @MockitoBean InstanceAccess instances;
  @MockitoBean JdbcTemplate jdbc;

  @org.springframework.beans.factory.annotation.Autowired
  com.maxcapital.challenge.demo.ConsoleSystem system;

  @Value("${local.server.port}")
  int port;

  @Test
  void rejectsForeignMutationBeforeCallingControlsOrPublisherAndReportsSocketFailure()
      throws Exception {
    // GIVEN
    clearInvocations(docker, kafka);

    // WHEN
    var foreignControl =
        request("/demo/api/services/app-1/start", "http://foreign.invalid", "application/json");

    // THEN
    assertThat(foreignControl.statusCode()).isEqualTo(403);

    // WHEN
    var foreignPublication =
        request("/demo/api/reports", "http://foreign.invalid", "application/json");

    // THEN
    assertThat(foreignPublication.statusCode()).isEqualTo(403);

    // WHEN
    var invalidContent =
        request("/demo/api/services/app-1/start", "http://localhost:8083", "text/plain");

    // THEN
    assertThat(invalidContent.statusCode()).isIn(403, 415);
    verifyNoInteractions(docker, kafka);

    // GIVEN
    when(docker.control("app-1", "start"))
        .thenThrow(new IllegalStateException("Socket Docker no disponible"));
    // WHEN
    var response =
        request("/demo/api/services/app-1/start", "http://localhost:8083", "application/json");

    // THEN
    assertThat(response.statusCode()).isEqualTo(503);
    assertThat(response.body()).contains("Socket Docker no disponible");
    // WHEN
    var page = get("/");

    // THEN
    assertThat(page.statusCode()).isEqualTo(200);
  }

  @Test
  void combinesStoppedListenerHealthyHttpAndUnknownInstanceAsSeparateSources() throws Exception {
    // GIVEN
    var json = new JsonMapper();
    when(jdbc.queryForObject("SELECT 1", Integer.class)).thenReturn(1);
    when(instances.read(eq("app-1"), anyString()))
        .thenReturn(Map.of("statusCode", 200, "data", json.readTree("{\"status\":\"UP\"}")));
    when(instances.read("app-1", "/internal/demo/consumer"))
        .thenReturn(
            Map.of(
                "statusCode",
                200,
                "data",
                json.readTree("{\"running\":false,\"assignments\":[]}")));
    when(instances.read(eq("app-2"), anyString()))
        .thenThrow(new IllegalStateException("No disponible"));
    // WHEN
    var response = get("/demo/api/system");

    // THEN
    assertThat(response.statusCode()).isEqualTo(200);
    var state = json.readTree(response.body());
    assertThat(state.path("database").path("available").asBoolean()).isTrue();
    assertThat(state.path("kafka").path("available").asBoolean()).isFalse();
    var first = state.path("instances").get(0);
    assertThat(first.path("readiness").path("data").path("statusCode").asInt()).isEqualTo(200);
    assertThat(first.path("consumer").path("data").path("data").path("running").asBoolean())
        .isFalse();
    assertThat(state.path("instances").get(1).path("consumer").path("available").asBoolean())
        .isFalse();
    // GIVEN
    when(instances.read(eq("app-1"), anyString()))
        .thenThrow(new IllegalStateException("Ahora no disponible"));

    // WHEN
    var stale = json.valueToTree(system.instance("app-1"));

    // THEN
    assertThat(stale.path("readiness").path("available").asBoolean()).isFalse();
    assertThat(stale.path("readiness").path("stale").asBoolean()).isTrue();
    assertThat(stale.path("readiness").path("observedAt").asString())
        .isEqualTo(first.path("readiness").path("observedAt").asString());
  }

  HttpResponse<String> request(String path, String origin, String content) throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      return client.send(
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
              .header("Origin", origin)
              .header("X-Demo-Console", "1")
              .header("Content-Type", content)
              .POST(HttpRequest.BodyPublishers.ofString("{}"))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }

  HttpResponse<String> get(String path) throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      return client.send(
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }
}
