package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.maxcapital.challenge.demo.DockerAccess;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DockerAccessTest {
  static final String ID = "abcdef0123456789";

  @Test
  void startsStoppedProjectContainerAndRedactsBoundedLogs() {
    // GIVEN
    var calls = new ArrayList<List<String>>();
    var docker =
        new DockerAccess(
            "demo-it",
            args -> {
              calls.add(List.copyOf(args));
              if (args.getFirst().equals("ps")) return ID;
              if (args.contains("{{json .Config.Labels}}")) return labels("demo-it", "app-1");
              if (args.getFirst().equals("logs")) return "ER pendiente: password123";
              return "running";
            },
            List.of("password123"));
    // WHEN
    var result = docker.control("app-1", "start");

    // THEN
    assertThat(result.get("containerState")).isEqualTo("running");
    assertThat(calls.getFirst())
        .contains(
            "-aq",
            "label=com.docker.compose.project=demo-it",
            "label=com.docker.compose.service=app-1");
    assertThat(calls).contains(List.of("start", ID));
    // WHEN
    var logs = docker.logs("app-1");

    // THEN
    assertThat(logs.get("text")).isEqualTo("ER pendiente: [oculto]");
  }

  @Test
  void rejectsForeignAmbiguousAndArbitraryTargetsBeforeControl() {
    // GIVEN
    var calls = new ArrayList<List<String>>();
    var listing = new AtomicReference<>(ID);
    var docker =
        new DockerAccess(
            "demo-it",
            args -> {
              calls.add(List.copyOf(args));
              return args.getFirst().equals("ps") ? listing.get() : labels("foreign", "app-1");
            },
            List.of());
    // WHEN
    var foreignProject = catchThrowable(() -> docker.control("app-1", "start"));

    // THEN
    assertThat(foreignProject).hasMessageContaining("autorizados");

    // GIVEN
    listing.set(ID + "\nabcdef0123456790");

    // WHEN
    var ambiguousTarget = catchThrowable(() -> docker.control("app-1", "stop"));

    // THEN
    assertThat(ambiguousTarget).hasMessageContaining("único");

    // WHEN
    var forbiddenAction = catchThrowable(() -> docker.control("app-1", "rm"));

    // THEN
    assertThat(forbiddenAction).isInstanceOf(IllegalArgumentException.class);

    // WHEN
    var forbiddenKill = catchThrowable(() -> docker.control("postgres", "kill"));

    // THEN
    assertThat(forbiddenKill).isInstanceOf(IllegalArgumentException.class);

    // WHEN
    var foreignService = catchThrowable(() -> docker.control("foreign", "start"));

    // THEN
    assertThat(foreignService).isInstanceOf(IllegalArgumentException.class);
    assertThat(calls).allSatisfy(args -> assertThat(args.getFirst()).isIn("ps", "inspect"));
  }

  static String labels(String project, String service) {
    return "{\"com.docker.compose.project\":\""
        + project
        + "\",\"com.docker.compose.service\":\""
        + service
        + "\"}";
  }
}
