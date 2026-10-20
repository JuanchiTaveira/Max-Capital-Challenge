package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.maxcapital.challenge.demo.DockerAccess;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DockerAccessTest {
  static final String ID = "abcdef0123456789";

  @Test
  void startsStoppedProjectContainerAndRedactsBoundedLogs() {
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
    assertThat(docker.control("app-1", "start").get("containerState")).isEqualTo("running");
    assertThat(calls.getFirst())
        .contains(
            "-aq",
            "label=com.docker.compose.project=demo-it",
            "label=com.docker.compose.service=app-1");
    assertThat(calls).contains(List.of("start", ID));
    assertThat(docker.logs("app-1").get("text")).isEqualTo("ER pendiente: [oculto]");
  }

  @Test
  void rejectsForeignAmbiguousAndArbitraryTargetsBeforeControl() {
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
    assertThatThrownBy(() -> docker.control("app-1", "start")).hasMessageContaining("autorizados");
    listing.set(ID + "\nabcdef0123456790");
    assertThatThrownBy(() -> docker.control("app-1", "stop")).hasMessageContaining("único");
    assertThatThrownBy(() -> docker.control("app-1", "rm"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> docker.control("postgres", "kill"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> docker.control("foreign", "start"))
        .isInstanceOf(IllegalArgumentException.class);
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
