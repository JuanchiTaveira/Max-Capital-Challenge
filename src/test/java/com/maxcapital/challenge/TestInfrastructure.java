package com.maxcapital.challenge;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.testcontainers.utility.DockerImageName;
import org.yaml.snakeyaml.Yaml;

final class TestInfrastructure {
  private TestInfrastructure() {}

  static DockerImageName image(String service, String compatible) {
    try {
      Map<String, Object> compose = new Yaml().load(Files.readString(Path.of("compose.yaml")));
      var services = (Map<?, ?>) compose.get("services");
      return DockerImageName.parse((String) ((Map<?, ?>) services.get(service)).get("image"))
          .asCompatibleSubstituteFor(compatible);
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }

  static String report(long order, long fix, String status, int accumulated, int leaves) {
    return """
        {"fixId":%d,"numericOrderId":%d,"status":"%s","nominalAmounts":10,
        "accumulativeNominalAmount":%d,"leavesNominalAmount":%d,
        "transactionTime":"2026-07-20T18:08:52.129","ticker":"VSCPC"}
        """
        .formatted(fix, order, status, accumulated, leaves);
  }
}
