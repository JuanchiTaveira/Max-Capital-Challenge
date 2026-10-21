package com.maxcapital.challenge;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** Proceso completo de aplicación; la prueba lo termina sin cerrar Spring. */
public final class RecoveryProcess {
  private RecoveryProcess() {}

  public static void main(String[] args) throws Exception {
    var app = new SpringApplicationBuilder(MaxCapitalChallengeApplication.class).run(args);
    var marker =
        Arrays.stream(args)
            .filter(a -> a.startsWith("--test.ready="))
            .findFirst()
            .orElseThrow()
            .substring(13);
    Files.writeString(
        Path.of(marker), app.getEnvironment().getRequiredProperty("local.server.port"));
  }
}
