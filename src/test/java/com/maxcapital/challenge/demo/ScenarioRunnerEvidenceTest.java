package com.maxcapital.challenge.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScenarioRunnerEvidenceTest {
  @Test
  void readingACompletedRunReturnsCapturedMemoryWithoutRepeatingScenarioActions() {
    var steps = mock(ScenarioSteps.class);
    doAnswer(
            invocation -> {
              var run = (DemoRun) invocation.getArgument(0);
              run.step(
                  "Publicar",
                  "Conservar el mensaje",
                  () -> {
                    run.message(new ConsoleKafka.Record("execution-reports", "0", "7", "9", "{}"));
                    return Map.of();
                  });
              return null;
            })
        .when(steps)
        .execute(any(), eq("interleaved-orders"), anySet());
    var runner = new ScenarioRunner(new ScenarioCatalog(), steps, new DemoGate(), 1000);
    try {
      String id = runner.start("interleaved-orders", "automatic").get("id").toString();
      await()
          .atMost(Duration.ofSeconds(2))
          .until(() -> runner.get(id).get("status").equals("FAILED"));

      var first = runner.get(id);
      var second = runner.get(id);
      assertThat(second).isEqualTo(first);
      verify(steps, times(1)).execute(any(), eq("interleaved-orders"), anySet());
      verify(steps, times(1)).restoreRecovery(any());
      var result = (Map<?, ?>) ((java.util.List<?>) first.get("scenarioResults")).getFirst();
      var evidence = (Map<?, ?>) result.get("evidence");
      assertThat(evidence.get("messages")).asList().hasSize(1);
    } finally {
      runner.close();
    }
  }
}
