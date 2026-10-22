package com.maxcapital.challenge.demo;

import jakarta.annotation.PreDestroy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
@Profile("evaluation-console")
public class ScenarioRunner {
  private final ScenarioCatalog catalog;
  private final ScenarioSteps steps;
  private final DemoGate gate;
  private final long timeout;
  private final Map<String, DemoRun> runs = new LinkedHashMap<>();
  private final java.util.concurrent.ExecutorService executor =
      Executors.newVirtualThreadPerTaskExecutor();

  public ScenarioRunner(
      ScenarioCatalog catalog,
      ScenarioSteps steps,
      DemoGate gate,
      @Value("${demo.step-timeout-ms:150000}") long timeout) {
    this.catalog = catalog;
    this.steps = steps;
    this.gate = gate;
    this.timeout = timeout;
  }

  public synchronized Map<String, Object> start(String scenario, String mode) {
    if (!"automatic".equals(mode) && !"step".equals(mode))
      throw new IllegalArgumentException("Modo inválido");
    List<String> selected;
    if ("available".equals(scenario)) selected = catalog.available();
    else {
      var entry =
          catalog.entries().stream()
              .filter(e -> e.id().equals(scenario))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("Escenario inexistente"));
      if (!entry.available())
        throw new ResponseStatusException(
            HttpStatus.CONFLICT, "Escenario pendiente de " + entry.dependency());
      selected = List.of(scenario);
    }
    var run = new DemoRun(UUID.randomUUID().toString(), mode, selected, timeout);
    gate.acquire(run.id);
    runs.put(run.id, run);
    while (runs.size() > 50) runs.remove(runs.keySet().iterator().next());
    executor.submit(() -> execute(run));
    return run.snapshot();
  }

  private void execute(DemoRun run) {
    String status = "PASSED", error = null;
    var restoration = new java.util.LinkedHashSet<String>();
    try {
      for (String scenario : run.scenarios) {
        run.checkCancellation();
        run.beginScenario(scenario);
        steps.execute(run, scenario, restoration);
        run.checkCancellation();
        run.passed();
      }
    } catch (CancellationException ex) {
      status = "CANCELLED";
      error = DemoRun.message(ex);
    } catch (RuntimeException ex) {
      status = "FAILED";
      error = DemoRun.message(ex);
    } finally {
      try {
        steps.restoreRecovery(run);
      } catch (RuntimeException ex) {
        status = "FAILED";
        error = "No se pudo restaurar recuperación: " + DemoRun.message(ex);
      }
      for (String service : restoration) {
        try {
          steps.restore(service);
        } catch (RuntimeException ex) {
          status = "FAILED";
          error = "No se pudo restaurar " + service + ": " + DemoRun.message(ex);
        }
      }
      gate.release(run.id);
      run.finish(status, error);
    }
  }

  private synchronized DemoRun find(String id) {
    var run = runs.get(id);
    if (run == null)
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND,
          "Ejecución no disponible; la consola conserva su historial sólo en memoria");
    return run;
  }

  public Map<String, Object> get(String id) {
    return find(id).snapshot();
  }

  public Map<String, Object> next(String id) {
    var run = find(id);
    run.next();
    return run.snapshot();
  }

  public Map<String, Object> cancel(String id) {
    var run = find(id);
    run.cancel();
    return run.snapshot();
  }

  @PreDestroy
  void close() {
    synchronized (this) {
      runs.values().forEach(DemoRun::cancel);
    }
    executor.close();
  }
}
