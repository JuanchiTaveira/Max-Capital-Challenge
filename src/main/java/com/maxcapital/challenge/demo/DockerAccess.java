package com.maxcapital.challenge.demo;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("evaluation-console")
public class DockerAccess {
  private static final Set<String> SERVICES = Set.of("app-1", "app-2", "postgres", "kafka");

  @FunctionalInterface
  public interface Commands {
    String execute(List<String> arguments);
  }

  private final String project;
  private final Commands commands;
  private final List<String> secrets;

  @Autowired
  public DockerAccess(
      @Value("${demo.compose-project:}") String project,
      @Value("${spring.datasource.username}") String user,
      @Value("${spring.datasource.password}") String password) {
    this(project, DockerAccess::execute, List.of(user, password));
  }

  public DockerAccess(String project, Commands commands, List<String> secrets) {
    this.project = project;
    this.commands = commands;
    this.secrets = secrets;
  }

  public Map<String, Object> control(String service, String action) {
    if (!Set.of("start", "stop", "restart", "kill").contains(action)
        || (action.equals("kill") && !Set.of("app-1", "app-2").contains(service)))
      throw new IllegalArgumentException("Acción no permitida para ese servicio");
    String id = resolve(service);
    var args = new ArrayList<String>();
    args.add(action);
    if (action.equals("kill")) args.add("--signal=KILL");
    if (action.equals("stop") || action.equals("restart")) {
      args.add("--time");
      args.add("10");
    }
    args.add(id);
    commands.execute(args);
    var result = ConsoleValues.observed("Docker");
    result.put("service", service);
    result.put("action", action);
    result.put(
        "containerState",
        commands.execute(List.of("inspect", "--format", "{{.State.Status}}", id)).trim());
    return result;
  }

  public Map<String, Object> logs(String service) {
    String id = resolve(service);
    var result = ConsoleValues.observed("Docker logs " + service);
    String text = commands.execute(List.of("logs", "--tail", "200", "--timestamps", id));
    for (String secret : secrets)
      if (secret != null && !secret.isBlank()) text = text.replace(secret, "[oculto]");
    result.put("text", text);
    result.put("limit", "200 líneas / 65536 bytes");
    return result;
  }

  private String resolve(String service) {
    if (!SERVICES.contains(service)) throw new IllegalArgumentException("Servicio no permitido");
    if (project == null || project.isBlank())
      throw new IllegalStateException("Proyecto Compose no configurado");
    String output =
        commands.execute(
            List.of(
                "ps",
                "-aq",
                "--filter",
                "label=com.docker.compose.project=" + project,
                "--filter",
                "label=com.docker.compose.service=" + service));
    var ids = output.lines().map(String::trim).filter(s -> !s.isEmpty()).toList();
    if (ids.size() != 1 || !ids.getFirst().matches("[a-f0-9]{12,64}"))
      throw new IllegalStateException(
          "Se requiere un único contenedor del servicio en el proyecto");
    String id = ids.getFirst();
    var labels =
        ConsoleValues.JSON.readTree(
            commands.execute(List.of("inspect", "--format", "{{json .Config.Labels}}", id)));
    if (!project.equals(labels.path("com.docker.compose.project").asString())
        || !service.equals(labels.path("com.docker.compose.service").asString()))
      throw new IllegalStateException(
          "El contenedor no pertenece al servicio y proyecto autorizados");
    return id;
  }

  private static String execute(List<String> arguments) {
    var command = new ArrayList<String>();
    command.add("docker");
    command.addAll(arguments);
    Process process = null;
    var executor = Executors.newVirtualThreadPerTaskExecutor();
    try {
      process = new ProcessBuilder(command).redirectErrorStream(true).start();
      final Process running = process;
      var output =
          executor.submit(
              () -> {
                try (var stream = running.getInputStream();
                    var bytes = new ByteArrayOutputStream()) {
                  byte[] buffer = new byte[4096];
                  int size;
                  while ((size = stream.read(buffer)) != -1) {
                    int remaining = 65536 - bytes.size();
                    if (remaining > 0) bytes.write(buffer, 0, Math.min(remaining, size));
                  }
                  return bytes.toString(StandardCharsets.UTF_8);
                }
              });
      if (!process.waitFor(25, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new IllegalStateException(
            "Docker excedió el plazo; verificar el estado antes de repetir");
      }
      String text = output.get(3, TimeUnit.SECONDS);
      if (process.exitValue() != 0)
        throw new IllegalStateException(
            "Docker no pudo completar la operación; revisar motor y permisos del socket");
      return text;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Control Docker interrumpido; estado incierto", ex);
    } catch (IllegalStateException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IllegalStateException("Motor Docker no disponible; revisar socket y permisos", ex);
    } finally {
      if (process != null && process.isAlive()) process.destroyForcibly();
      executor.close();
    }
  }
}
