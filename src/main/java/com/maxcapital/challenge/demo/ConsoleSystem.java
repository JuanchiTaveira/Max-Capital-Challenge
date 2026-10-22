package com.maxcapital.challenge.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@Profile("evaluation-console")
public class ConsoleSystem {
  private final InstanceAccess instances;
  private final JdbcTemplate jdbc;
  private final DemoGate gate;
  private final String bootstrap;
  private final String group;
  private final Map<String, Map<String, Object>> lastSuccessful =
      new java.util.concurrent.ConcurrentHashMap<>();

  public ConsoleSystem(
      InstanceAccess instances,
      JdbcTemplate jdbc,
      DemoGate gate,
      @Value("${spring.kafka.bootstrap-servers}") String bootstrap,
      @Value("${spring.kafka.consumer.group-id}") String group) {
    this.instances = instances;
    this.jdbc = jdbc;
    this.gate = gate;
    this.bootstrap = bootstrap;
    this.group = group;
  }

  public Map<String, Object> observe() {
    var result = ConsoleValues.observed("Consola");
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var first = executor.submit(() -> instance("app-1"));
      var second = executor.submit(() -> instance("app-2"));
      var kafka = executor.submit(() -> capture("Kafka Admin", this::group));
      var database =
          executor.submit(
              () ->
                  capture(
                      "PostgreSQL",
                      () ->
                          Map.of(
                              "connected", jdbc.queryForObject("SELECT 1", Integer.class) == 1)));
      result.put("instances", List.of(first.get(), second.get()));
      result.put("kafka", kafka.get());
      result.put("database", database.get());
      result.put("activeRunId", gate.active());
      return result;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Observación interrumpida", ex);
    } catch (Exception ex) {
      throw new IllegalStateException("No se pudo completar la observación", ex);
    }
  }

  public Map<String, Object> instance(String service) {
    var result = ConsoleValues.observed(service);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var readiness =
          executor.submit(
              () ->
                  capture(
                      service + " readiness",
                      () -> instances.read(service, "/actuator/health/readiness")));
      var liveness =
          executor.submit(
              () ->
                  capture(
                      service + " liveness",
                      () -> instances.read(service, "/actuator/health/liveness")));
      var consumer =
          executor.submit(
              () ->
                  capture(
                      service + " consumer",
                      () -> instances.read(service, "/internal/demo/consumer")));
      result.put("service", service);
      result.put("readiness", readiness.get());
      result.put("liveness", liveness.get());
      result.put("consumer", consumer.get());
      return result;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Observación interrumpida", ex);
    } catch (Exception ex) {
      throw new IllegalStateException("Instancia no disponible", ex);
    }
  }

  public Map<String, Object> group() {
    try (var admin =
        AdminClient.create(
            Map.of(
                "bootstrap.servers",
                bootstrap,
                "default.api.timeout.ms",
                3000,
                "request.timeout.ms",
                3000))) {
      var description =
          admin.describeConsumerGroups(List.of(group)).all().get(4, TimeUnit.SECONDS).get(group);
      var committed =
          admin
              .listConsumerGroupOffsets(
                  group, new ListConsumerGroupOffsetsOptions().requireStable(true))
              .partitionsToOffsetAndMetadata()
              .get(4, TimeUnit.SECONDS);
      var requests =
          Map.of(
              new TopicPartition("execution-reports", 0),
              OffsetSpec.latest(),
              new TopicPartition("execution-reports", 1),
              OffsetSpec.latest());
      var ends = admin.listOffsets(requests).all().get(4, TimeUnit.SECONDS);
      var partitions = new ArrayList<Map<String, Object>>();
      for (var entry : ends.entrySet()) {
        var tp = entry.getKey();
        var offset = committed.get(tp);
        var row = ConsoleValues.observed("Kafka Admin");
        row.put("topic", tp.topic());
        row.put("partition", Integer.toString(tp.partition()));
        row.put("committedOffset", offset == null ? null : Long.toString(offset.offset()));
        row.put("endOffset", Long.toString(entry.getValue().offset()));
        row.put(
            "lag",
            offset == null
                ? null
                : Long.toString(Math.max(0, entry.getValue().offset() - offset.offset())));
        partitions.add(row);
      }
      return Map.of(
          "groupId",
          group,
          "members",
          description.members().stream()
              .map(
                  m ->
                      Map.of(
                          "clientId",
                          m.clientId(),
                          "consumerId",
                          m.consumerId(),
                          "assignments",
                          m.assignment().topicPartitions().stream()
                              .map(
                                  p ->
                                      Map.of(
                                          "topic",
                                          p.topic(),
                                          "partition",
                                          Integer.toString(p.partition())))
                              .toList()))
              .toList(),
          "partitions",
          partitions);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Kafka interrumpido", ex);
    } catch (Exception ex) {
      throw new IllegalStateException("Grupo Kafka no disponible", ex);
    }
  }

  Map<String, Object> capture(String source, Supplier<?> read) {
    var result = ConsoleValues.observed(source);
    try {
      result.put("data", read.get());
      result.put("available", true);
      result.put("stale", false);
      lastSuccessful.put(source, new java.util.LinkedHashMap<>(result));
    } catch (RuntimeException ex) {
      result.put("available", false);
      var previous = lastSuccessful.get(source);
      result.put("stale", previous != null);
      if (previous != null) {
        result.put("attemptedAt", result.get("observedAt"));
        result.put("observedAt", previous.get("observedAt"));
        result.put("data", previous.get("data"));
      }
      result.put("error", "Consulta no disponible; revisar la fuente y reintentar");
    }
    return result;
  }
}
