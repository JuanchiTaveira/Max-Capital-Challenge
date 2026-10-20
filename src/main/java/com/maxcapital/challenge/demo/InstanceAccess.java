package com.maxcapital.challenge.demo;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("evaluation-console")
public class InstanceAccess {
  private final Map<String, String> addresses;

  public InstanceAccess(
      @Value("${demo.app-1-url}") String first, @Value("${demo.app-2-url}") String second) {
    addresses = Map.of("app-1", first, "app-2", second);
  }

  public Map<String, Object> read(String instance, String path) {
    String base = addresses.get(instance);
    if (base == null) throw new IllegalArgumentException("Instancia no permitida");
    try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
      var response =
          client.send(
              HttpRequest.newBuilder(URI.create(base + path))
                  .timeout(Duration.ofSeconds(3))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      var result = ConsoleValues.observed(instance + path);
      result.put("statusCode", response.statusCode());
      result.put("data", ConsoleValues.JSON.readTree(response.body()));
      return result;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Consulta interrumpida: " + instance, ex);
    } catch (Exception ex) {
      throw new IllegalStateException("Instancia no disponible: " + instance, ex);
    }
  }

  public Map<String, Object> order(String instance, String id) {
    ConsoleValues.positive(id);
    var result = read(instance, "/orders/" + id);
    if ((int) result.get("statusCode") == 200) {
      result.put(
          "data", ConsoleValues.publicOrder((tools.jackson.databind.JsonNode) result.get("data")));
    }
    return result;
  }
}
