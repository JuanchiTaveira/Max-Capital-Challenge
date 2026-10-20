package com.maxcapital.challenge.demo;

import java.util.List;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("evaluation-console")
public class ScenarioCatalog {
  public record Entry(
      String id,
      String title,
      boolean available,
      String dependency,
      String objective,
      List<String> preconditions,
      String expected,
      String explanation,
      List<String> actions) {
    Entry(
        String id,
        String title,
        boolean available,
        String dependency,
        String objective,
        List<String> preconditions,
        String expected) {
      this(
          id,
          title,
          available,
          dependency,
          objective,
          preconditions,
          expected,
          ScenarioGuidance.explanation(id),
          ScenarioGuidance.actions(id));
    }
  }

  public List<Entry> entries() {
    var ready =
        List.of(
            "PostgreSQL y Kafka disponibles",
            "Dos consumidores activos con una partición cada uno");
    return List.of(
        new Entry(
            "interleaved-orders",
            "Dos órdenes en paralelo",
            true,
            "F-02/F-03",
            "Observar la secuencia por orden en ambas particiones",
            ready,
            "Orden A completada con 4 mensajes aplicados y 4 entradas de historial; orden B cancelada con 3. Ambas instancias devuelven los mismos datos."),
        new Entry(
            "quantity-snapshots",
            "Las cantidades acumuladas no se suman",
            true,
            "F-02",
            "Comprobar acumulados sin sumarlos y secuencia independiente del reloj",
            ready,
            "Acumulados 0 → 3 → 7 → 10, sin sumarlos entre sí. La orden termina completada con 4 mensajes aplicados y 4 entradas de historial."),
        new Entry(
            "terminal-duplicates",
            "Los mensajes repetidos no duplican efectos",
            true,
            "F-03",
            "Republicar FILLED y NEW después de completar una orden",
            ready,
            "De 4 envíos, sólo 2 se aplican; los otros 2 se reconocen como duplicados. La orden completada conserva contador e historial en 2."),
        new Entry(
            "restart-persistence",
            "Los datos sobreviven al reinicio",
            true,
            "F-02/F-03",
            "Reiniciar ambas instancias después de consumir y republicar terminales",
            ready,
            "Las dos órdenes conservan estado e historial completos. Los terminales repetidos son duplicados. El reinicio sucede después del lote, no entre commits."),
        new Entry(
            "committed-input-only",
            "Las transacciones abortadas no afectan las órdenes",
            true,
            "F-02/F-03",
            "Observar qué entrada ve un consumidor read_committed",
            ready,
            "Sólo creación y finalización son visibles y se aplican: 2 entradas de historial. El parcial abortado no tiene resultado ni efecto SQL. No comprueba settlement."),
        new Entry(
            "settlement-once",
            "Settlement único",
            false,
            "F-04/F-05",
            "Comprobar salida exactamente una vez ante reentregas y fallas",
            List.of("Publicación y fronteras de recuperación implementadas"),
            "Un settlement por FILLED y ninguno por CANCELLED, observado con read_committed"),
        new Entry(
            "commit-boundary-recovery",
            "Caídas entre commits y rebalanceos",
            false,
            "F-05",
            "Ubicar una caída durante escritura o entre confirmaciones",
            List.of("Instrumentación determinista y evidencia de F-05"),
            "Sin pérdida ni doble aplicación; resultados originales recuperados"),
        new Entry(
            "poison-recovery",
            "Mensajes inválidos y recuperación ordenada",
            false,
            "F-06",
            "Probar JSON/key inválidos, identidad conflictiva y orden incompleta",
            List.of("Entorno de ensayo aislado y recuperación de F-06"),
            "Diagnóstico, conservación de pendientes y recuperación sin descartar ni saltar ER"));
  }

  public List<String> available() {
    return entries().stream().filter(Entry::available).map(Entry::id).toList();
  }
}
