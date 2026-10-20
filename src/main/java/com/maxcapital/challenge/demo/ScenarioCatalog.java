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
      String expected) {}

  public List<Entry> entries() {
    var ready =
        List.of(
            "PostgreSQL y Kafka disponibles",
            "Dos consumidores activos con una partición cada uno");
    return List.of(
        new Entry(
            "interleaved-orders",
            "Órdenes intercaladas",
            true,
            "F-02/F-03",
            "Observar la secuencia por orden en ambas particiones",
            ready,
            "FILLED con 4 ER y CANCELLED con 3; estado y ledger consistentes en ambas instancias"),
        new Entry(
            "quantity-snapshots",
            "Snapshots y reloj del origen",
            true,
            "F-02",
            "Comprobar acumulados sin sumarlos y secuencia independiente del reloj",
            ready,
            "Acumulados 0, 3, 7 y 10; cuatro entradas de ledger"),
        new Entry(
            "terminal-duplicates",
            "Duplicados después del terminal",
            true,
            "F-03",
            "Republicar FILLED y NEW después de completar una orden",
            ready,
            "Dos APPLIED y dos DUPLICATE; contador y ledger permanecen en dos"),
        new Entry(
            "restart-persistence",
            "Persistencia después del lote",
            true,
            "F-02/F-03",
            "Reiniciar ambas instancias después de consumir y republicar terminales",
            ready,
            "Estado y ledger completos iguales; nuevos resultados DUPLICATE. No prueba caídas entre commits"),
        new Entry(
            "committed-input-only",
            "Transacciones confirmadas y abortadas",
            true,
            "F-02/F-03",
            "Observar qué entrada ve un consumidor read_committed",
            ready,
            "NEW y FILLED aplicados; parcial abortado sin efecto SQL"),
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
