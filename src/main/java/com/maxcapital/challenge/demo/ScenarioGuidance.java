package com.maxcapital.challenge.demo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class ScenarioGuidance {
  private ScenarioGuidance() {}

  static String explanation(String id) {
    return switch (id) {
      case "interleaved-orders" ->
          "Cada orden conserva su secuencia aunque sus mensajes se mezclen con los de otra orden y se procesen en dos instancias.";
      case "quantity-snapshots" ->
          "Cada mensaje informa el total acumulado hasta ese momento. Un acumulado de 7 reemplaza al de 3: no se suman para obtener 10. El reloj del origen tampoco decide el orden.";
      case "terminal-duplicates" ->
          "Volver a publicar un mensaje ya aplicado no debe cambiar una orden ni agregar entradas a su historial, incluso después de completarla.";
      case "restart-persistence" ->
          "Las órdenes y su historial se guardan en PostgreSQL. Reiniciar ambas aplicaciones después de consumir el lote no debe perder ni duplicar esos datos.";
      case "committed-input-only" ->
          "Un mensaje dentro de una transacción abortada no debe llegar al procesamiento. Tener una posición en Kafka no significa que el mensaje sea visible ni aplicado.";
      case "settlement-once" ->
          "Una orden completada produce una única liquidación confirmada, incluso con duplicados y caída después de guardar SQL. Una cancelación no produce liquidación.";
      case "commit-boundary-recovery" ->
          "La consola observa una frontera real antes de provocar cada caída o reasignación. Recupera el mismo mensaje y verifica secuencia, datos y liquidación. Reinicia las aplicaciones afectadas y conserva los datos.";
      default -> "Esta comprobación requiere una feature del núcleo todavía pendiente.";
    };
  }

  static List<String> actions(String id) {
    return switch (id) {
      case "interleaved-orders" ->
          List.of(
              "Crear dos órdenes de 10 unidades en particiones diferentes.",
              "Intercalar creación, ejecuciones parciales y terminales de ambas órdenes.",
              "Comparar estado, historial y consultas de las dos instancias.");
      case "quantity-snapshots" ->
          List.of(
              "Crear una orden de 10 unidades.",
              "Enviar acumulados 0, 3, 7 y 10 con horas de origen decrecientes.",
              "Comprobar cada acumulado y sus unidades restantes, en secuencia de aplicación.");
      case "terminal-duplicates" ->
          List.of(
              "Crear una orden y completarla: dos mensajes aplicados.",
              "Republicar exactamente el mensaje de finalización.",
              "Republicar el mensaje de creación después del terminal y comparar resultados históricos.");
      case "restart-persistence" ->
          List.of(
              "Completar una orden y cancelar otra, y esperar la confirmación del lote.",
              "Capturar sus datos y reiniciar ambas aplicaciones.",
              "Republicar los dos terminales y comparar los datos completos antes y después.");
      case "committed-input-only" ->
          List.of(
              "Confirmar la transacción con el mensaje de creación.",
              "Abortar una transacción con una ejecución parcial de 7 unidades.",
              "Confirmar la finalización y verificar que sólo creación y finalización son visibles y aplicadas.");
      case "settlement-once" ->
          List.of(
              "Completar una orden y cancelar otra en particiones distintas.",
              "Republicar terminales y creación; contar salidas confirmadas y comparar su fuente SQL.",
              "Interrumpir otra finalización después de SQL, recuperar desde la otra instancia y restaurar ambas.");
      case "commit-boundary-recovery" ->
          List.of(
              "Crear órdenes nuevas y observar cada frontera antes de terminar procesos.",
              "Recuperar SQL abierto, SQL confirmado, envío aceptado, reasignación durante SQL y caída de ambas aplicaciones.",
              "Comprobar sucesores, resultado original, una liquidación y retorno a dos consumidores. Cada ventana crítica se resuelve dentro de un paso.");
      default -> List.of();
    };
  }

  static Map<String, Map<String, Object>> checks(String scenario) {
    var checks = new LinkedHashMap<String, Map<String, Object>>();
    add(
        checks,
        "ready",
        "Entorno preparado",
        "PostgreSQL y Kafka disponibles; dos consumidores con una partición cada uno");
    switch (scenario) {
      case "interleaved-orders", "restart-persistence" -> {
        add(
            checks,
            "partitions",
            "Procesamiento en paralelo",
            "Las órdenes A y B se publican en particiones diferentes");
        add(
            checks,
            "applied",
            "Mensajes aplicados",
            "7 resultados APPLIED, asociados a sus órdenes");
        add(
            checks,
            "order-a",
            "Orden A e historial",
            "Completada; acumulado 10; restante 0; 4 ER y 4 entradas en secuencia; ambas instancias coinciden");
        add(
            checks,
            "order-b",
            "Orden B e historial",
            "Cancelada; acumulado 7; restante 0; 3 ER y 3 entradas en secuencia; ambas instancias coinciden");
        if (scenario.equals("restart-persistence")) {
          add(
              checks,
              "recovered",
              "Consumo después del reinicio",
              "Ambas instancias vuelven a consumir con una partición cada una");
          add(
              checks,
              "duplicates",
              "Terminales republicados",
              "2 resultados DUPLICATE, sin aplicar otra vez");
          add(
              checks,
              "preserved",
              "Datos antes y después",
              "Estado, cantidades, contador y ledger completos iguales para ambas órdenes");
        }
      }
      case "quantity-snapshots" -> {
        for (int i = 0; i < 4; i++) {
          int amount = new int[] {0, 3, 7, 10}[i];
          add(
              checks,
              "snapshot-" + (i + 1),
              "Acumulado después del mensaje " + (i + 1),
              "Acumulado "
                  + amount
                  + "; restante "
                  + (10 - amount)
                  + "; "
                  + (i + 1)
                  + " ER y entradas en secuencia; ambas instancias coinciden");
        }
        add(
            checks,
            "clock",
            "Secuencia frente al reloj",
            "Acumulados 0 → 3 → 7 → 10 con horas de origen no crecientes");
      }
      case "terminal-duplicates" -> {
        add(
            checks,
            "new",
            "Creación de la orden",
            "Creada; 1 ER aplicado y 1 entrada de historial");
        add(
            checks,
            "terminal",
            "Finalización sin doble efecto",
            "Completada; 2 ER aplicados y 2 entradas, incluso tras repetir el terminal");
        add(
            checks,
            "duplicates",
            "Cuatro envíos y sus resultados",
            "2 APPLIED y 2 DUPLICATE en cuatro posiciones distintas");
        add(
            checks,
            "history",
            "Resultado original y duplicado tardío",
            "NEW original conserva estado Creada; repetición conserva Completada y referencia al mismo ledger");
        add(
            checks,
            "final",
            "Orden final e historial",
            "Completada; contador y ledger permanecen en 2; ambas instancias coinciden");
      }
      case "committed-input-only" -> {
        add(checks, "new", "Creación confirmada", "Creada; 1 ER aplicado y 1 entrada de historial");
        add(
            checks,
            "aborted",
            "Transacción parcial abortada",
            "El productor aborta el parcial; todavía falta verificar su ausencia de efectos");
        add(
            checks,
            "visibility",
            "Lectura confirmada del broker",
            "Sólo creación y finalización visibles; parcial abortado excluido tras observar el sucesor");
        add(
            checks,
            "no-result",
            "Ausencia de resultado del parcial",
            "Sin fila SQL para la posición abortada, después del sucesor confirmado");
        add(
            checks,
            "applied",
            "Mensajes confirmados aplicados",
            "2 resultados APPLIED: creación y finalización");
        add(
            checks,
            "final",
            "Orden final e historial",
            "Completada; acumulado 10; contador y ledger 2, con fixId 1 y 3; ambas instancias coinciden");
      }
      case "settlement-once" -> {
        add(
            checks,
            "filled",
            "Orden completada",
            "FILLED; 3 ER aplicados y ledger en secuencia; ambas consultas iguales");
        add(
            checks,
            "cancelled",
            "Orden cancelada",
            "CANCELLED con parcial 4; 3 ER y ledger en secuencia");
        add(
            checks,
            "duplicates",
            "Republicaciones",
            "3 DUPLICATE sin cambios de contador ni historial");
        add(
            checks,
            "settlement",
            "Salida confirmada",
            "Un settlement por FILLED y cero por CANCELLED; fuente y cantidades originales");
        add(
            checks,
            "recovery",
            "Recuperación después de SQL",
            "Mismo APPLIED y un settlement tras caída real y reasignación");
        add(
            checks,
            "restored",
            "Entorno recuperado",
            "Dos consumidores activos, una partición cada uno");
      }
      case "commit-boundary-recovery" -> {
        add(
            checks,
            "sql-open",
            "Caída durante SQL",
            "Rollback sin cambios parciales; reentrega y sucesores en secuencia");
        add(
            checks,
            "sql-committed",
            "Caída después de SQL",
            "Resultado original conservado; una liquidación confirmada");
        add(
            checks,
            "send-ack",
            "Caída tras envío aceptado",
            "Salida abierta invisible; una liquidación después de resolver intentos");
        add(
            checks,
            "rebalance",
            "Reasignación durante escritura",
            "Nuevo propietario observado antes de liberar al anterior; intento obsoleto rechazado");
        add(
            checks,
            "both-down",
            "Caída de ambas instancias",
            "Recuperación persistente, sucesores y nuevas órdenes en ambas particiones");
        add(
            checks,
            "restored",
            "Entorno recuperado",
            "Dos consumidores activos, una partición cada uno");
      }
      default -> throw new IllegalArgumentException("Escenario sin guía");
    }
    return checks;
  }

  private static void add(
      Map<String, Map<String, Object>> checks, String id, String label, String expected) {
    var check = new LinkedHashMap<String, Object>();
    check.put("id", id);
    check.put("label", label);
    check.put("expected", expected);
    check.put("observed", null);
    check.put("status", "NOT_OBSERVED");
    check.put("references", Map.of());
    checks.put(id, check);
  }
}
