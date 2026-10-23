# Verificación de F-06 simplificada

## Alcance observado

Dos tablas nuevas, posición original pendiente y pausa por partición. Correcciones
auditadas con resolución en la transacción SQL existente y reanudación por reinicio.
Retry de rollback SQL conocido con cinco intentos y backoff exponencial más jitter
en memoria. DLQ como aviso al menos una vez, sin confirmar la entrada inválida.

Las pruebas focalizadas verifican migración V2→V3 sin cambiar órdenes/ledger/resultados,
originales binarios, idempotencia/conflictos de corrección, rollback conjunto,
rechazo auditado, replay APPLIED/DUPLICATE y ausencia de settlement para CANCELLED.
Kafka verifica agotamiento del retry, recuperación transitoria, fallo de envío DLQ
sin avance, repetición del aviso con el mismo ID, dos miembros haciendo polls con
una partición pausada, reasignación OPEN y recuperación ordenada tras corregir.
Una JVM real se termina después del commit SQL de un FILLED corregido: el
sobreviviente conserva resultado/ledger y confirma un único settlement.

## Suite completa

El 23 de octubre de 2026, ` .\mvnw.cmd spotless:apply verify ` terminó con
BUILD SUCCESS: 21 unitarias y 57 integraciones, cero fallas, errores u omisiones.
Incluye nueve ensayos de settlement y seis de recuperación con procesos reales.
`openspec validate --all --strict`: 11 elementos aprobados. `git diff --check`: limpio.

La revisión completa detectó un fixture SQL manual sin registrar el nuevo adapter;
se corrigió registrando `JdbcQuarantineStore` y se volvió a ejecutar la suite completa.
El último ajuste de texto del título del lote usa la cantidad real de escenarios;
la imagen de consola se reconstruyó con Spotless y compilación incluidos.

## Recorridos Compose

Se utilizó el proyecto aislado `max-capital-f06-simple`, consola en puerto 8084 y
apps en 8085/8086. El entorno anterior conserva V3/V4/V5 de la implementación
descartada: no se modificaron esos datos, volúmenes ni su historial Flyway.
El override local de puertos está en `target/f06-compose.yaml`.

- `verify-console`: PASSED, lote `f609d6e8-6a1a-4a28-a1c0-562660111e46`;
  ocho escenarios, cero pendientes y ocho órdenes previas conservadas.
  Incluye los recorridos F-04/F-05, caída de PostgreSQL/Kafka y restauración.
- `verify-f06`: automático `be3292b6-0e84-405e-ac32-22f95202dfa3`,
  paso a paso `79d36d3f-1727-4509-94be-105b5d26559a` y cancelación restaurada
  `88c7417b-a5d7-40f9-9abe-31596b4c2ad4`: PASSED, 33 órdenes previas conservadas.
  Repite el recorrido con IDs distintos después de otro smoke F-06 y del lote.
- La web existente mostró el escenario 8, su explicación y sus siete verificaciones;
  se lanzó desde el botón Ejecutar escenario, sin agregar HTML ni formularios.

Comandos portables habituales, con el entorno correspondiente levantado:

```sh
docker compose --profile tools run --build --rm verify-f06
docker compose --profile tools run --build --rm verify-console
```

Los logs locales están en `target/f06-final-verify.log`,
`target/f06-console-smoke.log` y `target/f06-smoke-repeat.log`.

## Límites

La pausa afecta todas las órdenes de la partición. El presupuesto de retry se
renueva al reiniciar o cambiar de propietario. DLQ puede duplicar avisos entre sus
commits Kafka/SQL. La corrección exige identidad/routing compatibles y reinicio;
no hay reparación automática ni autenticación del responsable declarado.
La restauración del escenario depende de su consola viva; ante pérdida de ésta,
se consulta SQL/DLQ y se corrige y reinicia manualmente, conservando evidencia.
