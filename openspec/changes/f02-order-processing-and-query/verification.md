# Verificación de F-02

Fecha: 2026-10-18. Java 25.0.2, Maven Wrapper 3.9.11 y Docker Engine 29.8.2.
El enunciado `challenge-arch-max-capital..pdf` queda versionado en la raíz y
enlazado desde AGENTS.md y README.md.

## Evidencia observada

- `.\mvnw.cmd verify`: BUILD SUCCESS, cinco pruebas unitarias y once de
  integración; cero fallas, errores u omisiones.
- `.\mvnw.cmd spotless:check`: formato Java conforme.
- `openspec validate --all --strict`: change y specs válidos.
- Build Compose con las imágenes fijadas y ambas instancias saludables.
- `pwsh -File scripts/verify-f02.ps1 -SkipBuild`: éxito contra la imagen recién
  construida, dos consumidores con una partición cada uno, consulta desde ambos
  puertos, reinicio de aplicaciones y reentrega de terminales con lag cero.

## Cobertura de los criterios

| Área | Evidencia |
| --- | --- |
| Contrato | `OrderDomainTest`: ejemplo del PDF, conservación de campos, exactitud decimal, faltantes, key distinta, IDs fuera de rango, escala, estado y tiempo inválidos, claves JSON repetidas y contenido posterior al objeto |
| Dominio | `OrderDomainTest`: transiciones, NEW obligatorio, parciales como snapshots, FILLED, CANCELLED, acumulado regresivo y comparación de duplicado antes de terminal |
| SQL | `OrderPersistenceIT`: secuencia y contador, reentrega terminal idéntica, conflicto de contenido, restricción única, FK, PK y creación concurrente |
| Atomicidad | Trigger aislado de prueba que falla al insertar ledger: estado y contador vuelven a NEW/1 y el ledger conserva sólo el NEW; al quitar la falla se aplica el terminal |
| Kafka | `ExecutionReportKafkaIT`: órdenes intercaladas en dos particiones, confirmación posterior a SQL, duplicados y exclusión de ER abortados por el productor |
| Pendientes | Falla SQL deja offset en el ER fallido y no aplica su sucesor; tras quitar la falla y reiniciar el contenedor se aplican ambos en orden |
| Error permanente | JSON inválido detiene el contenedor, no confirma offset ni aplica el siguiente; vuelve a fallar tras reiniciar y un lector independiente encuentra el mensaje original |
| HTTP | `OrderPersistenceIT`: estado y ledger conjuntos, fixId en orden, campos adicionales, contador sin duplicados, 400 por ID inválido/desbordado y 404 por ausencia |
| Infraestructura | Tres pruebas de `RuntimeInfrastructureIT` conservan la verificación de salud, esquema, topics y transacciones Kafka de F-01 |
| Dos instancias | Smoke: ambas con una partición, respuestas iguales, IDs de ledger crecientes, snapshots y persistencia tras reinicio |

## Smoke observado

Órdenes aisladas conservadas para inspección:

- `179236104608900`, partición 0: NEW, dos parciales y FILLED; contador y ledger 4,
  acumulado 10, restante 0.
- `179236104608920`, partición 1: NEW, parcial y CANCELLED; contador y ledger 3,
  acumulado 7, restante 0.

Se publicaron nueve registros iniciales, incluidos duplicados de ambos terminales.
Después del reinicio se reenviaron ambos terminales; se esperó el avance de offsets
con lag cero y las cantidades/filas permanecieron iguales en 8081 y 8082.
No se eliminaron volúmenes, offsets ni datos ajenos.

## Límites

La deduplicación de F-02 usa `(numericOrderId, fixId)` en el único flujo de entrada;
el supuesto es que fixId es estable y único dentro de la orden. El contador incluye
NEW y terminal. CANCELLED exige restante cero. No hay settlement, resultado por
offset, recuperación automática de errores permanentes ni demostración completa
de todas las fronteras de caída y rebalanceo de F-03/F-04/F-05/F-06.

Ante error el contenedor de esa instancia se detiene: si tenía ambas particiones,
se detienen ambas. El mensaje permanece en Kafka y el diagnóstico identifica
topic/partición/offset/key. Las caídas de SQL y los mensajes permanentes son fallas
inyectadas en contenedores de prueba, no sobre datos ajenos de Compose.
