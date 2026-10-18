# Decisiones de diseño

Este documento justifica las decisiones del núcleo y sus límites. El README indica el
estado de las features. Las decisiones previstas se distinguen del comportamiento
implementado y de la evidencia disponible.

## 1. Broker: Kafka

Se elige Kafka porque conserva registros y permite releerlos por offset; sus particiones
y grupos de consumidores permiten repartir órdenes entre dos instancias sin necesitar un
coordinador propio. La contrapartida es que el orden sólo existe dentro de cada
partición y debemos respetar ese límite.

La demo usa un broker y volúmenes persistentes para reducir requisitos de ejecución.
Esto permite ensayar reinicios, pero no tolera la pérdida física del broker o de su
disco. Los topics no expiran durante el challenge; una operación prolongada necesitaría
una política de retención y capacidad.

## 2. Secuencia por orden y concurrencia

Todos los ER de una orden usan como key el `numericOrderId` en decimal canónico. Dos
particiones y un mismo grupo reparten el flujo entre las dos instancias. Cada instancia
tiene un consumidor y procesa sus registros secuencialmente, sin delegar ER de una
partición a tareas paralelas.

Se asume que el productor publica los ER en secuencia y con particionado estable por
key, como permite el enunciado. `transactionTime` se conserva como dato; no se usa para
ordenar. No se aumenta el número de particiones con órdenes en curso, porque una key
podría cambiar de partición.

Se resigna paralelismo dentro de la partición: una orden lenta afecta a otras que la
comparten. Con dos particiones hay hasta dos consumidores activos. El bloqueo SQL
protege la actualización concurrente, pero la secuencia del flujo depende del
particionado y del consumo en serie.

## 3. Identidad del ER e idempotencia

Muchos ER de una orden son normales. Se elige `(numericOrderId, fixId)` para identificar
cada reporte en el único flujo de entrada. Es un supuesto del contrato: `fixId` debe ser
estable en reentregas y único dentro de la orden; el enunciado no lo garantiza
expresamente. `secondaryTradeId` y `operationNumber` identifican ejecuciones, pero no se
asume que identifiquen todos los reportes de estado, incluidos NEW y CANCELLED.

Una restricción única sobre esa pareja protege el ledger. Bajo el lock de la orden se
compara el JSONB completo: contenido equivalente es un no-op; la misma identidad con
contenido distinto es un error. El duplicado se comprueba antes de validar la
transición, por lo que reenviar un terminal idéntico no incrementa el contador ni agrega
ledger.

## 4. Persistencia y atomicidad

Se usa PostgreSQL compartido porque las transacciones, restricciones y locks permiten
mantener orden y ledger consistentes entre ambas instancias. JDBC deja explícito el SQL
y Flyway versiona el esquema. Esto agrega una dependencia común y una escritura
transaccional por ER.

En una transacción SQL se inicializa la orden si falta, se bloquea su fila con `SELECT
FOR UPDATE`, se deduplica, se calcula el nuevo estado y se escriben estado, contador y
ledger. Si cualquier paso falla, todo se revierte. La base y Kafka conservan
transacciones independientes.

## 5. Estado mutable y ledger

El dominio calcula cada transición desde el estado anterior y el ER entrante. Exige NEW
inicial, cantidades compatibles y acumulado no decreciente; rechaza nuevos reportes
sobre FILLED o CANCELLED. Los acumulados son snapshots: se validan y adoptan, sin
sumarlos al acumulado previo. El contador se incrementa desde su valor anterior.

El ledger conserva el ER completo en JSONB, incluidos campos adicionales, con una
identidad global autoincremental por inserción. Puede haber huecos por rollback: la
clave ordena el ledger de cada orden y no representa un orden global de commits entre
órdenes distintas.

La consulta conjunta de estado y ledger, ordenado por su clave de inserción, todavía
está pendiente.

## 6. Recuperación y errores de procesamiento

El listener primero completa la transacción SQL y después confirma sincrónicamente el
offset Kafka. Una falla antes del commit SQL revierte sus escrituras. Si SQL confirmó
pero el offset no, la reentrega encuentra el ER ya aplicado y no vuelve a modificar la
orden. No hay una transacción ACID distribuida entre ambos sistemas.

Ante error se registra topic, partición, offset y key, y se detiene el contenedor
consumidor de esa instancia sin confirmar el registro fallido. Si tenía ambas
particiones, ambas se detienen. Para fallas transitorias se requiere resolver la causa y
reiniciar el consumidor.

Un mensaje inválido permanece en Kafka y vuelve a fallar al reiniciar; los ER
posteriores de la partición esperan. Esta política evita descartar información, pero aún
puede bloquear indefinidamente. F-06 debe distinguir errores, conservar pendientes por
orden y dar una recuperación operativa. La orden consultada representa sólo el prefijo
aplicado, sin indicador persistente de incompletitud.

La recuperación completa ante caídas y rebalanceos sigue pendiente de F-05; los
escenarios ya probados no bastan para afirmar todas sus fronteras de falla.

## 7. Settlement: garantía pendiente

La publicación de settlement todavía no está implementada. FILLED deberá originar uno
por orden y CANCELLED ninguno. Se descarta atribuir esa garantía a una outbox por sí
sola: si el publicador envía y cae antes de registrar el envío, puede republicar el
mismo evento.

F-03/F-04/F-05 deberán resolver y probar la frontera SQL/Kafka, incluidos resultados
inciertos del commit. Una alternativa en evaluación combina resultados persistidos por
registro de entrada con salidas y offset en una transacción Kafka. La garantía deberá
especificar qué observa un consumidor `read_committed`; los efectos externos de un
consumidor arbitrario necesitan su propio contrato de idempotencia.

## 8. Supuestos y límites elegidos

El contador considera aplicado cada ER aceptado, incluidos NEW y terminal; los
duplicados no cuentan. CANCELLED interpreta restante cero como cancelación de lo no
ejecutado y permite conservar el acumulado operado. El total de la orden no cambia. Se
usa JSON y cantidades decimales exactas con hasta 12 dígitos enteros y 8 decimales.

Se prioriza un servicio pequeño con dos instancias y persistencia compartida. Se dejan
fuera alta disponibilidad del broker/base, recuperación por pérdida física de volúmenes,
resets arbitrarios de offsets y recreación de topics. Una versión operativa necesitaría
replicación, respaldos y procedimientos de recuperación compatibles con la identidad y
la secuencia de los ER.
