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

La estrategia prevista es usar `numericOrderId` como key, dos particiones y un grupo
compartido por las dos instancias, con consumo en serie por partición. Su implementación
y prueba de ingesta todavía están pendientes.

Esto requiere que el productor respete la secuencia de publicación y el particionado por
key. No se usará `transactionTime` para ordenar ni se reparticionará con órdenes en
curso. Se acepta que órdenes de una misma partición compartan espera y que sólo haya dos
consumidores activos.

## 3. Identidad del ER e idempotencia

Muchos ER de una orden son normales. Se elige `(numericOrderId, fixId)` para identificar
cada reporte en el único flujo de entrada. Es un supuesto del contrato: `fixId` debe ser
estable en reentregas y único dentro de la orden; el enunciado no lo garantiza
expresamente. `secondaryTradeId` y `operationNumber` identifican ejecuciones, pero no se
asume que identifiquen todos los reportes de estado, incluidos NEW y CANCELLED.

Se prevé una restricción única y comparación de contenido antes de validar la transición
terminal. La deduplicación persistente todavía no está implementada; si el origen no
cumple el supuesto de identidad, será necesario acordar otra clave antes de atribuir
esta garantía.

## 4. Persistencia y atomicidad

Se elige PostgreSQL compartido por sus transacciones, restricciones y locks. Se prevé
escribir estado, contador y ledger en una transacción por ER, serializando
modificaciones de una orden mediante un lock de fila. JDBC hará explícito el SQL y
Flyway versionará el esquema. Las tablas y esta atomicidad de negocio todavía están
pendientes.

## 5. Estado mutable y ledger

Se prevé calcular transiciones desde el estado persistido más el ER: NEW inicial,
parciales y terminal FILLED/CANCELLED. Los acumulados serán snapshots, sin sumarlos como
deltas. El contador partirá del valor anterior. Estas reglas de dominio aún no están
implementadas.

Se prevé conservar el ER completo en un ledger con clave autoincremental y una entrada
por reporte efectivamente aplicado. Su persistencia todavía está pendiente.

La consulta conjunta de estado y ledger, ordenado por su clave de inserción, todavía
está pendiente.

## 6. Recuperación y errores de procesamiento

Se prevé confirmar el offset después del commit SQL y tolerar la reentrega mediante
deduplicación persistente. La ingesta y sus fronteras de falla aún están pendientes. Los
errores deberán conservarse y señalarse; el diseño de reintentos, cuarentena y
recuperación ordenada se completará con sus pruebas.

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
