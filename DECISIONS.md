# Decisiones de diseño

Este documento justifica las decisiones que resuelven los problemas del
[enunciado](challenge-arch-max-capital..pdf), sus supuestos y sus costos. El README
indica el estado de las features; los comandos y detalles operativos están allí y en
OpenSpec. Las propuestas pendientes se distinguen del comportamiento implementado y
de la evidencia disponible.

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

Topic, partición y offset identifican un registro Kafka. Una reentrega conserva esa
posición, pero republicar el mismo ER crea otro registro: deduplicar sólo por offset
no cubriría ese caso. La pareja elegida identifica el reporte de negocio. Un futuro
resultado persistido por posición Kafka complementaría esa identidad para recuperar el
procesamiento de cada registro.

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

La unicidad protege la creación concurrente de la primera fila; el lock por orden
serializa las lecturas y modificaciones posteriores. El segundo intento espera y evalúa
el estado confirmado. Una estrategia optimista requeriría detectar conflictos y
reintentar la transición completa; para el alcance del ejercicio, la espera explícita simplifica
el razonamiento. El lock termina con la transacción SQL y la secuencia del flujo sigue
dependiendo de Kafka.

La transacción del caso de uso se vincula explícitamente al gestor JDBC, aun cuando
exista también un gestor Kafka. La atomicidad de estado y ledger depende del commit
PostgreSQL.

## 5. Estado mutable y ledger

El dominio calcula cada transición desde el estado anterior y el ER entrante. Exige NEW
inicial, cantidades compatibles y acumulado no decreciente; rechaza nuevos reportes
sobre FILLED o CANCELLED. Los acumulados son snapshots: se validan y adoptan, sin
sumarlos al acumulado previo. El contador se incrementa desde su valor anterior por
cada ER aplicado, incluidos NEW y terminal; los duplicados no cuentan.

Se mantiene una entidad orden con el estado vigente para validar el siguiente ER y
consultarla sin reconstruir todo su historial. El ledger conserva la traza de cómo se
llegó a ese estado. Al escribir ambos juntos, cada incremento del contador corresponde
a una entrada aplicada del ledger.

Se asume que el total de la orden no cambia. CANCELLED exige restante cero y conserva
el acumulado informado, que puede incluir ejecuciones previas. Las cantidades usan
BigDecimal y NUMERIC para comparar y persistir valores decimales exactos sin redondeo
binario. Estos supuestos concretan aspectos que el ejemplo del enunciado no formaliza.

El ledger conserva el ER completo en JSONB, incluidos campos adicionales, con una
identidad global autoincremental por inserción. Puede haber huecos por rollback: la
clave ordena el ledger de cada orden y no representa un orden global de commits entre
órdenes distintas.

La consulta HTTP obtiene estado y ledger con un único SELECT y JOIN, bajo el mismo
snapshot confirmado. Ordena por `ledgerId`, evitando combinar un estado nuevo con un
ledger anterior. Se devuelve el ledger completo; el ejercicio no requiere paginación.

## 6. Recuperación y errores de procesamiento

El listener primero completa la transacción SQL y después confirma sincrónicamente el
offset Kafka. Se acepta la reentrega y se protege el efecto SQL con idempotencia. El
consumidor usa `read_committed` para excluir ER de transacciones productoras abortadas.
Esto controla la visibilidad de la entrada; la base y Kafka siguen teniendo commits
independientes.

| Momento de la falla | Consecuencia esperada |
| --- | --- |
| Antes de enviar el commit SQL | Se revierten las escrituras y el ER queda disponible para reentrega. |
| Después de SQL y antes de confirmar el offset | El ER puede reentregarse; la identidad y el ledger evitan aplicarlo otra vez. |
| Después de confirmar el offset | El cambio SQL ya quedó confirmado; el grupo puede continuar desde el siguiente registro. |

Ante error se registra topic, partición, offset y key, y se detiene el contenedor
consumidor de esa instancia sin confirmar el registro fallido. Si tenía ambas
particiones, ambas se detienen. Para fallas transitorias se requiere resolver la causa y
reiniciar el consumidor.

Un mensaje inválido permanece en Kafka y vuelve a fallar al reiniciar; los ER
posteriores de la partición esperan. Esta política evita descartar información, pero aún
puede bloquear indefinidamente, una brecha frente al enunciado. No hay clasificación
automática, reintentos acotados ni dead-lettering. F-06 debe resolver esos puntos,
conservar pendientes por orden y dar una recuperación operativa. Enviar sólo el ER
fallido a otro topic y aplicar sus sucesores dejaría una orden incompleta: también debe
definirse cómo se retienen o recuperan esos sucesores. La orden consultada representa
sólo el prefijo aplicado, sin indicador persistente de incompletitud.

Las pruebas actuales verifican rollback, duplicados y replay tras una falla SQL; el
smoke verifica persistencia y reentrega después de reinicios. Todavía no inyectan una
caída de proceso justo entre commit SQL y offset ni un rebalanceo durante una escritura.
La recuperación completa de esas fronteras sigue pendiente de F-05.

## 7. Settlement: garantía pendiente

La publicación de settlement todavía no está implementada. FILLED deberá originar uno
por orden y CANCELLED ninguno. Se descarta atribuir esa garantía a una outbox por sí
sola: si el publicador envía y cae antes de registrar el envío, puede republicar el
mismo evento.

La solución también debe cubrir la caída después de guardar FILLED y antes de publicar:
la reentrega sería un duplicado y no volvería a ejecutar la transición de dominio. Por
eso la intención y los datos del settlement deben quedar recuperables junto con el
resultado SQL, y la recuperación debe poder completar la publicación pendiente.

F-03/F-04/F-05 deberán resolver y probar la frontera SQL/Kafka, incluidos resultados
inciertos del commit. Una alternativa en evaluación combina resultados persistidos por
registro de entrada con salidas y offset en una transacción Kafka. La garantía deberá
especificar qué observa un consumidor `read_committed`; los efectos externos de un
consumidor arbitrario necesitan su propio contrato de idempotencia.

## 8. Límites y evidencia de las garantías

Se prioriza un servicio pequeño con dos instancias y persistencia compartida. Se dejan
fuera alta disponibilidad del broker/base, recuperación por pérdida física de volúmenes,
resets arbitrarios de offsets y recreación de topics. Una versión operativa necesitaría
replicación, respaldos y procedimientos de recuperación compatibles con la identidad y
la secuencia de los ER.

Se priorizan pruebas de las fronteras donde una falla puede romper las garantías:
transacción SQL, secuencia Kafka y reentrega. La evidencia actual incluye [pruebas SQL y
HTTP](src/test/java/com/maxcapital/challenge/OrderPersistenceIT.java) de rollback
conjunto, concurrencia y duplicados terminales, y [pruebas
Kafka](src/test/java/com/maxcapital/challenge/ExecutionReportKafkaIT.java) de órdenes
intercaladas, reentrega y errores sin avance de offset. El [smoke de dos
instancias](scripts/verify-f02.ps1) comprueba consulta y persistencia tras reinicio.
Estas pruebas no demuestran settlement ni recuperación completa de mensajes permanentes.

## 9. Resultado por posición Kafka: propuesta de F-03

El change `f03-persisted-processing-results` propone complementar la identidad del
ER con un resultado inmutable por `(topic, partition, offset)`. Todavía no está
implementado. Un registro nuevo se clasificaría como `APPLIED` o `DUPLICATE` y
conservaría identidad del ER, referencia al ledger y snapshot de la orden al
resolver esa posición, en la misma transacción SQL que sus efectos.

La reentrega de la misma posición recuperaría el resultado original, incluso un
`APPLIED` anterior, sin reclasificarlo a partir del estado actual. Republicar el
ER en otra posición produciría `DUPLICATE` sin agregar ledger ni contador. Para
ese duplicado, el snapshot sería el vigente al resolver la nueva posición y el
ledger seguiría apuntando al ER originalmente aplicado. Esta distinción evita
confundir un snapshot terminal con una nueva aplicación terminal.

Se mantendría la comparación completa JSONB y el lock por orden, con una segunda
lectura del resultado después de adquirirlo para resolver intentos concurrentes.
Los errores continuarían pendientes con la política actual: no se propone guardar
fallas como resultados definitivos ni habilitar avance del offset por ellas. La
ausencia de resultado no distinguiría un registro no intentado de uno fallido.

F-02 no conserva posiciones en su ledger, por lo que no se reconstruirían
resultados históricos ni se inventaría su relación con offsets. La cobertura
comenzaría al desplegar F-03 con ambas instancias detenidas durante la actualización;
una reentrega de un ER previo sin resultado se resolvería por identidad de negocio.

Esta base no garantiza settlements: F-04 debe definir la salida y coordinar su
publicación con el offset, y F-05 probar caídas, rebalanceos y resultados inciertos.
Un consumidor `read_committed` sólo tiene actualmente garantías sobre la entrada.
El diseño, sus alternativas, migración y criterios de aceptación quedan en el
change; esta sección se actualizará con evidencia al implementarlo.

El primer incremento de F-03 agrega la migración V2 y el adaptador SQL. La PK
es `(topic, partition_id, record_offset)`; una FK compuesta al ledger impide
asociar un resultado con una identidad distinta. Las pruebas de esquema verifican
base nueva, actualización desde V1 con datos, round-trip y comparación JSONB.
La integración atómica con el caso de uso todavía está pendiente en este punto.

La integración SQL del segundo incremento está verificada con transacciones
reales: resultado y efectos se confirman juntos; fallas al insertar resultado o
ledger revierten creación y actualización. Dos contextos concurrentes recuperan
el mismo resultado para una posición y distinguen republicaciones. Cerrar y
reabrir contextos mantiene snapshots originales. La integración Kafka y la
verificación integral siguen pendientes antes de declarar F-03 implementada.
