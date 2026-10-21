# Decisiones de diseño

Este documento justifica las decisiones que resuelven los problemas del
[enunciado](challenge-arch-max-capital.pdf), sus supuestos y sus costos. El README
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
no cubriría ese caso. La pareja elegida identifica el reporte de negocio. F-03 complementa esa identidad con un resultado
persistido por posición Kafka para recuperar el procesamiento de cada registro.

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
ledger y el resultado de esa posición Kafka. Si cualquier paso falla, todo se revierte. La base y Kafka conservan
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

El listener completa la transacción SQL dentro de una transacción Kafka de consumo;
al retornar, confirma el offset junto con la salida elegible de F-04. Se acepta la reentrega y se protege el efecto SQL con idempotencia. El
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

Las pruebas actuales verifican rollback, duplicados, resultados inmutables y replay
tras fallas SQL o una falla controlada de confirmación Kafka posterior al commit SQL.
El smoke verifica persistencia y republicación después de reinicios. SettlementKafkaIT agrega una caída real de proceso tras SQL y una reasignación
controlada; F-05 conserva pendientes las ampliaciones de recuperación operativa.

## 7. Settlement: protocolo transaccional

### Recuperación ampliada en F-05: evidencia del núcleo

`RecoveryKafkaIT` ejercita el camino real de la aplicación con PostgreSQL y Kafka,
incluyendo procesos JVM terminados abruptamente. Su primera ejecución dirigida
completó cuatro casos sin fallas ni omisiones; el cierre global y los recorridos
de consola todavía deben verificar la ampliación completa.

| Frontera | Estrategia y observación exigida |
| --- | --- |
| SQL abierto, antes del commit | El cierre abrupto de la conexión revierte el intento; el nuevo propietario aplica la misma posición una vez, sin orden parcial, contador ni ledger sobrante. |
| SQL confirmado, Kafka pendiente | Se recupera el resultado APPLIED original, sin recomputar su snapshot ni repetir efectos; se completa settlement y progreso mediante Kafka. |
| Send aceptado sin commit Kafka | La aceptación del envío no habilita visibilidad read_committed. Se resuelve el intento abierto y se recupera la posición desde progreso estable. |
| Commit Kafka confirmado con respuesta perdida | Se detiene la sesión incierta; otra sesión consulta progreso estable y no vuelve a publicar una finalización confirmada. La evidencia de respuesta perdida sigue siendo la inyección controlada de F-04. |
| Reasignación con SQL abierto | La pérdida de pertenencia no cancela SQL. El intento anterior puede confirmar o revertir; lock, segunda búsqueda y resultado por posición serializan el efecto. Kafka rechaza la confirmación del propietario obsoleto. |
| Caída de ambas aplicaciones | Datos SQL y offsets Kafka sobreviven en sus servicios; reiniciar las aplicaciones recupera pendientes y permite continuar en ambas particiones. No depende de memoria del proceso anterior. |

La recuperación del grupo por una instancia sobreviviente y el reinicio operativo
de una sesión detenida son mecanismos distintos. No se agrega un supervisor que
reinicie automáticamente cualquier error: clasificación, cuarentena y recuperación
de errores permanentes siguen correspondiendo a F-06.

La instrumentación de ensayo vive en wiring de demo y adaptadores, fuera del dominio.
Las barreras JDBC distinguen escrituras sin confirmar del resultado visible desde
otra conexión; la barrera de salida espera aceptación real de Kafka dentro de la
transacción del contenedor. Se seleccionan una identidad de ER y un intento, se
consumen una vez y vencen; los controles desaparecen al reiniciar y están ausentes
por defecto. No forman parte del protocolo durable de recuperación.

Para evaluar secuencia no basta observar una reasignación: se deben finalizar ambos
intentos, verificar rechazo del anterior y aplicar sucesores. Para evaluar unicidad
no basta una página vacía: se recorren ambas particiones read_committed después de
resolver transacciones abiertas. El ID estable sirve para correlación; no suprime
publicaciones duplicadas por sí solo.

Los tests de consola con Docker sustituido sólo demuestran coordinación. La evidencia
de procesos y los smokes Compose se registran separadamente en OpenSpec; no se
atribuyen al simple acceso HTTP o a un listener detenido. La consola conserva
resultados de ejecución en memoria y no promete reanudarlos tras perder su proceso.
Si se cancela, sus publicaciones confirmadas pueden completarse al restaurar consumo;
no se borran ni se compensan datos de negocio para limpiar un ensayo.

F-04 deriva un settlement sólo del resultado original APPLIED FILLED. El resultado
SQL contiene identidad de orden/ER/ledger, posición y cantidades finales: la
intención se recupera sin otra tabla ni migración. Un DUPLICATE, incluido NEW tardío
con snapshot FILLED, no origina salida. APPLIED identifica la aplicación original,
no una marca mutable de envío pendiente.

La proyección v1 normaliza cantidades como decimales planos, identificadores como
cadenas exactas e identidad estable `settlement:<numericOrderId>`. No usa reloj ni
UUID. El contrato debe mantenerse reconstruible al cambiar versiones. Un contrato
que incluyera datos variables necesitaría persistirlos antes de poder reintentarlo.

El contenedor abre una transacción Kafka, invoca el caso de uso JDBC explícito,
recibe su resultado después del commit SQL y publica si corresponde. Al retornar el
listener, Kafka incorpora el offset siguiente con los metadatos vigentes del grupo
y confirma salida y progreso en la misma transacción. KafkaTemplate y el gestor
Kafka comparten ProducerFactory; el gestor JDBC sigue indicado por nombre. No hay
commitSync independiente, transacción Kafka anidada ni commit distribuido SQL/Kafka.

Si SQL revierte no queda salida; si confirma y Kafka aborta, la reentrega conserva
APPLIED FILLED y reconstruye el mismo settlement sin repetir la transición. Si Kafka
confirma, su progreso evita volver a publicar esa posición. Republicar el mismo ER
en otro offset produce DUPLICATE y cero salidas. La unicidad combina idempotencia SQL,
resultado inmutable y transacción Kafka: ni un ID estable ni el productor idempotente
por sí solos deduplican los reenvíos de negocio.

Ante un commit Kafka incierto se detiene la sesión, se descarta el productor fallido
y se reanuda desde offsets estables; no se interpreta timeout como aborto ni se hace
seek al offset local. El AfterRollbackProcessor propio también cubre el error posterior
al retorno del listener y nunca recupera mediante descarte. El fencing detiene al
consumidor obsoleto. Cada instancia necesita un prefijo transaccional distinto;
los metadatos de generación y la espera de offsets transaccionales pendientes son
indispensables cuando una partición cambia de propietario.

Un lector read_committed observa sólo salidas confirmadas: exactamente un registro
por FILLED cubierto por la activación, cero por CANCELLED. SQL/HTTP pueden mostrar
FILLED mientras la salida sigue pendiente. Las transacciones abiertas pueden demorar
lecturas y recuperación. La garantía se refiere al registro publicado; los efectos
externos de otro consumidor requieren su propio manejo de recuperación.

Las pruebas SettlementKafkaIT verifican dos miembros del grupo y particiones distintas,
rollback SQL, falla de envío, aborto después de send aceptado, respuesta perdida tras
commit real, espera de offsets pendientes, kill de una JVM entre SQL y publicación y
reasignación por max.poll.interval con rechazo del propietario anterior. La respuesta
perdida es una inyección controlada debajo del wrapper de Spring, no un corte físico
de red. El kill termina un proceso real de prueba, sin cerrar Spring ordenadamente.
La evidencia final y el estado de implementación se conservan en README y OpenSpec.

La activación sobre F-03 incluye APPLIED FILLED con offset pendiente; posiciones ya
confirmadas no se liquidan retroactivamente, tampoco al republicar su terminal.
Un ledger F-02 sin resultado original sigue deduplicándose y no inventa intención
ni posición histórica. Se conservan base, grupo, topics, particiones y retención.
Detener ambas aplicaciones evita consumidores mixtos: volver a F-03 podría confirmar
pendientes sin publicar y no es un rollback seguro. No se garantiza recuperación
tras borrado de volúmenes, resets de offsets, recreación de topics o escrituras
externas al topic de settlement. F-05 amplía los casos de falla y recuperación;
F-06 mantiene pendientes cuarentena y recuperación de errores permanentes.

## 8. Límites y evidencia de las garantías

Se prioriza un servicio pequeño con dos instancias y persistencia compartida. Se dejan
fuera alta disponibilidad del broker/base, recuperación por pérdida física de volúmenes,
resets arbitrarios de offsets y recreación de topics. Una versión operativa necesitaría
replicación, respaldos y procedimientos de recuperación compatibles con la identidad y
la secuencia de los ER.

### Pruebas prioritarias para el evaluador

El recorrido recomendado empieza por las fronteras donde una falla puede romper
la secuencia, la idempotencia o la atomicidad. Los bloques GIVEN, WHEN y THEN
identifican las precondiciones, la acción y sus verificaciones; en escenarios de
recuperación se repiten para distinguir la falla del nuevo intento.

| Prioridad | Clase | Qué permite evaluar |
| --- | --- | --- |
| Primera | [SettlementKafkaIT](src/test/java/com/maxcapital/challenge/SettlementKafkaIT.java) | Salida/offset transaccionales, commit incierto, dos instancias, kill de proceso y reasignación. |
| Primera | [ExecutionReportKafkaIT](src/test/java/com/maxcapital/challenge/ExecutionReportKafkaIT.java) | Consumo real con Kafka/PostgreSQL, secuencia por orden, reentrega y offsets pendientes ante fallas. |
| Primera | [ProcessingResultsIT](src/test/java/com/maxcapital/challenge/ProcessingResultsIT.java) | Atomicidad SQL, concurrencia entre contextos de aplicación y recuperación de resultados durables. |
| Respaldo del núcleo | [OrderPersistenceIT](src/test/java/com/maxcapital/challenge/OrderPersistenceIT.java) | Consistencia de estado, contador y ledger; snapshots y deduplicación de terminales. |

En `ExecutionReportKafkaIT`, revisar primero:

- `recoversCommittedResultWhenOffsetCommitFailsBeforeReachingKafka`: inyecta una
  falla de confirmación del offset después del commit SQL. Comprueba resultado
  durable, sucesor pendiente y recuperación del resultado original sin repetir
  el efecto al reanudar.
- `keepsSqlFailureUncommittedAndResumesTheSameSequenceOnRestart`: una falla de
  inserción SQL revierte el intento y deja el offset pendiente; al retirar la
  falla y reiniciar el listener, el ledger conserva la secuencia completa.
- `processesInterleavedOrdersAndReplayAndIgnoresAbortedReports`: verifica órdenes
  intercaladas en dos particiones, un terminal duplicado sin incremento adicional
  y ausencia de efectos SQL para una transacción productora abortada.

En `ProcessingResultsIT`, revisar primero:

- `rollsBackAllWritesWhenResultOrLedgerInsertionFailsAndRetriesCleanly`: falla
  la escritura del resultado o del ledger, tanto al crear como al actualizar una
  orden; verifica rollback conjunto y reintento sin efectos duplicados.
- `serializesConcurrentSamePositionAndConcurrentRepublications`: dos contextos
  compiten por la misma posición o por el mismo ER en offsets distintos; sólo
  hay una aplicación y una entrada de ledger, con los resultados correspondientes.
- `recoversFromAnotherApplicationAndAfterClosingAllContexts`: recupera resultados
  APPLIED y DUPLICATE desde otro contexto y después de cerrar todos los anteriores,
  conservando sus snapshots aunque la orden ya haya avanzado.

En `OrderPersistenceIT`, revisar
`rollsBackStateCounterAndLedgerWhenAppendFails` para el rollback ante una falla
del ledger y `appliesSnapshotsAndRecognizesTerminalReplayBeforeTransition` para
la adopción de acumulados y la deduplicación antes de validar terminales.

Como lectura complementaria,
[DemoScenariosIT](src/test/java/com/maxcapital/challenge/DemoScenariosIT.java), en
`executesAvailableWithRealKafkaSqlAndTwoInstancesAndKeepsPendingExcluded`, ejecuta
los cinco escenarios disponibles con dos aplicaciones, Kafka y PostgreSQL reales.
Los controles Docker están sustituidos: ese test comprueba la coordinación del
reinicio, sin reiniciar procesos reales.
[EvaluationConsoleRuntimeIT](src/test/java/com/maxcapital/challenge/EvaluationConsoleRuntimeIT.java),
en `keepsExactlyTwoConsumersAndObservesStoppedListenerSeparatelyFromHttp`, verifica
dos miembros reales del grupo y una partición por aplicación, además de distinguir
un listener detenido de HTTP saludable. La composición del grupo por sí sola no
prueba recuperación ante caídas. El [smoke de dos instancias](scripts/verify-f02.ps1)
complementa esas pruebas con consulta y persistencia tras reinicios reales,
ejecutados después de consumir el lote.

### Límites de esta evidencia

La falla de confirmación del offset es controlada y ocurre antes de enviar el
commit al broker: no demuestra un commit Kafka de resultado incierto. Reiniciar
un listener o cerrar contextos no equivale a matar un proceso entre commits SQL
y Kafka, ni prueba un rebalanceo durante una escritura.

`preservesPoisonRecordAndDoesNotSkipToTheNextRecord` y
`keepsInvalidInputsPendingWithNoSuccessResultOrSuccessorApplication`, de
`ExecutionReportKafkaIT`, comprueban que mensajes inválidos y sus sucesores no se
descartan silenciosamente, que el offset no avanza y que se conserva diagnóstico.
También muestran el bloqueo actual: al reiniciar, el mensaje inválido vuelve a
fallar. No prueban una solución al bloqueo indefinido.

Las pruebas de F-02/F-03 anteriores no demuestran por sí solas unicidad de settlement.
La evidencia adicional de SettlementKafkaIT se describe en la sección 7 y en el
verification.md de F-04: incluye una JVM terminada tras SQL y reasignación real,
además de abortos y commits de respuesta incierta. F-05 conserva ampliaciones de
recuperación y F-06 cuarentena; el estado de cada feature sigue en README.

## 9. Resultado por posición Kafka

F-03 conserva un resultado inmutable por `(topic, partition, offset)`, materializado
como PK `(topic, partition_id, record_offset)`. La identidad de negocio sigue siendo
`(numericOrderId, fixId)`. Cada posición resuelta guarda `APPLIED` o `DUPLICATE`, la
referencia al ledger y el snapshot de estado, cantidades y contador de ese momento.
Una FK compuesta impide asociar un resultado con un ledger de otra identidad.

La misma posición devuelve su resultado original, incluso `APPLIED`, sin reclasificar
ni reconstruir el snapshot desde la orden actual. Un ER republicado en otro offset
genera `DUPLICATE` y referencia el ledger existente. Su snapshot es el vigente al
resolver esa nueva posición. Por eso un duplicado tardío de NEW puede mostrar
snapshot FILLED y apuntar al ledger de NEW: el snapshot por sí solo no determina
una nueva aplicación terminal ni una salida de settlement.

El caso de uso busca el resultado antes de tocar la orden y de nuevo después de
adquirir su lock, porque otro intento pudo confirmar durante la espera. Mantiene
la comparación JSONB completa y la deduplicación antes de validar terminales.
Resultado, estado, contador y ledger usan la misma transacción JDBC explícita;
una falla en cualquiera de sus escrituras revierte todo el intento. La PK de
posición y la unicidad del ER son respaldo SQL; no se capturan conflictos de
unicidad para devolver éxito desde una transacción abortada.

Las pruebas SQL verifican rollback en creación y actualización, concurrencia entre
dos contextos y recuperación de snapshots después de cerrar/reabrir contextos.
La prueba Kafka hace fallar una confirmación antes de enviarla al broker, después
del commit SQL: observa resultado durable, offset pendiente y sucesor sin aplicar;
al reanudar recupera el mismo resultado sin repetir el efecto. Esta inyección no
prueba kill de proceso, rebalanceo durante escritura ni commit incierto real (F-05).

Sólo se guardan resultados exitosos. JSON/key inválido, conflicto de identidad,
transición inválida y falla SQL mantienen la detención y diagnóstico existentes,
sin avance de offset. Ausencia de resultado no distingue registro no intentado de
intento fallido; F-06 debe definir errores persistidos, cuarentena y recuperación.

V2 es aditiva y conserva datos previos. F-02 no tenía posiciones en su ledger:
no se inventan resultados históricos. Una reentrega sin resultado conocido de un
ER previo genera `DUPLICATE` por identidad de negocio. El despliegue requiere
detener ambas aplicaciones para no mezclar consumidores de distintas versiones.
No hay purga durante el challenge; retención productiva debe coordinar replay y
resultados. Recreación de topics, cambios de cluster y resets arbitrarios quedan
fuera del contrato.

F-04 usa este resultado como intención durable y coordina salida/progreso Kafka
según la sección 7. SQL y Kafka conservan commits independientes. Las garantías
SQL de F-03 por sí solas no demuestran unicidad de publicación; la evidencia de
settlement se verifica por separado con read_committed y fallas de transacción.

## 10. Consola local de evaluación

Por pedido explícito se agrega una herramienta de evaluación al alcance del
challenge. La exclusión general de UI se mantiene para una interfaz de producto:
esta consola permite observar el núcleo y reproducir pruebas desde el navegador.
El README conserva el estado de implementación de esta feature.

Se reutiliza el artefacto Java bajo un perfil exclusivo en un proceso Compose
independiente. Esto evita perder la consola al detener una instancia y aprovecha
las dependencias Kafka/JDBC existentes. Se excluyen listener y Flyway del perfil,
y las aplicaciones sólo agregan observación de consumo habilitada en demo.
La integración de runtime verifica que siguen existiendo exactamente dos miembros
del grupo y que HTTP saludable no implica listener en ejecución.

La imagen de consola incorpora la CLI fijada y acceso al socket Docker local;
las imágenes de aplicación conservan su usuario y no incluyen ese acceso. El
socket otorga control del motor: la restricción de acciones de la API no es un
aislamiento del permiso real. La API limita servicios, acciones y proyecto,
rechaza mutaciones de otro origen y acota diagnósticos; no admite comandos libres
ni borrados. Los logs son diagnóstico temporal, no errores persistidos de F-06.
Una acción manual kill permite explorar fallas, pero no posiciona la caída entre
commits ni certifica recuperación F-05. Los escenarios completos requieren su
propia evidencia antes de atribuirles garantías.

La consola representa identificadores y cantidades como cadenas y conserva el
JSON del ER como texto en el navegador. Las consultas públicas mantienen su
contrato numérico; las comparaciones entre instancias se resuelven en Java. Se
verificaron IDs fuera del rango seguro de JavaScript y ocho decimales exactos.
Las escrituras de negocio ingresan por Kafka: las consultas de consola no alteran
estado, ledger ni resultados. Su preflight usa un snapshot y no reemplaza la
validación final del consumidor ante publicaciones concurrentes.
