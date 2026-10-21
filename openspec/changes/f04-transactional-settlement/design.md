# Design

## Context

Ver `proposal.md` para motivación y alcance. El enunciado exige publicar el efecto
terminal una vez por orden con dos instancias, sin implementar el consumidor de
settlement. Las specs nuevas definen esa garantía como unicidad del registro Kafka
confirmado observable mediante `read_committed`.

Estado observado al planificar:

- `ProcessExecutionReport.process` usa `@Transactional("orderTransactionManager")`;
  su retorno atraviesa el commit JDBC de estado, contador, ledger y resultado.
- `ProcessingResult` conserva posición, outcome, identidad, ledger y snapshot.
  Recuperar la misma posición mantiene APPLIED; una republicación produce DUPLICATE.
  Un NEW tardío duplicado puede tener snapshot FILLED.
- `ExecutionReportListener` llama al procesador y después a `acknowledge()`.
  La factory personalizada usa MANUAL_IMMEDIATE y no conecta un gestor Kafka.
- La configuración ya declara `transaction-id-prefix` por instancia. Compose fija
  `app-1` y `app-2`; los tres topics, incluido `settlements`, existen con dos
  particiones y retención sin expiración en la demo. No hay publicador de settlement.
- `ExecutionReportKafkaIT` inyecta una falla de `commitSync` anterior al broker;
  no demuestra la nueva frontera de publicación ni un commit de respuesta incierta.
  `EvaluationConsoleRuntimeIT` sirve como referencia para dos contextos y miembros
  reales del grupo, pero no prueba settlement.

`design.md` aplica porque la coordinación atraviesa JDBC, consumo, publicación y
recuperación, con compatibilidad de datos y offsets. Todo lo siguiente es diseño
propuesto; todavía no constituye evidencia de implementación.

## Goals / Non-Goals

**Goals:** definir un único protocolo recuperable de salida/progreso, derivar una
salida estable desde SQL confirmado y verificar sus fronteras críticas con dos
instancias. Separar unicidad por registro de entrada, dedup de ER y unicidad del
efecto por orden.

**Non-Goals:** transacción distribuida SQL/Kafka, ejecución financiera downstream,
publicador periódico independiente, consumidor de settlement, migración del estado
histórico confirmado ni mecanismos generales de retry/cuarentena. Las ampliaciones
F-05 no sustituyen las pruebas mínimas incluidas aquí.

## Decisions

### 1. Resultado F-03 como intención recuperable

El predicado es `outcome == APPLIED && state.status == FILLED`, evaluado sobre el
resultado original. En una aplicación nueva ese snapshot corresponde al ER que
completó la orden. En replay conserva ese significado aunque el estado actual sea
otro. DUPLICATE nunca crea salida, tampoco para un FILLED original republicado.

No se vuelve a consultar la orden para decidir, ni se confunde APPLIED con
publicación pendiente: el progreso Kafka estable determina si la posición debe
volver al procesamiento. Dos intentos de una misma posición pueden recuperar
APPLIED; el protocolo de grupo y transacción evita que ambos confirmen salidas.

Los datos necesarios ya son durables en V2, de modo que no se necesita V3 ni
actualizar resultados para marcar envíos. El resultado se conserva inmutable.
Se mantiene la comparación del payload y la dedup previa a terminales de F-03.

Alternativas descartadas: consultar sólo FILLED confunde duplicados tardíos; volver
a clasificar replay como DUPLICATE pierde la intención pendiente; una outbox con
marca SQL posterior al envío mantiene una ventana de republicación. Un relay
independiente además necesitaría su propio protocolo de unicidad durable.

### 2. Contrato v1 pequeño y reproducible

El evento informa la finalización de la orden; no calcula dinero, moneda, precio
promedio ni importes que el enunciado no define. Se publica JSON UTF-8 con:

| Campo | Representación y origen |
| --- | --- |
| key Kafka | Decimal canónico del numericOrderId |
| `schemaVersion` | Entero JSON 1 |
| `settlementId` | Cadena `settlement:<numericOrderId>`; estable por orden |
| `numericOrderId`, `fixId`, `ledgerId` | Cadenas decimales exactas del resultado |
| `source` | Objeto con `topic` cadena, `partition` entero y `offset` cadena decimal |
| `nominalAmounts`, `accumulativeNominalAmount`, `leavesNominalAmount` | Cadenas decimales del snapshot FILLED |

Normalizar cantidades con representación decimal plana, sin ceros fraccionarios
innecesarios y con cero como `"0"`. Así BigDecimal de entrada y NUMERIC recuperado
producen el mismo contenido. Mantener campos y orden de serialización estables en
v1. El ID no incorpora offset ni versión del esquema: una orden tiene un efecto.
No usar reloj, UUID, datos mutables ni el JSON crudo reentregado para reconstruirlo.

IDs y cantidades como cadenas conservan exactitud para lectores JavaScript; es una
API de salida nueva y no cambia el HTTP numérico existente. La key permite
particionado estándar de settlements, sin prometer orden global entre órdenes.
La estabilidad del ID ayuda a correlacionar, pero no deduplica registros en Kafka.

Agregar un valor de aplicación `Settlement`, una proyección pura desde
`ProcessingResult` y un puerto pequeño `SettlementPublisher`. Un coordinador de
aplicación `HandleExecutionReport` llama al procesador JDBC existente y después
publica si corresponde. No lleva una transacción JDBC exterior que retrase su
commit hasta después del envío. El listener delega en ese coordinador; el adaptador
de salida serializa y usa KafkaTemplate. Dominio permanece libre de infraestructura.

Persistir un payload adicional sólo sería necesario si el contrato requiriera
datos no recuperables o variables. Este v1 usa únicamente datos inmutables
existentes; cambios futuros deberán mantener su reconstrucción compatible.

### 3. Transacción Kafka del contenedor con commit SQL interior

Conectar explícitamente `KafkaTransactionManager` a
`executionReportContainerFactory`. KafkaTemplate y el gestor comparten
ProducerFactory transaccional. Mantener `orderTransactionManager` explícito; la
existencia de ambos gestores no cambia la atomicidad SQL.

```text
Contenedor: begin Kafka
    |
    +--> listener --> HandleExecutionReport
    |                    |
    |                    +--> ProcessExecutionReport --> commit JDBC
    |                    |
    |                    +--> SettlementPublisher si APPLIED FILLED
    |
    +--> sendOffsetsToTransaction(offset siguiente, metadatos vigentes del grupo)
    |
Contenedor: commit Kafka de salida y offset
```

Usar record listener y AckMode.RECORD, sin Acknowledgment ni commitSync/commitAsync
independiente. Mantener `enable.auto.commit=false`, `read_committed`, un consumidor
por instancia y `max.poll.records=1`. También NEW, parciales, CANCELLED y DUPLICATE
confirman offsets mediante transacciones Kafka, aunque no publiquen salida.

El envío participa en la transacción del contenedor, sin `executeInTransaction`
anidado, productor no transaccional ni `allowNonTransactional`. Propagar fallos de
serialización/envío; el commit transaccional debe comprobar los envíos pendientes.
No registrar éxito completo cuando sólo retornó el caso de uso JDBC.

Entrada, salida y offsets viven en el mismo cluster. Cada proceso activo usa un
prefijo transaccional distinto; preservar las identidades Compose existentes y
asignarlas explícitamente también a los contextos de prueba. EOS V2 incorpora los
metadatos reales de la generación del consumidor al confirmar offsets. No construir
sólo un ConsumerGroupMetadata con el nombre del grupo ni fijar un transactional.id
compartido entre instancias.

Alternativas: enviar antes de SQL puede publicar una orden que finalmente no
confirma; enviar después de confirmar el offset puede perder la salida; confirmar
salida y offset por separado permite repetición. Encadenar dos gestores tampoco
crea atomicidad distribuida. Aquí Kafka coordina sus propios datos y el resultado
SQL hace recuperable el intervalo entre commits.

### 4. Detención en fallas y recuperación de incertidumbre

La política F-02/F-03 de detener la instancia y conservar el registro continúa,
sin retries automáticos que salten poison. Para el contenedor transaccional:

- Dejar propagar las excepciones del listener para que Kafka revierta el intento.
- Configurar un AfterRollbackProcessor específico que registre posición y causa y
  solicite la detención del contenedor sin bloquear su propio hilo. No inicia otra
  transacción, no confirma recuperación, no descarta tras un número de intentos y
  no hace seek/reenvío dentro de la sesión que acaba de fallar.
- Esa política recibe también fallos del cierre transaccional posteriores al
  retorno del listener. El catch del listener por sí solo no cubre esa frontera.
- Activar detención por fencing y conservar contexto de posición en diagnóstico
  para errores de pertenencia al grupo/productor. No intentar publicar desde el
  propietario obsoleto.
- Mantener el descarte del productor fallido que realiza la ProducerFactory: un
  productor con commit incierto no vuelve al cache utilizable. No enviar abort
  como supuesto remedio si un commit ya puede estar en curso.
- Reanudar con una sesión nueva, por reinicio del listener/aplicación según la
  política existente. Se obtiene asignación y progreso estable del grupo antes
  de procesar; no se hace seek manual al offset local ni reset del grupo.

Un timeout de commit se trata como incierto. Si Kafka confirmó, el offset estable
ya cubre la entrada y no se vuelve a publicar. Si abortó, se recupera APPLIED y se
intenta otra transacción. Si hay commits transaccionales pendientes, la lectura de
offsets estables de EOS espera su resolución; leer un offset viejo mediante Admin
y usarlo para forzar replay sería incorrecto. La inicialización del productor
resuelve transacciones anteriores de su mismo ID; para cambios de propietario,
la coordinación de offsets/metadatos del grupo es indispensable.

La documentación operativa debe distinguir detención, reinicio y espera de
resolución. No se agrega un servicio supervisor ni se promete reanudación automática
de una instancia detenida. La otra instancia puede recuperar particiones liberadas;
ante dependencia caída debe volver a estar disponible antes de recuperar progreso.

### 5. Fronteras observables

| Frontera de falla | SQL | Kafka y recuperación |
| --- | --- | --- |
| Antes del commit JDBC | Rollback de todos los efectos del intento | Sin offset ni salida confirmados; replay vuelve a procesar |
| SQL confirmado, antes de send | APPLIED FILLED durable | Replay recupera intención y publica con su offset |
| Send aceptado, antes de commit Kafka | FILLED durable | Salida abierta o abortada invisible a read_committed; offset estable sin avanzar |
| Kafka aborta | Resultado original intacto | No hay salida visible; nueva sesión recupera resultado y confirma una salida |
| Kafka confirma, respuesta perdida | Resultado original intacto | Resolver progreso estable; no reenviar la posición confirmada |
| Se pierde propiedad de partición | SQL puede haber confirmado | Metadatos/fencing y offsets estables impiden dos resoluciones Kafka válidas |

SQL/HTTP pueden observar FILLED mientras el settlement sigue pendiente: no existe
un snapshot distribuido entre sus lecturas. Un consumidor `read_committed` ve
eventualmente un registro confirmado por FILLED cubierto, y cero por CANCELLED.
Transacciones abiertas pueden retrasar el avance de lectura de esa partición;
registros de control y abortos producen huecos de offsets legítimos. Releer un
registro en el consumidor de salida y sus efectos externos son otro contrato.

### 6. Evidencia requerida para completar F-04

Reutilizar Kafka/PostgreSQL Testcontainers con imágenes del proyecto. Agregar una
suite `SettlementKafkaIT` y soporte acotado de pruebas; adaptar las pruebas
existentes a la firma del listener y al commit transaccional. No dejar una prueba
verde cuya inyección `commitSync` ya no se ejecute: comprobar que cada falla
programada ocurrió en la frontera elegida.

La observación independiente usa consumidores `assign` + `read_committed`, sin
pertenecer al grupo de órdenes ni confirmarle offsets. Captura posiciones iniciales
de salida antes del caso y lee todas sus particiones hasta un límite estable
posterior al procesamiento, resolución de transacciones y cierre de intentos
obsoletos. Correlaciona por órdenes nuevas, sin vaciar topics. Para demostrar cero
salidas o unicidad no alcanza un poll vacío ni contar mientras un segundo intento
todavía está suspendido. Usar barreras, avance del grupo y límites observables.

Casos mínimos, con correspondencia a todos los escenarios de ambas specs:

1. Dos instancias reales del grupo, una partición por instancia; NEW/parciales/
   FILLED intercalados, CANCELLED con parcial y duplicados de terminal/NEW tardío.
   Verificar cardinalidad de salida, payload exacto, SQL y offsets finales.
2. SQL rollback, envío fallido y aborto después de send aceptado. Suspender antes
   de commit Kafka permite observar FILLED SQL, salida invisible y offset estable
   pendiente. Reanudar desde nueva sesión deja un único registro confirmado.
3. Una JVM hija de pruebas, lanzada con el classpath de la suite y Java del build,
   consume como instancia A junto a B. Un decorador de pruebas bloquea antes del
   envío después del commit SQL y comunica la barrera al padre. Terminar A
   abruptamente, sin cierre Spring; B recupera el resultado. Reiniciar A y verificar
   que no se duplica salida ni aplicación. Matar sólo el proceso creado por el test
   y limpiar recursos en finally. No equivale a cerrar un contexto ordenadamente.
4. Inyectar pérdida de respuesta por debajo del wrapper de productor de Spring:
   delegar un commit real exitoso y luego lanzar TimeoutException una sola vez.
   Verificar detención y descarte del productor, diagnóstico y nuevo consumo desde
   offset estable sin reenvío. Este caso demuestra respuesta perdida controlada;
   no se presenta como prueba de corte físico de red.
5. Inyectar commit que no llega al broker y deja resolución pendiente hasta cierre/
   timeout. Comprobar que la sesión nueva no usa un offset inestable y que al
   abortar se recupera el resultado original con una salida final.
6. Suspender A tras SQL, forzar pérdida real de su generación mediante expiración
   de max.poll.interval en configuración de pruebas y observar reasignación a B.
   Liberar A y completar ambos intentos antes de contar salidas; verificar rechazo
   de la resolución obsoleta y una sola publicación confirmada. No invocar al
   listener dos veces fuera del grupo como sustituto de este caso.
7. Preparar resultados F-03 APPLIED FILLED con offset pendiente y confirmado,
   respectivamente, sin invocar el nuevo publicador durante la preparación.
   Verificar recuperación del primero y ausencia de backfill del segundo,
   incluida su republicación. Verificar precisión y reconstrucción desde SQL.
8. Conservar errores JSON/key/identidad/transición y verificar reinicios repetidos
   sin skip ni aplicación del sucesor. La solución a ese bloqueo sigue en F-06.

Barreras e inyección viven en `src/test`, sin flags de producto, endpoints de fallo
ni sleeps como sincronización principal. Los plazos y transaction.timeout de las
pruebas permiten resolver intentos pendientes dentro de un timeout explícito.
La prueba de generación vieja y el kill mínimo son aceptación de F-04; F-05 queda
para ampliar fronteras SQL, combinaciones de caídas, reconexión y recuperación
operativa. No se atribuye a estas pruebas cobertura de todas esas combinaciones.

## Risks / Trade-offs

- **Dos commits independientes** -> El resultado SQL recupera la intención; se
  documenta la ventana FILLED visible sin salida y se prueba que converge.
- **Reintento ingenuo tras commit incierto** -> Detener sesión, cerrar productor
  fallido y obtener offsets estables; probar tanto commit efectivo como aborto.
- **Consumidor obsoleto recupera APPLIED** -> EOS V2 con metadatos reales y prueba
  de reasignación; el lock SQL por sí solo no demuestra unicidad Kafka.
- **Recuperación por defecto descarta tras varios fallos** -> Política específica
  de detención posterior al rollback, sin recoverer que salte registros.
- **Contrato reconstruido tras cambio de versión** -> Proyección v1 canónica y
  compatible; no usar datos variables. Un nuevo esquema exige revisar replay.
- **Timeouts y bloqueo de partición** -> Una transacción abierta puede demorar
  recuperación/lecturas. Documentar plazos observados; no confundir demora con
  pérdida ni ajustar offsets para conseguir progreso.
- **Datos de F-02 sin resultado APPLIED por posición** -> No inventar la intención
  histórica. La activación se apoya en F-03; un ledger legado deduplicado conserva
  DUPLICATE y no se usa como mecanismo de backfill. Identificar este límite en la
  documentación de compatibilidad y conservar el test legado de F-03.
- **Broker único y retención compartida con SQL** -> Mantener volúmenes y contrato
  de retención/grupo existente. Pérdida de disco, recreación de topics, resets
  arbitrarios o productores externos en settlements quedan fuera de la garantía.
- **Consola existente con escenario settlement pendiente** -> La lectura de
  Mensajes ya permite inspeccionar la salida; el escenario guiado permanece sin
  implementar. Documentar esa diferencia sin habilitarlo por disponibilidad del
  núcleo ni rediseñar UI dentro de este change.

## Migration Plan

1. Detener ambas aplicaciones F-03, conservar base, volúmenes, topics, particiones
   y grupo `max-capital-orders-v1`. No mezclar consumidores con/sin salida terminal.
2. Desplegar ambas instancias F-04 con prefijos transaccionales distintos. No hay
   migración SQL ni barrido de órdenes FILLED. Resolver transacciones anteriores
   antes de dar por estable el progreso del grupo.
3. Retomar offsets existentes. APPLIED FILLED pendiente de F-03 puede originar su
   primera salida; una posición ya confirmada no vuelve a consumirse. Su terminal
   republicado es DUPLICATE y tampoco origina salida retroactiva.
4. Comprobar salud, dos miembros, flujo nuevo y salida con read_committed. Publicar
   las instrucciones y evidencia real en README/DECISIONS durante implementación.
5. Ante falla de despliegue, detener ambos consumidores y conservar todo para
   diagnóstico/roll-forward. Volver a ejecutar F-03 confirmaría pendientes sin
   settlement; no es un rollback seguro del protocolo. No borrar salidas ni resetear
   offsets para simularlo. La reversión operativa requiere un plan separado.

## References

- [Enunciado](../../../challenge-arch-max-capital.pdf), efecto terminal y evidencia
  con dos instancias y reentrega; formato de settlement no especificado.
- [Spring Kafka 4.1.1: EOS](https://docs.spring.io/spring-kafka/reference/kafka/exactly-once.html).
- [Spring Kafka: manejo de errores y AfterRollbackProcessor](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html).
- [KafkaProducer: transacciones y commit incierto](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html).
- [KIP-447: generaciones y offsets transaccionales pendientes](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/103093950/KIP-447+Producer+scalability+for+exactly+once+semantics).
