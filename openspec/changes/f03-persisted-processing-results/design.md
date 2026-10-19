# Design

## Context

Ver `proposal.md` para la motivación. F-02 tiene un listener secuencial con
`MANUAL_IMMEDIATE`, commits síncronos y `CommonContainerStoppingErrorHandler`.
`ExecutionReportParser` valida key y payload antes de llamar a
`ProcessExecutionReport.process`, que hoy devuelve `void`. El caso de uso usa
`orderTransactionManager` explícito, inicializa y bloquea la orden y compara el
ER completo por igualdad JSONB antes de evaluar la transición.

`V1__create_orders_and_execution_report_ledger.sql` crea orden y ledger, cuya
identidad de negocio es `(numeric_order_id, fix_id)`. `OrderRepository.append`
todavía no devuelve `ledger_id`; `match` devuelve sólo un enum. Las pruebas
`OrderPersistenceIT` y `ExecutionReportKafkaIT` cubren rollback y duplicados,
pero no resultados por posición. Hay datos y offsets de F-02 que deben conservarse.

## Goals / Non-Goals

**Goals:**

- Introducir un contrato de resultado independiente de clases Kafka/JDBC que el
  listener pueda recuperar sin consultar el estado actual para reconstruirlo.
- Extender la transacción SQL existente sin debilitar locks, deduplicación ni
  secuencia por partición.
- Preparar datos durables para decidir en F-04 qué produjo una aplicación
  terminal, aun si una reentrega ya no ejecuta la transición de dominio.

**Non-Goals:**

- Agregar un coordinador distribuido, publisher, estado de envío o intención de
  settlement en F-03. Los datos conservados son una base; F-04 debe definir su
  contrato de salida y coordinar salida/offset en Kafka.
- Tratar ausencia de resultado como evidencia de ausencia de intentos, o como
  autorización para saltar un mensaje fallido.
- Atribuir exactamente una vez a las salidas, ni recuperación de todas las
  fronteras de crash a una prueba de excepción controlada.

## Decisions

### Dos identidades y un resultado inmutable por registro

La posición Kafka identifica el registro de transporte; `(numericOrderId, fixId)`
sigue identificando el ER de negocio. No se amplía el supuesto de unicidad de
`fixId` de F-02 ni se cambia a deduplicación por orden, offset o trade.

Se propone `InputPosition(topic, partition, offset)` y
`ProcessingResult(position, outcome, numericOrderId, fixId, ledgerId, state)` en
aplicación, sin importar tipos de Kafka, JDBC o Spring en el dominio. `outcome`
tiene `APPLIED` y `DUPLICATE`; `state` es el snapshot completo de `OrderState`
confirmado al resolver por primera vez esa posición. No se agrega un tercer
resultado `REPLAY`: la reentrega devuelve exactamente el original.

En una posición nueva con ER repetido se guarda `DUPLICATE`, su ledger existente
y el estado actual bajo lock. Por ejemplo, republicar un `NEW` después de `FILLED`
produce `DUPLICATE` con snapshot `FILLED`, pero su ledger sigue siendo el del
`NEW`. Una reentrega posterior conserva ese snapshot. F-04 no puede decidir
publicar sólo porque el snapshot sea terminal: necesita distinguir `APPLIED`,
el ER de origen y la coordinación transaccional de salida.

Alternativas: conservar sólo «procesado» no permite recuperar qué pasó; leer la
orden vigente altera retrospectivamente el resultado; volver a clasificar un
`APPLIED` como `DUPLICATE` en replay pierde la información necesaria para F-04.

### Esquema SQL aditivo y puertos concretos

Agregar una migración V2 para `app.processing_results`:

| Campo | Restricción o propósito |
| --- | --- |
| `topic`, `partition_id`, `record_offset` | PK compuesta de topic/partition/offset; topic no vacío, partición y offset no negativos |
| `outcome` | Obligatorio, `APPLIED` o `DUPLICATE` |
| `numeric_order_id`, `fix_id` | Identidad del ER; referencia coherente con el ledger |
| `ledger_id` | Referencia obligatoria a la entrada aplicada, también para `DUPLICATE` |
| `status`, `nominal_amounts`, `leaves_nominal_amount`, `accumulative_nominal_amount`, `applied_execution_count` | Copia inmutable del estado confirmado; mismas precisiones y restricciones de valores de F-02 |

La lectura recupera también el ER completo desde el ledger inmutable para
comparar contenido y disponer de campos adicionales. No se duplica ese JSON en
cada resultado ni se almacena sólo un hash. La comparación mantiene igualdad
JSONB de F-02, incluidos campos extra, orden de propiedades irrelevante y
equivalencia numérica. La key canónica debe coincidir con el `numericOrderId`
recuperado; el parser sigue validándola antes del caso de uso. Otro contenido
para la misma posición es error visible, nunca sobrescritura.

Un puerto específico de resultados ofrece búsqueda e inserción, con adaptador
JDBC sobre el mismo datasource. `OrderRepository` debe permitir recuperar el
ledger coincidente y devolver el ID de la inserción, sin búsquedas ambiguas por
estado terminal ni SQL en el listener. Las asociaciones resultado/identidad/ledger
se verifican al escribir y mediante integridad referencial SQL. No se crea una
abstracción genérica de inbox/outbox ni caché por instancia.

### Una transacción JDBC y doble lectura bajo el lock existente

El caso de uso recibe posición y ER parseado y devuelve `ProcessingResult`:

1. Dentro de `orderTransactionManager`, busca el resultado de esa posición. Si
   existe, compara identidad/contenido y lo devuelve sin modificar la orden.
2. Si falta, inicializa y bloquea la orden como en F-02. Vuelve a buscar el
   resultado después del lock: otra transacción pudo confirmarlo durante la espera.
3. Si sigue ausente, compara la identidad del ER contra el ledger antes de
   cualquier transición. Un conflicto aborta; un idéntico arma `DUPLICATE` con
   el snapshot vigente y el ledger existente.
4. Para un ER nuevo, aplica el dominio, guarda estado/contador y agrega ledger
   recuperando su ID; arma `APPLIED` con el nuevo estado.
5. Inserta el resultado y termina la transacción. El listener recibe éxito sólo
   después del commit del proxy transaccional y entonces confirma el offset.

El nivel habitual `READ_COMMITTED` permite que la segunda lectura vea al ganador
del lock. La PK de posición y la unicidad del ledger son respaldo SQL: una
colisión inesperada revierte el intento completo, sin capturar violaciones de
unicidad dentro de una transacción abortada ni devolver éxito desde ella. Dos
entregas normales de la misma posición comparten orden y convergen al resultado
original; dos posiciones con el mismo ER producen un `APPLIED` y un `DUPLICATE`.
Los locks no autorizan consumo paralelo de una partición ni sustituyen Kafka
para secuenciar ER distintos.

Alternativas: una transacción separada para el resultado puede dejar efecto sin
resultado; persistirlo antes del efecto puede dejar un éxito falso; una reserva
`PROCESSING` durable exige recuperación adicional. El lock existente, la segunda
lectura y el commit conjunto evitan esos estados intermedios sin otro coordinador.

### Fronteras de fallas y preparación para F-04

| Frontera | Conducta propuesta en F-03 |
| --- | --- |
| Parseo, conflicto o transición inválida | Sin resultado exitoso; diagnóstico y detención actuales |
| Error antes del commit SQL | Rollback de resultado, estado, contador y ledger; offset pendiente |
| Commit SQL de respuesta incierta | Propagar falla y no confirmar Kafka; la reentrega consulta SQL para saber si existe resultado confirmado |
| SQL confirmado y falla de confirmación Kafka | Resultado conservado; reentrega recupera el mismo resultado y vuelve a intentar confirmar |
| Offset confirmado | SQL y resultado ya durables; el grupo continúa |

La prueba de frontera usa infraestructura de test para hacer fallar la
confirmación antes de enviarla a Kafka, después del commit SQL. Debe observar el
resultado ya confirmado, offset pendiente, detención y el sucesor sin aplicar;
al reanudar sin la falla debe avanzar sin repetir efectos ni cambiar el resultado.
No se agrega una opción de fallo al producto ni un nuevo endpoint de pruebas.
F-05 queda responsable de kill de proceso, rebalanceos y commits inciertos reales.

F-03 no persiste errores como resultados definitivos: una falla SQL no puede
depender de otra escritura SQL exitosa y F-06 todavía debe definir clasificación,
cuarentena y pendientes por orden. Se mantienen los mensajes en Kafka y los
diagnósticos existentes. Una fila `APPLIED` tampoco significa settlement enviado:
F-04 debe resolver su publicación atómica con el offset y F-05 verificar qué ve
un consumidor `read_committed`. La configuración actual sólo aplica esa
visibilidad a la entrada.

## Risks / Trade-offs

- **Crecimiento por registro, incluso duplicados** → Conservar resultados y ledger
  durante el challenge, sin purga. Retención productiva requiere otro contrato de
  replay; no eliminar resultados aislados de sus efectos.
- **Identidad de posición limitada a un cluster/topic existente y un flujo** → No
  soportar recreación de topics, cambio de cluster ni resets arbitrarios del grupo.
- **Reentrega tras cambiar el parser** → F-03 conserva el contrato de entrada actual;
  cambios incompatibles futuros deben contemplar lectura de resultados históricos.
- **Mezclar binarios F-02 y F-03 deja registros sin resultado** → Despliegue detenido
  de ambas instancias; no rolling upgrade con consumidores de distinta versión.
- **El resultado conserva sólo éxitos** → Ausencia no explica un error; diagnóstico
  actual y registro Kafka siguen siendo necesarios hasta F-06.

## Migration Plan

Detener ambas aplicaciones preservando broker, base y offsets. Aplicar V2 antes
de iniciar consumidores F-03; no editar V1 ni borrar volúmenes. Verificar tanto
base nueva como base con órdenes y ledger de F-02.

No inferir resultados históricos: V1 no conserva la posición de los ER. Los
offsets ya confirmados siguen confirmados y sus filas no se rellenan con valores
inventados. Si un ER de F-02 aplicado pero sin resultado se reentrega al desplegar,
la identidad de negocio produce `DUPLICATE` y registra el primer resultado conocido
para esa posición. No se afirma conocer su resultado de transporte original.
El alcance temporal de settlements para órdenes previas deberá fijarse en F-04;
F-03 no promete publicar retrospectivamente terminales históricos.

Para revertir, detener ambas instancias y usar el binario anterior sin eliminar
la tabla aditiva. Volver a operar con F-02 deja nuevas posiciones sin resultado;
la cobertura de F-03 se reanuda al actualizar y no se reconstruye hacia atrás.

La aceptación exige integración PostgreSQL/Kafka, reinicio de contexto contra la
misma base y regresión del smoke F-02 con dos instancias. Registrar evidencia en
`verification.md`, actualizar README/DECISIONS sólo con garantías observadas y
archivar el change después de completar sus tareas, durante la implementación.
