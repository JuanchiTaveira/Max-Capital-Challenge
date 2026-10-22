# Design

## Context

Ver `proposal.md` para motivación y alcance. Esto es diseño propuesto, todavía sin implementación ni evidencia de ejecución.

El listener recibe un registro por poll y una transacción Kafka por entrega. `ProcessExecutionReport` confirma SQL antes de retornar; `HandleExecutionReport` publica settlement a partir del resultado inmutable y el contenedor confirma salida y offset. `StopAfterTransactionFailure` detiene ante cualquier rollback o commit incierto. F-05 ya prueba kills y cambios reales de propietario en esas fronteras.

V1 contiene órdenes y ledger; V2 conserva resultados exitosos APPLIED/DUPLICATE con referencia obligatoria al ledger. No hay representación durable de error, intento o trabajo diferido. `JdbcOrderRepository.initialize` participa del intento SQL: no se debe usar para representar una cuarentena antes de un NEW válido. La consola ya inspecciona `execution-reports-rejected`, rechaza borradores inválidos y tiene `poison-recovery` pendiente. El historial de escenarios vive en memoria: no sirve de coordinador de recuperación.

La decisión confirmada por el usuario es DLQ con backoff exponencial y jitter, más corrección auditada. Se mantiene el supuesto de particionado estable y emisión secuencial por orden; SQL, Kafka y sus volúmenes sobreviven. No se promete resolver automáticamente un mensaje cuyo contenido correcto requiere intervención humana.

## Goals / Non-Goals

**Goals:** aislar errores de datos sin perder originales; mantener la secuencia con dos instancias; continuar órdenes sanas cuando la identidad sea confiable; limitar reintentos también tras reinicios; recuperar mediante comandos durables; demostrar unicidad de rechazo y settlement en read_committed en las fronteras SQL/Kafka.

**Non-Goals:** procesar en paralelo registros de una misma partición, resetear offsets, corregir éxitos históricos, autenticar al responsable declarado, inferir identidades ambiguas, agregar retry topics, construir otro panel o un motor genérico de workflows. La única ampliación de consola es la necesaria para F-06 en vistas existentes. No hay cierre con descarte de sucesores.

## Decisions

### 1. Un solo canal de aplicación, con resoluciones distintas

Se conserva el consumidor del grupo como único lugar que aplica ER. En el adaptador se distinguen entregas ordinarias y comandos por un header reservado `max-capital-command=recovery.v1`; los ER sin ese header conservan el contrato actual. Un header de control no concede autoridad: el comando debe corresponder a una solicitud durable, revisión, contenido y partición válidos.

Todo ingreso conserva su posición de transporte. Sus resoluciones son conceptualmente `SUCCESS`, `DEFERRED`, `REJECTED` o resolución de control. Sólo SUCCESS referencia un APPLIED/DUPLICATE. Se consulta primero la resolución de la posición, antes de interpretar nuevamente sus bytes o comprobar la barrera actual: el replay debe reconstruir lo que ese intento decidió, no reclasificarlo.

Para cada entrega se mantiene la frontera existente:

```
capturar / recuperar resolucion SQL
                 |
              commit SQL
                 |
       salidas Kafka de esa resolucion
                 |
       commit Kafka con offset siguiente
```

Una captura DEFERRED puede confirmar progreso sin salida, después de SQL. REJECTED publica DLQ más progreso. Un éxito normal publica settlement si corresponde. Cada replay recupera exactamente las mismas intenciones. Ningún marcador SQL `published=true` se utiliza como prueba de commit Kafka.

Alternativa descartada: ampliar `processing_results.outcome` para mezclar errores y éxitos. Su FK al ledger y los consumidores actuales dan significado concreto a éxito; se conserva esa semántica.

### 2. Modelo durable aditivo y locks reales

Se propone V3 con tablas separadas, nombres finales sujetos a convenciones del adaptador:

| Entidad | Identidad y contenido principal |
| --- | --- |
| Ingreso/resolución | PK posición; key/value bytes nullable, headers binarios y ordenados, resolución original, referencia a éxito/rechazo y fuente efectiva opcional. |
| Intentos | Posición, ronda, política congelada, reservas consumidas, fecha límite, nextAttemptAt y diagnóstico. |
| Barrera | Scope orden o partición, primer pendiente, estado y revisión; existe aunque no haya fila orders. |
| Pendiente | Posición original única, orden confiable opcional, estado de retención, bloqueo y revisión efectiva; índice por scope y offset. |
| Corrección/solicitud | RequestId único, posición, revisión esperada, ER efectivo, motivo, responsable declarado, fecha y resolución; originales y revisiones append-only. |
| Comando/resolución | CommandId estable, posición de entrega, revisión, vínculo a solicitud y resultado; distinguir dueño original de copias equivalentes. |
| Despacho durable | CommandId único, destino, payload estable y estado de reenvío; se usa sólo para entregar comandos a Kafka. |

Cambiar el adaptador de entrada para conservar también key cruda (deserialización binaria y decodificación UTF-8 estricta) permite auditar bytes inválidos sin reconstrucción con pérdida. El contrato público de key decimal para ER válidos sigue igual.

Todos los caminos que leen o modifican barreras usan un lock SQL de coordinación por partición; después adquieren el lock de orden cuando corresponde. El orden fijo partición -> orden -> pendiente evita carreras entre apertura/cierre de barrera, captura de sucesores y propietario anterior/vigente. Se vuelve a consultar resolución después de adquirir locks. No se deja el lock durante backoff ni publicación Kafka.

Las transacciones de aplicación confirman juntas estado, contador, ledger, resultado, resolución del comando, avance del pendiente y programación del próximo trabajo. Ante falla revierte todo. Correcciones no sobrescriben resultados V2 ni ER de ledger ya aplicados. Un éxito de recuperación ocupa la posición de entrega del comando en processing_results, con referencia adicional a `originalPosition` y revisión efectiva. El ingreso original sigue siendo rechazado o diferido, nunca se convierte en éxito ficticio de sus bytes inválidos.

### 3. Clasificación explícita y presupuesto durable

Se introducen errores de validación con códigos y contexto explícitos; no se clasifica cualquier IllegalArgumentException como mensaje envenenado. Se distinguen validación/identidad/transición, transitorios SQL conocidos, indisponibilidad y fallas inesperadas. Las fallas de programación permanecen visibles y pendientes.

Parámetros iniciales propuestos: un intento inicial + cuatro reintentos, intervalo inicial 500 ms, multiplicador 2, máximo 5 s, jitter aditivo 200 ms y tiempo total de 30 s por ronda. El intervalo sorteado queda acotado entre el inicial y el máximo, conforme a ExponentialBackOff; no se presenta como full jitter. Se persisten política, plazo y próximo instante, para que un cambio de configuración o proceso no reinicie la ronda. Reloj de PostgreSQL para vencimientos compartidos; reloj/aleatoriedad inyectables para pruebas sin sleeps prolongados.

Se reserva durablemente cada intento antes de entrar al procesamiento. Una reserva interrumpida consume presupuesto de forma conservadora, aunque la JVM muera antes de ejecutar negocio. Después de un fallo se confirma el rollback, se registra diagnóstico y nextAttemptAt, y se espera fuera de transacciones. Al tomar la partición se recupera primero cualquier resolución exitosa; si no existe, se respeta la reserva/plazo persistidos. Si SQL no está disponible, no se ejecuta un intento no contabilizado ni se confirma entrada.

El backoff pausa consumo después de rollback y mantiene polls/heartbeats mediante soporte del contenedor, sin aplicar registros retenidos por él. Debe probarse con max.poll.interval.ms de la demo, que F-05 reduce a 10 s; aumentar ese valor para ocultar una espera defectuosa invalidaría sus ensayos. La pausa puede demorar temporalmente otras particiones del mismo contenedor durante la ventana acotada.

Al agotar la ronda se detiene la sesión con pendiente y diagnóstico operativo. Una acción local auditada puede solicitar otra ronda, mediante comando de control y registro durable de autorización; cuando la partición está detenida se admite registrar esa autorización para que el próximo propietario pueda reintentar la misma posición antes de alcanzar el comando. La autorización no aplica negocio fuera del consumidor ni borra el historial. Si persiste la causa, vuelve a agotarse. No se dead-letterea una caída de infraestructura por alcanzar un contador.

El AfterRollbackProcessor debe discriminar fallo del listener con rollback conocido de fallo/timeout durante commit del contenedor. Sólo el primero habilita retry planificado. Para commit incierto, fencing o pertenencia obsoleta se mantiene la detención y resolución desde offsets estables de F-04/F-05. No se reemplaza esa lógica por un seek/reintento genérico. Las primitivas DefaultAfterRollbackProcessor, BackOff y DeadLetterPublishingRecoverer son reutilizables, pero el presupuesto SQL y esta discriminación gobiernan la política.

### 4. DLQ transaccional y evidencia anterior al avance

Para un permanente, después de revertir el intento SQL fallido se abre una transacción SQL corta que conserva original, rechazo determinista y barrera. Se publica la notificación con el offset de la entrega en una nueva transacción Kafka con pertenencia vigente al grupo. La notificación no se publica dentro de la transacción Kafka ya abortada. Si el envío falla, se propaga el fallo sin confirmar progreso. Un commit incierto detiene la sesión.

El rechazo v1 usa key `rejectionId`, partición igual a la fuente y `rejectionId=rejection:<topic>:<partition>:<offset>`. Source usa partition entero y offset cadena decimal. Los campos de evidencia incluyen keyBase64/valueBase64 nullable (null es distinto de bytes vacíos), headers como lista ordenada nombre/base64 nullable, clasificación, código de causa, identidad confiable opcional, attempts y firstFailureAt congelados al resolver. La DLQ conserva el primer rechazo permanente por posición; las correcciones posteriores fallidas se registran como resoluciones de revisión, sin otra notificación del mismo rechazo inicial. Si un DEFERRED resulta inválido al recuperarlo, crea su primer rechazo usando la posición original y lo publica junto al offset del comando responsable.

El diagnóstico presentado usa códigos y mensajes filtrados; no copia SQL completo, credenciales o stacktraces al contrato. Se conserva payload original como evidencia, con los mismos límites de acceso locales que el entorno actual.

SQL puede mostrar cuarentena antes de que read_committed muestre el rechazo. Una reentrega recupera la resolución e intención iniciales. Una copia de comando en otro offset no adquiere otra intención de salida. La unicidad depende de resultado durable, dueño de entrega, secuencia del grupo y transacción Kafka; no se deriva sólo del rejectionId.

### 5. Barreras por orden y ambigüedad de partición

Key canónica con payload sin identidad interpretable permite identificar el alcance de orden desde la key, bajo el supuesto de routing existente. Si payload declara otra orden o la key no es confiable, no se elige entre ellas: se abre una barrera de partición. Se valida que todo ER con identidad confiable llegue a la partición esperada; un registro mal encaminado conserva bloqueo y diagnóstico, sin reubicarlo automáticamente.

Con barrera de orden, sucesores de esa orden se capturan con sus bytes y posición, sin validar transiciones todavía. Otra orden de la misma partición continúa. Con barrera de partición, todos los ER ordinarios posteriores se capturan, mientras los comandos válidos siguen ejecutándose. Se procesa un registro por vez; capturar no crea éxito ni settlement.

La barrera sólo se libera bajo el lock de partición cuando no queda trabajo de su alcance. Si aparece otra causa permanente, queda QUARANTINED nuevamente. Una barrera de partición también respeta pendientes de barreras de orden anteriores: se elige el menor offset de toda la cola relevante, no se adelanta un error ambiguo posterior. El costo es retención SQL y menor disponibilidad en casos ambiguos. Se acotan lecturas y lotes; si no puede conservarse más trabajo, se detiene sin confirmar el registro no guardado. No se eliminan pendientes por edad en F-06.

### 6. Corrección auditada por comandos, sin replay al final del topic

La API local de consola acepta una solicitud con requestId, source, expectedRevision, reason, declaredOperator y effectiveReport. Una transacción SQL corta registra solicitud inmutable y despacho; no aplica ER. POST repetido con igual contenido devuelve la misma solicitud; otro contenido con igual ID devuelve 409. La respuesta 202 significa aceptación durable, no aplicación. Validación de formato puede devolver 400; incompatibilidad posterior de transición se registra como resolución rechazada consultable, manteniendo la barrera.

Se proponen rutas de consola `/api/quarantine`, `/api/quarantine/{id}`, `/api/quarantine/corrections` y `/api/quarantine/requests/{requestId}`; los nombres definitivos pueden ajustarse a sus convenciones sin cambiar el contrato de comportamiento. El listado es paginado y permite filtro por posición/orden. Las dos aplicaciones exponen diagnóstico de orden con campo aditivo processing desde un mismo snapshot SQL. Sin orden aplicada se mantiene GET orders=404 y se usa la consulta independiente de cuarentena. La consola sólo agrega secciones a Mensajes/Órdenes y acciones locales; conserva el interceptor y DemoGate.

Las apps despachan solicitudes durables a execution-reports, con partición explícita igual a la original y header recovery.v1. No lo hace un hilo que aplica órdenes ni un consumidor de DLQ. El despacho usa claim SQL con lease y reenvío; puede duplicar comandos si el envío confirmó antes de caer. Esto es una outbox de comandos con entrega al menos una vez, no una demostración de publicación exactamente una vez.

Cada comando contiene commandId, requestId, originalPosition, expectedRevision y tipo APPLY_CORRECTION, RESUME o RETRY_AUTHORIZED; referencia el contenido durable por hash. El consumidor comprueba equivalencia completa con la solicitud, no sólo el hash. Las copias usan el mismo commandId. Una restricción/lock elige la primera posición de transporte que lo resuelve; esa posición conserva su intención. Reentregarla recupera idéntica intención; recibir copias en nuevas posiciones produce resolución sin salida. Un comando inexistente, obsoleto o mal encaminado registra rechazo de control sin mutar negocio y confirma su propia recepción para no bloquear el canal. RETRY_AUTHORIZED confirma la recepción de una autorización de ronda, que el listener puede haber consumido al recuperar el pendiente anterior; no vuelve a renovar presupuesto ni aplica un ER al alcanzarse.

Se corrige sólo el menor pendiente del scope. El ER efectivo debe cumplir el contrato y la transición real bajo lock. Si la identidad era confiable no puede cambiar numericOrderId. Para una identidad ambigua se exige asociación explícita y que el numericOrderId corregido corresponda a la partición original; si había key canónica se conserva esa identidad de routing. Si no puede corregirse sin cruzar particiones queda visible para intervención fuera del mecanismo automático, sin prometer una recuperación insegura. La resolución de una identidad ya aplicada sólo puede ser DUPLICATE con contenido equivalente o un ER efectivo con identidad nueva y transición válida; nunca se modifica su ledger histórico.

Tras aplicar o deduplicar una corrección se marca el pendiente resuelto y se programa durablemente un comando RESUME para el siguiente, en el mismo commit SQL. Cada RESUME aplica como máximo un pendiente. Si éste es inválido, conserva una nueva cuarentena y se detiene esa recuperación hasta otra corrección. Un nuevo ER de la orden se retiene mientras la barrera siga activa y será incluido por ese mismo mecanismo. Los comandos pendientes y sus claims sobreviven a caída de consola o de ambas apps.

```
original:    A-NEW -> A-error -> B-NEW -> A-FILLED -> B-FILLED
SQL:        aplicado  rechazo  aplicado  retenido    aplicado
comandos:   corregir A-error -> RESUME A-FILLED
efectos:    parcial efectivo -> FILLED + settlement
```

Alternativas descartadas: reinyectar bytes corregidos como un ER ordinario al final permite que sucesores lo adelanten; un worker SQL autónomo pierde la protección del offset transaccional del consumidor; drenar todos los pendientes en un comando sin límite amplía transacciones y complica recuperación. Un comando por pendiente conserva el modelo de una entrega y un commit de F-04.

### 7. Settlement conserva una entrega que protege la publicación

El input retenido ya pudo confirmar su offset sin aplicación. Por eso no se intenta asociar el settlement recuperado a ese progreso viejo. Se crea APPLIED/DUPLICATE en la posición del comando, con source original y revisión efectiva separados. El contrato settlement v1 conserva source del ER original y ledgerId del ER efectivo; no se cambia settlementId ni se agrega una segunda finalización.

Al reentregar el comando se busca su resolución antes de observar que el pendiente ya está resuelto. Así APPLIED FILLED sigue originando la misma intención hasta completar Kafka. Una copia del comando con otra posición referencia el éxito previo sin adquirir derecho a publicarlo. Mantener el orden del grupo asegura que no se alcanza esa copia antes de resolver el offset de la primera entrega. El intento obsoleto puede confirmar SQL pero no progreso Kafka con pertenencia vencida, igual que en F-05.

### 8. Evidencia, demo y compatibilidad

Se adapta el bloque de pruebas que hoy exige detención ante todos los permanentes: se mantienen sus afirmaciones de ausencia de efectos y se reemplaza la detención por cuarentena, DLQ y sucesores retenidos. Permanecen los tests de detención ante commit incierto y desconocidos.

Nuevas pruebas verifican política con reloj/RNG controlados; rollback de captura/barrera y recuperación; preservación binaria y migración V2->V3; concurrencia de solicitudes; dos miembros reales; supervivencia de presupuestos; fallas y kills en DLQ y comandos; conteos completos read_committed después de resolver transacciones. No se usa igualdad de IDs como sustituto de contar registros confirmados.

poison-recovery se habilita sólo con esas garantías. Los inválidos intencionales pasan exclusivamente por un control de ensayo opt-in y acotado a órdenes/posiciones creadas por esa ejecución; el editor ordinario mantiene preflight. La restauración usa correcciones predefinidas propias y conserva todos los originales; si falla deja diagnóstico y no declara limpio el entorno. Se agregan verify-f06 y verificaciones de consola para ocho escenarios, automático/paso a paso, recarga y cancelación. El smoke conserva datos ajenos y se ejecuta secuencialmente con los existentes.

Referencias técnicas: [manejo transaccional de errores y DLQ](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html), [límites de retry topics con transacciones](https://docs.spring.io/spring-kafka/reference/retrytopic.html), [pérdida de orden con retry topics](https://docs.spring.io/spring-kafka/reference/retrytopic/how-the-pattern-works.html), [ExponentialBackOff 7.0.9 con jitter](https://docs.spring.io/spring-framework/docs/7.0.9/javadoc-api/org/springframework/util/backoff/ExponentialBackOff.html). Las APIs deben contrastarse con las versiones del BOM al implementar; no se incorpora otra librería de retry.

## Risks / Trade-offs

- [Complejidad de dos commits] -> Resoluciones SQL inmutables por entrega, DLQ/salida más offset en Kafka y pruebas de aborto, kill y respuesta perdida. HTTP puede observar SQL antes de Kafka.
- [Duplicados del despacho] -> CommandId durable, dueño de primera resolución y receipts por posición; el despacho por sí solo no brinda exactly-once.
- [Retención y barreras largas] -> Diagnóstico de incompletitud, consultas paginadas y corrección explícita; la disponibilidad de una orden depende de una corrección humana válida. No se promete reparar contenido desconocido automáticamente.
- [Routing originalmente incorrecto] -> Barrera de partición y rechazo de asociaciones que crucen particiones. La garantía de recuperación presupone poder asociar el ER sin violar el routing estable.
- [Reservas interrumpidas consumen intentos] -> Se documenta el conteo conservador; nueva ronda auditada sin borrar el historial. El límite es de intentos reservados, no sólo fallos completados.
- [Auditoría sin autenticación] -> Responsable declarado, motivo e IDs durables dentro de la demo local; no se presenta como identidad verificada.
- [F-05 sensible a tiempos] -> Backoff fuera de transacciones con polls activos; regresión de sus barreras y smokes sin relajar su frontera observada.

## Migration Plan

1. En implementación crear V3 aditiva, probar desde V2 con resultados reales y sin backfill de éxitos ni borrado. Congelar contratos de comando y rechazo v1 en sus pruebas.
2. Detener ambas apps para actualizar juntas: una versión anterior no entiende comandos recovery.v1. Conservar base, grupo, topics, particiones, prefijos transaccionales y volúmenes. Asegurar rejected con dos particiones y configuración fijada.
3. Arrancar versión nueva; recuperar primero posiciones pendientes con resultado V2, después clasificar los ER sin éxito. No generar DLQ retrospectiva ni resetear offsets.
4. Ejecutar verify, smokes proporcionales y ensayo de activación; documentar comandos exactos y evidencia en README/verification al aplicar. Actualizar DECISIONS sólo distinguiendo las garantías observadas.
5. Ante fallo de despliegue detener ambas apps y corregir hacia adelante conservando datos. Una vez publicados comandos o confirmadas capturas diferidas, volver a F-05 puede romper el protocolo; no se propone downgrade automático ni rollback destructivo de V3.
