# Spec Delta

## Purpose

Conservar ER fallidos y sus sucesores con evidencia durable, aislar órdenes incompletas y permitir su recuperación secuencial mediante correcciones auditadas sin perder ni duplicar efectos.

## ADDED Requirements

### Requirement: Explicit failure classification
El sistema SHALL distinguir errores permanentes del ER, fallas transitorias conocidas, fallas de infraestructura y resultados Kafka inciertos. Una excepción desconocida SHALL conservar el pendiente y diagnóstico sin descartarlo. La clasificación SHALL considerar la etapa y el resultado transaccional, no sólo el tipo de excepción.

#### Scenario: Permanent invalid input
- **WHEN** un ER contiene JSON o campos inválidos, identidad conflictiva o transición inválida
- **THEN** se conserva en cuarentena sin reintentos automáticos de negocio ni efectos parciales
- **AND** se identifica posición, causa y alcance del bloqueo

#### Scenario: Unknown failure or unavailable database
- **WHEN** ocurre una falla no clasificada o no se puede persistir el diagnóstico durable
- **THEN** no se confirma el offset ni se envía el ER a DLQ como si fuera inválido
- **AND** se informa la intervención operativa necesaria

### Requirement: Durable bounded exponential retries
Las fallas transitorias reintentables SHALL usar backoff exponencial con jitter y límites configurables de reintentos, intervalo y tiempo total. El presupuesto y próximo intento SHALL sobrevivir a reinicios y rebalanceos por posición. Las esperas SHALL ocurrir sin transacción SQL ni Kafka abierta. No SHALL aplicarse un sucesor durante esa espera.

#### Scenario: Transient failure recovers within budget
- **WHEN** un intento revierte por una falla transitoria conocida y luego desaparece su causa
- **THEN** se reintenta la misma posición respetando sus esperas y se aplica una sola vez
- **AND** no aparece rechazo permanente ni aplicación anticipada de sus sucesores

#### Scenario: Restart during backoff
- **WHEN** una instancia cae después de reservar un reintento o mientras espera y otra toma la partición
- **THEN** conserva el presupuesto consumido y no adelanta el próximo instante permitido
- **AND** los reinicios repetidos no regeneran intentos ilimitados

#### Scenario: Budget exhausted or confirmation uncertain
- **WHEN** se agota un presupuesto transitorio o el commit Kafka queda incierto
- **THEN** se conserva el pendiente y se detiene la sesión con diagnóstico operativo, sin convertirlo automáticamente en error permanente
- **AND** reanudar no renueva el presupuesto agotado; una nueva ronda exige una acción auditada explícita

### Requirement: Immutable quarantine evidence
La cuarentena SHALL conservar por posición el valor y key originales en bytes, nulidad, headers, diagnóstico, clasificación, intentos y fechas. SHALL distinguir identidad confiable de identidad declarada y conservar sus revisiones. Los bytes originales y resultados exitosos previos SHALL ser inmutables; cuarentena SHALL ser distinta de APPLIED y DUPLICATE.

#### Scenario: Non JSON or null value
- **WHEN** se recibe un valor ilegible, no UTF-8 o nulo
- **THEN** el original puede reconstruirse exactamente desde la evidencia, incluso si no existe un JSON válido
- **AND** no se crea una orden ficticia ni una fila de ledger o resultado exitoso

#### Scenario: Redelivery after quarantine SQL commit
- **WHEN** se reentrega una posición cuya cuarentena fue confirmada antes de fallar Kafka
- **THEN** se recuperan el mismo rechazo y bloqueo sin agregar otra cuarentena ni reiniciar intentos

### Requirement: Transactional dead letter publication
Cada rechazo permanente SHALL publicar una única notificación confirmada en `execution-reports-rejected`, junto al offset de la entrega que lo resuelve en la misma transacción Kafka. La evidencia y bloqueo SHALL estar confirmados antes. Si publicar falla, SHALL conservarse el pendiente; un commit incierto SHALL resolverse desde progreso estable.

#### Scenario: Rejection transaction aborts
- **WHEN** Kafka aborta después de aceptar el envío del rechazo
- **THEN** read_committed no observa ese intento ni avanza el offset estable
- **AND** al recuperar se observa un único rechazo confirmado y permanece el original SQL intacto

#### Scenario: Rejection commit response lost
- **WHEN** Kafka confirma rechazo y progreso pero la aplicación pierde la respuesta
- **THEN** reiniciar desde progreso estable no publica otra notificación del mismo rechazo

#### Scenario: Deferred report fails during recovery
- **WHEN** un sucesor retenido se intenta por primera vez durante recuperación y resulta permanentemente inválido
- **THEN** su rechazo se confirma junto al offset del comando que hizo ese intento
- **AND** el rechazo conserva como fuente la posición original del sucesor y el bloqueo continúa

### Requirement: Versioned rejection contract
El rechazo SHALL ser JSON UTF-8 con schemaVersion 1, rejectionId estable por posición original, source con topic/partition/offset, key y valor originales codificados en base64 con nulidad explícita, headers, clasificación, código de causa e identidad confiable opcional. IDs y offsets SHALL preservar exactitud decimal; el diagnóstico SHALL excluir credenciales y trazas sin filtrar.

#### Scenario: Inspect rejection with invalid bytes
- **WHEN** se lee una notificación con read_committed
- **THEN** permite correlacionar la cuarentena y reconstruir key, valor y headers originales sin pérdida
- **AND** cambiar de instancia no cambia rejectionId ni fuente

### Requirement: Durable sequence barriers
Una orden con un ER sin resolver SHALL retener sus sucesores sin aplicarlos ni emitir settlement, conservando su secuencia original. Otras órdenes identificables SHALL continuar mediante consumo secuencial. Ante identidad desconocida o key/payload contradictorios SHALL retenerse conservadoramente la partición, sin inferir una asociación de orden.

#### Scenario: Healthy order shares partition
- **WHEN** A tiene un ER inválido y luego llegan ER de A y B en su misma partición, con identidades confiables
- **THEN** los sucesores de A quedan retenidos y B puede completar normalmente
- **AND** avanzar sus offsets sólo significa captura durable para los retenidos, no aplicación exitosa

#### Scenario: Missing key or contradictory identifiers
- **WHEN** el ER no tiene una key canónica confiable o su identidad contradice la key
- **THEN** no se aplican nuevos ER ordinarios de esa partición hasta una resolución auditada
- **AND** se capturan durablemente mientras las demás particiones continúan y los comandos de resolución siguen siendo atendibles

### Requirement: Audited correction commands
Una corrección SHALL identificar solicitud idempotente, posición original, revisión esperada, motivo, responsable declarado y ER efectivo. SHALL conservar original y revisiones y ejecutarse mediante Kafka. No SHALL cambiar una orden confiablemente identificada ni sobrescribir éxitos previos. Una asociación inicialmente ambigua SHALL exigir identidad explícita compatible con la partición original.

#### Scenario: Valid correction of earliest unresolved report
- **WHEN** se procesa una corrección válida para el primer pendiente del alcance bloqueado
- **THEN** se conserva el vínculo entre original, revisión, comando y resultado aplicado o duplicado
- **AND** el contador y ledger sólo cambian si el ER efectivo es nuevo y válido

#### Scenario: Stale conflicting or out of sequence correction
- **WHEN** la revisión es obsoleta, la solicitud se reutiliza con otro contenido, se intenta corregir un éxito o resolver un pendiente posterior
- **THEN** se registra o devuelve un rechazo explícito sin modificar el original, la revisión vigente ni la orden
- **AND** una corrección inválida no libera el bloqueo ni borra sus sucesores

#### Scenario: Repeated equivalent correction
- **WHEN** se repite la misma solicitud en otra posición Kafka o desde la otra instancia
- **THEN** se devuelve su resolución previa sin otra aplicación, revisión ni salida settlement

### Requirement: Ordered recovery under partition ownership
La recuperación SHALL usar comandos en la partición original y aplicar como máximo un ER retenido por comando bajo su propietario vigente. SHALL elegir el menor offset pendiente del alcance y conservar la barrera hasta agotar sus pendientes. El avance y las salidas SHALL ser recuperables ante caída, aborto o rebalanceo sin depender de la consola viva.

#### Scenario: Correct partial then recover FILLED
- **WHEN** se corrige un parcial inválido seguido por un FILLED retenido
- **THEN** primero se aplica el parcial efectivo y después el FILLED original
- **AND** el ledger refleja esa secuencia, con una aplicación por ER y un único settlement confirmado

#### Scenario: New reports arrive during recovery
- **WHEN** llegan ER nuevos para el alcance aún bloqueado mientras se recuperan sus pendientes
- **THEN** se incorporan al final de sus pendientes y no adelantan a los ya retenidos
- **AND** el bloqueo sólo se libera al resolver todos los pendientes del alcance

#### Scenario: Crash with recovery work remaining
- **WHEN** ambas instancias o la consola reinician después de aceptar una corrección o de aplicar un pendiente
- **THEN** las solicitudes aceptadas y el trabajo restante se retoman durablemente
- **AND** un comando duplicado o un propietario obsoleto no repite efectos ni salidas

### Requirement: Durable recovery visibility
La consulta de cuarentena SHALL exponer original, alcance del bloqueo, pendientes, intentos, revisión, motivo, responsable declarado, posiciones de comandos y resoluciones. SHALL distinguir captura durable, aplicación SQL y confirmación Kafka. Aceptar una solicitud SHALL ser distinto de aplicarla; el responsable declarado SHALL presentarse sin garantía de autenticación.

#### Scenario: Order missing NEW
- **WHEN** el primer ER de una orden está en cuarentena y todavía no existe una orden aplicada
- **THEN** se puede consultar su diagnóstico por posición o identidad confiable sin inventar un estado NEW ni modificar el 404 de consulta de orden

#### Scenario: Accepted request loses HTTP response
- **WHEN** una solicitud fue aceptada durablemente pero el cliente no recibe respuesta
- **THEN** repetir su identificador recupera la misma solicitud y permite observar si quedó pendiente o resuelta

### Requirement: Preservation across activation
La activación SHALL conservar órdenes, ledger, resultados y offsets previos. Una posición inválida pendiente de F-05 SHALL poder entrar a cuarentena al reanudarse. No SHALL crear rechazos retroactivos para posiciones ya confirmadas ni borrar pendientes por limpieza de demo o cambio de instancia.

#### Scenario: Upgrade with existing success and poison record
- **WHEN** ambas instancias actualizadas retoman el mismo grupo sobre datos V2 con éxitos y un ER inválido pendiente
- **THEN** los éxitos permanecen iguales y el inválido obtiene su primera cuarentena y rechazo confirmado
- **AND** no se resetean offsets ni se recrean topics o volúmenes
