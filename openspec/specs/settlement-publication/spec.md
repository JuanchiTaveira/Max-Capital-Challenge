# settlement-publication Specification

## Purpose

Publicar una liquidación por orden completada, recuperable ante reentregas y fallas,
con una única salida confirmada observable por consumidores Kafka `read_committed`.

## Requirements

### Requirement: Settlement eligibility from durable results
El sistema SHALL emitir settlement sólo para una posición cuyo resultado SQL
original sea `APPLIED` con snapshot `FILLED`. SHALL recuperar esa decisión en una
reentrega sin reclasificar el resultado ni repetir efectos SQL. Un resultado
`DUPLICATE` SHALL producir cero settlements, incluso con snapshot `FILLED`.

#### Scenario: First completion
- **WHEN** un ER válido completa una orden y confirma su resultado `APPLIED` con snapshot `FILLED`
- **THEN** esa posición origina un settlement en la transacción Kafka que confirma su progreso
- **AND** el contador y ledger reflejan una sola aplicación del ER

#### Scenario: Pending original completion is redelivered
- **GIVEN** un FILLED con resultado `APPLIED` confirmado en SQL y transacción Kafka abortada
- **WHEN** se reentrega su misma posición a cualquiera de las dos instancias
- **THEN** se recupera la intención y los mismos datos del settlement
- **AND** al completar la recuperación queda un solo settlement confirmado sin otra aplicación SQL

#### Scenario: Republished terminal and late duplicate
- **GIVEN** una orden FILLED con settlement confirmado
- **WHEN** se republican su FILLED idéntico y un NEW idéntico en posiciones nuevas
- **THEN** ambas posiciones conservan `DUPLICATE` y no generan settlements adicionales
- **AND** el snapshot FILLED del duplicado tardío no se interpreta como una nueva finalización

### Requirement: No settlement for cancellation or intermediate states
El sistema SHALL producir cero settlements para resultados `NEW`,
`PARTIALLY_FILLED` o `CANCELLED`, incluidas reentregas y republicaciones de sus ER.
Un ER inválido o conflictivo SHALL conservarse pendiente sin generar settlement.

#### Scenario: Cancellation after partial execution
- **WHEN** una orden aplica NEW, un parcial y CANCELLED válido, y después se republica su CANCELLED
- **THEN** su estado final es CANCELLED y no se observa settlement para esa orden
- **AND** el acumulado parcial no habilita una liquidación terminal

#### Scenario: Invalid terminal
- **WHEN** un FILLED viola una transición o reutiliza una identidad con contenido diferente
- **THEN** no genera settlement ni resultado exitoso para el intento
- **AND** el diagnóstico identifica la posición pendiente

### Requirement: Deterministic settlement contract
El settlement SHALL ser JSON UTF-8 en `settlements`, con key decimal canónica de
`numericOrderId`, `schemaVersion` 1, `settlementId` igual a `settlement:<numericOrderId>`,
`numericOrderId`, `fixId`, `ledgerId`, `source` con topic/partition/offset y cantidades
`nominalAmounts`, `accumulativeNominalAmount`, `leavesNominalAmount`. IDs y cantidades
SHALL ser cadenas decimales exactas; source.partition SHALL ser entero y source.offset
cadena decimal. Los datos SHALL provenir del resultado original.

#### Scenario: Output matches original completion
- **WHEN** se publica el settlement de un resultado APPLIED FILLED
- **THEN** identidad, posición y cantidades coinciden con ese resultado SQL
- **AND** nominal y acumulado son iguales y el restante es cero
- **AND** el mensaje puede decodificarse con el contrato v1 y correlacionarse con su ledger

#### Scenario: Stable reconstruction with exact values
- **GIVEN** un resultado con identificadores mayores que el rango entero exacto de JavaScript y cantidades con ocho decimales
- **WHEN** se reconstruye el settlement después de cerrar y abrir la aplicación
- **THEN** conserva identidad, campos y representación canónica sin redondeos
- **AND** no incorpora timestamps ni identificadores aleatorios del nuevo intento

### Requirement: Single committed settlement per order
Con ambas instancias en el mismo grupo, el sistema SHALL dejar exactamente un
registro confirmado en `settlements` por orden FILLED cubierta por la activación.
SHALL mantener esa unicidad ante reentregas y cambio de propietario de partición.
La identidad estable del mensaje SHALL servir para correlación, sin delegar la
supresión de publicaciones duplicadas al consumidor downstream.

#### Scenario: Two active instances and business duplicates
- **WHEN** dos instancias consumen órdenes intercaladas en ambas particiones y terminales republicados
- **THEN** cada orden FILLED tiene exactamente un settlement confirmado y cada CANCELLED tiene cero
- **AND** hay una sola aplicación SQL por ER individual y resultados por cada posición resuelta

#### Scenario: Old owner resumes after reassignment
- **GIVEN** una instancia suspendida después de confirmar el FILLED en SQL y antes de completar su transacción Kafka
- **WHEN** pierde la partición, otra instancia la recupera y luego se libera el intento anterior
- **THEN** sólo un intento confirma settlement y progreso del grupo para la finalización
- **AND** el intento obsoleto no confirma una segunda salida ni altera el resultado SQL original

### Requirement: Committed downstream visibility
Un consumidor `read_committed` SHALL observar sólo settlements de transacciones
confirmadas. Los intentos abortados SHALL permanecer invisibles y los abiertos
SHALL esperar resolución. Una lectura SQL SHALL poder mostrar FILLED antes de que
el settlement sea visible: no se promete un commit distribuido SQL/Kafka ni se
equipara unicidad del registro con ejecución única de efectos externos downstream.

#### Scenario: Send acknowledged before abort
- **GIVEN** un settlement enviado al broker dentro de una transacción todavía abierta
- **WHEN** se inspecciona con `read_committed` y luego se aborta el intento
- **THEN** ese settlement nunca se entrega a esa lectura
- **AND** después de recuperar el ER y confirmar Kafka se observa un solo settlement de la orden

#### Scenario: SQL visible while publication is pending
- **WHEN** SQL confirma FILLED y la publicación queda suspendida antes de confirmar Kafka
- **THEN** HTTP y SQL pueden mostrar FILLED mientras `read_committed` todavía no muestra su settlement
- **AND** al reanudar y confirmar Kafka el settlement aparece sin otro incremento del contador

### Requirement: Recovery after process failure and uncertain commit
Tras una caída o commit Kafka incierto, el sistema SHALL resolver el progreso
desde offsets estables del grupo antes de volver a publicar. Si el intento confirmó,
SHALL continuar después de su posición; si abortó, SHALL recuperar el resultado SQL
y completar la salida. SHALL conservar diagnóstico de incertidumbre y no tratar
un timeout como evidencia de aborto ni reenviar desde la posición local anterior.

#### Scenario: Process dies between SQL commit and publication
- **GIVEN** un proceso con FILLED y resultado APPLIED confirmados en SQL, sin settlement confirmado
- **WHEN** el proceso termina abruptamente en esa frontera y la otra instancia recupera la partición
- **THEN** recupera el mismo resultado y confirma un settlement junto al progreso Kafka
- **AND** al volver la instancia original no se repite ni el efecto SQL ni el settlement

#### Scenario: Broker commits but response is lost
- **GIVEN** settlement y offset confirmados realmente por Kafka
- **WHEN** la aplicación recibe un timeout en lugar de la respuesta y reinicia su sesión de consumo
- **THEN** recupera el progreso confirmado sin volver a publicar esa finalización
- **AND** queda un único settlement visible y el sucesor puede procesarse

#### Scenario: Process dies before terminal SQL commit
- **GIVEN** un FILLED con escrituras SQL todavía sin confirmar
- **WHEN** el proceso termina abruptamente y la partición se recupera
- **THEN** el intento incompleto no conserva efectos SQL ni settlement confirmado
- **AND** recuperar el ER deja una sola aplicación y exactamente un settlement confirmado con los datos del resultado original exitoso

#### Scenario: Process dies after acknowledged settlement send
- **GIVEN** un FILLED confirmado en SQL y un settlement cuyo envío fue aceptado dentro de una transacción Kafka todavía sin confirmar
- **WHEN** el proceso termina abruptamente antes del commit Kafka y otro propietario recupera la partición
- **THEN** el intento abierto se resuelve sin hacerse visible como una segunda salida read_committed
- **AND** después de resolver los intentos y recuperar el progreso existe exactamente un settlement confirmado y el resultado SQL original permanece igual

#### Scenario: Both applications restart with a pending completion
- **GIVEN** un FILLED con resultado SQL confirmado, Kafka pendiente y sucesores publicados
- **WHEN** ambas aplicaciones terminan abruptamente y vuelven a consumir el mismo grupo y datos
- **THEN** recuperan el resultado original, completan el progreso y procesan los sucesores sin repetir efectos
- **AND** después de resolver las transacciones pendientes se observa exactamente un settlement para FILLED y cero para órdenes CANCELLED del recorrido

### Requirement: Forward activation with pending F03 results
La activación sobre F-03 SHALL conservar datos y offsets e incluir finalizaciones
con resultado APPLIED FILLED y offset todavía pendiente. SHALL producir cero
salidas retroactivas para posiciones ya confirmadas antes de F-04, también cuando
se republica su ER como DUPLICATE. No SHALL inventar posiciones históricas ni
resetear offsets para obtener settlements anteriores.

#### Scenario: Preexisting pending F03 completion
- **GIVEN** un resultado APPLIED FILLED escrito por F-03 cuyo offset no fue confirmado
- **WHEN** ambas instancias se actualizan a F-04 y retoman el mismo grupo
- **THEN** su reentrega recupera el resultado existente y publica una única salida confirmada
- **AND** no modifica el resultado, contador ni ledger previos

#### Scenario: Preexisting acknowledged completion
- **GIVEN** una orden FILLED de F-03 cuyo offset ya estaba confirmado antes del despliegue
- **WHEN** se inicia F-04 y posteriormente se republica su terminal idéntico
- **THEN** no se realiza backfill y la republicación DUPLICATE no genera settlement
