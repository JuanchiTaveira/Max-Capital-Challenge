# Spec Delta

## Purpose

Permitir que el evaluador observe el flujo real del challenge y publique ER desde
una consola local, correlacionando mensajes, efectos confirmados y estado operativo.

## ADDED Requirements

### Requirement: Evaluation navigation
La consola SHALL ofrecer vistas de escenarios, mensajes, órdenes y sistema desde
un único origen local. SHALL mostrar estados de carga, vacío, error y última
observación, con navegación por teclado y controles con etiquetas visibles.

#### Scenario: Empty environment
- **WHEN** el evaluador abre la consola sin órdenes ni mensajes de demo
- **THEN** ve instrucciones para ejecutar un escenario o publicar un ER
- **AND** los estados vacíos no se presentan como fallas

#### Scenario: Temporary observation failure
- **WHEN** falla una actualización de Kafka, SQL o una instancia
- **THEN** la vista identifica qué consulta falló y permite reintentar
- **AND** los datos anteriores conservados quedan identificados como desactualizados

### Requirement: Broker record inspection
La consola SHALL consultar `execution-reports`, `settlements` y
`execution-reports-rejected` con visibilidad `read_committed`, mostrando topic,
partición, offset, key y payload completo. SHALL ofrecer lectura acotada y
continuación por partición sin incorporarse al grupo de procesamiento ni
modificar sus offsets o asignaciones.

#### Scenario: Inspection while applications consume
- **WHEN** el evaluador recarga o pagina mensajes mientras ambas aplicaciones consumen
- **THEN** cada registro mostrado conserva su posición Kafka y contenido
- **AND** la inspección no agrega miembros al grupo de las aplicaciones ni confirma offsets en él

#### Scenario: Aborted transaction
- **WHEN** una transacción productora abortada contiene un ER
- **THEN** ese registro no aparece entre los mensajes visibles `read_committed`
- **AND** un hueco entre offsets mostrados no se clasifica por sí solo como pérdida

#### Scenario: Output topics without implemented publishers
- **WHEN** se consultan settlements o rechazos sin publicaciones observables
- **THEN** la vista muestra el resultado vacío de la lectura y su alcance
- **AND** el catálogo identifica las verificaciones pendientes de F-04 a F-06 sin declarar esas garantías satisfechas

### Requirement: Current order and ledger inspection
La consola SHALL listar órdenes persistidas y permitir buscar por
`numericOrderId`. El detalle SHALL presentar estado, cantidades, contador de ER
aplicados y ledger completo ordenado por `ledgerId`, leídos desde un mismo
snapshot SQL confirmado. El contador SHALL distinguirse de la cantidad operada.

#### Scenario: Existing order
- **WHEN** se selecciona una orden con varios ER aplicados y republicaciones
- **THEN** el detalle muestra el estado actual y una entrada de ledger por ER aplicado
- **AND** cada entrada permite inspeccionar el ER completo, incluidos campos adicionales
- **AND** las republicaciones duplicadas no aparecen como nuevas entradas de ledger

#### Scenario: Unknown or invalid identifier
- **WHEN** se busca una orden inexistente o un identificador fuera del contrato
- **THEN** la consola distingue orden no encontrada de identificador inválido
- **AND** conserva el comportamiento 404/400 del endpoint público existente

### Requirement: Processing result correlation
La consola SHALL correlacionar un registro con su resultado por la tupla completa
`(topic, partition, offset)`. SHALL mostrar outcome, identidad del ER, referencia
al ledger y snapshot histórico, separados del estado actual. La falta de resultado
SHALL mostrarse como ausencia de éxito registrado, sin inferir un fallo.

#### Scenario: Late duplicate of NEW
- **GIVEN** una orden FILLED cuyo NEW se republicó después del terminal
- **WHEN** se consulta el resultado de esa republicación
- **THEN** se muestra `DUPLICATE`, la referencia al ledger de NEW y el snapshot FILLED registrado para esa posición
- **AND** el resultado original de NEW conserva su snapshot NEW

#### Scenario: Record without result
- **WHEN** un registro no tiene fila en `processing_results`
- **THEN** la vista muestra `Sin resultado registrado`
- **AND** permite consultar el diagnóstico disponible sin confundir falta de fila con error confirmado

### Requirement: Exact numeric representation
La consola SHALL preservar identificadores positivos de 64 bits, offsets,
contadores y cantidades con hasta doce dígitos enteros y ocho decimales en lectura,
edición y publicación. Mostrar o republicar un ER SHALL conservar sus valores
numéricos y campos adicionales sin redondeo del navegador.

#### Scenario: Values outside browser safe integer range
- **WHEN** se publica e inspecciona un ER con identificadores mayores que `9007199254740991` dentro del rango permitido y cantidad `999999999999.12345678`
- **THEN** la key, el ER publicado, el detalle de orden y el ledger conservan exactamente esos valores

### Requirement: Manual report publication
La consola SHALL permitir publicar un ER del contrato vigente con su key y JSON,
validar el formato y rechazar conflictos o transiciones inválidas conocidos antes
de enviarlo. SHALL informar la posición sólo tras confirmación del broker y
mostrar el resultado SQL por separado. Las escrituras de negocio SHALL pasar
exclusivamente por Kafka.

#### Scenario: Accepted report
- **WHEN** el evaluador envía un ER válido
- **THEN** obtiene topic, partición y offset confirmados por el broker
- **AND** la pantalla diferencia esa confirmación de la aplicación posterior o del diagnóstico real del consumidor

#### Scenario: Invalid draft
- **WHEN** el borrador tiene JSON inválido, key incompatible, una identidad conocida con distinto contenido o transición incompatible con el snapshot consultado
- **THEN** se muestra el motivo y ese envío no publica un registro
- **AND** probar intencionalmente esos errores se identifica como escenario pendiente de aislamiento y recuperación de F-06

#### Scenario: Broker acknowledgement unavailable
- **WHEN** la publicación falla o vence su plazo sin confirmación concluyente
- **THEN** la UI informa el error o resultado incierto y no inventa una posición ni un éxito SQL
- **AND** no reenvía automáticamente el ER para ocultar la incertidumbre

### Requirement: Exact record republication
La consola SHALL permitir republicar un ER válido de `execution-reports`
conservando su key y payload originales. SHALL identificar el nuevo offset y
describir la acción como republicación, diferenciándola de reentregar la misma
posición Kafka. No SHALL resetear offsets para simular una reentrega.

#### Scenario: Terminal republication
- **WHEN** se republica un FILLED ya aplicado
- **THEN** el nuevo registro usa la misma key y payload y otra posición Kafka
- **AND** su resultado eventual es `DUPLICATE`, sin aumentar contador ni ledger

### Requirement: Operational observation
La consola SHALL mostrar por instancia salud HTTP, estado observado del
consumidor y asignaciones actuales; por partición, progreso confirmado y lag.
SHALL distinguir información desconocida de consumidor detenido y atribuir
diagnósticos a su fuente y fecha. Las asignaciones actuales no SHALL presentarse
como autoría histórica de los resultados SQL.

#### Scenario: HTTP alive and consumer stopped
- **WHEN** una instancia responde HTTP pero su contenedor consumidor está detenido
- **THEN** la consola presenta ambos hechos por separado
- **AND** no la clasifica como consumiendo por su readiness HTTP

#### Scenario: Instance unavailable
- **WHEN** una instancia no responde
- **THEN** se muestra la indisponibilidad y el estado de consumo queda desconocido hasta obtener evidencia
- **AND** las demás consultas disponibles siguen accesibles

### Requirement: Bounded and safe presentation
La consola SHALL acotar consultas, refrescos y diagnósticos, permitir pausar el
refresco y presentar key, JSON y logs como texto. Una lectura parcial SHALL
indicar su rango o continuación. Los diagnósticos operativos no SHALL exponer
variables de entorno ni credenciales.

#### Scenario: Large history or hostile report text
- **WHEN** existen más mensajes que el límite de lectura o un campo contiene marcado HTML
- **THEN** la UI indica la continuación y muestra el contenido como texto inerte
- **AND** las actualizaciones no ejecutan el contenido ni bloquean los controles de navegación
