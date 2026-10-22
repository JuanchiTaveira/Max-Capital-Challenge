# Spec Delta

## MODIFIED Requirements

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
- **AND** consulta separadamente cuarentena, retención o resolución de comando por posición, sin inferirlos de la falta de resultado exitoso

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
- **AND** probar intencionalmente esos errores se limita al escenario aislado poison-recovery, conservando la validación de publicación ordinaria

#### Scenario: Broker acknowledgement unavailable
- **WHEN** la publicación falla o vence su plazo sin confirmación concluyente
- **THEN** la UI informa el error o resultado incierto y no inventa una posición ni un éxito SQL
- **AND** no reenvía automáticamente el ER para ocultar la incertidumbre

## ADDED Requirements

### Requirement: Quarantine and correction inspection
Las vistas existentes SHALL mostrar original, diagnóstico, alcance del bloqueo, pendientes, intentos, revisión, motivo, responsable declarado y vínculos a comando, ledger y rechazo. SHALL distinguir valor binario de JSON interpretable, conservar exactitud numérica y presentar incompletitud separada del estado de mercado.

#### Scenario: Inspect an invalid or deferred record
- **WHEN** se abre un mensaje sin resultado exitoso pero con cuarentena o captura diferida
- **THEN** se muestra su estado durable específico y el primer pendiente de su alcance
- **AND** puede inspeccionarse el original y su rechazo sin ejecutar marcado ni truncar silenciosamente evidencia binaria

### Requirement: Local audited correction request
La consola SHALL permitir solicitar una corrección con identificador idempotente, posición, revisión esperada, motivo, responsable declarado y ER efectivo. SHALL conservar controles locales y exclusión durante escenarios. Aceptación durable y aplicación posterior SHALL mostrarse por separado; errores de revisión o contenido SHALL ser explícitos.

#### Scenario: Submit and inspect a correction
- **WHEN** se solicita una corrección válida desde el origen local autorizado
- **THEN** se obtiene una referencia durable para consultar solicitud y comandos, sin afirmar aplicación por el solo envío
- **AND** al resolverla se muestran original, revisión aplicada, secuencia recuperada y sus resultados observados

#### Scenario: Foreign request or active scenario
- **WHEN** se intenta una corrección desde otro origen o una mutación manual mientras hay un escenario activo
- **THEN** se rechaza antes de publicar un comando o aceptar una solicitud
- **AND** la corrección predefinida del escenario sólo puede afectar posiciones pertenecientes a su ejecución

#### Scenario: Refresh after console restart
- **WHEN** la consola reinicia después de aceptar una corrección
- **THEN** se puede recuperar su estado durable por identificador sin depender del historial de escenarios en memoria
- **AND** no se envía otra solicitud automáticamente por perder la respuesta HTTP
