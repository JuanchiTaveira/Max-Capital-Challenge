# Spec Delta

## MODIFIED Requirements

### Requirement: Scenario availability catalog
La consola SHALL listar escenarios ejecutables y pendientes, indicando objetivo,
precondiciones, resultado esperado y dependencia. `Ejecutar disponibles` SHALL
excluir los pendientes e informar su cantidad por separado. No SHALL habilitar ni
aprobar un escenario pendiente por el solo hecho de disponer de controles manuales.

#### Scenario: Current implementation boundary
- **WHEN** se abre el catálogo después de completar F-06
- **THEN** están disponibles los siete escenarios anteriores y Mensajes inválidos y recuperación ordenada, conservando sus identificadores
- **AND** poison-recovery verifica rechazos, retención, corrección auditada y recuperación con órdenes nuevas
- **AND** ejecutar todos selecciona ocho escenarios y cada uno exige evidencia propia

#### Scenario: Pending execution request
- **WHEN** se intenta ejecutar un escenario pendiente desde la UI o su API
- **THEN** se rechaza la ejecución con su dependencia y no se modifica el entorno
- **AND** el resumen nunca lo cuenta como aprobado

### Requirement: Guided scenario explanation
El catálogo SHALL explicar qué demuestra cada escenario, la secuencia prevista y
el resultado esperado antes de ejecutar. SHALL ofrecer ocho escenarios ejecutables con nombres comprensibles y conservar sus identificadores.

#### Scenario: First individual scenario
- **WHEN** se abre la vista de escenarios sin una ejecución seleccionada
- **THEN** se selecciona Dos órdenes en paralelo y se explica el recorrido completo
- **AND** se permite ejecutar ese escenario o todos los disponibles sin ejecutar al seleccionar

#### Scenario: Recovery explanation before execution
- **WHEN** se selecciona uno de los dos escenarios habilitados por F-05
- **THEN** se explican la frontera de falla, las aplicaciones afectadas, las verificaciones y la restauración previstas
- **AND** se distingue reentrega de la misma posición de republicación y reinicio operativo de recuperación automática

### Requirement: Captured scenario verification
Las ejecuciones SHALL exponer resultados por escenario y verificaciones con
esperado, observado, estado y referencias estables a pasos, órdenes y mensajes.
Las conclusiones SHALL basarse en observaciones capturadas por el servidor y
conservar exactitud numérica y compatibilidad de los campos existentes.

#### Scenario: Five explained results
- **WHEN** finaliza el lote disponible
- **THEN** cada escenario tiene una conclusión, sus verificaciones y evidencia propias
- **AND** se explican estados y conteos 4/3, acumulados 0/3/7/10, dos duplicados, igualdad tras reinicio y exclusión del parcial abortado según corresponda
- **AND** el abort previsto no se presenta como falla y consultar la orden actual no reemplaza el resultado histórico

#### Scenario: Seven explained results
- **WHEN** finaliza el lote disponible
- **THEN** cada uno de los siete escenarios previos tiene una conclusión, sus verificaciones y evidencia propias
- **AND** se explican estados y conteos 4/3, acumulados 0/3/7/10, dos duplicados, igualdad tras reinicio y exclusión del parcial abortado según corresponda
- **AND** los escenarios de fallas muestran frontera observada, recuperación y conteos de settlement
- **AND** poison-recovery muestra originales, rechazos confirmados, pendientes retenidos, revisiones de corrección y secuencia finalmente aplicada
- **AND** el abort previsto no se presenta como falla y consultar la orden actual no reemplaza el resultado histórico

#### Scenario: Eight explained results
- **WHEN** finaliza el lote disponible de F-06
- **THEN** se conservan las conclusiones de los siete escenarios previos y se agrega la conclusión verificable de poison-recovery
- **AND** cada uno de los ocho tiene resultado propio y las verificaciones pendientes o fallidas no se cuentan como aprobadas

#### Scenario: Partial or unavailable observation
- **WHEN** falla, vence el plazo o se cancela una ejecución
- **THEN** se conservan las verificaciones observadas y se distinguen de las no verificadas y escenarios no ejecutados
- **AND** no se inventa un valor observado ni una conclusión de éxito para comprobaciones pendientes

## ADDED Requirements

### Requirement: Guided poison report recovery
El escenario poison-recovery SHALL demostrar JSON/key inválidos, identidad conflictiva, transición inválida y recuperación auditada con dos instancias. SHALL verificar reintentos transitorios con presupuesto, DLQ read_committed, retención de sucesores, progreso de otra orden de la misma partición y recuperación secuencial sin efectos duplicados.

#### Scenario: Correct an incomplete order and resume
- **WHEN** se ejecuta poison-recovery en modo automático o paso a paso
- **THEN** se observa la cuarentena antes de corregir, un sucesor retenido sin ledger y otra orden sana completada
- **AND** se aplica una corrección con motivo, se recuperan los sucesores y se comprueban contador, ledger y un settlement por FILLED y cero por CANCELLED
- **AND** el recorrido de key contradictoria muestra barrera de partición y su resolución explícita sin asociar silenciosamente la orden

#### Scenario: Durable correction survives process death
- **WHEN** una aplicación cae después del commit SQL de un comando de recuperación FILLED y antes del commit Kafka
- **THEN** la otra recupera la misma posición del comando y conserva una aplicación y un settlement confirmado
- **AND** duplicar la solicitud no agrega revisión, ledger ni salida y se restauran dos consumidores

### Requirement: Scoped quarantine demonstration
El ensayo SHALL usar órdenes nuevas, correcciones predefinidas y controles limitados a su ejecución. SHALL conservar datos previos, originales y auditoría y ofrecer smoke portable verify-f06. Cancelar o fallar SHALL informar y procurar resolver sólo los bloqueos creados por ese ensayo; nunca SHALL borrar cuarentena ni corregir datos ajenos para aparentar limpieza.

#### Scenario: Repeated portable run
- **WHEN** se ejecuta verify-f06 dos veces sobre un entorno con órdenes y cuarentenas previas
- **THEN** cada corrida usa nuevos identificadores y conserva los datos anteriores
- **AND** sus conteos recorren completamente los rangos estables observados sin confundir una lectura parcial con unicidad

#### Scenario: Cancel with a quarantined report
- **WHEN** se cancela después de publicar un inválido de la ejecución
- **THEN** se conservan mensajes y auditoría y se registra la corrección de restauración predefinida si corresponde
- **AND** si no puede completarse se informa el alcance bloqueado y la acción pendiente sin declarar restauración exitosa
