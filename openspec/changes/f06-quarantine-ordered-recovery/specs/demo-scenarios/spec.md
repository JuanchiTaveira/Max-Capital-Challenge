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
- **AND** poison-recovery demuestra cuarentena, pausa de partición y corrección auditada con órdenes nuevas
- **AND** ejecutar todos selecciona ocho escenarios y cada uno requiere sus verificaciones

#### Scenario: Pending execution request
- **WHEN** se intenta ejecutar un escenario pendiente desde la UI o su API
- **THEN** se rechaza la ejecución con su dependencia y no se modifica el entorno
- **AND** el resumen nunca lo cuenta como aprobado

### Requirement: Guided scenario explanation
El catálogo SHALL explicar qué demuestra cada escenario, la secuencia prevista y
el resultado esperado antes de ejecutar. SHALL ofrecer ocho escenarios ejecutables
con nombres comprensibles y conservar sus identificadores.

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
- **THEN** cada uno de los siete escenarios tiene una conclusión, sus verificaciones y evidencia propias
- **AND** se explican estados y conteos 4/3, acumulados 0/3/7/10, dos duplicados, igualdad tras reinicio y exclusión del parcial abortado según corresponda
- **AND** los dos escenarios nuevos muestran además frontera observada, recuperación y conteos de settlement
- **AND** el abort previsto no se presenta como falla y consultar la orden actual no reemplaza el resultado histórico

#### Scenario: Partial or unavailable observation
- **WHEN** falla, vence el plazo o se cancela una ejecución
- **THEN** se conservan las verificaciones observadas y se distinguen de las no verificadas y escenarios no ejecutados
- **AND** no se inventa un valor observado ni una conclusión de éxito para comprobaciones pendientes

#### Scenario: Eight explained results
- **WHEN** finaliza el lote disponible después de implementar F-06
- **THEN** los siete recorridos anteriores conservan su evidencia y poison-recovery muestra su resultado propio
- **AND** se distinguen cuarentena, corrección aceptada y secuencia finalmente aplicada, sin contar pendientes como éxitos

## ADDED Requirements

### Requirement: Guided poison recovery
El escenario poison-recovery SHALL demostrar un ER ilegible, su partición pausada y una corrección auditada seguida por sus sucesores. SHALL usar órdenes nuevas, el runner y los controles existentes, sin nuevas pantallas ni tablas de demo. Cancelación o fallo SHALL conservar datos e informar si no se pudo restaurar el consumo.

#### Scenario: Correct a partial and continue
- **WHEN** se ejecuta el recorrido en automático o paso a paso con dos instancias
- **THEN** NEW queda aplicado, el parcial inválido en cuarentena y FILLED esperando en Kafka; una orden de la otra partición puede completar
- **AND** se registra la corrección predefinida propia y se reinicia el consumidor con los controles existentes
- **AND** se verifica la secuencia completa, un settlement por FILLED y dos consumidores restaurados

#### Scenario: Cancel or repeat
- **WHEN** se cancela después de publicar el inválido o se repite un recorrido terminado
- **THEN** se procura resolver sólo la posición propia publicada y se usan IDs nuevos en otra corrida
- **AND** no se borra evidencia ni se modifica trabajo ajeno; una restauración fallida informa la posición y el procedimiento manual sin declarar éxito
