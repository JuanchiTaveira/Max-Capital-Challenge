# Spec Delta

## ADDED Requirements

### Requirement: Guided scenario explanation
El catálogo SHALL explicar qué demuestra cada escenario, la secuencia prevista y
el resultado esperado antes de ejecutar. SHALL ofrecer cinco escenarios con nombres
comprensibles, conservando sus identificadores y separando los tres pendientes.

#### Scenario: First individual scenario
- **WHEN** se abre la vista de escenarios sin una ejecución seleccionada
- **THEN** se selecciona Dos órdenes en paralelo y se explica el recorrido completo
- **AND** se permite ejecutar ese escenario o todos los disponibles sin ejecutar al seleccionar

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

#### Scenario: Partial or unavailable observation
- **WHEN** falla, vence el plazo o se cancela una ejecución
- **THEN** se conservan las verificaciones observadas y se distinguen de las no verificadas y escenarios no ejecutados
- **AND** no se inventa un valor observado ni una conclusión de éxito para comprobaciones pendientes
