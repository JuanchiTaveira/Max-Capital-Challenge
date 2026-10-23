# Spec Delta

## MODIFIED Requirements

### Requirement: Evaluation navigation
La consola SHALL abrir en Escenarios y ofrecer Mensajes, Órdenes y Sistema bajo
Herramientas avanzadas desde el mismo origen local, conservando sus funciones y
enlaces directos. SHALL mostrar carga, vacío, error y última observación, con
navegación por teclado y controles con etiquetas visibles.

#### Scenario: Empty environment
- **WHEN** el evaluador abre la consola sin órdenes ni mensajes de demo
- **THEN** ve instrucciones para ejecutar un escenario o publicar un ER
- **AND** los estados vacíos no se presentan como fallas

#### Scenario: Temporary observation failure
- **WHEN** falla una actualización de Kafka, SQL o una instancia
- **THEN** la vista identifica qué consulta falló y permite reintentar
- **AND** los datos anteriores conservados quedan identificados como desactualizados

#### Scenario: Advanced tools remain available
- **WHEN** el evaluador abre Herramientas avanzadas o un enlace directo existente a Mensajes, Órdenes o Sistema
- **THEN** puede inspeccionar y paginar registros, consultar órdenes, publicar o republicar ER y usar los controles locales existentes
- **AND** volver al escenario conserva la ejecución y su selección sin iniciar acciones
- **AND** las mutaciones siguen bloqueadas durante una ejecución activa

### Requirement: Guided evaluation workspace
La vista Escenarios SHALL integrar explicación, ejecución, mensajes, órdenes,
ledger, salidas y comparación Verificación/Esperado/Observado/Resultado en una
misma ficha. SHALL permitir inspeccionar la evidencia sin cambiar de vista,
conservar pasos, controles, selección por URL y acceso a herramientas avanzadas.
Seleccionar un escenario SHALL sólo abrir su ficha; Ejecutar SHALL iniciar la prueba.

#### Scenario: Individual guided progression
- **WHEN** se ejecuta un escenario y termina
- **THEN** se ve una conclusión legible y puede seleccionarse el siguiente sin iniciarlo
- **AND** en modo manual se explica el próximo paso antes de continuar

#### Scenario: Batch navigation and reload
- **WHEN** se ejecutan todos o se recarga una ejecución en curso
- **THEN** se conserva el avance y pueden consultarse resultados por escenario sin mezclar pasos
- **AND** no se interpreta el título de un paso ni su error para deducir qué verificó

#### Scenario: Accessible narrow viewport
- **WHEN** se usa teclado o una ventana de 375 px
- **THEN** selector, controles y resultados siguen accesibles, con foco visible y estados textuales
- **AND** la selección no pierde el foco por el refresco y el contenido técnico se presenta como texto inerte
- **AND** las secciones se apilan en la misma ficha sin exigir otra vista para inspeccionarlas

#### Scenario: Explicit execution
- **WHEN** se selecciona Dos órdenes en paralelo sin pulsar Ejecutar
- **THEN** se muestran su objetivo breve, acciones y resultado esperado, sin publicar ER ni controlar servicios
- **WHEN** se pulsa Ejecutar en el modo elegido
- **THEN** la ficha muestra avance y evidencia de esa ejecución, con cancelación y continuación cuando corresponda

#### Scenario: Interleaved evidence in one workspace
- **WHEN** termina Dos órdenes en paralelo
- **THEN** en la misma ficha se ven los siete envíos, sus posiciones y resultados, y las órdenes A y B con sus ledgers ordenados
- **AND** A muestra FILLED con cuatro ER aplicados y cuatro entradas, y B CANCELLED con tres ER y tres entradas
- **AND** comprobar un mensaje, su orden y su fila de ledger no exige abrir Herramientas avanzadas ni leer JSON

## ADDED Requirements

### Requirement: Inline message effect inspection
La ficha SHALL correlacionar cada envío con su resultado por topic, partición y
offset, y con su orden y ledger. SHALL distinguir estado informado por el ER,
snapshot SQL del resultado y estado actual. Seleccionar mensaje, orden o ledger
SHALL mantener el contexto y ofrecer el payload completo como detalle desplegable.

#### Scenario: Applied message selection
- **WHEN** se selecciona un mensaje con resultado APPLIED
- **THEN** se identifican su orden y fila de ledger dentro de la ficha, con estado informado, posición y resultado
- **AND** el detalle permite abrir el ER completo y el snapshot asociado sin navegar a otra pantalla

#### Scenario: Duplicate points to an existing entry
- **WHEN** se selecciona un NEW republicado después de FILLED con resultado DUPLICATE
- **THEN** se muestra NEW como estado del mensaje y FILLED como snapshot de esa republicación
- **AND** la referencia señala la fila original de NEW sin agregar ni representar otra entrada de ledger
- **AND** el resultado de la posición original de NEW conserva su propio snapshot NEW

#### Scenario: No result is not an inferred failure
- **WHEN** un envío todavía no tiene resultado SQL observado
- **THEN** se indica Sin resultado registrado o Sin observación disponible según la lectura, sin inventar aplicación, rechazo ni referencia de ledger
- **AND** la confirmación de envío al broker se presenta separada de aplicación SQL y verificación del escenario

#### Scenario: Aborted input is producer evidence
- **WHEN** el escenario registra un envío dentro de una transacción abortada
- **THEN** la ficha lo identifica como envío abortado del productor, sin mostrarlo como registro visible read_committed ni ER aplicado
- **AND** la ausencia verificada de resultado y ledger se explica como comportamiento esperado, no como fallo

### Requirement: Understandable quantity presentation
La consola SHALL mostrar cantidades como Cantidad solicitada, Cantidad ejecutada
y Pendiente de ejecutar, conservando los nombres técnicos en el detalle. SHALL
distinguir ER aplicados y filas de ledger de unidades ejecutadas, preservar
exactitud numérica y explicar el remanente sin ejecutar de una cancelación.

#### Scenario: Cancellation after partial execution
- **WHEN** una orden CANCELLED conserva nominal 10, acumulado 7 y restante 0
- **THEN** se muestran solicitada 10, ejecutada 7 y pendiente de ejecutar 0
- **AND** se explica que tres unidades quedaron sin ejecutar al cancelarse, sin afirmar que se ejecutaron diez
- **AND** el contador de tres ER se presenta por separado de las siete unidades ejecutadas

#### Scenario: Exact values with readable formatting
- **WHEN** se muestran IDs mayores que 9007199254740991 y cantidades con ocho decimales
- **THEN** tarjetas, tablas, selección y detalle conservan los valores exactos
- **AND** eliminar ceros fraccionarios innecesarios para presentación no modifica el payload, el contrato ni el valor numérico

### Requirement: Scenario evidence provenance
La ficha SHALL mostrar sólo evidencia de la ejecución y escenario seleccionados,
identificando paso, fuente y fecha de observación. SHALL separar snapshots
capturados de consultas actuales y mostrar salidas o incidentes pertinentes con
su correlación. Una lectura incompleta SHALL identificarse sin concluir ausencia
o unicidad por la mera falta de registros visibles.

#### Scenario: Output and recovery details
- **WHEN** se inspecciona settlement, una caída o cuarentena
- **THEN** se presentan en la ficha la salida y su fuente, la frontera y recuperación observadas o el original y su corrección, según corresponda
- **AND** los avisos DLQ no se presentan como settlements ni como aplicación de negocio
- **AND** una misma posición corregida conserva diferenciados el original, el ER efectivo y el ledger aplicado

#### Scenario: Captured and current order state
- **WHEN** se consulta el estado actual de una orden después de una observación capturada
- **THEN** ambos se identifican por su fuente y fecha y la lectura actual no reemplaza la evidencia anterior
- **AND** en persistencia se puede comparar antes y después, y en snapshots se conservan los acumulados observados por paso

#### Scenario: Scenario switch during refresh
- **WHEN** se cambia de escenario o ejecución mientras hay lecturas en curso
- **THEN** las respuestas anteriores no reemplazan el contenido de la nueva selección
- **AND** refrescar conserva el detalle seleccionado y los desplegables compatibles con esa selección

#### Scenario: Unavailable evidence
- **WHEN** una consulta falla, se cancela la prueba o una salida sigue sin resolverse
- **THEN** se mantienen los datos capturados disponibles con sus fechas y se identifica lo pendiente, fallido o desactualizado
- **AND** una ficha vacía o parcial no convierte comprobaciones pendientes en aprobadas
