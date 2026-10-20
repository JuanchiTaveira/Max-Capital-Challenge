# demo-scenarios Specification

## Purpose

Ofrecer escenarios del challenge reproducibles desde el navegador, con evidencia
observable, control local de servicios y límites explícitos de lo verificado.

## Requirements

### Requirement: Scenario availability catalog
La consola SHALL listar escenarios ejecutables y pendientes, indicando objetivo,
precondiciones, resultado esperado y dependencia. `Ejecutar disponibles` SHALL
excluir los pendientes e informar su cantidad por separado. No SHALL habilitar ni
aprobar un escenario pendiente por el solo hecho de disponer de controles manuales.

#### Scenario: Current implementation boundary
- **WHEN** se abre el catálogo sobre F-01 a F-03
- **THEN** están disponibles órdenes intercaladas, snapshots con reloj no secuencial, duplicados, persistencia tras reinicio y visibilidad de transacciones productoras
- **AND** settlement único figura pendiente de F-04/F-05
- **AND** caídas en fronteras de commit y rebalanceo durante escritura figuran pendientes de F-05
- **AND** mensajes inválidos, conflictos intencionales, cuarentena y recuperación ordenada figuran pendientes de aislamiento y F-06

#### Scenario: Pending execution request
- **WHEN** se intenta ejecutar un escenario pendiente desde la UI o su API
- **THEN** se rechaza la ejecución con su dependencia y no se modifica el entorno
- **AND** el resumen nunca lo cuenta como aprobado

### Requirement: Observable scenario execution
Cada ejecución SHALL tener identificador, pasos ordenados, estados de avance,
plazo acotado y verificaciones de resultados reales. SHALL ofrecer modo automático
y paso a paso, y permitir recuperar su avance al recargar el navegador mientras
la consola siga ejecutándose. Un timeout SHALL conservar el último diagnóstico y
clasificarse como ejecución fallida, sin inventar una causa.

#### Scenario: Step by step execution
- **WHEN** el evaluador ejecuta un escenario paso a paso y recarga el navegador
- **THEN** puede volver al mismo avance y continuar el próximo paso explícitamente
- **AND** ve mensajes emitidos, posiciones, órdenes y verificaciones asociadas a su ejecución

#### Scenario: Failed assertion or timeout
- **WHEN** una observación contradice el resultado esperado o vence el plazo
- **THEN** la ejecución muestra la condición esperada y la observación o error disponible
- **AND** se detiene el avance de ese escenario sin borrar mensajes, órdenes ni evidencia ya obtenida

### Requirement: Interleaving and snapshot scenarios
El escenario intercalado SHALL usar órdenes nuevas en ambas particiones y observar
las asignaciones de ambas instancias. Los escenarios de secuencia SHALL comprobar
estado, contador, ledger y resultados por posición. El escenario de snapshots
SHALL usar acumulados no sumados y timestamps no crecientes para comprobar que el
reloj del origen no determina la secuencia.

#### Scenario: Interleaved FILLED and CANCELLED
- **WHEN** se ejecuta el escenario intercalado
- **THEN** una orden llega a FILLED con cuatro ER aplicados y otra a CANCELLED con tres
- **AND** ambos ledgers conservan su secuencia de publicación y ambas consultas HTTP muestran el estado esperado
- **AND** las particiones reales publicadas son distintas y se observa inicialmente una asignada a cada instancia

#### Scenario: Accumulated quantities are snapshots
- **WHEN** se publican NEW, parciales de acumulado 3 y 7, y FILLED de total 10, con timestamps deliberadamente no crecientes
- **THEN** cada paso aplicado conserva acumulado 0, 3, 7 y 10 respectivamente y el restante correspondiente
- **AND** contador y ledger terminan en cuatro, ordenados por aplicación y no por timestamp

### Requirement: Business duplicate scenario
El escenario de duplicados SHALL publicar NEW, FILLED y republicaciones idénticas
de FILLED y NEW después del terminal. SHALL comprobar posiciones nuevas y
resultados `DUPLICATE`, conservando contador, ledger y snapshots históricos
originales. Su evidencia SHALL describir republicación y no reentrega de la misma
posición.

#### Scenario: Terminal and late NEW duplicates
- **WHEN** termina el escenario de duplicados
- **THEN** la orden permanece FILLED con contador dos y dos entradas de ledger
- **AND** hay cuatro resultados: dos APPLIED y dos DUPLICATE
- **AND** el resultado original de NEW conserva snapshot NEW y su republicación conserva snapshot FILLED con la misma referencia al ledger de NEW

### Requirement: Restart persistence scenario
El escenario de persistencia SHALL esperar los efectos SQL y offsets del lote,
capturar estado y ledger, reiniciar ambas instancias y republicar sus terminales.
SHALL verificar igualdad de estado y ledger y nuevos resultados DUPLICATE. SHALL
identificar expresamente que el reinicio sucede después del lote y no demuestra
una caída dentro de las fronteras de commit.

#### Scenario: Persisted effects after restart
- **WHEN** ambas instancias vuelven a consumir después del reinicio y procesan las republicaciones
- **THEN** las consultas de ambas conservan estado, cantidades, contador y ledger completos
- **AND** se confirman las nuevas posiciones con resultados DUPLICATE
- **AND** el escenario de caídas durante escritura sigue pendiente de F-05

### Requirement: Transactional input visibility scenario
El escenario de visibilidad SHALL publicar ER en transacciones Kafka confirmadas
y abortadas, comprobar visibilidad `read_committed` y efectos SQL de los confirmados,
y verificar ausencia de efectos del ER abortado después de observar un sucesor
confirmado de su misma partición.

#### Scenario: Aborted partial report
- **WHEN** se confirma un NEW, se aborta un parcial y se confirma un FILLED posterior de la misma orden
- **THEN** sólo NEW y FILLED se observan como entrada confirmada y tienen resultado y ledger
- **AND** el parcial abortado no tiene efecto SQL
- **AND** la evidencia distingue el envío abortado de un error de consumo o de una garantía de salida settlement

### Requirement: Exclusive and repeatable runs
La consola SHALL admitir una sola ejecución de escenario activa y rechazar
mutaciones manuales concurrentes que la interfieran. Cada ejecución SHALL usar
identificadores de órdenes nuevos, conservar datos previos y registrar el alcance
de las verificaciones. Cancelar SHALL detener próximos pasos y conservar efectos
ya confirmados.

#### Scenario: Repeated or concurrent execution
- **WHEN** se repite un escenario terminado
- **THEN** se usan órdenes nuevas y la evidencia anterior sigue consultable en SQL y Kafka
- **WHEN** se solicita otro escenario o una publicación/control manual durante uno activo
- **THEN** se devuelve conflicto sin iniciar esa mutación

#### Scenario: Cancellation after publication
- **WHEN** se cancela después de confirmar publicaciones
- **THEN** no se emiten los pasos siguientes ni se borran o revierten registros confirmados
- **AND** la UI informa la cancelación y permite inspeccionar las posiciones ya emitidas

### Requirement: Project scoped service controls
La consola SHALL permitir iniciar, detener y reiniciar `app-1`, `app-2`, `postgres`
y `kafka`, y terminar abruptamente `app-1` o `app-2`, únicamente en su proyecto
Compose. SHALL resolver también contenedores detenidos, rechazar destinos ambiguos
y mostrar el resultado real del control. No SHALL aceptar comandos, rutas ni
proyectos arbitrarios ni eliminar datos, topics o offsets.

#### Scenario: Abrupt application termination
- **WHEN** el evaluador solicita terminar abruptamente app-1 fuera de una ejecución activa
- **THEN** la consola mantiene su UI disponible, identifica la acción y permite iniciar esa misma instancia nuevamente
- **AND** muestra las observaciones disponibles del grupo sin dar por aprobada recuperación en fronteras de commit

#### Scenario: Stop and start a dependency
- **WHEN** se detiene y luego se inicia PostgreSQL o Kafka desde la UI
- **THEN** la consola muestra la acción y la indisponibilidad y recuperación observadas
- **AND** permite reiniciar las aplicaciones si sus consumidores quedaron detenidos

#### Scenario: Stopped, foreign or ambiguous target
- **WHEN** el servicio autorizado está detenido y existe un único contenedor suyo en el proyecto
- **THEN** iniciar resuelve ese contenedor sin exigir que ya esté ejecutándose
- **WHEN** el destino pertenece a otro proyecto, no está autorizado o su resolución es ambigua
- **THEN** se rechaza el control sin actuar sobre otro contenedor

### Requirement: Local control requests
Las mutaciones de la consola SHALL aceptar sólo solicitudes JSON del origen local
de la consola y acciones predefinidas. La indisponibilidad o falta de permisos del
motor Docker SHALL degradar controles y diagnósticos dependientes sin impedir
consultas HTTP, SQL o Kafka que sigan disponibles.

#### Scenario: Unavailable Docker socket
- **WHEN** no se puede acceder al socket Docker
- **THEN** los controles informan esa limitación y no simulan éxito
- **AND** la consola conserva su navegación y las lecturas independientes disponibles

#### Scenario: Foreign origin or arbitrary command
- **WHEN** una solicitud de mutación usa otro origen, un formato no permitido o una acción fuera del catálogo
- **THEN** se rechaza antes de ejecutar un control Docker o una publicación

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
