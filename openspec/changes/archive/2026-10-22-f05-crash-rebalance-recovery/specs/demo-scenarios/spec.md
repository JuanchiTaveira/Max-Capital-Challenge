# Spec Delta

## MODIFIED Requirements

### Requirement: Scenario availability catalog
La consola SHALL listar escenarios ejecutables y pendientes, indicando objetivo,
precondiciones, resultado esperado y dependencia. `Ejecutar disponibles` SHALL
excluir los pendientes e informar su cantidad por separado. No SHALL habilitar ni
aprobar un escenario pendiente por el solo hecho de disponer de controles manuales.

#### Scenario: Current implementation boundary
- **WHEN** se abre el catálogo después de completar F-05
- **THEN** están disponibles los cinco escenarios anteriores, Settlement único y Caídas entre commits y rebalanceos, conservando sus identificadores
- **AND** mensajes inválidos, conflictos intencionales, cuarentena y recuperación ordenada permanecen pendientes de aislamiento y F-06
- **AND** ejecutar todos selecciona siete escenarios y excluye únicamente el pendiente de F-06

#### Scenario: Pending execution request
- **WHEN** se intenta ejecutar un escenario pendiente desde la UI o su API
- **THEN** se rechaza la ejecución con su dependencia y no se modifica el entorno
- **AND** el resumen nunca lo cuenta como aprobado

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
- **AND** la evidencia remite a Caídas entre commits y rebalanceos para verificar fallas durante procesamiento, sin atribuirlas a este reinicio posterior al lote

### Requirement: Guided scenario explanation
El catálogo SHALL explicar qué demuestra cada escenario, la secuencia prevista y
el resultado esperado antes de ejecutar. SHALL ofrecer siete escenarios ejecutables
con nombres comprensibles, conservando sus identificadores y separando el pendiente
de F-06.

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

## ADDED Requirements

### Requirement: Guided settlement uniqueness
El escenario `settlement-once` SHALL comprobar con dos instancias exactamente un
settlement confirmado por FILLED y cero por CANCELLED, incluyendo republicaciones
y recuperación de un FILLED tras caída. SHALL comparar la salida con el resultado
original y contar sobre una lectura completa del rango observado con
`read_committed` después de resolver los intentos.

#### Scenario: Terminals and business duplicates
- **WHEN** se ejecuta el escenario con órdenes nuevas FILLED y CANCELLED en particiones distintas y se republican sus terminales y el NEW de la orden FILLED
- **THEN** se observa un settlement para FILLED y ninguno para CANCELLED
- **AND** cada republicación tiene resultado DUPLICATE y no agrega contador ni ledger
- **AND** key, identidad, posición fuente y cantidades del settlement coinciden con el APPLIED FILLED original

#### Scenario: Completion redelivered after process death
- **GIVEN** un FILLED nuevo cuyo resultado SQL se observó confirmado y cuyo proceso está suspendido antes de publicar
- **WHEN** la consola termina abruptamente ese proceso y observa la recuperación de su misma posición desde la otra instancia
- **THEN** el resultado SQL original permanece idéntico y existe un único settlement confirmado
- **AND** la consola vuelve a iniciar la instancia caída y verifica dos consumidores activos sin otra publicación

### Requirement: Guided commit boundary recovery
El escenario `commit-boundary-recovery` SHALL reproducir caídas con SQL abierto,
después de SQL, después de un envío aceptado y antes del commit Kafka, reasignación
durante SQL y caída de ambas instancias. Cada caso SHALL identificar su frontera
antes de actuar y verificar efectos, progreso y continuación en secuencia. SHALL
ejecutarse en modos automático y paso a paso sin depender de pausas humanas para
alcanzar la frontera.

#### Scenario: Open SQL transaction killed and recovered
- **WHEN** se alcanza una barrera después de las escrituras SQL sin confirmar y se termina abruptamente su proceso
- **THEN** la evidencia muestra ausencia de cambios parciales confirmados y recuperación de la misma posición con una aplicación
- **AND** el ledger, contador y cantidades coinciden con la secuencia completa, incluidos los sucesores publicados

#### Scenario: Post SQL and acknowledged send process deaths
- **WHEN** se repiten los casos con órdenes nuevas, suspendiendo después del commit SQL y después del envío de settlement aceptado antes del commit Kafka
- **THEN** antes de la caída se observa el resultado SQL y ausencia de settlement confirmado
- **AND** al recuperar se conserva ese resultado y queda exactamente un settlement por orden FILLED después de resolver las transacciones abiertas

#### Scenario: Reassignment while SQL remains open
- **GIVEN** un intento suspendido con SQL abierto y dos miembros del grupo
- **WHEN** se observa la pérdida de pertenencia y la asignación a la otra instancia antes de liberar al propietario anterior
- **THEN** el propietario vigente resuelve la posición y los sucesores sin duplicar efectos
- **AND** se conserva evidencia del rechazo del intento Kafka obsoleto y del conteo final después de terminar ambos intentos

#### Scenario: Both applications killed and returned
- **WHEN** se alcanza SQL confirmado con progreso pendiente, se terminan abruptamente ambas aplicaciones y luego se inician sin resetear datos ni offsets
- **THEN** se recupera el resultado original y se resuelven los sucesores en secuencia
- **AND** se verifica una salida por FILLED, cero por CANCELLED y nuevas publicaciones procesadas en ambas particiones con dos miembros activos

#### Scenario: Manual progression and reload
- **WHEN** se ejecuta paso a paso y se recarga el navegador antes del próximo caso de falla
- **THEN** se conserva el avance y se explica la siguiente acción antes de continuar
- **AND** cada ventana crítica comprende armar, observar, provocar o liberar y resolver dentro del mismo paso acotado, sin mantener una transacción abierta durante la pausa humana

### Requirement: Recovery scenario cleanup
Los escenarios con fallas SHALL liberar o invalidar sus barreras y restaurar
aplicaciones y consumo al terminar, cancelar o fallar. La limpieza SHALL conservar
datos y evidencia; un fallo de restauración SHALL impedir el éxito y explicar qué
acción operativa sigue pendiente. Una nueva ejecución SHALL empezar sin barreras
de una ejecución anterior.

#### Scenario: Cancellation or timeout at a barrier
- **WHEN** se cancela o vence el plazo después de armar o alcanzar una barrera
- **THEN** se detienen próximos pasos de negocio, se invalidan controles pendientes y se intenta restablecer ambas aplicaciones y consumidores
- **AND** se conservan las posiciones publicadas, verificaciones observadas y diagnóstico de la restauración
- **AND** no se eliminan órdenes, mensajes, topics, offsets ni volúmenes

#### Scenario: Restoration or barrier observation fails
- **WHEN** una barrera no se alcanza, una acción Docker queda incierta o no se consigue restaurar el consumo
- **THEN** el escenario no se declara aprobado ni atribuye una frontera que no observó
- **AND** se informa el estado conocido y la acción necesaria para recuperar el entorno
