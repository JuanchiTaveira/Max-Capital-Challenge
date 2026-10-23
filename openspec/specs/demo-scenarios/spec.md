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
- **WHEN** se abre el catálogo después de completar F-06
- **THEN** están disponibles los siete escenarios anteriores y Mensajes inválidos y recuperación ordenada, conservando sus identificadores
- **AND** poison-recovery demuestra cuarentena, pausa de partición y corrección auditada con órdenes nuevas
- **AND** ejecutar todos selecciona ocho escenarios y cada uno requiere sus verificaciones

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
- **AND** la evidencia remite a Caídas entre commits y rebalanceos para verificar fallas durante procesamiento, sin atribuirlas a este reinicio posterior al lote

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
