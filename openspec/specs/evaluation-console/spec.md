# evaluation-console Specification

## Purpose

Permitir que el evaluador observe el flujo real del challenge y publique ER desde
una consola local, correlacionando mensajes, efectos confirmados y estado operativo.

## Requirements

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

### Requirement: Guided evaluation workspace
La vista Escenarios SHALL ofrecer selector y explicación, pasos agrupados por
escenario y comparación Verificación/Esperado/Observado/Resultado sin abrir JSON.
SHALL mantener otras vistas, recuperación por URL y controles de ejecución;
la evidencia técnica SHALL quedar desplegable con enlaces a registros y órdenes.

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

### Requirement: Opt in recovery instrumentation
La instrumentación de recuperación SHALL estar deshabilitada por defecto y
activarse explícitamente en el entorno de demo. Sus controles SHALL aceptar sólo
operaciones predefinidas sobre el ER nuevo de una ejecución, sin comandos libres
ni escrituras de negocio. La consola SHALL rechazar interferencias manuales
mientras exista una ejecución activa, sin agregar consumidores al grupo de negocio.

#### Scenario: Instrumentation disabled
- **WHEN** se inicia una aplicación sin habilitar instrumentación de recuperación
- **THEN** los controles de barrera no están disponibles y no pueden suspender el procesamiento
- **AND** el procesamiento ordinario conserva sus transacciones y contratos existentes

#### Scenario: Scoped barrier control
- **WHEN** la consola arma una barrera para una ejecución, instancia, identidad de ER y frontera permitidas
- **THEN** el control afecta sólo a ese ER y se consume una sola vez
- **AND** otro run, una identidad no prevista o una operación fuera del catálogo no puede liberar, reemplazar ni reutilizar esa barrera
- **AND** la instrumentación y las lecturas de evidencia no agregan miembros al grupo consumidor

### Requirement: Observable recovery barriers
Una barrera SHALL identificar ejecución, instancia e intento, identidad del ER,
frontera, posición Kafka alcanzada y estado observado. SHALL tener vencimiento
acotado e invalidarse al reiniciar el proceso. La consola SHALL distinguir una
barrera armada de una alcanzada y no atribuir la falla a una frontera sin observar
su alcance antes de actuar.

#### Scenario: Arm before publishing and inspect after arrival
- **WHEN** se arma una barrera para el próximo ER del escenario y se publica ese ER
- **THEN** el estado inicial es armado y sólo al suspenderse el intento se informa la posición real y la frontera alcanzada
- **AND** la evidencia diferencia escrituras SQL no confirmadas, SQL confirmado y envío Kafka aceptado sin commit

#### Scenario: Expiry or process restart
- **WHEN** vence la barrera, se cancela su ejecución o se reinicia su proceso
- **THEN** deja de poder suspender nuevos intentos y una orden de liberación vieja no controla otro intento
- **AND** el vencimiento no declara éxito, no descarta el ER ni permite continuar desde progreso Kafka incierto
- **AND** el control informa la expiración o la ausencia del intento anterior

### Requirement: Recovery evidence across unavailable instances
Las verificaciones de recuperación SHALL registrar progreso estable, resultado SQL
histórico, salida confirmada y cambios observados del grupo sin inferir autoría
histórica desde asignaciones actuales. Durante una caída SHALL consultar fuentes
disponibles y reservar la comparación HTTP entre ambas instancias para después de
su recuperación. Una lectura parcial SHALL impedir concluir unicidad final.

#### Scenario: Original instance unavailable during takeover
- **WHEN** una aplicación termina abruptamente y la otra recupera la partición
- **THEN** la consola conserva su avance, posición pendiente y observaciones SQL/Kafka y del grupo disponibles
- **AND** no exige una respuesta HTTP del proceso terminado para verificar la recuperación en curso
- **AND** tras restaurarlo compara las consultas de ambas instancias y observa dos consumidores activos

#### Scenario: Open output transactions and complete counting
- **WHEN** se inspeccionan settlements durante y después de una falla
- **THEN** se distinguen el rango estable leído y las transacciones todavía pendientes de resolución
- **AND** el conteo final recorre ambas particiones hasta el límite estable posterior a la finalización de los intentos del ensayo
- **AND** una página vacía o un límite anterior a la resolución no se presenta como prueba de ausencia de duplicados
