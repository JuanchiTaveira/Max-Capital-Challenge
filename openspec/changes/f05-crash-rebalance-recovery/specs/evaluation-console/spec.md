# Spec Delta

## ADDED Requirements

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
