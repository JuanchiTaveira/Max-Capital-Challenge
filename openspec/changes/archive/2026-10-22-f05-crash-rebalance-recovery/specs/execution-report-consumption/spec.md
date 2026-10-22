# Spec Delta

## ADDED Requirements

### Requirement: Recovery from interrupted SQL writes
Ante la terminación abrupta de un proceso con escrituras SQL sin confirmar, el
sistema SHALL recuperar la misma posición Kafka sin conservar efectos parciales.
El ER SHALL aplicarse una sola vez y sus sucesores SHALL conservar la secuencia
de la partición al continuar desde el propietario vigente.

#### Scenario: Process killed with an open SQL transaction
- **GIVEN** dos instancias activas y un ER suspendido después de sus escrituras SQL pero antes del commit, seguido por otros ER válidos en la misma partición
- **WHEN** se termina abruptamente el proceso suspendido y la otra instancia recibe su partición
- **THEN** las escrituras no confirmadas desaparecen y el offset del ER permanece pendiente hasta su recuperación
- **AND** el ER tiene una sola aplicación, una entrada de ledger y un resultado APPLIED al recuperarse
- **AND** el ledger, contador y cantidades finales reflejan exactamente la secuencia de ER publicados

#### Scenario: Initial order creation interrupted
- **GIVEN** un NEW que inicializó la orden y escribió sus efectos dentro de SQL sin confirmar
- **WHEN** el proceso termina abruptamente antes del commit y otro propietario recibe la posición
- **THEN** no queda una orden parcialmente creada y el NEW se aplica una sola vez antes de los ER posteriores

### Requirement: Partition reassignment during SQL processing
Si cambia el propietario durante una transacción SQL, el sistema SHALL resolver
el mismo ER sin duplicar efectos y preservar los resultados originales ya
confirmados. El propietario anterior SHALL ser incapaz de confirmar progreso
Kafka con pertenencia obsoleta; el vigente SHALL resolver el pendiente antes de
aplicar sus sucesores.

#### Scenario: Previous owner commits SQL after losing membership
- **GIVEN** un propietario suspendido con escrituras SQL abiertas y un segundo consumidor activo
- **WHEN** se observa la reasignación real de su partición, se libera el intento anterior y éste consigue confirmar SQL
- **THEN** el propietario vigente recupera el resultado original sin repetir estado, contador ni ledger
- **AND** el intento anterior no confirma progreso Kafka válido después de perder pertenencia
- **AND** tras finalizar ambos intentos, los sucesores conservan su secuencia y existe una sola resolución durable por posición

#### Scenario: Previous owner rolls back after losing membership
- **GIVEN** una transacción SQL abierta cuyo consumidor perdió la partición
- **WHEN** ese intento revierte y el propietario vigente reintenta la posición
- **THEN** se conserva una única aplicación exitosa y el flujo continúa sin saltar el ER revertido

### Requirement: Recovery after both application processes stop
Tras terminar abruptamente ambas instancias, el sistema SHALL reanudar desde
datos SQL y offsets estables del mismo grupo, conservando pendientes y resultados
confirmados. Al recuperar las dos instancias SHALL volver a repartir las dos
particiones y procesar nuevos ER sin pérdida ni doble aplicación.

#### Scenario: Both processes killed with pending work
- **GIVEN** ambas instancias activas, un ER con SQL confirmado y Kafka pendiente, y sucesores publicados sin resolver en su partición
- **WHEN** se terminan abruptamente ambas aplicaciones y se vuelven a iniciar conservando PostgreSQL, Kafka y el grupo
- **THEN** se recupera el mismo resultado SQL para el pendiente y se resuelven sus sucesores en secuencia
- **AND** las posiciones ya confirmadas no generan nuevos efectos
- **AND** se observan dos miembros con una partición cada uno y se procesan nuevos ER en ambas particiones

#### Scenario: Failed consumption session requires operational restart
- **WHEN** una falla detiene una sesión consumidora y se resuelve su causa
- **THEN** reiniciar la aplicación permite recuperar pendientes desde progreso estable
- **AND** el estado HTTP saludable por sí solo no se presenta como evidencia de consumo reanudado
