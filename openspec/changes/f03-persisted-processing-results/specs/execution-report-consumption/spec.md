# Spec Delta

## MODIFIED Requirements

### Requirement: Commit after persistence
El consumidor SHALL confirmar el progreso Kafka sólo después de confirmar la
transacción SQL del ER y su resultado por `(topic, partition, offset)`, o de
recuperar un resultado ya confirmado para ese registro; ante una falla SHALL
conservar el registro pendiente y señalizar el error, sin descartarlo
silenciosamente.

#### Scenario: Database failure
- **WHEN** falla la transacción SQL al procesar un ER, incluida la escritura de su resultado
- **THEN** no se confirma su offset ni se observa una actualización parcial de la orden
- **AND** el ER permanece disponible para reprocesamiento

#### Scenario: Invalid transition
- **WHEN** un ER válido en formato viola las reglas de transición de la orden
- **THEN** no se confirma su offset y el diagnóstico identifica la orden y la causa

#### Scenario: SQL committed and offset confirmation fails
- **GIVEN** un resultado SQL confirmado para un registro Kafka
- **WHEN** falla la confirmación de su offset y el registro se reentrega al reanudar el consumidor
- **THEN** se recupera el mismo resultado sin repetir efectos SQL
- **AND** el offset puede confirmarse después de recuperar el resultado
- **AND** no se aplica el siguiente ER de la partición antes de resolver el pendiente

#### Scenario: Republished terminal duplicate
- **GIVEN** una orden terminal y su ER terminal ya aplicado
- **WHEN** el mismo ER se publica en otra posición Kafka
- **THEN** esa posición conserva un resultado `DUPLICATE` sin cambiar orden ni ledger
- **AND** se confirma su offset sólo después del commit de ese resultado

#### Scenario: Invalid input remains uncommitted
- **WHEN** falla JSON/key o el ER tiene una identidad aplicada con contenido distinto
- **THEN** no se registra un resultado exitoso para esa posición ni se confirma su offset
- **AND** se conserva el diagnóstico y se detiene el consumo de esa instancia sin saltar al siguiente registro
