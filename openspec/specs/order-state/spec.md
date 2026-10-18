# order-state Specification

## Purpose

Mantener el estado vigente de cada orden junto con un historial de los ER
efectivamente aplicados y un contador consistente con ese historial.

## Requirements

### Requirement: Order lifecycle
La primera transición de una orden SHALL ser `NEW`. Desde `NEW` o
`PARTIALLY_FILLED` SHALL admitirse `PARTIALLY_FILLED`, `FILLED` o `CANCELLED`;
`FILLED` y `CANCELLED` SHALL ser terminales.

#### Scenario: Complete fill
- **GIVEN** una orden en `NEW` o `PARTIALLY_FILLED`
- **WHEN** llega un ER `FILLED` válido
- **THEN** la orden queda en `FILLED`

#### Scenario: Cancellation
- **GIVEN** una orden en `NEW` o `PARTIALLY_FILLED`
- **WHEN** llega un ER `CANCELLED` válido
- **THEN** la orden queda en `CANCELLED`

#### Scenario: New distinct report after terminal state
- **GIVEN** una orden en `FILLED` o `CANCELLED`
- **WHEN** llega un ER nuevo con otro `fixId`
- **THEN** el estado, contador y ledger permanecen intactos y el ER queda pendiente con error visible

### Requirement: Quantity snapshots
La orden SHALL conservar `nominalAmounts` del primer ER y reemplazar
`accumulativeNominalAmount` y `leavesNominalAmount` con los valores del ER
aplicado, sin sumarlos al estado anterior.

#### Scenario: Successive partial fills
- **GIVEN** una orden con `nominalAmounts` 10 y `accumulativeNominalAmount` 3
- **WHEN** llega un ER `PARTIALLY_FILLED` con `accumulativeNominalAmount` 7 y `leavesNominalAmount` 3
- **THEN** la orden expone acumulado 7 y restante 3, sin sumar el acumulado anterior

### Requirement: Quantity-state consistency
Para `NEW`, el acumulado SHALL ser cero y el restante igual al total. Para
`PARTIALLY_FILLED`, el acumulado SHALL aumentar sin alcanzar el total y el
restante SHALL ser total menos acumulado. Para `FILLED`, acumulado SHALL igualar
total y restante SHALL ser cero. Para `CANCELLED`, acumulado SHALL no decrecer
ni superar el total, y restante SHALL ser cero.

#### Scenario: Terminal snapshots
- **WHEN** se aplican ER `FILLED` o `CANCELLED` válidos
- **THEN** el estado conserva el último acumulado informado y expone restante cero

#### Scenario: Inconsistent quantity
- **WHEN** un ER reduce el acumulado, cambia `nominalAmounts`, declara `FILLED` sin completar el total o informa un restante incompatible con el estado
- **THEN** no modifica la orden y queda pendiente con error visible

### Requirement: Individual report identity
En el único flujo de entrada de F-02, la identidad de un ER SHALL ser
`(numericOrderId, fixId)`. Una identidad ya aplicada con el mismo contenido
SHALL ser un no-op antes de evaluar el estado terminal. La misma identidad con
contenido diferente SHALL producir un error visible sin modificar la orden.

#### Scenario: Replay after terminal state
- **GIVEN** un ER `FILLED` ya aplicado
- **WHEN** llega nuevamente el mismo ER con igual `numericOrderId`, `fixId` y contenido
- **THEN** no cambia el estado, contador ni ledger y puede confirmarse su offset

#### Scenario: Conflicting identity
- **GIVEN** un ER ya aplicado
- **WHEN** llega otro con igual identidad y diferente contenido
- **THEN** no modifica la orden y queda pendiente con error visible

### Requirement: Atomic state and ledger
Por cada ER nuevo aplicado, el sistema SHALL confirmar juntos el estado vigente,
un incremento unitario de `appliedExecutionCount` y una fila de ledger con key
autoincremental. El contador SHALL incluir `NEW` y los ER terminales aplicados;
un duplicado no lo incrementa ni agrega una fila.

#### Scenario: Successful sequence
- **WHEN** se aplican `NEW`, `PARTIALLY_FILLED` y `FILLED` a una orden
- **THEN** el contador es 3 y el ledger contiene exactamente esas tres entradas en orden de aplicación

#### Scenario: Persistence error
- **WHEN** falla cualquier escritura de una transición
- **THEN** no se observa un cambio parcial de estado, contador o ledger
