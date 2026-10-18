# Spec Delta

## Purpose

Permitir que un cliente consulte por HTTP el estado confirmado de una orden y el
detalle de su ledger en orden de inserción, desde cualquiera de las instancias.

## ADDED Requirements

### Requirement: Order state and ledger query
`GET /orders/{numericOrderId}` SHALL devolver JSON con `numericOrderId`, `status`,
`appliedExecutionCount`, `nominalAmounts`, `accumulativeNominalAmount`,
`leavesNominalAmount` y `ledger`. Cada entrada de `ledger` SHALL incluir una key
`ledgerId` autoincremental y el ER aplicado. Las entradas SHALL ordenarse por
`ledgerId` ascendente y sólo reflejar transacciones SQL confirmadas.

#### Scenario: Existing order
- **GIVEN** una orden con tres ER aplicados e intercalados con ER de otra orden
- **WHEN** se consulta su identificador en cualquiera de las dos instancias
- **THEN** la respuesta 200 contiene el estado y contador confirmados y exactamente sus tres ER en orden de inserción

#### Scenario: Replay not listed twice
- **GIVEN** una orden cuyo ER se recibió dos veces con igual identidad y contenido
- **WHEN** se consulta la orden
- **THEN** `appliedExecutionCount` y `ledger` cuentan ese ER una sola vez

### Requirement: Query errors
La consulta SHALL devolver 404 para un `numericOrderId` positivo sin orden
persistida y 400 para un identificador no numérico o no positivo.

#### Scenario: Unknown order
- **WHEN** se consulta un `numericOrderId` positivo sin orden persistida
- **THEN** la respuesta es 404

#### Scenario: Invalid order identifier
- **WHEN** se consulta `/orders/0` o `/orders/abc`
- **THEN** la respuesta es 400
