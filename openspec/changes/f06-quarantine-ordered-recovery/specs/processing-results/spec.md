# Spec Delta

## ADDED Requirements

### Requirement: Corrected report result at original position
Una corrección SHALL producir o recuperar APPLIED/DUPLICATE en la posición original, usando el ER efectivo auditado. SHALL confirmar su resolución junto a estado, contador, ledger y resultado. Los resultados exitosos previos SHALL permanecer inmutables y el original inválido SHALL conservarse en cuarentena separado del ER efectivo.

#### Scenario: Corrected report commits
- **WHEN** el consumidor aplica una corrección válida sobre una posición sin éxito previo
- **THEN** el resultado usa topic, partición y offset originales y el ledger conserva el ER efectivo
- **AND** cuarentena y auditoría permiten distinguirlo de los bytes originalmente recibidos

#### Scenario: Replay after SQL commit
- **WHEN** se reentrega una posición corregida que ya confirmó SQL pero no Kafka
- **THEN** se vuelve a seleccionar la corrección aplicada y se recupera exactamente su resultado original
- **AND** no se compara el ledger efectivo con el JSON inválido ni se repite estado, contador o ledger

#### Scenario: Atomic correction resolution fails
- **WHEN** falla SQL antes de completar la resolución de corrección y negocio
- **THEN** todas las escrituras de ese intento se revierten y la posición sigue recuperable
- **AND** no se marca la cuarentena RESOLVED con un resultado de negocio sin confirmar
