# Spec Delta

## MODIFIED Requirements

### Requirement: Failed attempts remain pending
Un ER con JSON/key inválido, conflicto de identidad o transición inválida, o cuyo
intento SQL falla, SHALL permanecer pendiente con diagnóstico visible. No SHALL
crearse un resultado exitoso para ese intento ni alterarse un resultado anterior.
La ausencia de resultado SHALL significar ausencia de éxito registrado. Diagnóstico, cuarentena y retención SHALL consultarse por separado, sin inferirlos de esa ausencia.

#### Scenario: Invalid report or conflicting business identity
- **WHEN** falla la validación de entrada, transición o comparación del ER con el ledger
- **THEN** no se crea resultado para esa posición ni se modifican orden o ledger
- **AND** se conserva diagnóstico con topic, partición, offset y key y se aplica la política de cuarentena o detención operativa según la clase de falla

#### Scenario: Successful retry after rollback
- **GIVEN** un intento SQL revertido y sin resultado persistido
- **WHEN** se reintenta el mismo registro después de resolver la causa
- **THEN** el ER puede producir su primer resultado exitoso y una única aplicación

## ADDED Requirements

### Requirement: Recovery result source and delivery identity
Un éxito producido por recuperación SHALL identificarse por la posición Kafka del comando que lo resuelve y referenciar la posición original retenida y revisión efectiva. SHALL conservar APPLIED o DUPLICATE, ledger y snapshot originales. La posición retenida SHALL conservar su evidencia sin convertirse retroactivamente en éxito del ER original inválido.

#### Scenario: Correction produces a new application
- **WHEN** un comando aplica una corrección válida del primer pendiente
- **THEN** su resultado conserva posición del comando, fuente original, revisión y snapshot posterior a esa única aplicación
- **AND** el ledger conserva el ER efectivo y la auditoría permite recuperar los bytes originales

#### Scenario: Same command redelivered after SQL commit
- **WHEN** Kafka reentrega la posición del comando cuyo éxito SQL ya confirmó
- **THEN** se recuperan exactamente su resultado e intención de salida originales, aunque el pendiente ya figure resuelto
- **AND** no se convierte su APPLIED original en DUPLICATE ni se recalcula desde el estado actual

### Requirement: Idempotent recovery command resolution
Cada comando SHALL conservar una resolución durable por posición y una asociación idempotente por solicitud y revisión. Reentregar la misma posición SHALL recuperar su resolución; una copia equivalente en otra posición SHALL ser un no-op sin nueva intención de salida. Conflictos de solicitud SHALL ser visibles sin alterar resoluciones previas.

#### Scenario: Duplicate command at another offset
- **WHEN** una solicitud ya resuelta llega con el mismo contenido en otra posición
- **THEN** esa entrega referencia la resolución original sin aplicar el ER ni emitir otro settlement o rechazo
- **AND** confirmar su offset no cambia el resultado APPLIED de la primera entrega

#### Scenario: Failure during atomic recovery writes
- **WHEN** falla SQL al actualizar orden, ledger, resultado, auditoría, barrera o programación del siguiente pendiente
- **THEN** todas las escrituras de ese intento se revierten juntas
- **AND** reentregar permite una única resolución exitosa sin perder el trabajo restante
