# Spec Delta

## ADDED Requirements

### Requirement: Order processing completeness
La consulta de una orden existente SHALL agregar `processing` con estado `ACTIVE`, `QUARANTINED` o `RECOVERING`, alcance del bloqueo, cantidad de pendientes, primer pendiente y enlace al diagnóstico. SHALL leerlo junto con estado y ledger desde un snapshot SQL confirmado. Los estados de mercado y el contador SHALL reflejar sólo ER efectivamente aplicados.

#### Scenario: Nonterminal order has a rejected partial
- **WHEN** se consulta una orden NEW cuyo parcial está en cuarentena y su FILLED retenido
- **THEN** conserva status NEW, su contador y ledger aplicados, con processing QUARANTINED y dos pendientes
- **AND** ambas instancias exponen el mismo diagnóstico confirmado sin simular CANCELLED o FILLED

#### Scenario: Recovery in progress and complete
- **WHEN** una corrección aceptada empieza a recuperar pendientes
- **THEN** processing muestra RECOVERING mientras quede trabajo y ACTIVE al agotarlo sin otro bloqueo
- **AND** aceptar la solicitud por HTTP no equivale por sí solo a aplicar la corrección

#### Scenario: Partition barrier affects an existing order
- **WHEN** existe una barrera de partición que impide aplicar nuevos ER de una orden consultada
- **THEN** el diagnóstico identifica el alcance de partición sin atribuirle una identidad de error no comprobada
- **AND** la consulta no declara ACTIVE omitiendo esa barrera

### Requirement: Quarantine lookup without applied order
El sistema SHALL permitir consultar cuarentena por posición y por identidad confiable aunque todavía no haya orden aplicada. Esa consulta SHALL distinguir orden inexistente de ER fallido o retenido y preservar los errores 404/400 del endpoint de órdenes existente.

#### Scenario: Invalid initial NEW
- **WHEN** un NEW inválido queda en cuarentena antes de crear una orden aplicada
- **THEN** GET de la orden sigue devolviendo 404 y la consulta de cuarentena expone su original, causa y bloqueo
- **AND** no se crea una orden con contador cero para representar el error
