# Spec Delta

## ADDED Requirements

### Requirement: Settlement from corrected input
Una corrección APPLIED FILLED SHALL usar el mecanismo existente de settlement y offset en una transacción Kafka, conservando como source la posición original pendiente. Su reentrega SHALL recuperar el mismo resultado SQL. La notificación DLQ SHALL ser independiente y no se le atribuirá la garantía de unicidad del settlement.

#### Scenario: Process dies after corrected completion SQL
- **WHEN** una corrección FILLED confirma SQL y el proceso cae antes de confirmar Kafka
- **THEN** el propietario vigente recupera la misma posición y su resultado sin otra aplicación
- **AND** al resolver el intento existe un único settlement read_committed con source y ledgerId correspondientes

#### Scenario: Cancellation duplicate or unresolved error
- **WHEN** la corrección es APPLIED CANCELLED, DUPLICATE o aún no se pudo aplicar
- **THEN** no genera settlement adicional
- **AND** recibir avisos DLQ repetidos tampoco produce efectos de negocio
