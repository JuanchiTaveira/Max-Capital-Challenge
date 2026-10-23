# Spec Delta

## Purpose

Conservar un ER inválido y permitir su corrección auditada, manteniendo pendiente su posición Kafka para recuperar la secuencia con un mecanismo pequeño y explícito.

## ADDED Requirements

### Requirement: Failure classification and bounded retry
El sistema SHALL distinguir errores permanentes del ER, rollback transitorio conocido y fallas operativas o inciertas. Sólo el rollback transitorio conocido SHALL reintentarse automáticamente, hasta cinco intentos por sesión con backoff exponencial, jitter y espera máxima de 5 s. Las esperas SHALL ocurrir fuera de transacciones.

#### Scenario: Transient rollback recovers
- **WHEN** un intento revierte por deadlock o serialización y su causa desaparece dentro del límite
- **THEN** se reintenta la misma posición y se aplica una sola vez sin DLQ ni sucesores adelantados
- **AND** las esperas parten de 500 ms, duplican su base y agregan entre 0 y 200 ms de jitter sin superar 5 s

#### Scenario: Attempts are exhausted or the process restarts
- **WHEN** se agotan los intentos de la sesión
- **THEN** se pausa la partición con diagnóstico y el offset permanece pendiente
- **AND** resolver la causa y reiniciar habilita otro ciclo; no se promete conservar contador ni espera entre procesos o rebalanceos

#### Scenario: Infrastructure or uncertain commit
- **WHEN** SQL no está disponible, la excepción es desconocida o Kafka deja un commit incierto
- **THEN** no se clasifica el ER como inválido ni se lo descarta o confirma
- **AND** se conserva la detención operativa de F-05 para incertidumbre y no se reenvía desde progreso local incierto

### Requirement: Quarantine without skipping input
Un permanente SHALL conservarse una sola vez en SQL por posición, con key y valor originales, causa y fecha. SHALL pausarse su partición sin confirmar esa posición ni aplicar sucesores. La cuarentena SHALL ser consultable sin crear una orden o resultado exitoso ficticio.

#### Scenario: Invalid partial followed by completion
- **WHEN** un parcial inválido sigue a NEW y precede a FILLED
- **THEN** quedan visibles el original y su causa, la orden conserva NEW y el FILLED espera en Kafka sin ledger
- **AND** tampoco avanzan otras órdenes de esa partición; otras particiones pueden continuar

#### Scenario: Partition is reassigned
- **WHEN** otro consumidor recibe la partición antes de registrar una corrección
- **THEN** encuentra la misma cuarentena y pausa esa partición sin volver a aplicar el original
- **AND** no confirma el offset para liberar artificialmente el bloqueo

### Requirement: At least once dead letter notification
Después de conservar la cuarentena, el sistema SHALL publicar un aviso en `execution-reports-rejected` con schemaVersion, rejectionId estable, source y causa. El aviso SHALL omitir el payload completo, disponible en SQL. No SHALL confirmar el offset inválido. La publicación SHALL admitir duplicados identificables ante fallas entre Kafka y el marcador SQL de notificación.

#### Scenario: Notification fails or its confirmation is lost
- **WHEN** falla publicar o no se sabe si confirmó la notificación
- **THEN** el original y su offset siguen pendientes y se intenta completar el aviso al reanudar
- **AND** puede repetirse rejectionId sin que eso represente otra aplicación o settlement

#### Scenario: Inspect the original
- **WHEN** se consulta la posición indicada por la DLQ
- **THEN** se obtiene el original SQL aunque no sea JSON válido o no exista una orden aplicada
- **AND** no se necesitan modalidades de evidencia ni hashes en el contrato DLQ

### Requirement: Audited correction acceptance
La API local SHALL aceptar requestId, source, key y ER efectivos, motivo y responsable declarado. SHALL conservar el original y cada corrección, permitir una sola ACCEPTED por posición y rechazar cambios de identidad confiable, routing incompatible o éxitos previos. Aceptar SHALL guardar metadata sin aplicar negocio ni reanudar consumo.

#### Scenario: Repeat or conflict
- **WHEN** se repite requestId con el mismo contenido
- **THEN** se devuelve la misma corrección sin agregar auditoría ni efectos
- **AND** reutilizarlo con otro contenido o intentar otra corrección mientras existe una ACCEPTED devuelve conflicto

#### Scenario: Unknown identity or applied position
- **WHEN** no hay key confiable o se intenta corregir una posición ya exitosa
- **THEN** una identidad desconocida requiere key explícita compatible con la partición original y una posición exitosa se rechaza
- **AND** una key original canónica conserva su numericOrderId; no se mueve trabajo entre particiones

### Requirement: Ordered recovery from the original position
Después de registrar una corrección y reiniciar el consumidor, éste SHALL usar el ER efectivo en la misma posición original. La resolución de cuarentena/corrección SHALL compartir el commit SQL de negocio. Los sucesores SHALL continuar por el consumo ordinario sólo después de resolver ese offset.

#### Scenario: Correction completes the sequence
- **WHEN** se corrige el parcial pendiente y se reinicia el consumidor
- **THEN** se aplica el parcial efectivo antes del FILLED y el ledger conserva esa secuencia
- **AND** no se usa un comando de recuperación, una republicación al final del topic ni una cola SQL de sucesores

#### Scenario: Correction fails or SQL rolls back
- **WHEN** la corrección viola reglas de negocio o falla su transacción SQL
- **THEN** no quedan efectos parciales y el offset sigue pendiente
- **AND** una corrección permanentemente inválida queda REJECTED y habilita otra, conservando todo el historial

#### Scenario: Console or application restarts
- **WHEN** se reinicia la consola o ambas aplicaciones con una corrección aceptada
- **THEN** SQL conserva esa corrección y Kafka conserva el offset para reentregarlo al propietario vigente
- **AND** no se depende de un worker, outbox o historial del runner para aplicar negocio
