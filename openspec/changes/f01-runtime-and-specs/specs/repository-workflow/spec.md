## ADDED Requirements

### Requirement: Feature traceability
El repositorio SHALL conservar instrucciones operativas en AGENTS.md, decisiones
en DECISIONS.md y una única tabla de estado F-01 a F-08 en README.md.

#### Scenario: Feature completion
- **WHEN** una feature satisface sus escenarios y verificaciones observadas
- **THEN** sus tareas se completan, el change se archiva y README indica Implementado
- **AND** las demás features conservan su estado

#### Scenario: Required verification unavailable
- **WHEN** una verificación requerida no puede ejecutarse
- **THEN** se registra la causa y la feature permanece Pendiente

### Requirement: Reproducible contributor workflow
El repositorio SHALL documentar arranque, formato, pruebas, persistencia y comandos
de Windows y Linux, preservando commits locales por unidades coherentes.

#### Scenario: Reviewer follows instructions
- **WHEN** el evaluador sigue README y dispone de los prerrequisitos documentados
- **THEN** puede ejecutar y verificar F-01 sin configurar un IDE ni instalar Maven
