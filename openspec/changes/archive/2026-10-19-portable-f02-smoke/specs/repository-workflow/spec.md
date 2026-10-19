## ADDED Requirements

### Requirement: Portable order processing smoke
El repositorio SHALL permitir ejecutar el smoke F-02 con Docker y Compose como únicos
requisitos locales en Windows, Linux y macOS con contenedores Linux. La verificación
SHALL conservar datos existentes y terminar con código distinto de cero ante error.

#### Scenario: Reviewer runs the portable command
- **WHEN** el evaluador ejecuta el comando Compose documentado con Docker disponible
- **THEN** se publican dos órdenes intercaladas en distintas particiones y ER duplicados
- **AND** se verifican estado, contador y ledger desde ambas instancias
- **AND** el reinicio de ambas aplicaciones y replay terminal no alteran los resultados
- **AND** se muestran los IDs y URLs de consulta y se conservan los volúmenes

#### Scenario: Windows PowerShell compatibility
- **WHEN** el evaluador usa el lanzador `.ps1` con Windows PowerShell 5.1
- **THEN** se ejecuta la misma verificación sin necesitar `pwsh` instalado
- **AND** el lanzador propaga el código de salida del comando Compose

#### Scenario: Verification failure
- **WHEN** Docker no está disponible o una condición de verificación no se satisface
- **THEN** el comando termina con código distinto de cero y un diagnóstico explícito
- **AND** no se informa un resultado exitoso ni se eliminan volúmenes
