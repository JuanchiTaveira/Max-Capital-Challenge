## Purpose

Conservar un formato Java determinista en producción y pruebas, con corrección
explícita y validación obligatoria antes de las etapas posteriores del build.

## ADDED Requirements

### Requirement: Deterministic Java formatting
Spotless SHALL formatear fuentes Java de producción y pruebas con Google Style,
imports ordenados, UTF-8, LF y salto final, excluyendo fuentes generadas.

#### Scenario: Apply formatting
- **WHEN** se ejecuta spotless:apply sobre fuentes con desvíos
- **THEN** quedan conformes y una segunda aplicación no modifica archivos

### Requirement: Mandatory format validation
Maven SHALL ejecutar spotless:check en validate sin modificar fuentes.

#### Scenario: Nonconforming source
- **WHEN** una fuente Java tiene un desvío de formato
- **THEN** spotless:check y validate fallan antes de compilar o ejecutar integración

#### Scenario: Container build
- **WHEN** se construye la imagen de la aplicación
- **THEN** el build conserva el control obligatorio de formato
