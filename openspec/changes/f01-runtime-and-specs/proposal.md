# Proposal

## Why

El challenge necesita una base reproducible para probar las garantías con dos
instancias. La infraestructura, el formato y las instrucciones del agente deben
quedar verificables antes de incorporar lógica de órdenes.

## What Changes

- Inicializar Git, Maven Wrapper, Java 25 y Spring Boot 4.1.1.
- Ejecutar PostgreSQL, Kafka KRaft y dos instancias mediante Compose.
- Exponer salud e identidad; verificar infraestructura real con Testcontainers.
- Incorporar Spotless en validate y documentación operativa en español.
- Definir el flujo OpenSpec y el estado de las ocho features.

## Capabilities

### New Capabilities

- `runtime-environment`: construcción, arranque, salud y persistencia del entorno.
- `repository-workflow`: instrucciones, trazabilidad y cierre de features.
- `code-formatting`: formato Java determinista y control obligatorio en el build.

### Modified Capabilities

Ninguna.

## Impact

Nuevo proyecto backend y configuración Docker. APIs técnicas de Actuator únicamente.
La ingesta de ER, dominio, ledger y settlement se implementarán desde F-02.
