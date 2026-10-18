# Proposal

## Why

F-01 deja dos instancias conectadas a Kafka y PostgreSQL, pero todavía no consume
ExecutionReports ni permite consultar órdenes. F-02 incorpora el primer flujo de
negocio verificable: procesar reportes en secuencia por orden y consultar el estado
y su historial desde cualquiera de las dos instancias.

## What Changes

- Consumir `execution-reports` con `numericOrderId` como clave Kafka y sin
  procesamiento paralelo dentro de una partición.
- Codificar los campos relevantes del ER del enunciado, sus transiciones de estado
  y el tratamiento explícito de mensajes inválidos o fuera de secuencia.
- Persistir atómicamente la orden, su contador de reportes aplicados y un ledger
  ordenado; reconocer un ER repetido antes de evaluar si la orden ya es terminal.
- Exponer por un endpoint HTTP el estado de una orden junto con su ledger desde
  la base compartida.
- Verificar el flujo con Kafka y PostgreSQL reales, incluida la ejecución con dos
  instancias y la visibilidad de los datos en ambas.

## Capabilities

### New Capabilities

- `execution-report-consumption`: contrato de entrada, secuencia por orden y
  conservación visible de mensajes que no pueden aplicarse.
- `order-state`: transiciones, deduplicación básica por identidad de ER y
  persistencia atómica del estado, contador y ledger.
- `order-query`: consulta HTTP conjunta del estado de la orden y su ledger ordenado.

### Modified Capabilities

Ninguna. El entorno y el workflow existentes ya contemplan la incorporación
gradual de features de negocio.

## Impact

Agrega dominio y casos de uso, adaptadores Kafka/JDBC/HTTP, migraciones Flyway y
pruebas de integración. No introduce settlement ni resultados persistidos por
offset: esas garantías corresponden a F-04 y F-03/F-05, respectivamente. F-06
incorporará la cuarentena y recuperación operativa de entradas inválidas.
