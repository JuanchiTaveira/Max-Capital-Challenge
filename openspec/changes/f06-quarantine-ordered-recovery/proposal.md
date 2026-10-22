# Proposal

## Why

Hoy un ER inválido detiene el consumidor y bloquea indefinidamente su partición. F-06 debe distinguir fallas transitorias de permanentes, conservar evidencia y permitir corregir el ER para recuperar la secuencia de su orden sin perder ni duplicar efectos.

## What Changes

- Clasificar errores explícitamente y reintentar fallas transitorias conocidas sobre la misma posición, con presupuesto durable, backoff exponencial y jitter. Fallas de infraestructura agotadas y commits Kafka inciertos permanecen pendientes con diagnóstico operativo.
- Publicar rechazos permanentes en `execution-reports-rejected` junto al progreso de entrada en una transacción Kafka, después de persistir original, diagnóstico y bloqueo. No convertir cuarentena en éxito de negocio.
- Retener durablemente sucesores de la orden afectada, permitiendo continuar otras órdenes identificables. Ante identidad ambigua, retener la partición hasta resolverla de forma explícita.
- Incorporar correcciones auditadas e idempotentes mediante comandos Kafka, conservando originales, revisiones y motivos. Recuperar un pendiente por comando, en secuencia original y bajo el propietario de la partición; conservar settlement único también durante recuperación.
- Exponer diagnóstico de incompletitud separado del estado de mercado y habilitar el escenario existente `poison-recovery` con evidencia y smoke portable aislados de datos previos. Reutilizar la consola existente, sin ampliar su navegación.
- **BREAKING**: sustituir la detención obligatoria por mensaje permanente de F-03 por cuarentena durable y avance transaccional; extender la entrada con comandos internos versionados. Las posiciones exitosas existentes permanecen inmutables.

## Capabilities

### New Capabilities

- `report-quarantine`: clasificación, reintentos durables, DLQ, barreras, corrección auditada y recuperación ordenada.

### Modified Capabilities

- `execution-report-consumption`: resolución durable de pendientes y entrada de comandos sin paralelizar particiones.
- `processing-results`: distinguir éxito, cuarentena y recepción de comandos, preservando resultados históricos.
- `settlement-publication`: asociar la salida recuperada al progreso del comando que la origina.
- `order-query`: exponer incompletitud y posiciones pendientes sin inventar estados de mercado.
- `demo-scenarios`: habilitar y verificar `poison-recovery`, incorporándolo al lote disponible.
- `evaluation-console`: inspeccionar cuarentena, correcciones y resultados desde las vistas existentes; conservar validación de publicación ordinaria.

## Impact

Cambian listener/configuración Kafka y política posterior al rollback, casos de uso y puertos de aplicación, adaptadores JDBC/Kafka/HTTP, migración aditiva desde V2, configuración de reintentos y controles de demo, pruebas y smokes. Se conserva Java 25, Spring Boot y dependencias del BOM; no se agregan servicios ni librerías de retry. README y DECISIONS se actualizarán durante implementación con evidencia observada; F-06 continúa pendiente hasta entonces. Quedan fuera cierre con descarte de pendientes, reconstrucción de éxitos históricos, cambios de particionado, auth, una UI nueva, retención productiva automática y F-07/F-08.
