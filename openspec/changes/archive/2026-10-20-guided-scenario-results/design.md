# Design

## Context

Ver proposal.md. Actualmente DemoRun conserva pasos planos y evidencia JSON;
ScenarioSteps ya consulta SQL, HTTP y Kafka y ejecuta las aserciones reales.
Los campos consumidos por verify-console se conservan; no se modifica el núcleo.

## Goals / Non-Goals

**Goals:** guía y resultados legibles basados en observaciones históricas reales.
**Non-Goals:** nuevas garantías de F-04/F-05/F-06, rediseño de otras vistas,
migraciones, retención durable de runs o instalación de dependencias.

## Decisions

- API aditiva: catálogo agrega explanation/actions; run agrega scenarioResults,
  cada uno con scenarioId/status/conclusion/orders/checks. Pasos y mensajes agregan
  id/scenarioId/stepId. Se conservan listas planas y campos previos del smoke.
- Registrar verificaciones al comprobar datos, no resumir JSON en el navegador.
  Cada check tiene id,label,expected,observed,status,observedAt,stepId y references.
  Los checks previstos se declaran antes de observar; quedan NOT_OBSERVED sin datos.
  observedAt aparece sólo al registrar una observación y stepId al observar o
  diagnosticar, sin inventar metadata para comprobaciones todavía no intentadas.
  Una condición transitoria queda WAITING durante reintentos, FAILED al terminar
  en fallo, e INCOMPLETE si se cancela. PASSED exige datos efectivamente observados.
- Resultados se guardan en memoria del run, con snapshots ya consultados y cadenas
  exactas. La navegación nunca los reemplaza con una consulta actual de la orden.
- Escenarios pendientes de ejecución en un lote quedan NOT_RUN si éste se interrumpe.
  Un fallo de restauración no convierte un escenario comprobado en una garantía
  operativa: se muestra el error global además de las comprobaciones obtenidas.
- Usar HTML/CSS/JS locales. Selector lateral, detalle principal y pendientes aparte;
  en móvil una columna. Base #eef2f6, superficies #fff, tinta #172a3a, acción #145cb5,
  aprobado #15613e y fallo #a02726; tipografía Segoe UI y monoespaciada sólo en evidencia.
  La pieza principal es la comparación de observaciones, no un panel de métricas.
- Selección inicial interleaved-orders; siguientes en orden de catálogo sin envíos
  implícitos. Hash #scenarios/{runId}/{scenarioId} conserva selección en recarga.
  El lote permite inspeccionar cualquiera mientras el progreso global sigue visible.
  El botón Ejecutar todos corresponde a la operación `available`, anteriormente
  presentada como Ejecutar disponibles; el contrato de ejecución no cambia.

## Risks / Trade-offs

- Evidencia excesiva → tabla principal resumida, pasos y datos técnicos desplegables.
- Observaciones parciales → estados explícitos y conclusión exitosa sólo tras completar.
- Actualización frecuente → nodos estables y restauración de foco; no cambiar selección
  del usuario al avanzar el lote, salvo selección inicial de la ejecución.
- Compatibilidad → comprobar smoke existente y listas planas además del modelo nuevo.

## Migration Plan

Desplegar reconstruyendo sólo la consola; el historial de runs en memoria se pierde
como ya está documentado. Rollback de imagen sin cambios SQL/Kafka. Verificar con
integraciones, smoke real y navegador, actualizar README, sincronizar y archivar.
