# Proposal

## Why

Hoy el evaluador sale del escenario para inspeccionar mensajes y órdenes, y debe
reconstruir su relación con los resultados SQL y el ledger. Una ficha con evidencia
integrada permite verificar los ocho escenarios sin cambiar de pantalla y explica
cantidades, duplicados y fallas con datos concretos.

## What Changes

- Hacer de la ficha de escenario el recorrido principal: seleccionar muestra una
  explicación breve y lo esperado; sólo pulsar Ejecutar inicia acciones.
- Mostrar dentro de esa ficha mensajes de la ejecución, resultados por posición,
  órdenes afectadas, ledger y salidas observadas, con detalle seleccionable y JSON
  desplegable. Conservar conclusión, esperado/observado, pasos y controles.
- Relacionar cada mensaje con su orden y su ledger sin navegar a otra vista;
  separar estado informado por el ER, resultado histórico y estado actual.
- Explicar cantidades como Solicitada, Ejecutada y Pendiente de ejecutar, y aclarar
  las unidades sin ejecutar al cancelar. Distinguir ER aplicados de unidades.
- Conservar Mensajes, Órdenes y Sistema bajo Herramientas avanzadas, incluyendo
  publicación manual, inspección general y controles locales existentes.
- Completar en `demo` la evidencia estructurada que necesita la ficha, conservando
  contratos existentes, exactitud numérica y datos parciales o históricos. Los
  registros abortados del productor se distinguen de lecturas `read_committed`.
- Actualizar el recorrido breve del README y explicar que el paquete Java `demo`
  y sus recursos web son auxiliares de evaluación: UI, escenarios, consultas y
  controles de ensayo, incluida la instrumentación opcional de `demo.recovery`.

## Capabilities

### New Capabilities

Ninguna; se amplían las capacidades de evaluación existentes.

### Modified Capabilities

- `evaluation-console`: navegación principal por escenarios con herramientas
  avanzadas; inspección correlacionada en la misma ficha y cantidades comprensibles.
- `demo-scenarios`: evidencia estructurada por ejecución, escenario y paso que
  permite presentar mensajes, snapshots, salidas y recuperación sin interpretar
  textos descriptivos ni mezclar observaciones de corridas diferentes.

## Impact

Se prevén cambios en los recursos HTML/CSS/JavaScript de la consola, clases del
paquete `com.maxcapital.challenge.demo`, sus pruebas, el smoke existente si requiere
comprobaciones adicionales y README. La respuesta de ejecución se amplía de forma
aditiva; se mantienen endpoints, campos, IDs de escenario y rutas de navegación
existentes. No se incorporan framework frontend, dependencias, tablas o servicios.

El alcance no cambia dominio, transacciones SQL/Kafka, migraciones, semántica de
cuarentena, barreras de recuperación ni garantías de settlement. Se mantienen los
ocho escenarios, automático/paso a paso, ejecución exclusiva, restauración y datos.
Es una mejora de F-09; no inicia F-08 ni redefine decisiones del núcleo.

Este change contiene planificación. La nueva interfaz y el ajuste del README
quedan pendientes de implementación y verificación.
