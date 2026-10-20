# Proposal

## Why

El evaluador ve pasos aprobados pero debe interpretar JSON y nombres técnicos para
entender el resultado. El recorrido necesita explicar la prueba y comparar lo
esperado con evidencia observada sin abrir detalles técnicos.

## What Changes

- Selector guiado de cinco escenarios, comenzando individualmente por dos órdenes
  en paralelo; explicación previa, avance manual y selección del siguiente sin ejecutar.
- Resultados separados por escenario, conclusión y verificaciones esperado/observado.
- API aditiva con relaciones estables entre escenarios, pasos, órdenes y mensajes;
  observaciones históricas registradas por las comprobaciones reales del servidor.
- Evidencia técnica desplegable y pendientes separados; preservar exactitud numérica,
  compatibilidad del smoke, cancelación y recuperación por URL.

## Capabilities

### New Capabilities

Ninguna.

### Modified Capabilities

- `demo-scenarios`: guía previa, agrupación y verificaciones legibles con alcance explícito.
- `evaluation-console`: navegación guiada y separación visual de evidencia técnica.

## Impact

Cambios limitados al perfil de consola, recursos HTML/CSS/JS, integraciones del
runner, smoke y README. Sin migraciones, rutas nuevas, framework ni servicios nuevos.
Se conservan las otras vistas y las features pendientes F-04 a F-06. El usuario
autorizó implementar este plan completo, incluidos artefactos y archivo posterior.
