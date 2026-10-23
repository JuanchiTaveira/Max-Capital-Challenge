# Proposal

## Why

F-07 sigue pendiente aunque Compose, la consola y los ocho escenarios ya cubren los
entregables funcionales del challenge. Falta demostrar el recorrido completo desde
un clon limpio de la versión actual y cerrar la documentación y el estado de entrega.

## What Changes

- Verificar una revisión Git identificada desde un clon local limpio, con un proyecto
  Compose aislado y volúmenes nuevos, sin depender de archivos de `target`, overrides
  anteriores ni herramientas del host adicionales a los prerrequisitos publicados.
- Ejecutar la suite Maven con Docker y el smoke existente `verify-console`, que recorre
  los ocho escenarios; comprobar el acceso web indicado en la guía rápida.
- Registrar comandos, revisión, resultados, límites y trazabilidad con los entregables
  del PDF en `verification.md` de este change, sin presentar evidencia histórica como nueva.
- Corregir el enlace archivado de F-06 y revisar instrucciones, enlaces y referencias a
  F-07 en README; actualizar su estado sólo después de satisfacer la verificación local.
- Revisar DECISIONS.md contra los temas exigidos por el enunciado y ajustar únicamente
  discrepancias documentales observadas, sin cambiar las garantías del núcleo.
- Comprobar en modo lectura la visibilidad pública del remoto y la disponibilidad de la
  revisión entregable; distinguir cierre local verificado de publicación pendiente.

## Capabilities

### New Capabilities

Ninguna.

### Modified Capabilities

Ninguna. `repository-workflow`, `runtime-environment` y `demo-scenarios` ya describen
el comportamiento aplicable. Este change sólo cierra documentación y evidencia;
declara `skip_specs: true`, sin alterar specs ni contratos existentes.

## Impact

Archivos previstos: README.md, DECISIONS.md sólo si se detectan discrepancias, y los
artefactos de este change. Se reutilizan Compose, Maven Wrapper, consola y smokes.
No hay cambios previstos de Java, API, esquema SQL, dependencias, UI o CI/F-08.
Si aparece un defecto funcional, se registrará el bloqueo y se reevaluará su alcance
antes de convertir este cierre documental en otra implementación.

No se autoriza push, publicación, cambio de visibilidad ni eliminación de datos.
La entrega pública efectiva, si falta, queda como acción separada del cierre local.
