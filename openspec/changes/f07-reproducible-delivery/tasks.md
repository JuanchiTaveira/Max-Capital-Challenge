# Tasks

## 1. Documentación del candidato

- [x] 1.1 Corregir el enlace de evidencia F-06 al archivo `2026-10-23-f06-quarantine-ordered-recovery`, revisar enlaces locales/anclas e instrucciones de README y contrastar DECISIONS.md con los ocho temas del PDF; verificar destinos y registrar la correspondencia y cualquier ajuste justificado, manteniendo F-07 pendiente durante las pruebas.
- [x] 1.2 Confirmar las correcciones por una unidad coherente con Conventional Commits y registrar el SHA candidato; verificar `git status` y que el candidato incluye sólo archivos previstos, sin publicar nada.

## 2. Ensayo reproducible aislado

- [x] 2.1 Inventariar Docker, proyectos, etiquetas de imagen y puertos; registrar un proyecto exclusivo para F-07 y verificar que sus operaciones no afecten servicios ni volúmenes existentes. Si el recorrido con puertos documentados está bloqueado, conservar el diagnóstico sin detener recursos ajenos ni declarar aceptación con un override.
- [x] 2.2 Crear un clon local separado del SHA candidato, conservando historial, y comprobar checkout limpio y ausencia inicial de `target`, `.env` y overrides heredados; registrar ruta, SHA, versiones y procedencia en `verification.md` del change.
- [x] 2.3 Ejecutar en el clon la validación Compose y el arranque con build y nombre de proyecto exclusivo; observar ambas apps saludables y con identidades distintas, tres topics de dos particiones y exactamente dos consumidores con una partición cada uno. Registrar comandos, resultados y evidencia sin depender del entorno F-06.
- [x] 2.4 Ejecutar Maven Wrapper `verify` desde el clon con JDK 25 y Docker, sin omitir tests; registrar comando, resultado, conteos observados y cero fallas/errores/omisiones o el bloqueo concreto. Un build de imagen no satisface esta tarea.
- [x] 2.5 Seguir la guía rápida en navegador y ejecutar `verify-console` secuencialmente dos veces en el proyecto F-07; comprobar ocho escenarios aprobados por corrida, observaciones de secuencia/duplicados/settlement/recuperación/cuarentena, conservación de órdenes previas en la repetición y restauración de dos consumidores. Registrar IDs y resultados en `verification.md`, conservando datos del ensayo.

## 3. Estado de entrega y cierre local

- [x] 3.1 Comprobar en lectura la disponibilidad anónima del repositorio remoto y su historial respecto del SHA probado; registrar URL, fecha, revisión remota observada y commits locales pendientes. Si falta acceso o publicación, indicarlo expresamente sin hacer push ni cambiar visibilidad; la tarea verifica y documenta el estado, no autoriza publicarlo.
- [x] 3.2 Consolidar en `verification.md` la matriz de entregables del PDF con referencias a los resultados de este clon, diferenciando evidencia histórica, cierre local y entrega pública; verificar que el resumen durable no depende de logs ignorados y no atribuye al candidato pruebas de otras revisiones.
- [x] 3.3 Con toda la aceptación local aprobada, marcar únicamente F-07 como Implementado en README, enlazar su evidencia y ajustar la referencia pendiente de F-07 conservando los límites del smoke F-02; si falta una verificación requerida, mantener Pendiente y explicar el impedimento. Comprobar enlaces y que F-08 y demás features conservan su estado.
- [x] 3.4 Ejecutar `openspec validate --all --strict` y `git diff --check`, revisar que no cambiaron contratos ni código y conservar el cierre documental en commits locales coherentes. Registrar los commits posteriores al SHA probado como documentales; si hubo un cambio ejecutable, repetir antes las verificaciones afectadas.

## Workflow follow-up

- Archivar el change con el flujo de archivo una vez completas y revisadas las tareas; verificar el enlace de evidencia después del movimiento al archivo.
- Si falta publicación, informar la acción y commits pendientes para una autorización específica; no confundir F-07 implementada localmente con entrega pública completada.
- Detenerse antes de F-08 o cualquier otra feature.
