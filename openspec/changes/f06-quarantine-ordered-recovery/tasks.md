# Tasks

## 1. Cuarentena y retry

- [x] 1.1 Agregar V3 con `quarantined_reports` y `report_corrections`; probar original binario, unicidad por posición y conservación de órdenes/ledger/resultados V2. No agregar tablas de sucesores, barreras, intentos ni comandos.
- [x] 1.2 Clasificar errores explícitos e integrar hasta cinco intentos con backoff exponencial y jitter en memoria; probar límites, recuperación de un transitorio y detención o pausa ante agotamiento, infraestructura y commit incierto según el diseño.
- [x] 1.3 Capturar permanentes después del rollback, publicar el aviso DLQ y pausar sólo la partición sin confirmar su offset; probar idempotencia de captura, fallo de envío sin avance y aviso repetible con el mismo rejectionId. Mantener polls activos y la configuración F-05.

## 2. Corrección y reanudación

- [x] 2.1 Implementar GET por posición y POST de corrección auditada con una sola corrección ACCEPTED por original; probar repetición de requestId, conflicto, éxito previo, original inexistente e identidad/routing incompatibles. La API sólo guarda metadata.
- [x] 2.2 Seleccionar el ER efectivo al reentregar la posición original y resolver corrección/cuarentena en la transacción de negocio existente; probar rollback conjunto, corrección rechazada y conservación de originales e historial. No agregar comandos Kafka ni cambiar `processing_results`.
- [x] 2.3 Verificar reentrega de la corrección después del commit SQL: mismo resultado, contador y ledger, un settlement para APPLIED FILLED y cero para CANCELLED/DUPLICATE. Reutilizar la publicación transaccional F-04/F-05.

## 3. Pruebas y recorrido reproducible

- [x] 3.1 Probar con Kafka y dos consumidores: un inválido pausa su partición sin adelantar sucesores, la otra continúa, y corrección más reinicio completa la secuencia. Incluir una caída después de SQL de una corrección FILLED; reutilizar los demás ensayos F-05.
- [x] 3.2 Habilitar `poison-recovery` reutilizando runner, render y controles actuales; verificar automático/paso a paso y restauración propia al cancelar, con diagnóstico si falla. No agregar pantallas, formularios ni tablas de demo.
- [x] 3.3 Agregar smoke portable `verify-f06` y adaptar el catálogo/lote a ocho escenarios; repetir el recorrido con IDs nuevos y comprobar que conserva los datos previos utilizados como referencia.

## 4. Entrega

- [x] 4.1 Ejecutar Spotless y `mvnw verify` con Docker, el smoke F-06 y el smoke de consola. Registrar evidencia real y regresión de F-04/F-05 sin ampliar innecesariamente la matriz de pruebas.
- [x] 4.2 Actualizar README/DECISIONS con flujo, ejemplo de corrección y límites: partición bloqueada, retry por sesión, reinicio operativo y DLQ al menos una vez. Marcar F-06 implementada sólo con evidencia; validar `openspec validate --all --strict` y `git diff --check`.

Conservar commits locales Conventional Commits y detenerse al entregar F-06. No implementar otras features ni archivar el change automáticamente.
