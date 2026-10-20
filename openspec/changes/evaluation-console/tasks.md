# Tasks

Todos los ítems corresponden a implementación futura. Conservar cambios ajenos,
formatear Java editado con Spotless y generar commits locales coherentes con
Conventional Commits. Los comandos `verify-console` y las clases de pruebas citadas
abajo son entregables por incorporar, no herramientas que ya existan.

## 1. Proceso de consola y observación de instancias

- [x] 1.1 Incorporar el perfil `evaluation-console`, su activación exclusiva y recursos de UI propios, excluyendo listener y Flyway y tolerando dependencias caídas al iniciar; verificar con `EvaluationConsoleRuntimeIT` que el servidor responde sin SQL/Kafka disponibles y no modifica el esquema.
- [x] 1.2 Crear targets de imagen separados y servicio Compose en `127.0.0.1:8083`, agregando Docker CLI fijada/socket sólo a la consola y healthcheck propio; verificar `docker compose config`, build de ambos targets e inspección de imágenes/montajes, conservando el runtime de app-1/app-2.
- [x] 1.3 Agregar observación del listener en `GET /internal/demo/consumer`, habilitada por propiedad de Compose, y clientId identificable por instancia; verificar con integración HTTP que consumidor detenido y proceso saludable se informan por separado y que el endpoint queda deshabilitado fuera de demo.
- [x] 1.4 Comprobar con `EvaluationConsoleRuntimeIT` dos aplicaciones y una consola contra Kafka/PostgreSQL reales, exactamente dos miembros del grupo y una partición por aplicación; verificar que refrescar observaciones no cambia esa composición.
- [x] 1.5 Documentar en README el puerto, perfil, prerrequisitos y estado de arranque; registrar en DECISIONS el proceso independiente y la excepción de UI autorizada para evaluación, sin adelantar el estado implementado de la consola ni de F-04 a F-06; verificar correspondencia con Compose y el contrato de runtime.

## 2. Consultas SQL y representación exacta

- [x] 2.1 Incorporar listado acotado de órdenes con cursor y detalle consistente reutilizando `OrderQueries`; verificar con `EvaluationConsoleQueriesIT` estado/contador/ledger, campos adicionales, paginación y errores de identificador sin alterar el endpoint público existente.
- [x] 2.2 Exponer resultados por `(topic, partition, offset)` y enlaces a ledger/orden; verificar el NEW original y su duplicado tardío tras FILLED, resultados con igual offset en particiones distintas y `result: null` sin clasificarlo como fallo.
- [x] 2.3 Definir DTO de consola con enteros/decimales como cadenas y reportes como texto JSON, y comparación HTTP de ambas instancias en el servidor; verificar ida y vuelta de IDs mayores que `9007199254740991`, límites válidos de 64 bits y `999999999999.12345678` sin cambios en SQL ni redondeos.
- [x] 2.4 Documentar en README estado actual frente a snapshot histórico, significado del contador y limitaciones de observación entre fuentes; verificar los ejemplos con los resultados de las integraciones de este grupo.

## 3. Lectura y publicación Kafka

- [x] 3.1 Exponer lectura acotada `read_committed` de los tres topics mediante asignación/seek y cursores por partición; verificar en `DemoPublishingIT` exclusión de transacciones abortadas, continuación con huecos, timeout y ausencia de commits o miembros nuevos en el grupo de aplicación.
- [x] 3.2 Implementar publicación `{key, payload}` con preflight de contrato, duplicado antes de terminal y transición contra snapshot, manteniendo la validación definitiva en el consumidor; verificar rechazo sin publicación de JSON/key/identidad/transición conocidos como inválidos y un ER válido que ingresa únicamente por Kafka.
- [x] 3.3 Devolver metadata real del broker y estados de fallo o confirmación incierta separados del éxito SQL; verificar con integración la correlación de la posición confirmada y con una falla controlada que no se inventa metadata ni se reenvía automáticamente desde la aplicación.
- [x] 3.4 Implementar republicación por posición exacta conservando bytes/key; verificar FILLED y NEW tardío como DUPLICATE, valores numéricos y campos adicionales intactos, y rechazo de posiciones inexistentes/abortadas sin publicar el registro siguiente por error.
- [x] 3.5 Documentar en README confirmación de publicación frente a aplicación, republicación frente a reentrega, límites del preflight y lectura de salidas todavía pendientes; verificar que los ejemplos se corresponden con evidencia de `DemoPublishingIT`.

## 4. Estado operativo y control local

- [x] 4.1 Incorporar el adaptador Docker con proyecto fijo de Compose, búsqueda que incluya detenidos, validación de etiquetas y lista de acciones/servicios; verificar con fixtures del ejecutor que resuelve un contenedor detenido, rechaza destinos ambiguos o ajenos y ejecuta sólo vectores de argumentos permitidos.
- [x] 4.2 Exponer start/stop/restart de los cuatro servicios y kill SIGKILL sólo de las aplicaciones con timeout y resultado observado; verificar los rechazos de acciones arbitrarias y que una falla del socket no bloquea las consultas independientes mediante integración de la API de consola.
- [x] 4.3 Agregar agregación parcial de salud, listener, asignaciones, offsets/lag y logs acotados, con fuente/fecha y valores desconocidos; verificar instancia inaccesible, listener detenido con HTTP UP, datos stale y logs como diagnóstico temporal sin atribuir autoría histórica a asignaciones actuales.
- [x] 4.4 Exigir origen local, encabezado de consola y JSON en mutaciones sin habilitar CORS abierto; verificar solicitudes de origen ajeno, formato inválido y operación permitida, comprobando que los rechazos no llaman al ejecutor Docker ni al publicador.
- [x] 4.5 Documentar en README permisos reales del socket, servicios controlables y reanudación manual tras una dependencia caída; registrar en DECISIONS el alcance del control y que kill manual no prueba una frontera de commit; verificar la guía con los endpoints y acciones implementados.

## 5. Escenarios y coordinación

- [x] 5.1 Incorporar catálogo con los cinco escenarios disponibles y tarjetas pendientes de F-04/F-05/F-06, rechazando ejecución pendiente por API; verificar dependencias, conteos separados y que `Ejecutar disponibles` nunca declara aprobación de pendientes.
- [x] 5.2 Implementar runs asíncronos de escenario o lote `available` con ID, un solo activo, pasos manuales/automáticos, deadlines, historial acotado, recuperación tras recarga y cancelación que conserve efectos; verificar con `DemoScenariosIT` conflictos 409, lote secuencial sin pendientes, continuidad por ID, timeout con última observación y detención de nuevos envíos al cancelar.
- [x] 5.3 Implementar `interleaved-orders` con IDs nuevos en particiones distintas y evidencia de ambas asignaciones, y `quantity-snapshots` con reloj no creciente; verificar con Kafka/PostgreSQL reales metadata, estados FILLED/CANCELLED, conteos 4/3, acumulados y secuencia del ledger y de resultados por paso.
- [x] 5.4 Implementar `terminal-duplicates` con NEW/FILLED y dos republicaciones tardías; verificar dos APPLIED, dos DUPLICATE, contador/ledger dos y preservación de snapshots y referencias al ledger.
- [x] 5.5 Implementar `restart-persistence` esperando las posiciones propias antes de reiniciar y restaurando los servicios alterados al fallar/cancelar; verificar en `DemoScenariosIT` la coordinación de reinicio y restauración con adaptador controlado, igualdad completa de consultas y resultados DUPLICATE nuevos, y cubrir el reinicio real en el smoke integrado.
- [x] 5.6 Implementar `committed-input-only` con productor transaccional propio, abort/close de limpieza y observación hasta un sucesor confirmado de la misma partición; verificar sólo dos efectos SQL y ausencia de resultado/ledger para el parcial abortado, sin atribuirlo a settlement.
- [x] 5.7 Documentar en README precondiciones, resultados esperados, IDs/posiciones observables, conservación de datos, historial volátil de runs y límites de cada escenario; verificar repetición con órdenes nuevas y diagnóstico de precondición incumplida sin resets ni borrados.

## 6. Interfaz del evaluador

- [x] 6.1 Crear la página de cuatro vistas con tablas, búsqueda de órdenes y JSON/ledger/resultados enlazados, usando recursos locales y estados de carga/vacío/error/stale; verificar en navegador recorrido de entorno vacío, escenario intercalado y consulta del duplicado tardío sin redondear valores.
- [x] 6.2 Incorporar editor y republicación exacta, mostrando confirmación Kafka separada del procesamiento SQL y errores junto al envío; verificar desde navegador un NEW/FILLED válido, republicación terminal y rechazo de borrador inválido sin crear registros.
- [x] 6.3 Incorporar controles locales y ejecución automática/paso a paso, avance recuperable por URL, cancelación y tarjetas pendientes deshabilitadas; verificar recarga a mitad de escenario, conflicto de acciones, ejecución pendiente rechazada y disponibilidad de consola al detener e iniciar una aplicación.
- [x] 6.4 Implementar refresco sin solicitudes solapadas con pausa, etiquetas, foco, contraste y presentación textual de payloads/logs; verificar navegación completa por teclado, ventana estrecha, pausa/reanudación y un campo con marcado HTML que se muestre inerte.
- [x] 6.5 Completar la guía breve del evaluador en README desde `docker compose up` hasta ejecutar disponibles e inspeccionar evidencia; verificar el recorrido literalmente desde un navegador sin CLI, SQL ni clientes de broker adicionales.

## 7. Verificación integrada y evidencia

- [ ] 7.1 Incorporar el smoke portable `verify-console` al perfil tools, usando la API real de consola para ejecutar disponibles y controlar sólo servicios del proyecto, incluyendo stop/start de dependencias y kill/start de una aplicación; verificar con `docker compose --profile tools run --build --rm verify-console` que hay diagnóstico por falla, consola operativa, servicios restaurados y datos conservados, sin declarar verificación de fronteras F-05.
- [ ] 7.2 Ejecutar `.\mvnw.cmd spotless:apply`, `.\mvnw.cmd spotless:check` y `.\mvnw.cmd verify` con Docker, incluyendo las integraciones nuevas y regresiones existentes; verificar éxito sin omisiones y registrar comando, entorno y resultados, sin sustituirlos por un build de imagen.
- [ ] 7.3 Ejecutar secuencialmente `docker compose --profile tools run --build --rm verify-f02` y el nuevo smoke `verify-console`, además de la revisión de UI del grupo 6; verificar exactamente dos consumidores, concordancia HTTP, persistencia, controles del proyecto y conservación de datos anteriores.
- [ ] 7.4 Registrar correspondencia entre todos los escenarios de las delta specs y evidencia observada en `verification.md`, actualizar la fila propia de consola en README sólo si pasó y conservar estados pendientes de F-04/F-05/F-06; verificar `openspec validate --all --strict`, `git diff --check` y el diff final de documentación antes de declarar completa la implementación.

## Workflow follow-up

- Sincronizar las specs y archivar `evaluation-console` después de completar y verificar todos los ítems; validar nuevamente OpenSpec tras el archivo.
- Conservar commits locales por unidad coherente y reportar evidencia y límites, sin push ni publicación.
- Detenerse al completar esta feature. Habilitar los escenarios pendientes sólo en los changes de sus features y con nueva evidencia; esas implementaciones quedan fuera de este checklist.
