# Tasks

## 1. Barreras opt-in y fronteras reales

- [ ] 1.1 Implementar estado de barrera por ejecución/instancia/ER, token de intento y generación de proceso, con consumo único y vencimiento; verificar con pruebas de concurrencia dirigida que tokens viejos, otra ejecución y otra identidad no liberan ni reemplazan el intento.
- [ ] 1.2 Incorporar wiring de demo condicionado por `app.demo.recovery-enabled` y controles internos enumerados para armar, observar e invalidar/liberar; verificar HTTP con controles deshabilitados por defecto, operaciones inválidas rechazadas y habilitación explícita sin consumidores adicionales.
- [ ] 1.3 Decorar persistencia de resultados en el wiring de demo para alcanzar BEFORE_SQL_COMMIT y AFTER_SQL_COMMIT mediante la transacción JDBC real; verificar desde otra conexión ausencia/presencia de escrituras en cada frontera, rollback al vencer antes del commit y resultado durable pendiente al vencer después.
- [ ] 1.4 Incorporar la barrera AFTER_SEND_ACK_BEFORE_KAFKA_COMMIT sobre el productor real sin alterar el commit del contenedor; verificar aceptación del envío, salida invisible read_committed y offset estable sin avanzar hasta resolución.
- [ ] 1.5 Configurar Compose de demo y presupuestos coherentes de poll, sesión, JDBC, barrera y transacción; documentar flag/fronteras/tiempos y vencimiento en README, y verificar que el perfil ordinario conserva sus valores y la instrumentación deshabilitada.

## 2. Recuperación del núcleo con procesos reales

- [ ] 2.1 Extender el harness de procesos para usar barreras observables y terminar JVM sin cierre ordenado; verificar PID/instancia/posición antes del kill, salida abrupta y limpieza de hijos al fallar el test.
- [ ] 2.2 Probar kill antes del commit SQL tanto al crear NEW como al actualizar una orden con sucesores publicados; verificar rollback conjunto, una aplicación por ER, ledger/contador/cantidades y secuencia completa tras toma de la partición.
- [ ] 2.3 Probar reasignación real con SQL abierto y desenlace commit del propietario anterior; verificar asignación del nuevo antes de liberar, rechazo Kafka obsoleto, resultado original intacto y sucesores sin efectos duplicados después de terminar ambos intentos.
- [ ] 2.4 Probar el desenlace rollback del propietario anterior tras reasignación, y kill después de envío de settlement aceptado; verificar recuperación desde la misma posición, progreso estable y exactamente una salida final read_committed.
- [ ] 2.5 Probar terminación abrupta de ambas JVM con SQL confirmado/Kafka pendiente y sucesores publicados; verificar recuperación persistente, posiciones previas intactas, FILLED con una salida, CANCELLED con cero y nuevas órdenes procesadas en ambas particiones al volver dos miembros.
- [ ] 2.6 Actualizar las secciones de recuperación, settlement y límites de DECISIONS.md con estrategias por frontera y referencias a los casos observados; verificar que diferencia garantías probadas, supuestos, reinicio operativo y trabajo pendiente de F-06, sin adelantar evidencia de consola.

## 3. Coordinación de ensayos y restauración

- [ ] 3.1 Extender el acceso interno de consola para gestionar barreras del run y seleccionar el propietario de la partición antes de publicar; verificar alcance real antes de actuar, rechazo de interferencia manual y ausencia de kills si la barrera no se alcanzó.
- [ ] 3.2 Incorporar un colaborador de escenarios de recuperación y lectura completa de settlements por cursores de ambas particiones; verificar paginación, transacciones abiertas, correlación con resultado original y rechazo de un conteo parcial como prueba de unicidad.
- [ ] 3.3 Extender la restauración del runner con invalidación de barreras, inicio de procesos terminados y reinicio de listeners detenidos mediante su aplicación; verificar cancelación, vencimiento y error Docker con presupuesto de limpieza independiente, exclusión hasta terminar y dos consumidores o fallo explícito.
- [ ] 3.4 Documentar en README el alcance de la restauración y el procedimiento tras perder la consola, y en DECISIONS.md el carácter temporal de las barreras; verificar mediante pruebas que un proceso reiniciado no conserva controles viejos y una restauración fallida nunca produce PASSED.

## 4. Escenario Settlement único

- [ ] 4.1 Implementar `settlement-once` con FILLED/CANCELLED en particiones distintas, republicaciones terminales y NEW tardío; verificar APPLIED/DUPLICATE, contador/ledger, mensaje contra resultado original y conteos uno/cero sobre el rango completo.
- [ ] 4.2 Incorporar al escenario un FILLED nuevo con kill observado después de SQL y recuperación por la instancia sobreviviente; verificar mismo resultado por posición, una salida, retorno de la instancia caída y consumo de dos miembros.
- [ ] 4.3 Agregar guía previa, checks, conclusiones y referencias del escenario al modelo y vista existentes; verificar ejecución automática y paso a paso, recarga y resultados sin abrir JSON, y documentar en README qué demuestra y cómo ejecutarlo.

## 5. Escenario Caídas entre commits y rebalanceos

- [ ] 5.1 Implementar los tres casos con órdenes nuevas para SQL abierto, SQL confirmado y send aceptado; verificar frontera antes de kill, recuperación de la misma posición y sucesores, resultado original donde corresponde y una salida por FILLED tras resolver transacciones.
- [ ] 5.2 Implementar el caso de reasignación con SQL abierto; verificar pérdida real de pertenencia y asignación al nuevo propietario antes de liberar al anterior, rechazo de su intento Kafka y conteo final después de terminar ambos intentos.
- [ ] 5.3 Implementar el caso de caída de ambas aplicaciones y su recuperación; verificar pendientes y resultados históricos preservados, nuevas órdenes en ambas particiones, dos miembros y comparación HTTP después del retorno.
- [ ] 5.4 Agregar guía, checks y conclusiones de cada frontera, haciendo indivisible la ventana crítica dentro de un paso; verificar modos automático/paso a paso, recarga entre casos, cancelación en barrera y preservación de observaciones incompletas sin aprobarlas.
- [ ] 5.5 Habilitar ambos IDs en catálogo y actualizar las pruebas y textos que fijaban cinco disponibles/tres pendientes; verificar siete disponibles/un pendiente, lote completo, rechazo de poison-recovery y conservación de los cinco recorridos existentes. Documentar los casos y sus límites en README y DECISIONS.md según la evidencia obtenida.

## 6. Reproducción Compose y evaluación en navegador

- [ ] 6.1 Agregar runner portable `verify-f05` que ejecute ambos escenarios nuevos sobre Compose con procesos reales; verificar posiciones, resultados, ledgers, offsets y settlements, conservación de datos previos, restauración y salida no exitosa ante verificación incompleta.
- [ ] 6.2 Actualizar `verify-console` a siete escenarios/un pendiente y sus plazos para el lote extendido; verificar el lote sobre Compose sin sustituir Docker y documentar en README comandos ejecutables y ejecución secuencial de smokes.
- [ ] 6.3 Revisar ambos escenarios en navegador en modos automático y paso a paso, con recarga, cancelación y restauración; registrar evidencia de explicación previa, avance, checks, enlaces y resultados accesibles sin JSON, corrigiendo únicamente la UI necesaria para esos flujos.
- [ ] 6.4 Completar DECISIONS.md con la matriz final de recuperación y evidencia de consola, diferenciando tests con Docker sustituido, JVM reales y Compose; verificar que no atribuye exactamente una vez a la identidad del mensaje por sí sola ni promete cuarentena o recuperación de pérdida de volúmenes.

## 7. Verificación integrada y cierre de implementación

- [ ] 7.1 Ejecutar `.\mvnw.cmd spotless:apply`, `.\mvnw.cmd spotless:check` y `.\mvnw.cmd verify`; registrar resultados y omisiones reales en verification.md del change y no completar F-05 si Docker u otra dependencia impide los criterios.
- [ ] 7.2 Ejecutar secuencialmente los smokes portables verify-f02, verify-f04, verify-f05 y verify-console con Compose reconstruido; registrar evidencia de datos conservados, fronteras observadas, dos miembros finales y conteos de salida, sin borrar volúmenes ni resetear offsets.
- [ ] 7.3 Contrastar cada escenario de las cuatro delta specs con su evidencia en verification.md, actualizar sólo entonces la tabla F-05 y los límites pertinentes de README, y verificar `openspec validate --all --strict` y `git diff --check`; conservar commits locales coherentes sin push.

## Workflow follow-up

- Después de completar y verificar las tareas, sincronizar las cuatro delta specs con sus capacidades existentes y archivar el change mediante el workflow de archivo.
- Verificar las specs finales con `openspec validate --all --strict`, el archivo y el estado Git; entregar evidencia y detenerse antes de F-06.
