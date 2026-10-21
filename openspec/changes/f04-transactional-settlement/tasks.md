# Tasks

## 1. Contrato y decisión de settlement

- [x] 1.1 Agregar el valor de aplicación Settlement y su proyección desde ProcessingResult según la spec v1; verificar elegibilidad APPLIED FILLED, exclusión de CANCELLED/DUPLICATE y el duplicado tardío de NEW con snapshot FILLED mediante pruebas de la decisión.
- [x] 1.2 Implementar serialización JSON UTF-8 con key canónica, identidad estable, source completo e IDs/cantidades como cadenas; verificar igualdad de reconstrucción antes/después de recuperar NUMERIC y resultados SQL, IDs fuera del rango seguro JavaScript y ocho decimales sin redondeos.
- [x] 1.3 Agregar SettlementPublisher y HandleExecutionReport para coordinar el procesador JDBC existente y la salida, conservando explícito orderTransactionManager; verificar con una prueba de integración que la publicación sólo se intenta después de SQL confirmado y que un rollback SQL no llama al publicador.
- [x] 1.4 Documentar en DECISIONS la proyección determinística, la intención recuperable y el contrato propuesto; agregar el esquema de salida y su representación exacta al README, manteniendo F-04 pendiente mientras no exista evidencia completa. Verificar ejemplos contra el serializador y el resultado SQL de prueba.

## 2. Transacción Kafka del consumo y política de errores

- [ ] 2.1 Conectar explícitamente KafkaTransactionManager a la factory personalizada, compartir ProducerFactory con KafkaTemplate y cambiar el listener a AckMode.RECORD sin Acknowledgment ni commits independientes; verificar con Kafka/PostgreSQL reales que NEW, parcial, CANCELLED y DUPLICATE confirman progreso por transacción sin salida y que FILLED incorpora settlement al mismo commit.
- [ ] 2.2 Implementar el adaptador Kafka de SettlementPublisher con publicación en la transacción del contenedor y sin executeInTransaction anidado ni envío no transaccional; verificar que falla fuera de transacción y que un error de serialización/envío conserva SQL confirmado sin avanzar el offset ni publicar una salida confirmada.
- [ ] 2.3 Configurar la detención posterior al rollback para errores del listener y del cierre Kafka, sin recoverer que descarte, sin seek dentro de sesión incierta y con detención por fencing; verificar diagnóstico de posición/causa, sucesor pendiente y reinicios repetidos ante JSON/key/transición/identidad inválidos sin skip.
- [ ] 2.4 Adaptar ExecutionReportKafkaIT y sus contenedores auxiliares a la firma nueva, declarar los topics de salida en los fixtures afectados y conservar identidades transaccionales distintas entre contextos; sustituir la inyección commitSync por una frontera transaccional comprobada. Verificar que la falla inyectada efectivamente ocurre, que se recupera el mismo resultado F-03 y que pasan los casos de entrada abortada, rollback y poison existentes.
- [ ] 2.5 Documentar en README el nuevo protocolo de confirmación y cómo reanudar un listener detenido; actualizar DECISIONS con commits independientes, tratamiento de errores y ventana SQL FILLED/salida pendiente. Contrastar cada afirmación con las pruebas de este grupo y conservar F-06 pendiente.

## 3. Fronteras de publicación y commit incierto

- [ ] 3.1 Crear soporte de SettlementKafkaIT con consumidores de observación assign/read_committed, posiciones iniciales de salida y lectura hasta límites estables posteriores a resolver todos los intentos; verificar que no agrega miembros ni modifica offsets del grupo de órdenes y que inspecciona ambas particiones sin borrar datos.
- [ ] 3.2 Agregar barreras e interceptores de productor sólo en pruebas para suspender después de SQL, después de send aceptado y antes de commit; verificar FILLED consultable, salida todavía invisible, progreso estable pendiente y aborto sin settlement visible, seguido de recuperación con un único registro confirmado.
- [ ] 3.3 Inyectar una sola pérdida de respuesta después de delegar commitTransaction exitosamente al broker, por debajo del wrapper de productor de Spring; verificar que salida/offset están realmente confirmados, se detiene la sesión con diagnóstico, se descarta el productor fallido y el nuevo consumo no vuelve a enviar esa finalización ni bloquea al sucesor.
- [ ] 3.4 Inyectar un commit que no llega al broker después de incorporar offsets a la transacción y deja resolución pendiente; verificar espera por offsets estables, aborto/resolución dentro del plazo de prueba y replay del APPLIED original con un único settlement final, sin seek manual ni reenvío desde la sesión fallida.
- [ ] 3.5 Registrar en el verification.md del change las fronteras observadas, posiciones y resultados de estas pruebas; documentar en DECISIONS la diferencia entre aborto probado, respuesta perdida controlada y corte físico de red no ensayado. Verificar que no se afirma cobertura de una falla distinta de la inyectada.

## 4. Dos instancias y recuperación indispensable de F-04

- [ ] 4.1 Ejecutar dos instancias con prefijos transaccionales distintos sobre Kafka/PostgreSQL reales y observar un miembro por partición; verificar órdenes intercaladas, duplicados FILLED/NEW tardío, CANCELLED después de parcial, payload y exactamente una salida confirmada por FILLED y cero por CANCELLED después de completar todo el lote.
- [ ] 4.2 Agregar una JVM hija de pruebas que ejecute una instancia real junto a la segunda, con barrera después del commit SQL y antes del envío; verificar que el padre identifica y puede finalizar únicamente el proceso propio y que cleanup/finally cierra todos los recursos incluso ante timeout, sin flags ni endpoints de producto.
- [ ] 4.3 Terminar abruptamente esa JVM en la barrera, observar recuperación de la partición y del resultado SQL por la otra instancia y reiniciar la primera; verificar una sola aplicación/ledger del FILLED, resultado original intacto y exactamente un settlement confirmado al cerrar todos los intentos.
- [ ] 4.4 Suspender un propietario tras SQL, provocar pérdida real de generación con max.poll.interval de prueba y observar reasignación; liberar el intento obsoleto y verificar rechazo de su resolución Kafka, recuperación por el propietario vigente y un solo settlement después de completar ambos intentos, sin sustituir el rebalanceo por llamadas directas al listener.
- [ ] 4.5 Incorporar evidencia del kill y la reasignación en verification.md y actualizar DECISIONS con el alcance efectivamente demostrado; verificar que F-05 sigue pendiente y sus ampliaciones quedan distinguidas de estos casos mínimos de aceptación de F-04.

## 5. Activación compatible y evaluación reproducible

- [ ] 5.1 Preparar fixtures F-03 APPLIED FILLED con offset pendiente y confirmado sin ejecutar el nuevo publicador; verificar primera salida del pendiente, ausencia de backfill del confirmado y exclusión de su terminal republicado, conservando resultados, ledger, contador y offsets existentes.
- [ ] 5.2 Conservar el comportamiento del ledger legado sin resultado original y las pruebas ProcessingResultsIT de F-03; verificar que no se inventan posiciones ni se usa DUPLICATE para emitir retroactivamente y documentar el límite de compatibilidad en DECISIONS/README.
- [ ] 5.3 Documentar y ejercitar desde Compose un recorrido con dos instancias, órdenes nuevas FILLED/CANCELLED, republicaciones e inspección read_committed del topic settlements; registrar comandos, IDs y cardinalidades reales en verification.md. Verificar que el recorrido es reproducible con Docker, preserva datos y no depende de un consumidor downstream implementado.
- [ ] 5.4 Documentar en README despliegue detenido de ambas instancias, ausencia de backfill, prefijos distintos, retención/grupo estables y recuperación del commit incierto; contrastar el procedimiento con los fixtures de activación y el recorrido Compose. Explicar que volver a F-03 puede consumir pendientes sin settlement y no constituye rollback seguro.
- [ ] 5.5 Aclarar en README que la vista Mensajes puede inspeccionar settlements pero el escenario guiado de consola continúa pendiente; comprobar que no se habilitan escenarios sin implementación ni se cambian UI, endpoints HTTP existentes o estados F-05/F-06 por este change.

## 6. Integración y cierre de evidencia

- [ ] 6.1 Ejecutar .\mvnw.cmd spotless:apply sobre Java editado y después .\mvnw.cmd spotless:check y .\mvnw.cmd verify con Docker; comprobar que pasan todas las suites, incluidos kill/reasignación, sin tests omitidos y con cleanup de procesos propios. Registrar comandos y resultados; si una dependencia impide verificar, conservar F-04 pendiente e informar el bloqueo.
- [ ] 6.2 Revisar la correspondencia de todos los escenarios de las dos delta specs con pruebas/evidencia en verification.md; ejecutar openspec validate --all --strict y git diff --check, comprobar conservación de invariantes F-02/F-03 y cambiar únicamente F-04 a implementado en README cuando todos sus criterios estén observados. Verificar coherencia final entre README, DECISIONS, specs, tareas y evidencia, conservando commits locales por unidad y sin push.

## Workflow follow-up

- Después de implementar y verificar todos los criterios, sincronizar las delta specs y archivar el change mediante el workflow openspec-archive-change, según AGENTS.md; validar el resultado archivado.
- Detenerse al entregar F-04 y su evidencia. No comenzar ampliaciones F-05, cuarentena F-06 ni escenarios nuevos de consola sin otro pedido.
