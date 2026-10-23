# Tasks

## 1. Evidencia estructurada y compatible en demo

- [ ] 1.1 Agregar `scenarioResults[].evidence` con messages, orderSnapshots y outputs conforme al diseño, conservando los campos/APIs actuales; verificar en pruebas de demo la correlación por posición completa, paso y ledgerId y que los campos usados por los smokes siguen presentes.
- [ ] 1.2 Capturar envíos, resúmenes exactos, lecturas SQL y snapshots estado/ledger en los puntos existentes de observación, completando posiciones resueltas y órdenes auxiliares de los ocho recorridos; ampliar las pruebas focalizadas para 7 envíos y ledgers 4/3, los cuatro acumulados y antes/después del reinicio, sin sobrescribir capturas.
- [ ] 1.3 Capturar salidas desde las lecturas read_committed y original/corrección desde cuarentena; verificar settlement/source, alcance de lectura, aviso DLQ leído frente a marcador notified, y posición original frente a ER efectivo en las pruebas de recuperación/demo pertinentes, sin modificar barreras ni transacciones.
- [ ] 1.4 Verificar con pruebas focalizadas del modelo/runner los cuatro estados de observación SQL, el NEW tardío DUPLICATE con snapshot FILLED, abort previsto, fallo/cancelación y aislamiento de corridas; incluir IDs mayores que 9007199254740991 y decimales exactos. Verificar además que consultar un run terminado no vuelve a leer SQL/Kafka ni altera sus capturas.

## 2. Ficha integrada e inspección dentro del escenario

- [ ] 2.1 Reorganizar la ficha en explicación breve/esperado, ejecución/avance, comprobaciones, mensajes, órdenes/ledger y salidas/incidentes, con automático inicial y controles existentes; comprobar en navegador que seleccionar no ejecuta y que el escenario 1 muestra siete mensajes y órdenes A/B con ledger 4/3 sin cambiar de vista ni abrir JSON.
- [ ] 2.2 Implementar selección correlacionada de mensaje/orden/ledger, tabla de historial y detalle desplegable con payload intacto; comprobar aplicado, duplicado que apunta a una fila existente, original inválido/corrección y ausencia de resultado. Verificar que estado del ER, snapshot del resultado y orden actual tienen rótulos distintos.
- [ ] 2.3 Presentar Solicitada/Ejecutada/Pendiente de ejecutar con códigos técnicos en detalle, conteos de ER/ledger separados y explicación de cancelación; comprobar 10 solicitadas, 7 ejecutadas, 0 pendientes y 3 sin ejecutar, y probar formato exacto de grandes IDs/decimales sin convertirlos a Number del navegador.
- [ ] 2.4 Ofrecer snapshots anteriores por paso, comparación antes/después y consulta actual separada; mostrar salidas y casos de recuperación/cuarentena desde datos estructurados. Verificar los contenidos de los ocho escenarios contra la matriz del diseño, con observaciones parciales o desactualizadas identificadas sin aprobar pendientes.
- [ ] 2.5 Conservar selección, foco, desplegables y contexto ante refresco, cambios de escenario y respuestas tardías; comprobar en navegador navegación por teclado, payload como texto inerte y disposición a 1440 px y 375 px, incluyendo cancelación y recarga de un recorrido paso a paso.

## 3. Herramientas avanzadas y documentación del recorrido

- [ ] 3.1 Agrupar Mensajes, Órdenes y Sistema bajo Herramientas avanzadas conservando publicación/republicación, paginación, diagnóstico y controles; comprobar sus hashes anteriores, volver a la ficha sin perder selección/run y rechazo de mutaciones concurrentes, con las pruebas de consola existentes y revisión en navegador.
- [ ] 3.2 Actualizar sólo las instrucciones breves de evaluación del README y agregar el párrafo sobre paquete demo, recursos web y demo.recovery como soporte auxiliar de UI/escenarios/ensayos; verificar que describe la navegación implementada, distingue instrumentación de reglas de negocio y mantiene la tabla de features y F-08 sin afirmaciones anticipadas.

## 4. Integración y evidencia de entrega

- [ ] 4.1 Formatear el Java editado con `.\mvnw.cmd spotless:apply` y ejecutar `.\mvnw.cmd verify` con Docker; registrar resultado y conteos observados sin omitir pruebas. Si falta una dependencia, conservar el comando y bloqueo sin declarar implementación verificada.
- [ ] 4.2 Reconstruir la consola en el entorno de demo correspondiente y ejecutar secuencialmente `docker compose --profile tools run --build --rm verify-console`, ampliando ese runner sólo con comprobaciones del nuevo contrato si hacen falta; observar ocho escenarios aprobados, evidencia por corrida y restauración de dos consumidores, conservando datos y sin operar sobre proyectos ajenos.
- [ ] 4.3 Consolidar en verification.md de este change la revisión probada, comandos, resultado del smoke y QA de navegador: escenario 1 sin salir de ficha, duplicados, abort, snapshots, persistencia, settlement, caídas/cuarentena, herramientas avanzadas, recarga y 375 px. Enlazar la mejora de F-09 desde README sólo con esa evidencia, conservando estados existentes.
- [ ] 4.4 Ejecutar `openspec validate --all --strict` y `git diff --check`, revisar que los cambios ejecutables están limitados a la consola/demo y conservar commits locales coherentes; verificar que no se alteraron núcleo, contratos públicos de órdenes, esquema SQL, dependencias ni garantías y detenerse antes de otra feature.

## Workflow follow-up

- Archivar con el flujo OpenSpec correspondiente sólo después de completar y revisar implementación y evidencia; comprobar el enlace de README tras mover el change.
- No publicar ni hacer push sin autorización específica.
