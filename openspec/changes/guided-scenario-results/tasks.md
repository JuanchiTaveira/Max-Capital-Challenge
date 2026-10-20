# Tasks

## 1. Contratos y evidencia del servidor

- [ ] 1.1 Ampliar catálogo con los cinco nombres, explicación y acciones previstas; verificar catálogo y contratos previos en integración.
- [ ] 1.2 Incorporar agrupación por escenario, IDs estables y checks esperado/observado/referencias capturados por las aserciones; verificar los cinco escenarios con Kafka/SQL reales y conservar campos del smoke.
- [ ] 1.3 Conservar checks parciales y distinguir no observado, fallo, cancelación y no ejecutado; ampliar integración de timeout/cancelación/restauración y documentar contratos en design.md.

## 2. Recorrido del evaluador

- [ ] 2.1 Implementar selector, explicación previa y pendientes separados, ejecución individual inicial y siguiente sin envíos; verificar en navegador navegación y selección.
- [ ] 2.2 Mostrar conclusión, tabla esperado/observado y pasos por escenario, controles manuales y evidencia desplegable; verificar cinco resultados, lote, recarga, cancelación y fallo de precondiciones sin necesitar JSON.
- [ ] 2.3 Aplicar presentación responsive y accesible preservando otras vistas; verificar teclado, foco, pausa, ancho 375 px y enlaces a órdenes/mensajes; actualizar guía README y comprobar recorrido.

## 3. Verificación integrada

- [ ] 3.1 Ejecutar spotless:apply, spotless:check y verify con Docker; comprobar éxito sin omisiones y registrar evidencia por criterio en verification.md.
- [ ] 3.2 Ejecutar smoke de consola real, comprobar compatibilidad, restauración y conservación de datos; validar openspec --all --strict y git diff --check, revisar documentación y commits locales.

## Workflow follow-up

- Sincronizar specs y archivar tras verificar todos los criterios; validar nuevamente.
- Conservar cambios ajenos y datos; no push; detenerse sin implementar F-04 a F-06.
