# Verificación de evaluation-console

Fecha: 2026-10-20. Windows/PowerShell, Java 25.0.2, Maven Wrapper 3.9.11,
Docker Engine 29.8.2 con contenedores Linux y Compose. Pruebas reales de Kafka
4.2.1 y PostgreSQL 17; sin omisiones por ausencia de Docker.

## Comandos y resultados

| Comando | Evidencia observada |
| --- | --- |
| `.\mvnw.cmd spotless:apply` | Formato aplicado al Java editado antes de los commits |
| `.\mvnw.cmd spotless:check` | BUILD SUCCESS; comprobación final con 53 archivos limpios |
| `.\mvnw.cmd verify` | BUILD SUCCESS; 7 pruebas unitarias y 36 de integración, cero fallos, errores u omisiones; 1:43 min |
| `docker compose config` y builds de ambos targets | Puerto 8083 ligado a loopback; app con usuario 10001 sin Docker CLI; consola con CLI 29.0.0 fijada y socket |
| `docker compose up -d --build --wait` | Aplicaciones, infraestructura y consola saludables; imágenes construidas desde las fuentes |
| `docker compose --profile tools run --build --rm verify-f02` | F-02 OK; FILLED/contador 4 y CANCELLED/contador 3, dos instancias y reinicio real con persistencia |
| `docker compose --profile tools run --build --rm verify-console` | CONSOLE OK; cinco escenarios aprobados, tres pendientes excluidos, 26 órdenes previas conservadas |
| `node --check src/main/resources/demo/console.js` | Sintaxis válida |

Los smokes se ejecutaron secuencialmente, sin borrar volúmenes, topics, offsets
ni datos. El smoke de consola utilizó únicamente su API HTTP para controles y
escenarios; terminó con ambos consumidores y una partición por instancia.
Run del smoke: `b414a798-a8d7-4617-a5ae-ff051a1ef731`.

Los logs de esta ejecución quedaron en `target/console-full-verify.log`,
`target/console-f02-smoke.log`, `target/console-smoke.log` y
`target/console-final-format.log`; son evidencia local ignorada por Git.
Los comandos y las pruebas versionadas permiten repetir la verificación.

## Correspondencia con evaluation-console/spec.md

| Escenario de aceptación | Evidencia |
| --- | --- |
| Empty environment | Navegador con fixture de respuestas vacías y recursos reales: órdenes sin filas y broker vacío muestran instrucciones, sin error. No se vació el entorno persistente para esta revisión visual |
| Temporary observation failure | `ConsoleOperationsIT` verifica fuente desconocida, caché stale y fecha conservada; UI tras stop de app-1 mostró desconocido y última observación; smoke con SQL/Kafka caídos |
| Inspection while applications consume | `DemoPublishingIT` comprueba inspección sin agregar miembros ni alterar offsets; `EvaluationConsoleRuntimeIT` observa exactamente dos consumidores |
| Aborted transaction | `DemoPublishingIT` verifica huecos/cursor y posición abortada; escenario real `committed-input-only` observa el sucesor confirmado y ausencia SQL del parcial |
| Output topics without implemented publishers | Navegador mostró settlements vacío con instrucciones; catálogo exhibe pendientes deshabilitados; lectura acotada de los tres topics por la misma API |
| Existing order | `EvaluationConsoleQueriesIT`, escenario intercalado y revisión de orden/ledger desde enlaces del navegador |
| Unknown or invalid identifier | `EvaluationConsoleQueriesIT` conserva respuestas 404/400 y prueba identificadores fuera del contrato |
| Late duplicate of NEW | Integraciones de consultas/publicación/escenarios; navegador inspeccionó `execution-reports/1/53`: NEW republicado, resultado DUPLICATE con snapshot FILLED, contador 2 y referencia ledger 73 |
| Record without result | Integración de consultas verifica `result: null`; smoke confirma ausencia de resultado del registro abortado; texto de UI distingue ausencia de fallo confirmado |
| Values outside browser safe integer range | Integraciones de consultas/publicación y publicación manual real desde navegador detallada abajo |
| Accepted report | Navegador publicó NEW y FILLED con metadata Kafka real y resultado APPLIED separado; `DemoPublishingIT` correlaciona posición y SQL |
| Invalid draft | Integración rechaza JSON/key/identidad/transición conocidos; navegador envió `{`, vio el error y mantuvo fin de partición en 46 |
| Broker acknowledgement unavailable | `DemoPublishingIT` usa broker inaccesible y verifica falta de metadata inventada; no hay reenvío automático de aplicación |
| Terminal republication | Navegador republicó FILLED: DUPLICATE y contador/ledger 2; integración conserva key/payload exactos y rechaza posición abortada/inexistente |
| HTTP alive and consumer stopped | Runtime real detiene sólo el listener y conserva HTTP UP; agregador y UI usan fuentes separadas |
| Instance unavailable | UI tras detener app-1 conservó navegación, SQL y diagnóstico desconocido; smoke verificó kill/start real |
| Large history or hostile report text | Paginación en consultas y Kafka, cursores y límites; navegador mostró `<b>solo texto</b>` literalmente; revisión de pausa, teclado, foco y ancho 375 px |

## Correspondencia con demo-scenarios/spec.md

| Escenario de aceptación | Evidencia |
| --- | --- |
| Current implementation boundary | Catálogo de ocho entradas: cinco disponibles y tres pendientes con dependencia; `DemoScenariosIT` y navegador |
| Pending execution request | `DemoScenariosIT` y smoke reciben 409; botones pendientes deshabilitados; conteo excluido 3 |
| Step by step execution | Integración recupera por ID; navegador recargó la ejecución en WAITING y conservó URL/modo Paso a paso; continuó pasos e inspeccionó NEW |
| Failed assertion or timeout | `DemoScenariosIT` impone deadline y comprueba última observación, FAILED y datos conservados; falla de reinicio controlada comprueba restauración |
| Interleaved FILLED and CANCELLED | Lote real verifica particiones distintas, ambas asignaciones, cantidades, contador 4/3, ledger, resultados y ambas consultas HTTP; smokes F-02/consola |
| Accumulated quantities are snapshots | Lote real NEW/parciales 3 y 7/FILLED 10 con timestamps no crecientes; verifica cada snapshot, ledger y contador 4 |
| Terminal and late NEW duplicates | Lote real: dos APPLIED, dos DUPLICATE, dos ledgers, snapshots NEW original y FILLED tardío con referencia común |
| Persisted effects after restart | Integración controla coordinación/restauración con adaptador de reinicio sustituido; ambos smokes y navegador ejecutan reinicio Docker real después del lote y comparan estado/ledger completos |
| Aborted partial report | Lote real confirma NEW, aborta parcial y confirma FILLED de la misma partición; sólo dos efectos SQL, sin resultado/ledger abortado |
| Repeated or concurrent execution | Integración y repeticiones en navegador/smoke usan órdenes nuevas; POST concurrentes reciben 409 y UI deshabilita mutaciones |
| Cancellation after publication | Integración y navegador cancelan después de NEW confirmado; orden `3034193430705743335` conserva NEW, contador 1 y ledger 61 |
| Abrupt application termination | Smoke llama kill/start de app-1 por API real y consulta UI/SQL durante la caída; no atribuye recuperación en fronteras de commit |
| Stop and start a dependency | Smoke detiene/inicia PostgreSQL y Kafka, comprueba 503 de su consulta y disponibilidad de la fuente independiente; restaura apps y dos consumidores |
| Stopped, foreign or ambiguous target | `DockerAccessTest` prueba detenido/ajeno/ambiguo y vectores permitidos; smoke inicia contenedores detenidos y rechaza servicio ajeno |
| Unavailable Docker socket | `ConsoleOperationsIT` verifica error explícito y UI accesible sin llamar controles exitosos ficticios |
| Foreign origin or arbitrary command | Integración rechaza antes de llamar adaptadores; smoke recibe 403 de origen ajeno y 400 de servicio fuera de lista |

## Correspondencia con runtime-environment/spec.md

| Escenario de aceptación | Evidencia |
| --- | --- |
| Clean startup | Builds Compose con targets fijados, healthchecks saludables y puerto loopback; runtime Testcontainers crea infraestructura aislada y dos aplicaciones. No se reemplazó el checkout ni se borraron volúmenes para simular un clon limpio |
| Exactly two processing consumers | `EvaluationConsoleRuntimeIT` inicia dos apps y consola reales y verifica grupo/asignaciones; consola no registra listener/Flyway ni migra base vacía; smokes observan dos miembros al estabilizar |
| Console remains available during outages | Runtime inicia consola con SQL/Kafka indisponibles; smoke comprueba UI/API en caídas reales de app, PostgreSQL y Kafka |

## Recorrido visual y datos conservados

Se siguió la guía del README desde un navegador, sin cliente SQL/Kafka ni comandos
adicionales para operar los escenarios. Run visual automático
`20308370-eb88-4e43-8f80-81dc00482ae1`: 5/5 aprobados, 3 pendientes excluidos.
Se inspeccionaron orden, ledger, posiciones y duplicado tardío. Se verificaron las
cuatro vistas, teclado, foco visible, pausa/reanudación, ventana estrecha de 375 px
y stop/start manual de app-1 con resultado Docker observado.

Publicación manual visual: orden `1792518009967270166`, fixIds
`9007199254740993` y `9007199254740994`, cantidad `999999999999.12345678`.
NEW/FILLED en partición 1 offsets 43/44 y republicación terminal en 45.
Payload, key, detalle y ledger conservaron esos valores; marcado HTML adicional
se mostró como texto. No se usó `Number` para representar esos identificadores.

El historial de runs es volátil y acotado a 50; reconstruir/reiniciar la consola
puede perder sus IDs de coordinación. Kafka y SQL conservaron la evidencia.
El smoke preservó las 26 órdenes encontradas al iniciar. Además, se compararon
antes/después las 13 órdenes existentes al comenzar este trabajo, filtrando sus
IDs originales. Hashes sin cambios:

| Datos originales | MD5 del conjunto ordenado de filas |
| --- | --- |
| orders | `1e530495c41eb2b5ccf5e0a1391a6ad6` |
| execution_report_ledger | `65cbf18a5a8acf6fe3383114b2c9c6bc` |
| processing_results | `8d0d2f63e0349f3cfd557a816a7a3f9f` |

## Límites de la evidencia

F-04, F-05 y F-06 permanecen pendientes. El kill manual y reinicio posterior al
lote no sitúan una caída entre commits, y visibilidad transaccional de entrada
no demuestra settlement exactamente una vez. El preflight no elimina carreras
con productores externos. No se publicaron mensajes inválidos intencionales al
grupo compartido. Las asignaciones son actuales; los logs son diagnóstico temporal.
