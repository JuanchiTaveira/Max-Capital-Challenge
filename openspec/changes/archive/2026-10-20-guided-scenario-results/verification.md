# Verificación de guided-scenario-results

Fecha: 2026-10-20. Windows/PowerShell, Java 25.0.2, Docker Engine 29.8.2,
Kafka 4.2.1 y PostgreSQL 17 reales. Sin dependencias nuevas ni migraciones.

## Pruebas automatizadas

| Comando | Resultado observado |
| --- | --- |
| `.\mvnw.cmd spotless:apply spotless:check` | BUILD SUCCESS; 54 archivos Java limpios |
| `.\mvnw.cmd verify '-Dit.test=DemoScenariosIT'` | Cuatro integraciones del runner aprobadas; Kafka/SQL y dos aplicaciones reales, control de reinicio sustituido en el fixture |
| `.\mvnw.cmd spotless:apply spotless:check verify` | BUILD SUCCESS; 7 pruebas unitarias y 36 de integración, cero fallos/errores/omisiones; 1:48 min |
| `node --check src/main/resources/demo/console.js` | Sintaxis válida |

Logs locales ignorados: `target/guided-scenarios-it.log`,
`target/guided-final-verify.log`, `target/guided-final-format.log`.
Los comandos y las integraciones versionadas permiten repetir las comprobaciones.

Se ampliaron las integraciones existentes para verificar el catálogo, cinco
resultados agrupados, todos los checks PASSED con observación/fecha/referencias,
correlación de mensajes con pasos de su escenario y snapshots históricos NEW/FILLED
distintos. Los campos anteriores conservan 27 mensajes y conteos del lote.
El timeout verifica observado nulo, diagnóstico real y futuros escenarios NOT_RUN;
la cancelación conserva la creación verificada y checks futuros NOT_OBSERVED.
El reinicio fallido conserva comprobaciones anteriores y restaura servicios propios.

## Correspondencia con los escenarios de aceptación

| Spec / escenario | Evidencia |
| --- | --- |
| demo-scenarios / First individual scenario | Navegador abre Dos órdenes en paralelo con objetivo, secuencia y resultado previsto; ejecución individual aprobada `c4d4351b-b073-49cd-91a9-dfa465d50e67`; siguiente conserva runId, selecciona cantidades y no ejecuta |
| demo-scenarios / Five explained results | Lote desde navegador `77f556bc-1d3d-409d-a6e9-0074f8b55e22`, 5/5 aprobados; se inspeccionaron las cinco tablas y conclusiones sin abrir JSON; integraciones verifican checks, referencias y snapshots históricos |
| demo-scenarios / Partial or unavailable observation | Navegador cancela tras crear una orden y conserva 1 mensaje aplicado; timeout real con app-1 detenida termina FAILED sin órdenes emitidas, diagnóstico de 1 consumidor y comprobaciones restantes sin verificar; integraciones cubren futuros NOT_RUN y restauración |
| evaluation-console / Individual guided progression | Selección inicial, explicación previa, ejecución y siguiente sin publicación comprobados; el foco termina en el título del escenario siguiente |
| evaluation-console / Batch navigation and reload | Resultados agrupados por IDs, navegación entre los cinco; recarga del modo manual conserva runId, escenario, modo y propósito del próximo paso; no se deducen resultados desde títulos o errores |
| evaluation-console / Accessible narrow viewport | Revisión real con iframe de 375 px: una columna y comparaciones verticales con etiquetas; navegación por teclado entre las cuatro vistas y foco visible; pausa/reanudación y lectura de IDs/cantidades exactos |

## Recorrido visual y fallas

- Cancelación visual: `745a8dca-bee3-424c-b9a2-46b68882910c`, duplicados en modo
  manual. Se recargó antes de continuar, se ejecutaron entorno y creación y se canceló
  antes del terminal. La tabla mantuvo creación verificada, una entrada de historial
  y comprobaciones restantes sin verificar; la conclusión no declaró éxito.
- Precondición fallida visual: `8db02d4d-432d-4ea7-8c7b-5874ead5a0ea`.
  app-1 se detuvo e inició desde Sistema. El plazo real terminó con FAILED,
  cero órdenes y diagnóstico «Esperados dos miembros del grupo; observados 1».
  UI, SQL y otras vistas siguieron disponibles. La ausencia de observación no
  se presentó como un valor obtenido ni como aprobación de la prueba.
- Los enlaces de Evidencia técnica se recorrieron hacia un mensaje NEW, su
  resultado APPLIED histórico y ledger 157, y luego hacia la orden actual
  `7875424670384523257`, completada con contador 4. El snapshot original continuó
  NEW/contador 1. No se sustituyeron observaciones históricas al navegar.
- Se consultó la orden previa `1792518009967270166`: ID y cantidad
  `999999999999.12345678` intactos. Se conservaron las vistas de Mensajes,
  Órdenes y Sistema; la guía revisada del README coincide con los controles.
- La revisión estrecha utilizó un servidor temporal ligado a loopback y el
  contenido real de la consola dentro del iframe. Se cerró la pestaña y se detuvo
  el helper; no se incorporaron fixtures ni herramientas al runtime de producto.

## Smoke real y conservación

`docker compose --profile tools run --build --rm verify-console` terminó con
CONSOLE OK, run `8a27f52f-5b71-4e21-81ef-a94c52e4c789`: cinco escenarios aprobados,
tres pendientes excluidos y 49 órdenes previas conservadas. El smoke comprueba
campos anteriores y nuevos, observaciones/referencias por escenario, asociación de
mensajes con sus pasos, kill/start de app-1 y stop/start de PostgreSQL/Kafka. Al
terminar restauró los servicios y observó dos consumidores, una partición por instancia.
Log local: `target/guided-console-smoke.log`; snapshot: `target/guided-smoke-run.json`.

Además se compararon por SQL las 37 órdenes existentes antes del despliegue de
este trabajo, filtrando sus IDs originales. Las filas de estado, ledger y resultados
conservaron exactamente los hashes del conjunto ordenado:

| Conjunto original | MD5 antes y después |
| --- | --- |
| orders | `488da07f6b0e25b5de559c2f42015da8` |
| execution_report_ledger | `b25e98f5fea8a74475ce7ebb1ee3396f` |
| processing_results | `59a945c6669fd6f1824b4c4c6b36e1d5` |

`openspec validate --all --strict` y `git diff --check` completaron sin errores.
La tabla del README mantiene F-09 implementado y F-04 a F-06 pendientes; el cambio
mejora su presentación y evidencia, sin avanzar a otra feature.

## Alcance

El historial de runs sigue siendo volátil y acotado; reconstruir la consola pierde
la coordinación, mientras SQL y Kafka conservan datos. F-04 a F-06 permanecen
pendientes. La presentación no agrega garantías de settlement, fronteras de commit
ni recuperación de mensajes permanentes; la transacción abortada es entrada prevista.
