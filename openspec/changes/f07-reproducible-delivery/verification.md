# Verificación F-07

## Revisión documental — 23 de octubre de 2026

README: corregidos el enlace al archivo F-06 y el nombre visible del PDF.
Comprobados todos los destinos locales y anclas del README: sin faltantes.
DECISIONS.md no requiere cambios: broker y secuencia en sección 1, dedup en 2,
motor en 3, coherencia estado/ledger en 3–4, errores en 7, settlement en 5,
recuperación en 6 y trade-offs/supuestos distribuidos en esas secciones.
La revisión no agrega garantías nuevas ni modifica evidencia histórica.

## Preparación

JDK 25.0.2 y Docker Compose v5.6.0. El motor Linux estaba detenido y se inició
con `docker desktop start`. Todos los contenedores previos estaban detenidos;
los puertos 8081/8082/8083/9092/15432 estaban libres. Proyecto del ensayo:
`max-capital-f07`. No se reutilizan sus bases ni sus volúmenes.

El usuario autorizó reconstruir las etiquetas de imagen compartidas conservando
las imágenes anteriores con etiquetas de respaldo, sin modificar contenedores ni
volúmenes previos. Esto habilita el Compose original sin overrides de imágenes.

## Estado público observado

Consulta anónima a la API GitHub: repositorio público, `private=false`, rama `main`.
URL: https://github.com/JuanchiTaveira/Max-Capital-Challenge
`git ls-remote origin refs/heads/main` devolvió
`d094b70d0c86a964a9d6ad04e6df352d36009ebd`.
La propuesta `c11cb78` y los commits de este cierre son locales. No hubo push.

## Aceptación local

Aceptación local completada con los resultados siguientes. La publicación de los
commits locales queda separada y pendiente de autorización.

## Clon y arranque observados

Candidato: `f0420af33ee878ad8188b005d45d77a44e0ccbc5`.
Clon local con historial: `git clone --no-hardlinks . target/f07-clean`.
Checkout inicialmente limpio, sin `target` ni `.env` dentro del clon.
Ruta: `D:/Programacion/Entrevista_Max_Capital/target/f07-clean`.
Docker Engine 29.8.2. Las tres imágenes anteriores tienen respaldo `f07-before-build`.
El historial remoto observado es ancestro del candidato; sus dos commits adicionales
son `c11cb78` y `f0420af`, ambos documentales.

Desde el clon:

```sh
docker compose -p max-capital-f07 config --quiet
docker compose -p max-capital-f07 up -d --build --wait
```

Ambos comandos terminaron con código 0. Sin overrides ni copias de archivos ignorados.
Se permite caché Docker: esto prueba suficiencia de archivos versionados, no build
sin caché ni reproducibilidad binaria. HTTP info identificó app-1/app-2; readiness
saludable. Kafka Admin mostró dos consumidores app-1-0/app-2-0, con particiones 0/1.
`kafka-topics.sh --describe` confirmó los tres topics de negocio de dos particiones.
La consola abrió en navegador en localhost:8083 y mostró catálogo, conclusión del
escenario intercalado aprobado y tabla Esperado/Observado con conteos 4/3.

## Maven del candidato

`.\mvnw.cmd verify` desde el clon: código 0, BUILD SUCCESS, 5:59 minutos.
21 unitarias y 57 integraciones; cero fallas, errores u omisiones.
Incluye Spotless, Testcontainers, nueve pruebas SettlementKafkaIT y seis
RecoveryKafkaIT. Es resultado nuevo del SHA candidato, no copia de F-06.
Log de apoyo: `target/f07-maven.log` del checkout de trabajo.

## Primer smoke

`docker compose -p max-capital-f07 --profile tools run --build --rm verify-console`:
código 0, `CONSOLE OK run=216fd321-6248-45d5-8dc4-8e57176d3061
scenarios=8 pending=0 previousOrders=0`.
Los ocho scenarioResults quedaron PASSED: interleaved-orders, quantity-snapshots,
terminal-duplicates, restart-persistence, committed-input-only, settlement-once,
commit-boundary-recovery y poison-recovery. El runner completó además caída de
app-1, PostgreSQL/Kafka, restauración y comprobación de datos.
Log de apoyo: `target/f07-console-1.log`.

## Matriz de entregables

| Entregable del PDF | Evidencia de F-07 |
| --- | --- |
| Compose con dos apps, broker y persistencia | Arranque del clon con código 0, identidades HTTP, topics y grupo inspeccionados. |
| Stream de ER y guía de escenarios clave | README y consola accesibles; verify-console ejercita intercalados, duplicados y caídas reales. |
| DECISIONS.md | Revisados los ocho temas; se conservan supuestos y límites implementados. |
| Tests focalizados | Maven verify: 21 unitarias y 57 integraciones, sin omisiones. |
| Instrucciones breves | Guía rápida observada en navegador; enlaces locales/anclas comprobados. |
| Repo público e historial | API anónima pública; remoto d094b70 es ancestro del candidato; publicación de commits locales pendiente. |

El resumen queda versionado aquí; los logs ignorados sólo amplían el diagnóstico.
No se afirma prueba de Linux/macOS como host ni pérdida física de volúmenes.
La evidencia histórica de F-01/F-06 conserva su fecha y alcance propios.

## Repetición y cierre

Mismo comando verify-console, segunda corrida: código 0,
`CONSOLE OK run=1f7fe7c9-f798-4280-b89f-7aeabe2b9675 scenarios=8 pending=0
previousOrders=25`. Sus ocho scenarioResults están PASSED. El runner comparó
las 25 órdenes previas sin cambios y completó restauración de dependencias/apps;
la consulta final a `/demo/api/system` confirmó dos miembros.
Log de apoyo: `target/f07-console-2.log`.

Los contenedores y volúmenes del proyecto F-07 quedan conservados para inspección.
El contenedor previo max-capital-challenge-app-1-1 conserva su imagen original
82846a3a57bb y estado exited. No se borraron datos, topics ni offsets previos.
El cierre posterior a f0420af modifica sólo README y artefactos de este change;
no cambian fuentes, configuración ejecutable, contratos ni DECISIONS.md.

Validación final: OpenSpec estricto 12 elementos aprobados, git diff --check limpio,
enlaces locales/anclas de README sin faltantes. F-08 y demás estados conservados.

Cierre documental conservado en 8dc4a60; el commit de seguimiento sólo actualiza
este registro y el checklist. Una eliminación del enlace final de recuperación en DECISIONS.md
apareció durante el trabajo y se preservó fuera de los commits de F-07.
