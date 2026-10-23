# Verification

Fecha: 2026-10-23. Cambio sobre `343661a`, con modificaciones locales de consola/demo.
Implementación conservada en el commit local `c03b87a`.

## Automated checks

- `.\mvnw.cmd spotless:apply`: formato aplicado al Java editado.
- `.\mvnw.cmd verify`: BUILD SUCCESS; 26 pruebas unitarias y 57 de integración, sin fallas, errores ni omisiones.
- `node --check src/main/resources/demo/console.js`: correcto.
- `python -m py_compile scripts/verify-console.py`: correcto.
- `openspec validate --all --strict`: 12 elementos válidos.
- `git diff --check`: correcto.

## Browser review

- Seleccionar un escenario abre explicación y controles; no inicia el recorrido.
- Escenario 1 aprobado: siete envíos, órdenes A/B, ledgers 4/3 y cantidades visibles dentro de la ficha.
- Cancelación de orden: 10 solicitadas, 7 ejecutadas, 0 pendientes y explicación de 3 sin ejecutar.
- Duplicado tardío NEW: resultado DUPLICATE, snapshot FILLED y referencia al ledger del NEW original.
- Entrada abortada: marcada como envío abortado, sin resultado SQL ni ledger; explicación de invisibilidad read_committed.
- Historial de cantidades: snapshots 0, 3, 7 y 10 seleccionables, con paso y hora.
- Mensaje seleccionado y payload desplegado conservados al refrescar; volver desde Mensajes conserva corrida y selección.
- Herramientas avanzadas: hashes `#messages`, `#orders` y `#system`, consultas disponibles y publicación deshabilitada durante una corrida.
- Vista a 375 px: se corrigió el desbordamiento de ledger con contención horizontal local. Revisión final: ancho de documento 360 px y scrollWidth 360 px (viewport de 375 px con barra vertical).
- Teclado: Tab mantiene foco visible; Enter sobre un ledger selecciona su mensaje relacionado.
- Consulta actual separada: estado FILLED, hora propia y cantidades sin reemplazar el snapshot guardado.
- Settlements: posición propia, posición de entrada y rango de lectura read_committed visibles.
- Cuarentena en la versión final: corrida `1495fccc-a5f0-4208-8dce-70b7981c4b0c` aprobada; auditoría APPLIED, posición original y campos del ER original visibles como tablas, con payload original y ER efectivo desplegables.

## Compose smoke

`docker compose -p max-capital-f07 --profile tools run --build --rm verify-console`:
código 0, `CONSOLE OK run=ea7cece5-92bd-466f-8496-c82c44cba06f scenarios=8 pending=0 previousOrders=79`.
Restauró PostgreSQL, Kafka y los dos consumidores; conservó las 79 órdenes anteriores.
Se usó el proyecto existente de este repositorio para respetar los puertos y volúmenes.

La primera corrida aprobó los ocho escenarios, pero una aserción nueva comparaba
también las horas de lectura del reinicio. Se corrigió para comparar el estado y
ledger sin metadatos de observación y se repitió el smoke completo con éxito.

La recarga de una corrida paso a paso conservó los siete envíos y retomó en la espera
de verificación SQL. Cancelar conservó los mensajes y dejó pendientes sin aprobar.
Los payloads se construyen como texto con textContent, sin interpretarse como HTML.

## Scope review

Los cambios ejecutables se limitan a demo, recursos de consola y su runner de smoke.
No cambian dominio, contratos públicos de órdenes, esquema SQL, dependencias ni
fronteras transaccionales. La evidencia de salidas proviene de lecturas read_committed;
el marcador SQL de notificación se distingue del registro Kafka efectivamente leído.

Se preserva el cambio ajeno de DECISIONS.md y los volúmenes del entorno `max-capital-f07`.
