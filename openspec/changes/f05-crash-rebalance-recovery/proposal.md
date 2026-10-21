# Proposal

## Why

F-04 ya tiene evidencia de caída después de SQL, reasignación y commit Kafka incierto,
pero faltan caídas y rebalanceos con SQL abierto, reinicio abrupto de ambas instancias
y reproducción guiada por consola. F-05 debe cerrar esas fronteras y explicar sus
estrategias sin confundir controles manuales o cierres ordenados con recuperación probada.

## What Changes

- Completar pruebas deterministas con procesos reales para rollback tras caída,
  cambio de propietario durante SQL y recuperación después de terminar ambas instancias.
- Conservar resultados por posición, secuencia por orden y commit transaccional de
  settlement/progreso; corregir el núcleo sólo si la nueva evidencia detecta una brecha.
- Habilitar `settlement-once` y `commit-boundary-recovery` desde la consola en modos
  automático y paso a paso, con barreras observables y verificaciones capturadas.
- Incorporar instrumentación acotada de demo, deshabilitada por defecto y activada
  explícitamente en Compose, sin comandos libres ni cambios de negocio por HTTP.
- Restaurar barreras, consumidores e instancias al terminar, cancelar o fallar;
  conservar datos y diagnósticos y no declarar éxito ante observaciones incompletas.
- Documentar las estrategias, supuestos y límites en `DECISIONS.md`; actualizar
  instrucciones, catálogo y evidencia en README sólo después de verificar F-05.

## Capabilities

### New Capabilities

Ninguna. La recuperación pertenece a consumo y publicación existentes; los
controles de ensayo y sus recorridos amplían la consola existente.

### Modified Capabilities

- `execution-report-consumption`: recuperación con SQL abierto, reasignación durante
  escritura y reinicio abrupto de ambas instancias, conservando secuencia y progreso.
- `settlement-publication`: ampliar recuperación a caída antes del commit SQL y
  después del envío aceptado, y comprobar continuidad tras recuperar ambas instancias.
- `demo-scenarios`: habilitar ambos escenarios, verificar sus fronteras y efectos,
  actualizar siete disponibles/un pendiente y restaurar el entorno ante interrupciones.
- `evaluation-console`: controlar y observar barreras de demo acotadas, y distinguir
  evidencia de falla, progreso estable y resultados históricos en los recorridos nuevos.

## Impact

Adaptadores JDBC/Kafka y wiring de demo, `ScenarioCatalog`, `ScenarioSteps`,
`ScenarioRunner`, `ScenarioGuidance`, `DemoRun`, acceso interno a instancias y recursos
de consola afectados por sus resultados. Configuración de demo/Compose y runners
portables, pruebas SQL/Kafka/procesos y de consola, README y DECISIONS.

Se reutilizan Java 25, Spring Boot, Kafka y PostgreSQL actuales. No se propone una
migración SQL, cambio del contrato ER/settlement v1 ni reset de offsets. F-06,
reintentos automáticos, alta disponibilidad, pérdida de volúmenes y efectos externos
del consumidor downstream quedan fuera. La actualización de DECISIONS y el código
se realizarán durante apply; esta propuesta no cambia el estado de F-05 en README.
