# Evidencia de portabilidad del smoke F-02

Fecha: 2026-10-19. Host Windows, Windows PowerShell 5.1.26100.9711,
Docker Desktop con motor Linux 29.8.2. Runner Python 3.13.8 y Docker CLI 29.0.0
ejecutados dentro de la imagen fijada. Sin cambios en Java ni en el esquema SQL.

## Verificaciones observadas

| Verificación | Resultado |
| --- | --- |
| `docker compose --profile tools run --build --rm verify-f02` | Exit 0, construcción de aplicaciones y runner, servicios saludables, secuencia y duplicados, offsets confirmados y replay tras reinicio |
| Órdenes de la ejecución Compose | `179243027228300`: FILLED, contador/ledger 4; `179243027228320`: CANCELLED, contador/ledger 3 |
| `powershell -NoProfile -File scripts/verify-f02.ps1 -SkipBuild` | Exit 0 bajo Windows PowerShell 5.1, sin invocar `pwsh`; mismo smoke completo |
| Órdenes de la ejecución Windows PowerShell | `179243031920702`: FILLED, contador/ledger 4; `179243031920720`: CANCELLED, contador/ledger 3 |
| Lanzador con `DOCKER_CONTEXT` inexistente | Exit 1 y diagnóstico Docker; no se inicia el smoke |
| Runner con `DOCKER_HOST=unix:///missing-smoke.sock` y `run --no-deps` | Exit 1, `F-02 FALLÓ`, sin publicar ER ni reiniciar servicios |
| Runner con `COMPOSE_PROJECT_NAME=smoke-nonexistent-project` y `run --no-deps` | Exit 1 antes de publicar; no toma Kafka del proyecto real |
| `docker compose config --services` | Sólo postgres, kafka, kafka-init, app-1 y app-2; tools no se activa por defecto |
| `docker compose -p smoke-config-check --profile tools config --format json` | La variable del runner resuelve el proyecto alternativo, sin arrancarlo |
| `docker compose config --quiet` | Exit 0 |
| `openspec validate --all --strict` | Exit 0 |
| `git diff --check` | Exit 0 |

Las consultas de ambas instancias se compararon como objetos JSON y el ledger completo
anterior al reinicio se comparó con el posterior. Se conservaron las órdenes y los
volúmenes; los contenedores temporales del runner se retiraron con `--rm`.

## Límites

No se ejecutó en hosts Linux/macOS ni en ARM. El comando compartido ejecuta la misma
imagen Linux sin dependencias de shell del host; las imágenes base declaran variantes
amd64/arm64. La ejecución observada fue Windows amd64 con contenedores Linux.

No se ejecutó Maven `verify`: este cambio sólo modifica tooling y documentación;
los builds de imagen usan `-DskipTests` y no se presentan como ejecución de la suite.
El smoke sí corrió contra Kafka, PostgreSQL y dos aplicaciones reales.
F-01 mantiene su herramienta PowerShell 7 de desarrollo. F-05/F-07 permanecen
pendientes: reiniciar después de consumir no prueba un crash durante un commit.

La primera ejecución del nuevo runner se interrumpió antes de publicar ER para adaptar
la lectura de las columnas adicionales de epochs del CLI Kafka 4.2.1. Las dos
ejecuciones completas exitosas usaron el parser corregido.
