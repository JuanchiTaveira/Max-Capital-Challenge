# Design

## Context

El smoke F-02 mezcla generación de ER y administración Compose en PowerShell.
Se conserva su alcance; F-01 sigue siendo una herramienta de desarrollo independiente.

## Goals / Non-Goals

**Goals:** un comando Compose común a las tres plataformas, sin intérpretes locales,
con idénticas verificaciones observables y un lanzador Windows opcional.

**Non-Goals:** completar F-07, ensayar crashes durante un commit (F-05), publicar
settlements o sustituir la suite Maven.

## Decisions

- Servicio `verify-f02` en perfil `tools`, dependiente de ambas aplicaciones saludables.
  Python estándar y Docker CLI en imagen multiarch fijada por digest; no se instala
  nada en el host. Se evita distribuir variantes Bash y PowerShell del mismo smoke.
- Docker CLI usa el socket Linux del motor. Los contenedores se identifican por
  labels de proyecto y servicio; no se asumen nombres generados ni rutas del host.
  Se accede a HTTP por DNS interno y se imprimen URLs con los puertos del host.
- Se publican los mismos snapshots con el productor Kafka y el particionado estándar.
  Se comparan objetos JSON de ambas consultas y se esperan offsets confirmados.
- El `.ps1` sólo delega a Compose, preserva `-SkipBuild` y propaga exit status.
  El comando principal funciona desde cualquier shell con Docker disponible.
- README explica requisitos, acceso al socket, resultados, persistencia y diferencia
  entre reinicio posterior al consumo y caída durante procesamiento.

## Risks / Trade-offs

- Acceso al socket otorga control del motor al contenedor de prueba: uso local optativo,
  sin servidor expuesto; sólo opera sobre servicios del proyecto seleccionado.
- Rootless o socket no estándar: variable `SMOKE_DOCKER_SOCKET`; Docker remoto sin
  socket local queda fuera del comando ofrecido. Windows/macOS usan motor Linux.
- Se validará en Docker Desktop para Windows y el runtime Linux del contenedor;
  no se atribuirá ejecución observada en otros hosts o arquitecturas.

## Migration Plan

No hay migraciones SQL. El perfil optativo no altera el arranque normal. Restaurar
scripts/Compose revierte la herramienta sin modificar datos existentes.
