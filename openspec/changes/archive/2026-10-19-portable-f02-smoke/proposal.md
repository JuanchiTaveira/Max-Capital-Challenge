# Proposal

## Why

El smoke de F-02 exige `pwsh`, que no viene con Windows PowerShell ni con Linux/macOS.
El evaluador debe poder reproducirlo con Docker y Compose como únicos requisitos locales.

## What Changes

- Ejecutar el smoke de F-02 en un servicio optativo de herramientas con imágenes fijadas.
- Mantener secuencia, duplicados, consultas en dos instancias y replay tras reinicio.
- Convertir el `.ps1` en un lanzador compatible con Windows PowerShell 5.1 y PowerShell 7.
- Documentar comandos comunes y límites observados, sin dar por completas F-05/F-07.

## Capabilities

### New Capabilities

Ninguna.

### Modified Capabilities

- `repository-workflow`: smoke F-02 reproducible sin runtimes instalados en el host.

## Impact

Compose, scripts, imagen de herramientas y README. Sin cambios de Java, APIs ni datos
de negocio. El contenedor necesita acceso al socket Docker para publicar mediante
el contenedor Kafka y reiniciar las aplicaciones del proyecto seleccionado.
