# Guía de trabajo

## Objetivo y alcance

Challenge backend de Max Capital: procesar ExecutionReports con secuencia por orden,
idempotencia y recuperación, ejecutando dos instancias. El README es la única fuente
del estado de features. Trabajá en la feature solicitada y sus dependencias indispensables;
al completarla, informá la evidencia y detenete antes de comenzar la siguiente.

## Fuentes y decisiones

- Consultá README.md para ejecución, estado y comandos.
- Consultá y actualizá DECISIONS.md con las decisiones importantes del núcleo,
  sus supuestos y límites. Reservá comandos y detalles operativos para README
  y OpenSpec; distinguí garantías implementadas de propuestas pendientes.
- Para implementar, leé los artefactos del change OpenSpec activo. Actualizalos si
  un hallazgo cambia el diseño; mantené sus criterios de aceptación verificables.
- Distinguí comportamiento implementado de arquitectura propuesta. No afirmes
  garantías que todavía no tienen pruebas.
- Resolvé detalles rutinarios dentro del alcance; preguntá por decisiones que
  cambien requisitos o comprometan datos ajenos.

## Arquitectura y código

- Java 25 sin preview, Spring Boot, Maven Wrapper, Kafka y PostgreSQL.
- Identificadores y commits en inglés; explicaciones y documentación en español.
- Dominio independiente de Spring, Kafka y JDBC. Aplicación coordina casos de uso;
  adaptadores resuelven transporte y persistencia. Creá abstracciones con necesidad real.
- Usá inyección por constructor, responsabilidades pequeñas y límites de transacción
  explícitos. No agregues UI, autenticación ni servicios ajenos al núcleo.
- Gestioná dependencias con el BOM; fijá plugins e imágenes. No uses tags `latest`.

## Invariantes del núcleo (F-02 en adelante)

- Preservar secuencia por numericOrderId; no paralelizar ER de la misma partición.
- Deduplicar ER individuales antes de evaluar transiciones terminales.
- Actualizar estado, contador y ledger atómicamente; snapshots no son deltas.
- Conservar errores y pendientes; no descartar mensajes silenciosamente.
- Una outbox por sí sola no demuestra publicación exactamente una vez. Verificá
  las fronteras SQL/Kafka y documentá qué observa un consumidor read_committed.

## Flujo de implementación

1. Revisá estado Git y spec de la feature; preservá cambios ajenos.
2. Implementá incrementos pequeños con las pruebas significativas correspondientes.
3. Marcá tareas completas sólo con comportamiento implementado y evidencia observada.
4. Actualizá documentación y tabla del README. Validá OpenSpec y archivá el change
   después de verificar todos sus criterios.
5. Conservá commits locales por unidad coherente. No publiques, hagas push ni
   elimines volúmenes o archivos ajenos sin autorización específica.

## Comandos

En PowerShell usá `.\mvnw.cmd`; en Linux/macOS, `./mvnw`.

- Formatear Java editado: `.\mvnw.cmd spotless:apply`.
- Comprobar formato: `.\mvnw.cmd spotless:check`; también corre en `validate`.
- Build y pruebas completas: `.\mvnw.cmd verify` (requiere Docker para Testcontainers).
- Ejecutar entorno: `docker compose up --build`.
- Smoke de dos instancias, fallas y persistencia: `pwsh -File scripts/verify-f01.ps1`.
- Validar specs: `openspec validate --all --strict`.

Spotless es la autoridad de formato Java: Google Style, dos espacios e imports
ordenados. Corregí formato antes de un commit. No agregues reglas manuales que
contradigan al formatter ni formatees código generado.

## Evidencia y entrega

Ejecutá pruebas proporcionales al riesgo. No agregues tests de código trivial ni
porcentajes arbitrarios de cobertura. Si Docker u otra dependencia impide verificar,
reportá el comando y el motivo; no saltees tests silenciosamente ni marques la
feature como implementada. Un build de imagen omite pruebas: no reemplaza `verify`.
Usá Conventional Commits y describí brevemente qué cambió, por qué y cómo se probó.
