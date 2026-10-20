# Proposal

## Why

Hoy evaluar el challenge exige combinar comandos Kafka, consultas HTTP, SQL y
reinicios manuales. Una consola local de evaluación permite publicar ER, observar
su efecto real y repetir los escenarios disponibles desde el navegador, manteniendo
visibles las garantías que todavía dependen de F-04 a F-06.

## What Changes

- Incorporar el servicio independiente `evaluation-console` en Compose, accesible
  en `http://localhost:8083`, con HTML, CSS y JavaScript sencillos y cuatro vistas:
  escenarios, mensajes, órdenes y sistema.
- Mostrar registros Kafka con key, partición, offset y contenido completo;
  correlacionarlos con resultados persistidos, snapshots, estado actual y ledger.
  Incluir lectura de los topics de settlement y rechazos sin atribuir garantías
  a su existencia o a que estén vacíos.
- Permitir publicar ER válidos y republicar un registro conservando key y payload,
  usando Kafka como única vía de ingreso y preservando identificadores y decimales.
- Ejecutar escenarios reproducibles de F-02/F-03, automáticamente o paso a paso,
  mostrando resultados esperados, observaciones, verificaciones y diagnóstico.
- Mostrar salud HTTP, estado real de los consumidores, asignaciones y progreso
  por partición. Permitir iniciar, detener y reiniciar los servicios locales
  autorizados, y terminar abruptamente una instancia desde la UI.
- Identificar y deshabilitar los escenarios automáticos todavía pendientes de
  settlement, fronteras de caída y recuperación de mensajes permanentes. Este
  change no implementa F-04, F-05 ni F-06.
- Documentar el recorrido del evaluador, el acceso local al motor Docker y los
  límites de cada escenario; actualizar README y DECISIONS al implementar.

## Capabilities

### New Capabilities

- `evaluation-console`: navegación, consulta correlacionada de Kafka/SQL/HTTP,
  observación operativa y publicación manual de ER desde la consola local.
- `demo-scenarios`: catálogo, ejecución verificable y control acotado del entorno
  para reproducir escenarios del challenge desde el navegador.

### Modified Capabilities

- `runtime-environment`: el arranque habitual de Compose también incorpora una
  consola independiente de la disponibilidad de las dos aplicaciones.

## Impact

Se agregarán adaptadores y recursos de demo bajo un perfil Spring exclusivo, un
target de imagen y un servicio Compose para la consola. El mismo artefacto Java 25
usará las dependencias administradas existentes; sólo la imagen de consola
incluirá la CLI Docker fijada y acceso al socket, siguiendo el precedente del smoke.

Las aplicaciones expondrán observación de sus consumidores habilitada en Compose.
Se conservarán el contrato HTTP público, el dominio, las migraciones, los offsets
del grupo y las fronteras SQL/Kafka existentes. Las consultas nuevas serán de
lectura; las mutaciones de negocio seguirán entrando exclusivamente por ER.

La excepción a la exclusión general de UI en AGENTS responde al pedido explícito
de una herramienta de evaluación. El README continúa siendo la fuente del estado
de las features; esta propuesta no cambia ninguno a implementado.
