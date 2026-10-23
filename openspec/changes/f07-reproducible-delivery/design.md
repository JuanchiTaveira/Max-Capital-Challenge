# Design

## Context

Ver `proposal.md` para la motivación. La evidencia de F-01 incluye un clon limpio,
pero antecede a la consola, settlement, recuperación y cuarentena. F-06 registra
21 unitarias, 57 integraciones y ocho escenarios aprobados en un proyecto aislado;
su override estaba en `target`, por lo que no sirve como prerrequisito de entrega.
README ya contiene la guía rápida y los comandos portables, pero conserva un enlace
de F-06 anterior al archivo y una advertencia sobre F-07 pendiente.

Las specs vigentes ya exigen arranque limpio, dos consumidores, escenarios reales
y trazabilidad por feature. No se modifica su contrato. Se incluye este diseño
porque el aislamiento de Docker y la distinción entre revisión local y pública
requieren decisiones explícitas antes de ejecutar las verificaciones.

## Goals / Non-Goals

**Goals:**
- Obtener evidencia atribuible a un SHA versionado, sin fuentes sin confirmar ni
  archivos ignorados heredados del entorno de desarrollo.
- Mantener intactos los proyectos, volúmenes y datos previos, incluidos los de
  implementaciones descartadas que tienen otro historial Flyway.
- Permitir al evaluador repetir el recorrido publicado y distinguir qué se probó
  localmente, qué está publicado y qué queda pendiente.

**Non-Goals:**
- Probar builds idénticos byte a byte, entornos offline o toda plataforma de host.
- Agregar tests triviales, otro runner, escenarios nuevos o CI.
- Cambiar el núcleo, migraciones, configuración ordinaria o garantías existentes.

## Decisions

### 1. Clonar una revisión local identificada y conservar su historial

Después de confirmar las correcciones documentales por una unidad coherente,
crear un clon local separado del repositorio y seleccionar el SHA candidato.
Comprobar checkout limpio y ausencia inicial de `target`/`.env` heredados.
El clon usa sólo archivos versionados; no copiar overrides ni fixtures locales.
Registrar el SHA probado y los commits posteriores exclusivamente documentales.
Si cambian fuentes o configuración ejecutable, repetir las verificaciones afectadas.

Alternativa descartada: probar sólo el checkout actual; podría usar archivos
ignorados y no demostrar que el material entregable alcanza para arrancar.

### 2. Aislar recursos de Docker antes de arrancar

Inventariar proyectos y puertos mediante lecturas. Usar un nombre Compose exclusivo
con `-p` en todos los comandos del ensayo, incluidos los smokes, y volúmenes nuevos.
El nombre por sí solo no aísla puertos ni etiquetas de imágenes: comprobar también
esas colisiones y evitar reemplazar imágenes usadas por otros ensayos.

El recorrido de aceptación debe usar el Compose versionado y sus puertos publicados
en README. Si están ocupados por servicios ajenos, no detenerlos ni reconfigurarlos;
registrar el bloqueo. Se pueden realizar diagnósticos aislados con un override temporal
explícito, pero éstos no sustituyen la comprobación del comando documentado sin
override. No usar como entrada `target/f06-compose.yaml` ni otros archivos heredados.

Conservar contenedores y volúmenes del ensayo para inspección, o detener únicamente
sus propios servicios; no ejecutar borrados de volúmenes ni operaciones sobre otro
proyecto. No hace falta instalar dependencias de producción adicionales en el host.

Alternativa descartada: reutilizar el proyecto F-06; mezcla datos y no demuestra
inicialización de una base nueva.

### 3. Reutilizar las verificaciones existentes con resultados observados

Desde el clon, ejecutar `docker compose -p <proyecto> config --quiet` y el arranque
documentado con build. Observar dos apps saludables, sus identidades, tres topics
de dos particiones y exactamente dos consumidores, sin sumar la consola al grupo.
Abrir la consola según README y comprobar el acceso y la explicación de escenarios.

Ejecutar `mvnw.cmd verify` en PowerShell o `./mvnw verify` en Linux/macOS con JDK 25
y Docker; esos son prerrequisitos de desarrollo, distintos de los del recorrido web.
El build de imagen omite tests y no sustituye esta suite.
Ejecutar secuencialmente `verify-console` dos veces en el proyecto aislado: la primera
debe aprobar los ocho escenarios y restaurar dos consumidores; la segunda además
debe conservar las órdenes previas. Esto reutiliza las comprobaciones reales de
secuencia, duplicados, snapshots, fallas, settlement read_committed y cuarentena.
No repetir los runners individuales salvo que haya una falla o un cambio que lo exija.

Registrar fecha, entorno, SHA, comandos, códigos de salida, conteos reales de Maven,
IDs de runs, ocho resultados y restauración. Mantener el resumen durable dentro
de `verification.md`; los logs temporales son apoyo, no el único entregable.

### 4. Corregir documentación con alcance acotado

Comprobar enlaces locales y anclas de README, incluidos F-06 y el enlace final F-07.
Conservar la guía rápida existente y ajustar sólo instrucciones contradictorias,
faltantes comprobados y referencias que quedan obsoletas al cerrar F-07.
Contrastar los ocho temas de decisiones del PDF con DECISIONS.md: broker, secuencia,
dedup, motor, coherencia estado/ledger, errores, settlement y trade-offs.
No trasladar comandos operativos a DECISIONS.md ni reescribir evidencia histórica.

### 5. Separar implementación local de disponibilidad pública

Comprobar en lectura la visibilidad del repositorio remoto y si contiene la revisión
probada, usando acceso anónimo para acreditar disponibilidad pública y comparación
de revisiones/historial para acreditar contenido. Un remoto configurado o un acceso
autenticado exitoso por sí solos no prueban que sea público.

F-07 puede indicar Implementado cuando el cierre local tenga evidencia completa;
el registro de entrega debe indicar por separado URL, revisión remota observada y
commits locales pendientes. Esto mantiene README como estado de implementación,
sin afirmar entrega pública completa si faltan commits o la visibilidad no se pudo
verificar. La publicación o cambio de visibilidad requiere autorización específica;
no forma parte de este change. Una falla de red se informa como no verificado.

Alternativa descartada: hacer push para completar el checklist; excede la autorización
del repositorio. Tampoco se equipara Implementado con entregado públicamente.

## Risks / Trade-offs

- Puertos ocupados o Docker/JDK indisponible → registrar comando y causa; mantener
  F-07 pendiente si impiden el recorrido local requerido, sin omitir tests.
- Etiquetas de imágenes compartidas → verificar consumidores antes del build y,
  si no hay aislamiento suficiente, registrar el bloqueo sin sobrescribirlas.
- Evidencia histórica con conteos diferentes → identificar fecha y revisión de
  cada ensayo; no atribuir resultados de F-06 al clon de F-07.
- Ocho escenarios con fallas tardan varios minutos → ejecutar secuencialmente y
  esperar sus resultados acotados, sin controles manuales concurrentes.
- Un defecto funcional aparece en la entrega → documentar y mantener pendiente;
  no ampliar este change documental sin reevaluar el alcance.

## Migration Plan

No hay migraciones ni despliegue sobre datos existentes. Los ajustes de documentación
se conservan en commits locales. El cierre valida OpenSpec, actualiza el estado con
evidencia suficiente y deja el change listo para archivarlo con su flujo específico.
No cambia F-08 ni inicia otra feature. Revertir documentación no modifica datos.
