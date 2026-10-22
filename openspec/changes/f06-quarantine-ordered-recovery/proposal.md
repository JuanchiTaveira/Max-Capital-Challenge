# Proposal

## Why

Un ER inválido hoy detiene el consumo sin un procedimiento de corrección. F-06 debe distinguir errores transitorios de permanentes, conservar el original y permitir retomar la secuencia. Para este challenge alcanza una solución pequeña que reutilice las garantías de F-04/F-05.

## What Changes

- Reintentar únicamente fallas transitorias con rollback conocido: hasta cinco intentos por sesión, con backoff exponencial y jitter. El contador vive en memoria; reiniciar o cambiar de propietario inicia otro ciclo acotado.
- Guardar el ER permanente en una tabla de cuarentena y notificar su posición y causa en `execution-reports-rejected`. La notificación admite duplicados identificables; el original completo permanece en SQL.
- Pausar la partición afectada sin confirmar el offset del ER fallido. Los sucesores esperan en Kafka y las otras particiones pueden continuar. No se crea una cola SQL ni se intenta aislar cada orden dentro de una partición.
- Registrar una corrección con key, JSON efectivo, motivo y responsable declarado. Al reiniciar el consumidor se aplica esa corrección en la misma posición original, antes de sus sucesores. Una segunda tabla conserva las correcciones y sus resultados.
- Reutilizar procesamiento, deduplicación, ledger y settlement existentes. La corrección se registra en SQL; el negocio sólo se aplica al consumir la posición pendiente de Kafka.
- Ofrecer consulta y corrección por API local y habilitar el escenario existente `poison-recovery`, sin agregar pantallas ni formularios.

Se eliminan del diseño anterior los comandos internos, outbox, leases, barreras por orden, captura de sucesores, rondas durables de retry, autorización de nuevas rondas y un contrato de DLQ con modalidades o hashes.

## Capabilities

### New Capabilities

- `report-quarantine`: cuarentena, DLQ, reintentos acotados y corrección auditada con reanudación operativa.

### Modified Capabilities

- `execution-report-consumption`: pausar un error pendiente y seleccionar su corrección al reentregar la misma posición.
- `processing-results`: relacionar el resultado de la corrección con la posición original y conservar su auditoría.
- `settlement-publication`: comprobar que una corrección FILLED conserva la recuperación transaccional existente.
- `demo-scenarios`: habilitar un recorrido pequeño de error, corrección y continuación ordenada.

`order-query` y `evaluation-console` conservan sus contratos: no se agrega `processing` al GET de órdenes ni una interfaz de administración de cuarentena.

## Impact

Cambian el manejo de errores del listener, la selección del ER efectivo y la transacción de aplicación; se agregan dos tablas en una migración aditiva, una API local y pruebas focalizadas. No se incorporan servicios, dependencias ni un protocolo nuevo de Kafka. README y DECISIONS se actualizarán al implementar; F-06 permanece pendiente.

El trade-off elegido es disponibilidad: una orden inválida también demora otras órdenes de su partición hasta que un operador la corrija y reinicie el consumidor. No hay plazo garantizado de reparación humana. La DLQ es una notificación al menos una vez; la garantía existente de settlement no se relaja. La recuperación presupone una corrección válida compatible con la identidad y partición originales.
