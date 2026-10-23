"""Verificación portable de la consola, usando únicamente su API HTTP."""

import json
import os
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request, ProxyHandler, build_opener

BASE = os.environ.get("CONSOLE_URL", "http://evaluation-console:8080")
ORIGIN = os.environ.get("DEMO_ORIGIN", "http://localhost:8083")
HTTP = build_opener(ProxyHandler({}))


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def request(path, body=None, expected=200, origin=ORIGIN):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = Request(BASE + path, data=data)
    if body is not None:
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Demo-Console", "1")
        req.add_header("Origin", origin)
    try:
        response = HTTP.open(req, timeout=45)
    except HTTPError as error:
        response = error
    with response:
        text = response.read().decode("utf-8")
        require(response.code == expected, f"{path}: HTTP {response.code}, esperado {expected}: {text[:1200]}")
        return json.loads(text) if text and path != "/" else text


def api(path, body=None, expected=200, origin=ORIGIN):
    return request("/demo/api" + path, body, expected, origin)


def wait(check, description, timeout=180):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return check()
        except (RuntimeError, OSError, ValueError, KeyError) as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Timeout: {description}. {error}") from error
            time.sleep(1)


def ready():
    state = api("/system")
    require(state["database"]["available"] and state["kafka"]["available"], str(state))
    require(len(state["kafka"]["data"]["members"]) == 2, "No hay dos consumidores")
    for instance in state["instances"]:
        source = instance["consumer"]
        require(source["available"] and source["data"]["statusCode"] == 200, str(instance))
        consumer = source["data"]["data"]
        require(consumer["running"] and len(consumer["assignments"]) == 1, str(instance))
    return state


def order_snapshot(order):
    state = api("/orders/" + order)
    return {key: value for key, value in state.items() if key not in ("observedAt", "source")}


def existing_orders():
    snapshots = {}
    cursor = None
    while True:
        page = api("/orders?limit=200" + ("&cursor=" + cursor if cursor else ""))
        for order in page["orders"]:
            key = order["numericOrderId"]
            snapshots[key] = order_snapshot(key)
        cursor = page["nextCursor"]
        if not cursor:
            return snapshots


def control(service, action):
    result = api(f"/services/{service}/{action}", {})
    print(f"  {service} {action}: {result['containerState']}", flush=True)
    require("Consola de evaluación" in request("/"), "La UI de consola dejó de responder")
    return result


def main():
    print("[1/5] Verificar consola, dos consumidores y datos previos", flush=True)
    wait(ready, "entorno preparado")
    before = existing_orders()
    catalog = api("/catalog")
    require(sum(entry["available"] for entry in catalog) == 8, "Catálogo disponible incorrecto")
    require(all(entry["available"] for entry in catalog), "Catálogo pendiente incorrecto")
    api("/services/foreign/start", {}, 400)
    api("/services/app-1/start", {}, 403, "http://foreign.invalid")

    print("[2/5] Ejecutar los ocho escenarios disponibles por la API real", flush=True)
    run_id = api("/runs", {"scenario": "available", "mode": "automatic"})["id"]

    # Una falla definitiva se informa sin esperar todo el plazo de una condición transitoria.
    deadline = time.monotonic() + 1200
    while True:
        run = api("/runs/" + run_id)
        if run["status"] in ("PASSED", "FAILED", "CANCELLED"):
            require(run["status"] == "PASSED", json.dumps(run, ensure_ascii=False)[-5000:])
            break
        require(time.monotonic() < deadline, f"La ejecución {run_id} excedió el plazo total")
        time.sleep(1)
    require(run["passedScenarios"] == 8 and run["excludedPending"] == 0, str(run))
    require(all(step["status"] == "PASSED" for step in run["steps"]), "Hay pasos sin verificación")
    require(len(run["scenarioResults"]) == 8, "Faltan resultados por escenario")
    evidence_by_id = {}
    for scenario in run["scenarioResults"]:
        require(scenario["status"] == "PASSED" and scenario["conclusion"], "Conclusión de escenario incompleta")
        require(all(check["status"] == "PASSED" and check["observed"] and check["references"]
                    for check in scenario["checks"]), "Faltan observaciones reales en un escenario")
        evidence = scenario.get("evidence")
        require(isinstance(evidence, dict), f"Falta evidencia estructurada: {scenario['scenarioId']}")
        require(all(isinstance(evidence.get(name), list) for name in ("messages", "orderSnapshots", "outputs")),
                f"Colecciones de evidencia incompletas: {scenario['scenarioId']}")
        scenario_messages = [message for message in run["messages"] if message["scenarioId"] == scenario["scenarioId"]]
        require(len(evidence["messages"]) == len(scenario_messages),
                f"Mensajes de evidencia incompletos: {scenario['scenarioId']}")
        for message in evidence["messages"]:
            position = message["source"]
            require(message["stepId"] and message["observedAt"], "Mensaje sin paso u hora de observación")
            require(position["topic"] == "execution-reports" and all(isinstance(position[field], str)
                    for field in ("partition", "offset")), "Posición de entrada perdió su valor exacto")
            require(message["sqlObservationStatus"] in
                    ("NOT_QUERIED", "NO_RESULT", "RESULT_FOUND", "QUERY_FAILED"),
                    f"Estado SQL desconocido: {message['sqlObservationStatus']}")
        for snapshot in evidence["orderSnapshots"]:
            require(snapshot["stepId"] and snapshot["observedAt"] and snapshot["source"] == "PostgreSQL",
                    f"Snapshot sin procedencia: {scenario['scenarioId']}")
            require(snapshot["snapshot"]["numericOrderId"] == snapshot["numericOrderId"],
                    "Snapshot asociado a otra orden")
        for output in evidence["outputs"]:
            require(output["source"]["topic"] in ("settlements", "execution-reports-rejected"),
                    "Tipo de salida confundido con otro topic")
            require(output["readSource"] == "Kafka read_committed" and output["startCursors"] and output["endOffsets"],
                    "Salida sin fuente o rango de lectura")
        evidence_by_id[scenario["scenarioId"]] = evidence

    parallel = evidence_by_id["interleaved-orders"]
    require(len(parallel["messages"]) == 7, "El escenario paralelo no conservó sus siete envíos")
    parallel_ledgers = sorted(len(snapshot["snapshot"]["ledger"]) for snapshot in parallel["orderSnapshots"])
    require(parallel_ledgers == [3, 4], f"Ledgers esperados 3/4: {parallel_ledgers}")

    quantities = evidence_by_id["quantity-snapshots"]["orderSnapshots"]
    amounts = [snapshot["snapshot"]["accumulativeNominalAmount"] for snapshot in quantities]
    require(amounts == ["0.00000000", "3.00000000", "7.00000000", "10.00000000"],
            f"Snapshots de acumulado incorrectos: {amounts}")

    restart_snapshots = evidence_by_id["restart-persistence"]["orderSnapshots"]
    require(len(restart_snapshots) == 4, "El escenario de reinicio perdió una captura anterior o posterior")
    by_order = {}
    for snapshot in restart_snapshots:
        state_snapshot = {key: value for key, value in snapshot["snapshot"].items()
                          if key not in ("observedAt", "source")}
        by_order.setdefault(snapshot["numericOrderId"], []).append(state_snapshot)
    require(len(by_order) == 2 and all(len(history) == 2 and history[0] == history[1]
            for history in by_order.values()), "El estado o ledger cambió entre capturas del reinicio")

    duplicates = evidence_by_id["terminal-duplicates"]["messages"]
    require(len(duplicates) == 4, "El escenario de duplicados debe tener cuatro envíos")
    original = duplicates[0]["sqlObservations"][0]["result"]
    late = duplicates[3]["sqlObservations"][0]["result"]
    require(original["status"] == "NEW" and late["status"] == "FILLED"
            and original["ledgerId"] == late["ledgerId"],
            "La republicación NEW no conserva su snapshot FILLED y ledger original")

    aborted = next(message for message in evidence_by_id["committed-input-only"]["messages"]
                   if message["transaction"] == "ABORTED")
    require(aborted["sqlObservationStatus"] == "NO_RESULT", "El envío abortado se presentó como aplicado")

    recovery_outputs = evidence_by_id["settlement-once"]["outputs"]
    require(any(output["type"] == "SETTLEMENT" for output in recovery_outputs),
            "No se capturó la salida settlement observada")
    poison = evidence_by_id["poison-recovery"]
    rejection = next((output for output in poison["outputs"] if output["type"] == "REJECTION"), None)
    require(rejection is not None, "No se leyó el aviso DLQ desde read_committed")
    rejection_payload = json.loads(rejection["payload"])
    require({output["type"] for output in poison["outputs"]} == {"REJECTION", "SETTLEMENT"},
            "La cuarentena no distingue aviso DLQ de settlements")
    original_poison = next(message for message in poison["messages"] if message["payload"].startswith("{invalid-json:"))
    require(rejection_payload["source"]["offset"] == original_poison["source"]["offset"],
            "El aviso DLQ no apunta al original ilegible")
    recovered = next(check for check in next(item for item in run["scenarioResults"]
                     if item["scenarioId"] == "poison-recovery")["checks"] if check["id"] == "recovered")
    require(recovered["references"]["original"]["offset"] == original_poison["source"]["offset"]
            and recovered["references"]["correction"]["outcome"] == "APPLIED",
            "La corrección no conserva la posición original y su ER efectivo")
    steps_by_id = {step["id"]: step for step in run["steps"]}
    require(all(steps_by_id[record["stepId"]]["scenarioId"] == record["scenarioId"]
                for record in run["messages"]), "Hay mensajes asociados al escenario incorrecto")
    aborted = next(record for record in run["messages"] if record["transaction"] == "ABORTED")
    require(api(f"/results?topic={aborted['topic']}&partition={aborted['partition']}&offset={aborted['offset']}")["result"] is None,
            "El envío abortado tiene resultado SQL")
    sample = run["orders"][0]
    preserved_sample = order_snapshot(sample)

    print("[3/5] Terminar e iniciar una instancia; consola y SQL disponibles", flush=True)
    try:
        control("app-1", "kill")
        require(order_snapshot(sample) == preserved_sample, "Cambió el estado al terminar app-1")
        control("app-1", "start")
        wait(ready, "dos consumidores tras iniciar app-1")

        print("[4/5] Caídas de dependencias con consultas degradadas explícitas", flush=True)
        control("postgres", "stop")
        api("/orders/" + sample, expected=503)
        api("/messages?topic=execution-reports&partition=0&cursor=0&limit=1")
        control("postgres", "start")
        wait(lambda: order_snapshot(sample), "SQL recuperado")
        control("kafka", "stop")
        require(order_snapshot(sample) == preserved_sample, "SQL dejó de consultar durante caída Kafka")
        api("/messages?topic=execution-reports&partition=0&cursor=0&limit=1", expected=503)
        control("kafka", "start")
    finally:
        errors = []
        for service in ("postgres", "kafka", "app-1", "app-2"):
            try:
                control(service, "start" if service in ("postgres", "kafka") else "restart")
            except Exception as error:
                errors.append(f"{service}: {error}")
        require(not errors, "Restauración incompleta: " + "; ".join(errors))
        wait(ready, "servicios restaurados")

    print("[5/5] Verificar conservación del estado y datos previos", flush=True)
    require(order_snapshot(sample) == preserved_sample, "Cambió el estado tras recuperar dependencias")
    for order, snapshot in before.items():
        require(order_snapshot(order) == snapshot, f"Se modificó una orden previa: {order}")
    print(f"CONSOLE OK run={run_id} scenarios=8 pending=0 previousOrders={len(before)}", flush=True)
    print("Consola: http://localhost:8083 · se conservan órdenes, mensajes y volúmenes.", flush=True)
    print("Los escenarios guiados verifican fronteras observadas y cuarentena con corrección auditada.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"CONSOLE FALLÓ: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
