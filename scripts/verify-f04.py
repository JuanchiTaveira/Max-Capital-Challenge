"""Smoke F-04: usa la lectura read_committed existente, sin consumidor de negocio."""

import importlib.util
import json
import os
from pathlib import Path
import sys
import time

spec = importlib.util.spec_from_file_location("f02_support", Path(__file__).with_name("verify-f02.py"))
support = importlib.util.module_from_spec(spec)
spec.loader.exec_module(support)


def page(partition, cursor):
    return support.get_json("evaluation-console", f"/demo/api/messages?topic=settlements&partition={partition}&cursor={cursor}&limit=200")


def read_to_end(starts):
    records = []
    for partition, start in enumerate(starts):
        cursor = start
        for _ in range(100):
            result = page(partition, cursor)
            records.extend(result["records"])
            cursor = int(result["nextCursor"])
            if cursor >= int(result["endOffset"]):
                break
        else:
            raise RuntimeError("Lectura de settlements no alcanzó su límite estable")
    return records


def main():
    project = os.environ.get("COMPOSE_PROJECT_NAME")
    support.require(project, "Falta COMPOSE_PROJECT_NAME; ejecutar con Compose")
    kafka = support.container(project, "kafka")
    support.wait_applications(kafka)
    starts = [int(page(p, 0)["endOffset"]) for p in (0, 1)]
    base = time.time_ns() // 1_000_000 * 100
    filled_order = support.order_id(base, 0)
    cancelled_order = support.order_id(base + 20, 1)
    filled = support.report(filled_order, 3, "FILLED", 10, 0)
    cancelled = support.report(cancelled_order, 3, "CANCELLED", 4, 0)
    initial = support.report(filled_order, 1, "NEW", 0, 10)
    support.publish(kafka, [initial, support.report(cancelled_order, 1, "NEW", 0, 10),
                           support.report(filled_order, 2, "PARTIALLY_FILLED", 7, 3),
                           support.report(cancelled_order, 2, "PARTIALLY_FILLED", 4, 6),
                           filled, cancelled, filled, initial, cancelled])
    support.wait_consumed(kafka)

    def verify():
        view = support.check_order(filled_order, "FILLED", 3, 10)
        support.check_order(cancelled_order, "CANCELLED", 3, 4)
        selected = [r for r in read_to_end(starts) if r["key"] in (str(filled_order), str(cancelled_order))]
        support.require(len(selected) == 1 and selected[0]["key"] == str(filled_order),
                        f"Cardinalidad incorrecta: {selected}")
        message = json.loads(selected[0]["payload"])
        support.require(message["schemaVersion"] == 1 and message["settlementId"] == f"settlement:{filled_order}", str(message))
        support.require(message["numericOrderId"] == str(filled_order) and message["fixId"] == "3"
                        and message["ledgerId"] == str(view["ledger"][-1]["ledgerId"]), str(message))
        support.require([message[k] for k in ("nominalAmounts", "accumulativeNominalAmount", "leavesNominalAmount")] == ["10", "10", "0"], str(message))
        source = message["source"]
        support.require(source["topic"] == "execution-reports" and source["partition"] == 0, str(source))
        print(json.dumps({"filled": str(filled_order), "cancelled": str(cancelled_order),
                          "settlements": {str(filled_order): 1, str(cancelled_order): 0},
                          "record": selected[0]}, ensure_ascii=False))

    support.wait(verify, "estado SQL y un único settlement confirmado")
    support.require(len(read_to_end(starts)) >= 1, "No se leyó salida confirmada")
    support.wait_applications(kafka)
    print("F-04 OK: dos miembros, FILLED=1, CANCELLED=0, terminales/NEW duplicados sin salida adicional.")
    print("Datos conservados. Este smoke no inyecta caídas entre commits; esas fronteras se verifican en SettlementKafkaIT.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"F-04 FALLÓ: {error}", file=sys.stderr)
        sys.exit(1)
