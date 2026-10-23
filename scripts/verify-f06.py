"""F-06: recorrido repetible, ambos modos y cancelación después del poison propio."""

import importlib.util
import json
import sys
import time
from pathlib import Path

spec = importlib.util.spec_from_file_location("f05", Path(__file__).with_name("verify-f05.py"))
f05 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f05)
support = f05.support


def cancel_after_poison():
    run_id = support.api("/runs", {"scenario": "poison-recovery", "mode": "step"})["id"]
    deadline = time.monotonic() + 240
    cancelled = False
    while time.monotonic() < deadline:
        run = support.api("/runs/" + run_id)
        if run["status"] in ("PASSED", "FAILED", "CANCELLED"):
            support.require(cancelled and run["status"] == "CANCELLED", json.dumps(run, ensure_ascii=False))
            support.require(run["restoration"].get("status") == "RESTORED", str(run["restoration"]))
            print(f"  Cancelación restaurada: {run_id}", flush=True)
            return run
        if run["status"] == "WAITING":
            paused = any(c["id"] == "paused" and c["status"] == "PASSED"
                         for result in run["scenarioResults"] for c in result["checks"])
            if paused:
                support.api("/runs/" + run_id + "/cancel", {})
                cancelled = True
            elif not cancelled:
                support.api("/runs/" + run_id + "/next", {})
        time.sleep(0.3)
    raise RuntimeError("Cancelación sin restauración observada: " + run_id)


def main():
    support.wait(support.ready, "dos consumidores preparados")
    before = support.existing_orders()
    automatic = f05.execute("poison-recovery")
    step = f05.execute("poison-recovery", "step")
    support.require(set(automatic["orders"]).isdisjoint(step["orders"]), "Se reutilizaron IDs entre ensayos")
    cancelled = cancel_after_poison()
    support.wait(support.ready, "consumo restaurado después de cancelar")
    for order, snapshot in before.items():
        support.require(support.order_snapshot(order) == snapshot, "Orden previa modificada: " + order)
    print(f"F-06 OK: automático={automatic['id']} paso={step['id']} cancelado={cancelled['id']} previousOrders={len(before)}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"F-06 FALLÓ: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
