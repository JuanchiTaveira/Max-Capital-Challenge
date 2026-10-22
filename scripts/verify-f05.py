"""F-05: escenarios reales por consola; conserva datos y captura evidencia completa."""

import json
import sys
import time
from pathlib import Path
import importlib.util

spec = importlib.util.spec_from_file_location("console_support", Path(__file__).with_name("verify-console.py"))
support = importlib.util.module_from_spec(spec)
spec.loader.exec_module(support)


def execute(scenario, mode="automatic"):
    run_id = support.api("/runs", {"scenario": scenario, "mode": mode})["id"]
    deadline = time.monotonic() + 1200
    last_step = None
    while True:
        run = support.api("/runs/" + run_id)
        current = run["steps"][-1] if run["steps"] else None
        if current and current["id"] != last_step:
            last_step = current["id"]
            print(f"  {scenario}: {current['title']}", flush=True)
        if run["status"] in ("PASSED", "FAILED", "CANCELLED"):
            print(json.dumps(run, ensure_ascii=False), flush=True)
            support.require(run["status"] == "PASSED", f"Run fallido: {run_id}: {run.get('error')}")
            support.require(run["restoration"].get("status") == "RESTORED", "Restauración no comprobada")
            for result in run["scenarioResults"]:
                support.require(result["status"] == "PASSED", str(result))
                support.require(all(c["status"] == "PASSED" and c["references"] for c in result["checks"]), "Checks incompletos")
            return run
        support.require(time.monotonic() < deadline, "Tiempo total excedido: " + run_id)
        if mode == "step" and run["status"] == "WAITING":
            support.api("/runs/" + run_id + "/next", {})
        time.sleep(1)


def main():
    support.wait(support.ready, "dos consumidores y dependencias")
    before = support.existing_orders()
    runs = [execute("settlement-once"), execute("commit-boundary-recovery")]
    support.wait(support.ready, "entorno restaurado")
    for order, snapshot in before.items():
        support.require(support.order_snapshot(order) == snapshot, "Orden previa modificada: " + order)
    print("F-05 OK: " + ", ".join(r["id"] for r in runs) + "; fronteras reales, datos previos conservados y dos consumidores.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"F-05 FALLÓ: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
