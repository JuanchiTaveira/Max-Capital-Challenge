"""Smoke F-02 ejecutado dentro del servicio Compose verify-f02."""

import json
import os
import re
import subprocess
import sys
import time
from decimal import Decimal
from urllib.request import ProxyHandler, build_opener


KAFKA_BIN = "/opt/kafka/bin/"
GROUP = "max-capital-orders-v1"
APPLICATIONS = (("app-1", 8081), ("app-2", 8082))
HTTP = build_opener(ProxyHandler({}))


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def docker(*arguments, input_text=None):
    result = subprocess.run(
        ["docker", *arguments], input=input_text, text=True,
        encoding="utf-8", capture_output=True, timeout=60,
    )
    require(result.returncode == 0,
            f"docker {' '.join(arguments)} falló:\n{result.stderr}{result.stdout}")
    return result.stdout.strip()


def container(project, service):
    ids = docker(
        "ps", "--filter", f"label=com.docker.compose.project={project}",
        "--filter", f"label=com.docker.compose.service={service}",
        "--format", "{{.ID}}",
    ).splitlines()
    require(len(ids) == 1,
            f"Se esperaba un contenedor activo de {service} en el proyecto {project}: {ids}")
    return ids[0]


def kafka(kafka_id, command, *arguments, input_text=None):
    return docker("exec", "-i", kafka_id, KAFKA_BIN + command,
                  "--bootstrap-server", "localhost:29092", *arguments,
                  input_text=input_text)


def wait(check, description, timeout=150):
    deadline = time.monotonic() + timeout
    while True:
        try:
            check()
            return
        except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired) as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Timeout: {description}. {error}") from error
            time.sleep(1)


def partition(order_id):
    # Murmur2 de Kafka: particionado estándar de una key UTF-8, dos particiones.
    data = str(order_id).encode("utf-8")
    mask, multiplier = 0xFFFFFFFF, 0x5BD1E995
    value = 0x9747B28C ^ len(data)
    index = 0
    while index + 4 <= len(data):
        chunk = int.from_bytes(data[index:index + 4], "little")
        chunk = (chunk * multiplier) & mask
        chunk ^= chunk >> 24
        chunk = (chunk * multiplier) & mask
        value = ((value * multiplier) & mask) ^ chunk
        index += 4
    tail = data[index:]
    if tail:
        value ^= int.from_bytes(tail, "little")
        value = (value * multiplier) & mask
    value ^= value >> 13
    value = (value * multiplier) & mask
    value ^= value >> 15
    return (value & 0x7FFFFFFF) % 2


def order_id(start, target_partition):
    while partition(start) != target_partition:
        start += 1
    return start


def report(order, fix_id, status, accumulated, leaves):
    return {
        "fixId": fix_id, "numericOrderId": order, "status": status,
        "nominalAmounts": 10, "accumulativeNominalAmount": accumulated,
        "leavesNominalAmount": leaves, "transactionTime": "2026-07-20T18:08:52.129",
        "ticker": "VSCPC", "executionPrice": 104.30,
        "secondaryTradeId": f"smoke-{order}-{fix_id}",
    }


def publish(kafka_id, reports):
    lines = [f"{item['numericOrderId']}|{json.dumps(item, separators=(',', ':'))}"
             for item in reports]
    kafka(kafka_id, "kafka-console-producer.sh", "--topic", "execution-reports",
          "--property", "parse.key=true", "--property", "key.separator=|",
          "--producer-property", "acks=all", input_text="\n".join(lines) + "\n")


def get_json(service, path):
    with HTTP.open(f"http://{service}:8080{path}", timeout=5) as response:
        return json.load(response, parse_float=Decimal)


def wait_applications(kafka_id):
    def readiness():
        for service, _ in APPLICATIONS:
            health = get_json(service, "/actuator/health/readiness")
            require(health["status"] == "UP", f"Readiness de {service} no está UP")

    wait(readiness, "readiness de ambas instancias")

    def members():
        output = kafka(kafka_id, "kafka-consumer-groups.sh", "--describe",
                       "--group", GROUP, "--members", "--verbose")
        assignments = re.findall(
            r"(?m)^\S+\s+\S+\s+\S+\s+\S+\s+1\s+(?:\S+\s+)?"
            r"execution-reports[:(]([01])\)?(?:\s+.*)?$",
            output,
        )
        require(sorted(assignments) == ["0", "1"], output)

    wait(members, "dos consumidores con una partición cada uno")


def wait_consumed(kafka_id):
    def consumed():
        output = kafka(kafka_id, "kafka-consumer-groups.sh", "--describe", "--group", GROUP)
        partitions = re.findall(
            r"(?m)^\S+\s+execution-reports\s+([01])\s+\d+\s+\d+\s+0\s+\S+", output,
        )
        require(sorted(partitions) == ["0", "1"], output)

    wait(consumed, "offsets confirmados sin lag en ambas particiones")


def check_order(order, status, count, accumulated):
    views = []
    for service, _ in APPLICATIONS:
        view = get_json(service, f"/orders/{order}")
        require(view["status"] == status, f"Estado incorrecto en {service}: {view}")
        require(view["appliedExecutionCount"] == count and len(view["ledger"]) == count,
                f"Contador o ledger incorrecto: {view}")
        require(view["accumulativeNominalAmount"] == accumulated
                and view["leavesNominalAmount"] == 0, f"Snapshot incorrecto: {view}")
        previous = 0
        for index, entry in enumerate(view["ledger"], 1):
            require(entry["ledgerId"] > previous, "Ledger fuera de orden")
            require(entry["report"]["fixId"] == index, "Secuencia fixId incorrecta")
            require(entry["report"]["ticker"] == "VSCPC", "Se perdió un campo adicional")
            previous = entry["ledgerId"]
        views.append(view)
    require(views[0] == views[1], "Las instancias devuelven estados diferentes")
    return views[0]


def main():
    project = os.environ.get("COMPOSE_PROJECT_NAME")
    require(project, "Falta COMPOSE_PROJECT_NAME; ejecutar mediante Docker Compose")
    print(f"[1/4] Verificar Docker e instancias del proyecto {project}")
    docker("info", "--format", "{{.ServerVersion}}")
    kafka_id = container(project, "kafka")
    apps = [container(project, service) for service, _ in APPLICATIONS]
    wait_applications(kafka_id)

    base_id = time.time_ns() // 1_000_000 * 100
    first = order_id(base_id, 0)
    second = order_id(base_id + 20, 1)
    filled = report(first, 4, "FILLED", 10, 0)
    cancelled = report(second, 3, "CANCELLED", 7, 0)
    reports = [
        report(first, 1, "NEW", 0, 10), report(second, 1, "NEW", 0, 10),
        report(first, 2, "PARTIALLY_FILLED", 3, 7),
        report(second, 2, "PARTIALLY_FILLED", 7, 3),
        report(first, 3, "PARTIALLY_FILLED", 7, 3), cancelled, filled, filled, cancelled,
    ]
    print(f"[2/4] Publicar órdenes intercaladas {first} (partición 0), {second} (partición 1)")
    publish(kafka_id, reports)
    wait_consumed(kafka_id)

    initial = []

    def check_initial():
        initial[:] = [check_order(first, "FILLED", 4, 10),
                      check_order(second, "CANCELLED", 3, 7)]

    wait(check_initial, "estado, snapshots, ledger y duplicados iniciales")
    print("[3/4] Secuencia y duplicados OK; reiniciar ambas aplicaciones y reenviar terminales")
    docker("restart", *apps)
    wait_applications(kafka_id)
    publish(kafka_id, [filled, cancelled])
    wait_consumed(kafka_id)

    def check_persisted():
        current = [check_order(first, "FILLED", 4, 10),
                   check_order(second, "CANCELLED", 3, 7)]
        require(current == initial, "El estado o ledger cambió después del reinicio y replay")

    wait(check_persisted, "persistencia tras reinicio y reentrega terminal")
    print("[4/4] F-02 OK: ambas instancias consumen y consultan; estado y ledger persisten.")
    for order, status, count in ((first, "FILLED", 4), (second, "CANCELLED", 3)):
        print(f"Orden {order}: {status}, contador y ledger = {count}")
        for _, port in APPLICATIONS:
            print(f"  http://localhost:{port}/orders/{order}")
    print("Se conservan los volúmenes y las órdenes de prueba para inspección.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(f"F-02 FALLÓ: {error}", file=sys.stderr)
        sys.exit(1)
