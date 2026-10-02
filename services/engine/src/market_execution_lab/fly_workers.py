import json
import os
from time import monotonic, sleep
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from market_execution_lab.observability import request_id_context


class FlyWorkers:
    def __init__(self):
        self.app = os.environ["FLY_APP_NAME"]
        self.token = os.environ["FLY_WORKER_TOKEN"]

    def request(self, path: str, method: str = "GET"):
        request = Request(
            f"https://api.machines.dev/v1/apps/{self.app}/machines{path}",
            data=b"{}" if method == "POST" else None,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            method=method,
        )
        with urlopen(request, timeout=10) as response:
            return json.load(response)

    def wake(self) -> dict[str, str]:
        machines = self.request("")
        hosts = {}
        for role in ("engine", "persistence"):
            candidates = [machine for machine in machines if machine["config"].get("metadata", {}).get("fly_process_group") == role]
            if len(candidates) != 1:
                raise RuntimeError("configure exactly one machine per worker process group")
            machine = candidates[0]
            host = f"http://{machine['id']}.vm.{self.app}.internal:8000"
            deadline = monotonic() + 45
            while monotonic() < deadline:
                if machine["state"] in {"stopped", "suspended", "created"}:
                    try:
                        self.request(f"/{machine['id']}/start", "POST")
                    except HTTPError as error:
                        if error.code not in {409, 412}:
                            raise
                try:
                    with urlopen(f"{host}/health", timeout=2) as response:
                        if json.load(response)["role"] == role:
                            hosts[role] = host
                            break
                except (URLError, TimeoutError, ConnectionError):
                    pass
                sleep(1)
                machine = self.request(f"/{machine['id']}")
            else:
                raise RuntimeError("worker startup timed out")
        return hosts

    def execute(self, hosts: dict[str, str], run_id, partition: int) -> None:
        payload = json.dumps({"run_id": str(run_id), "partition": partition}).encode()
        for role in ("engine", "persistence"):
            request = Request(f"{hosts[role]}/jobs", data=payload, headers={"Content-Type": "application/json", "x-request-id": request_id_context.get()})
            with urlopen(request, timeout=60) as response:
                if json.load(response)["status"] != "completed":
                    raise RuntimeError("worker job failed")
