import os
import subprocess
import sys

port = os.environ.get("PORT", "8000")

worker = subprocess.Popen(
    [sys.executable, "-m", "app.worker.dm_worker"]
)

try:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            port,
        ]
    )
finally:
    worker.terminate()
    worker.wait()