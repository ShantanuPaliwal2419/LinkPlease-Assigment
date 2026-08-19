import asyncio
import os
import signal
import sys

PORT = os.environ.get("PORT", "8000")


async def run_process(cmd: list[str], name: str):
    """Runs a subprocess, streams its logs, and restarts it if it exits unexpectedly."""
    while True:
        print(f"[{name}] Starting process...")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

        # Wait for the process to terminate
        exit_code = await process.wait()
        print(f"[{name}] Process exited with code {exit_code}. Restarting in 2s...")
        await asyncio.sleep(2)


async def main():
    web_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        "0.0.0.0",
        "--port",
        PORT,
    ]
    worker_cmd = [sys.executable, "-m", "app.worker.dm_worker"]

    # Run both processes concurrently in the asyncio event loop
    web_task = asyncio.create_task(run_process(web_cmd, "WEB"))
    worker_task = asyncio.create_task(run_process(worker_cmd, "WORKER"))

    # Handle graceful termination signals from Render (SIGTERM/SIGINT)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        print("Shutdown signal received. Cancelling tasks...")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    # Wait until a termination signal is received
    await stop_event.wait()

    # Cancel tasks on exit
    web_task.cancel()
    worker_task.cancel()
    await asyncio.gather(web_task, worker_task, return_exceptions=True)
    print("All processes stopped. Exiting.")


if __name__ == "__main__":
    asyncio.run(main())