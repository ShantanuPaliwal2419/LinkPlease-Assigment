import asyncio
import os
import signal
import sys
from typing import List

PORT = os.environ.get("PORT", "8000")

# Store running process references globally so signal handlers can terminate them
active_processes: List[asyncio.subprocess.Process] = []


async def run_process(cmd: list[str], name: str):
    """Runs a subprocess, streams logs, and handles clean restarts."""
    while True:
        print(f"[{name}] Starting process: {' '.join(cmd)}")
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=None,  # Inherit standard output from parent process
                stderr=None,  # Inherit standard error from parent process
            )
            active_processes.append(process)

            # Wait for the process to exit
            exit_code = await process.wait()
            
            if process in active_processes:
                active_processes.remove(process)

            print(f"[{name}] Exited with code {exit_code}.")
            
            # Pause briefly before restarting
            await asyncio.sleep(2)

        except asyncio.CancelledError:
            # Task was cancelled during shutdown -> break loop
            print(f"[{name}] Supervisor task cancelled.")
            break
        except Exception as e:
            print(f"[{name}] Unexpected manager error: {e}")
            await asyncio.sleep(2)


async def shutdown(tasks: list[asyncio.Task]):
    """Gracefully terminates all child processes before stopping the supervisor."""
    print("\n[SUPERVISOR] Shutting down child processes...")

    # 1. Send SIGTERM to all child processes
    for proc in active_processes:
        if proc.returncode is None:  # Process is still running
            try:
                proc.terminate()
                print(f"[SUPERVISOR] Sent SIGTERM to PID {proc.pid}")
            except ProcessLookupError:
                pass

    # 2. Wait up to 5 seconds for child processes to terminate cleanly
    if active_processes:
        await asyncio.sleep(0.5)
        for proc in list(active_processes):
            if proc.returncode is None:
                try:
                    proc.kill()  # Force kill if still hanging
                    print(f"[SUPERVISOR] Force killed PID {proc.pid}")
                except ProcessLookupError:
                    pass

    # 3. Cancel supervisor tasks
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    print("[SUPERVISOR] Cleanup complete. Exiting.")


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

    web_task = asyncio.create_task(run_process(web_cmd, "WEB"))
    worker_task = asyncio.create_task(run_process(worker_cmd, "WORKER"))
    tasks = [web_task, worker_task]

    # Signal Handling
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        if not stop_event.is_set():
            stop_event.set()

    # Support POSIX (Linux/Render/macOS)
    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, signal_handler)

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        await shutdown(tasks)


if __name__ == "__main__":
    asyncio.run(main())