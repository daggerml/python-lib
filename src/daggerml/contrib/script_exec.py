import fcntl
import json
import logging
import os
import re
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


def get_exec_logger(cache_dir):
    def writer(x):
        with open(f"{cache_dir}/exec-log", "a") as f:
            f.write(str(x) + "\n")


@contextmanager
def file_lock(file_path, mode="r+"):
    try:
        # create the file if it doesn't exist
        fd = os.open(file_path, os.O_RDWR | os.O_CREAT)
        with open(fd, mode) as file:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                file.seek(0)
                yield file
                file.flush()
            finally:
                fcntl.flock(file, fcntl.LOCK_UN)  # Unlock
    except Exception as e:
        logger.exception("could not acquire lock (%r)...", e)
        raise


def proc_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@dataclass
class RunInfo:
    cache_key: str
    cache_dir: str = field(init=False)

    def __post_init__(self):
        status = subprocess.run(["dml", "status"], check=True, capture_output=True)
        config_dir = json.loads(status.stdout.decode())["config_dir"]
        config_dir = os.getenv("DML_FN_CACHE_DIR", config_dir)
        self.cache_dir = f"{config_dir}/cache/daggerml.contrib/{self.cache_key}"
        os.makedirs(self.cache_dir, exist_ok=True)

    @property
    def pid_file(self):
        return f"{self.cache_dir}/pid"

    @property
    def script_loc(self):
        return f"{self.cache_dir}/script.py"

    @property
    def stdout_loc(self):
        return f"{self.cache_dir}/stdout"

    @property
    def stderr_loc(self):
        return f"{self.cache_dir}/stderr"

    @property
    def result_loc(self):
        return f"{self.cache_dir}/result"

    @property
    def exec_log(self):
        return f"{self.cache_dir}/exec.log"

    def submit(self, script, dump):
        with open(self.script_loc, "w") as f:
            f.write(script)
        subprocess.run(["chmod", "+x", self.script_loc], check=True)
        proc = subprocess.Popen(
            [self.script_loc, self.result_loc],
            stdout=open(self.stdout_loc, "w"),
            stderr=open(self.stderr_loc, "w"),
            stdin=subprocess.PIPE,
            start_new_session=True,
            text=True,
        )
        proc.stdin.write(dump)
        proc.stdin.close()
        with open(self.exec_log, "w") as f:
            f.write("starting")
        return proc.pid

    def log(self, msg):
        with open(self.exec_log, "a") as f:
            f.write(f"{msg}\n")


def cli():
    data = json.loads(sys.stdin.read())
    run = RunInfo(data["cache_key"])
    try:
        run.log("getting lock")
        with file_lock(run.pid_file) as lockf:
            run.log("lock acquired")
            pid = lockf.read()
            if pid == "":  # need start
                run.log("pidfile was empty... submitting job")
                try:
                    pid = run.submit(data["kwargs"]["script"][-1], data["dump"])
                except Exception as e:
                    run.log(f"job submission failed with error: {e}")
                lockf.seek(0)
                lockf.truncate()
                lockf.write(f"{pid}")
                logger.info("started %r with pid: %r", data["cache_key"], pid)
                run.log(f"job submitted with {pid = }. Exiting...")
                return
            pid = int(pid)
            run.log(f"PID file nonempty ({pid = })")
            if proc_exists(pid):
                run.log(f"{pid = } is still running... exiting...")
                logger.info("job %r with pid: %r still running", data["cache_key"], pid)
            elif os.path.isfile(run.result_loc):
                run.log("result file exists. Reading and exiting...")
                with open(run.result_loc, "r") as f:
                    print(f.read())
            else:
                wait_msg = "result file does not exist."
                run.log(wait_msg)
                with open(run.exec_log) as f:
                    n = len([line for line in f if re.match(f"^{wait_msg}$", line.strip())])
                if n > 20:
                    run.log(f"Raising error after {n} attempts to get result file.")
                    raise RuntimeError(f"{pid = } does not exist and neither does result file")
                with open(run.exec_log, "w") as f:
                    f.write(str(n + 1))
                run.log(f"Exiting after {n} attempts to get result file.")
    except Exception:
        logger.exception("could not acquire lock and update... try again?")
