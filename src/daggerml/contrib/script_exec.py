import fcntl
import logging
import os
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field

from daggerml.core import Dml

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
    dml: Dml
    cache_dir: str = field(init=False)

    def __post_init__(self):
        config_dir = self.dml("status")["config_dir"]
        config_dir = os.getenv("DML_FN_CACHE_DIR", config_dir)
        self.cache_dir = f"{config_dir}/cache/daggerml.contrib/{self.dml.cache_key}"
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

    def submit(self):
        with self.dml.new("foo", "bar") as dag:
            script = dag.argv[1].value()
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
            proc.stdin.write(self.dml.data)
            proc.stdin.close()
        with open(self.exec_log, "w") as f:
            f.write("0")
        return proc.pid


def cli():
    with Dml(data=sys.stdin.read()) as dml:
        run = RunInfo(dml)
        try:
            with file_lock(run.pid_file) as lockf:
                pid = lockf.read()
                if pid == "":  # need start
                    pid = run.submit()
                    lockf.seek(0)
                    lockf.truncate()
                    lockf.write(f"{pid}")
                    logger.info("started %r with pid: %r", dml.cache_key, pid)
                    return
                pid = int(pid)
                if proc_exists(pid):
                    logger.info("job %r with pid: %r still running", dml.cache_key, pid)
                elif os.path.isfile(run.result_loc):
                    with open(run.result_loc, "r") as f:
                        print(f.read())
                else:
                    with open(run.exec_log) as f:
                        n = int(f.read().strip())
                    if n > 20:
                        raise RuntimeError(f"{pid = } does not exist and neither does result file")
                    with open(run.exec_log, "w") as f:
                        f.write(str(n + 1))
        except Exception:
            logger.exception("could not acquire lock and update... try again?")
