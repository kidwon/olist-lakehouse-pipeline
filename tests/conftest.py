import os
import sys
import time
import uuid
from pathlib import Path

import pytest

# PySpark 4 needs Java 17 or 21 (Hadoop breaks on 24+). If JAVA_HOME is missing or too new,
# fall back to Homebrew's openjdk@17 when it is installed.
_BREW_JDK17 = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"


def _java_major(home: str) -> int | None:
    release = Path(home) / "release"
    if not release.exists():
        return None
    for line in release.read_text().splitlines():
        if line.startswith("JAVA_VERSION="):
            return int(line.split("=")[1].strip('"').split(".")[0])
    return None


_current = os.environ.get("JAVA_HOME")
if (not _current or (_java_major(_current) or 0) > 21) and Path(_BREW_JDK17).exists():
    os.environ["JAVA_HOME"] = _BREW_JDK17

# PySpark converts naive Python datetimes with the process time zone; pin it so test data
# means the same thing on a laptop in Tokyo and on a CI runner.
os.environ["TZ"] = "UTC"
time.tzset()

sys.path.insert(0, str(Path(__file__).parent))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from olist_pipeline.cli import local_spark

    base = tmp_path_factory.mktemp("spark")
    s = local_spark(str(base))
    s.sparkContext.setLogLevel("ERROR")
    s.conf.set("spark.sql.session.timeZone", "UTC")
    s.sql("CREATE DATABASE IF NOT EXISTS unit")
    yield s
    s.stop()


@pytest.fixture
def table_name():
    return f"unit.t_{uuid.uuid4().hex[:8]}"
