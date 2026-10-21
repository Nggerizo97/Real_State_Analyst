"""
scripts/ci_prepare_venv.py
==========================
Prepara el venv del job self-hosted de MercadoLibre (scraper_cron.yml).

Lo ejecuta el Python base que localiza el paso "Locate Python". Deja el venv
en RUNNER_TOOL_CACHE para que sobreviva entre corridas (el checkout borra el
workspace) y exporta VENV_PY para los pasos siguientes.

Uso (en CI):
    "%PYTHON_EXE%" scripts\\ci_prepare_venv.py
"""

import os
import shutil
import subprocess
import sys
import venv
from pathlib import Path

REQUIREMENTS = Path(__file__).resolve().parent.parent / "requirements-scraper.txt"


def _venv_python(env_dir: Path) -> Path:
    return env_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _is_usable(py: Path) -> bool:
    if not py.exists():
        return False
    try:
        return subprocess.call([str(py), "-c", "import sys"]) == 0
    except OSError:
        return False


def main() -> None:
    base = Path(os.environ.get("RUNNER_TOOL_CACHE") or Path.home())
    env_dir = base / "real-state-analyst-venv"
    py = _venv_python(env_dir)

    # Si se reinstala o mueve el Python base, el venv queda apuntando a un
    # intérprete que ya no existe: se recrea en vez de fallar con un error raro.
    if env_dir.exists() and not _is_usable(py):
        print(f"venv inutilizable en {env_dir}, se recrea.")
        shutil.rmtree(env_dir)
    if not env_dir.exists():
        print(f"Creando venv en {env_dir} con {sys.executable}")
        venv.create(env_dir, with_pip=True)

    def run(*args: str) -> None:
        subprocess.run([str(py), *args], check=True)

    run("-m", "pip", "install", "--upgrade", "pip")
    run("-m", "pip", "install", "-r", str(REQUIREMENTS))
    run("-m", "playwright", "install", "chromium")

    github_env = os.environ.get("GITHUB_ENV")
    if github_env:
        with open(github_env, "a", encoding="utf-8") as f:
            f.write(f"VENV_PY={py}\n")
    print(f"VENV_PY={py}")


if __name__ == "__main__":
    main()
