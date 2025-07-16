import os
from dotenv import load_dotenv

# Cargar .env solo si NO estamos en producción
if not os.getenv("RENDER"):
    load_dotenv()

# Variables obligatorias
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
GITHUB_SUBFOLDER = os.getenv("GITHUB_SUBFOLDER", "")

# Validación (lanza error si falta algo esencial)
if not GITHUB_TOKEN or not GITHUB_REPO:
    raise EnvironmentError("❌ Faltan variables obligatorias para GitHub en el entorno")
