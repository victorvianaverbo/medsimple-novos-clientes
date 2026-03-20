"""
publish_baseline.py
Publica o .tmp/new_clients.csv como um GitHub Gist privado.

Na primeira execução: cria o Gist e salva o ID no .env.
Nas seguintes: atualiza o mesmo Gist.

Pré-requisito:
  GITHUB_GIST_TOKEN no .env — Personal Access Token com scope "gist"
  Criar em: https://github.com/settings/tokens/new → selecionar "gist"
"""

import os
import re
import requests
from dotenv import load_dotenv

load_dotenv()

BASELINE_FILE = os.path.join(os.path.dirname(__file__), "..", ".tmp", "new_clients.csv")
ENV_FILE = os.path.join(os.path.dirname(__file__), "..", ".env")

GITHUB_API = "https://api.github.com"


def get_env(key):
    return os.getenv(key, "").strip()


def update_env_var(key, value):
    """Atualiza ou adiciona uma variável no .env."""
    with open(ENV_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    pattern = rf"^{key}=.*$"
    replacement = f"{key}={value}"

    if re.search(pattern, content, flags=re.MULTILINE):
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    else:
        content += f"\n{replacement}\n"

    with open(ENV_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"[.env] {key} atualizado.")


def main():
    print("=" * 55)
    print("PUBLISH BASELINE — GitHub Gist Privado")
    print("=" * 55)

    token = get_env("GITHUB_GIST_TOKEN")
    if not token:
        print("\n[ERRO] GITHUB_GIST_TOKEN não encontrado no .env")
        print("Crie em: https://github.com/settings/tokens/new")
        print("Scope necessário: 'gist'")
        return

    if not os.path.exists(BASELINE_FILE):
        print(f"\n[ERRO] Arquivo não encontrado: {BASELINE_FILE}")
        print("Execute primeiro: python execution/identify_new_clients.py")
        return

    with open(BASELINE_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    print(f"Baseline carregado: {len(content.splitlines())-1} registros")

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }

    payload = {
        "description": "Medsimple — Baseline novos clientes (gerado automaticamente)",
        "public": False,
        "files": {
            "new_clients.csv": {"content": content}
        }
    }

    gist_id = get_env("GITHUB_GIST_ID")

    if gist_id:
        print(f"\nAtualizando Gist existente: {gist_id}...")
        resp = requests.patch(f"{GITHUB_API}/gists/{gist_id}", json=payload, headers=headers, timeout=30)
        action = "atualizado"
    else:
        print("\nCriando novo Gist privado...")
        resp = requests.post(f"{GITHUB_API}/gists", json=payload, headers=headers, timeout=30)
        action = "criado"

    if resp.status_code in (200, 201):
        data = resp.json()
        gist_id = data["id"]
        gist_url = data["html_url"]
        update_env_var("GITHUB_GIST_ID", gist_id)
        print(f"\n[OK] Gist {action} com sucesso!")
        print(f"ID: {gist_id}")
        print(f"URL: {gist_url}")
        print("\nAdicione ao Streamlit Cloud secrets:")
        print(f'  GITHUB_GIST_TOKEN = "{token}"')
        print(f'  GITHUB_GIST_ID = "{gist_id}"')
    else:
        print(f"\n[ERRO] Status {resp.status_code}: {resp.text[:300]}")


if __name__ == "__main__":
    main()
