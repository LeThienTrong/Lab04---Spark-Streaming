#!/usr/bin/env bash
# One-shot environment setup for Ubuntu (tested on 24.04 LTS; works on 22.04).
#
#   bash scripts/setup_ubuntu.sh
#
# Installs Docker from Docker's own repository, a JDK that Spark supports,
# and a Python virtual environment with the project dependencies.
# It is safe to re-run: every step checks before acting.
set -euo pipefail

step() { echo; echo "=== $* ==="; }
ok()   { echo "  ok    $*"; }
warn() { echo "  warn  $*"; }

step "0. System"
. /etc/os-release
echo "  $PRETTY_NAME"
if [ "${VERSION_ID%%.*}" -lt 22 ]; then
  warn "Ubuntu 22.04 or newer is recommended."
fi
MEM_GB=$(( $(grep MemTotal /proc/meminfo | awk '{print $2}') / 1024 / 1024 ))
echo "  RAM: ${MEM_GB} GB"
if [ "$MEM_GB" -lt 8 ]; then
  warn "This stack wants ~6 GB free. With ${MEM_GB} GB you may need to run the"
  warn "Spark job after the Neo4j ingestion finishes rather than concurrently."
fi

step "1. Base packages"
sudo apt-get update -qq
sudo apt-get install -y -qq \
  curl git ca-certificates gnupg jq unzip \
  python3-venv python3-pip
ok "base packages installed"

step "2. Java (Spark 3.5 supports 8/11/17; Ubuntu 24.04 ships 21)"
if ! dpkg -l | grep -q openjdk-17-jdk; then
  sudo apt-get install -y -qq openjdk-17-jdk
fi
JAVA17=$(dirname "$(dirname "$(readlink -f "$(which javac 2>/dev/null || echo /usr/lib/jvm/java-17-openjdk-amd64/bin/javac)")")")
if [ -d /usr/lib/jvm/java-17-openjdk-amd64 ]; then
  JAVA17=/usr/lib/jvm/java-17-openjdk-amd64
fi
ok "JDK 17 at $JAVA17"
echo
echo "  Add this to your ~/.bashrc so spark-submit uses Java 17:"
echo "      export JAVA_HOME=$JAVA17"
echo "      export PATH=\$JAVA_HOME/bin:\$PATH"

step "3. Docker Engine (from Docker's repository, not snap)"
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  ok "docker + compose plugin already present: $(docker --version)"
else
  if snap list docker >/dev/null 2>&1; then
    warn "The snap version of Docker is installed. It has known problems with"
    warn "bind mounts and is not recommended. Remove it with:"
    warn "    sudo snap remove docker"
  fi
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $VERSION_CODENAME stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update -qq
  sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io \
    docker-buildx-plugin docker-compose-plugin
  ok "docker installed"
fi

step "4. Run docker without sudo"
if groups "$USER" | grep -qw docker; then
  ok "$USER is already in the docker group"
else
  sudo groupadd -f docker
  sudo usermod -aG docker "$USER"
  warn "Added $USER to the docker group."
  warn "LOG OUT AND BACK IN (or run 'newgrp docker') before continuing."
fi

step "5. Kernel limit for Neo4j / Kafka"
CUR=$(sysctl -n vm.max_map_count)
if [ "$CUR" -lt 262144 ]; then
  sudo sysctl -w vm.max_map_count=262144
  echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf > /dev/null
  ok "raised vm.max_map_count to 262144 (persisted)"
else
  ok "vm.max_map_count is $CUR"
fi

step "6. Python virtual environment"
# Ubuntu 23.04+ enforces PEP 668: pip refuses to install system-wide.
# A venv is the correct answer, not --break-system-packages.
if [ ! -d .venv ]; then
  python3 -m venv .venv
  ok "created .venv"
else
  ok ".venv already exists"
fi
# shellcheck disable=SC1091
source .venv/bin/activate
pip install -q --upgrade pip

PYMINOR=$(python -c 'import sys; print(sys.version_info[1])')
echo "  Python 3.$PYMINOR"
if [ "$PYMINOR" -ge 12 ]; then
  warn "kafka-python 2.0.2 is broken on Python 3.12+ (kafka.vendor.six.moves)."
  warn "Installing the maintained fork kafka-python-ng instead - same import name."
  pip install -q -r requirements-py312.txt
else
  pip install -q -r requirements.txt
fi
ok "python dependencies installed"

step "7. Verify"
python - <<'PY'
mods = ["kafka", "neo4j", "pyspark", "pymongo", "jsonschema", "pandas", "nbformat"]
bad = []
for m in mods:
    try:
        __import__(m)
        print(f"  ok    import {m}")
    except Exception as e:
        bad.append(m)
        print(f"  FAIL  import {m}: {e}")
import jupyter_book
v = jupyter_book.__version__
print(f"  {'ok   ' if v.startswith('1.') else 'FAIL '} jupyter-book {v} (must be 1.x, not 2.x)")
raise SystemExit(1 if bad else 0)
PY

cat <<'EOF'

=== Setup complete ===

Next steps:

  1. If you were just added to the docker group, log out and back in.

  2. Put JAVA_HOME in your shell profile (see step 2 above), then:
         source ~/.bashrc
         java -version        # should report 17

  3. Fetch the Neo4j connector and build the Connect image:
         bash scripts/fetch_neo4j_connector.sh
         docker compose build connect

  4. Start the stack and check it:
         docker compose up -d
         bash scripts/healthcheck.sh

  Remember to activate the venv in every new terminal:
         source .venv/bin/activate
EOF
