# Running this lab on Ubuntu

Verified against **Ubuntu 24.04.4 LTS**. Everything also applies to 22.04, with
one difference noted below.

Ubuntu is the easiest platform for this lab — Docker runs natively rather than
inside a VM — but it has four specific traps. All four are handled by
`scripts/setup_ubuntu.sh`; this document explains what they are so you can
diagnose them if something still goes wrong.

---

## The four Ubuntu-specific traps

### 1. Python 3.12 breaks `kafka-python`

Ubuntu 24.04 ships Python 3.12. The pinned `kafka-python==2.0.2` fails on
import:

```
ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
```

The library vendored an old copy of `six` that relies on import machinery
removed in 3.12. **You do not need to install Python 3.11.** Use the maintained
fork, which has the same import name:

```bash
pip install kafka-python-ng     # import kafka  -> unchanged
```

`requirements-py312.txt` already does this. On Ubuntu 22.04 (Python 3.10) the
original `requirements.txt` works fine.

### 2. PEP 668 blocks system-wide pip

Ubuntu 23.04 and later mark the system Python as externally managed:

```
error: externally-managed-environment
```

The correct fix is a virtual environment, **not** `--break-system-packages`,
which can corrupt apt-managed Python packages:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Remember to activate it in **every new terminal** — you will need at least three
open at once (Spark job, parser, queries).

### 3. Java 21 vs Spark 3.5

Ubuntu 24.04 ships JDK 21. Spark 3.5 officially supports Java 8, 11 and 17;
Java 21 support arrived in Spark 4.

In testing, Spark 3.5.1 ran fine on Java 21 for basic DataFrame work, so you may
never hit a problem. But the Kafka and MongoDB connectors exercise more of the
JVM's module system, which is exactly where Java 21 tends to throw
`IllegalAccessError` or `InaccessibleObjectException`. Install 17 and point
`JAVA_HOME` at it:

```bash
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
java -version        # must report 17
```

Put those two `export` lines in `~/.bashrc`. This does not remove Java 21 —
`update-alternatives` still lets other applications use it.

### 4. `vm.max_map_count` is too low

Ubuntu's default is 65530. Neo4j wants 262144 and will either refuse to start or
behave erratically under load:

```bash
sudo sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf   # persist
```

---

## Docker: install from Docker's repository, not snap

```bash
sudo snap list docker           # if this shows anything, remove it
sudo snap remove docker
```

The snap package runs confined and has long-standing problems with bind mounts
and host networking — both of which this stack uses. `scripts/setup_ubuntu.sh`
installs Docker Engine from `download.docker.com` and adds you to the `docker`
group.

After being added to the group you **must** log out and back in (or run
`newgrp docker`) before `docker` works without `sudo`. This catches almost
everyone once.

---

## Memory

The stack needs roughly 6 GB. The compose file caps each JVM heap for an 8 GB
machine: Kafka 1 GB, Connect 1 GB, Neo4j 1 GB heap plus 512 MB page cache.

```bash
free -h        # check "available", not "free"
```

On 8 GB, run the pipeline in two phases rather than concurrently:

1. Start the stack, register the Neo4j connectors, run the parser, let the graph
   ingestion finish.
2. Then start the Spark job. It reads `cpg.metadata` from the beginning of the
   topic, so nothing is lost by starting it late.

On 16 GB you can run everything at once and raise the heap settings.

If Docker gets OOM-killed you will see containers exiting with code 137:

```bash
docker inspect neo4j --format '{{.State.ExitCode}} {{.State.OOMKilled}}'
```

---

## Full walkthrough

### Phase 0 — Setup (once, ~20 minutes)

```bash
unzip lab04_complete.zip && cd lab04
bash scripts/setup_ubuntu.sh
# then, as the script instructs:
#   log out and back in (docker group)
#   add JAVA_HOME to ~/.bashrc, then: source ~/.bashrc
```

### Phase 1 — Build the Connect image with the Neo4j plugin baked in

This is the step that removes the biggest single risk in the lab. Instead of
letting the container run `confluent-hub install` at startup — which fails
silently behind proxies and leaves you with a running Connect that has no Neo4j
plugin — the JAR is baked into the image at build time.

```bash
source .venv/bin/activate
bash scripts/fetch_neo4j_connector.sh      # resolves + downloads the release JAR
docker compose build connect
docker compose up -d
bash scripts/healthcheck.sh
```

If `fetch_neo4j_connector.sh` cannot reach the GitHub API it prints manual
instructions — download the self-contained JAR for *Neo4j Connector for Apache
Kafka* (not the Confluent `.zip`) from
<https://github.com/neo4j/neo4j-kafka-connector/releases> into
`docker/connect/plugins/`, then re-run `docker compose build connect`.

The Dockerfile fails the build if that directory is empty, so you find out
immediately rather than at connector-registration time.

Do not proceed until `healthcheck.sh` prints `ALL CHECKS PASSED`.

### Phase 2–5

From here the process is platform-independent. Follow `README.md`, sections
"Phase 2" through "Phase 4".

One Ubuntu note for the Spark step: run `spark-submit` **on the host**, inside
your venv, using `localhost:9092`. The compose file no longer ships a Spark
container — running it natively is simpler and avoids container-to-container
networking issues. `pip install pyspark` provides `spark-submit` on your PATH.

```bash
source .venv/bin/activate
which spark-submit          # .venv/bin/spark-submit
```

---

## Three terminals

You will want these open simultaneously, each with the venv activated:

| Terminal | Purpose |
|---|---|
| 1 | `spark-submit` — long-running, leave it printing batch logs |
| 2 | parser runs, replay, sweep |
| 3 | `docker exec` queries against Neo4j and MongoDB |

---

## Taking screenshots

The lab requires database UI screenshots. On a desktop install:

- Neo4j Browser: <http://localhost:7474> in Firefox, log in `neo4j` / `password`
- MongoDB Compass: `sudo apt install ./mongodb-compass_*.deb` after downloading
  the `.deb` from mongodb.com, then connect to `mongodb://localhost:27017`
- Spark UI: <http://localhost:4040> **while the job is running** (the page
  disappears when the job stops — capture it before you stop Spark)

Ubuntu's built-in screenshot tool is `PrtSc`, or `gnome-screenshot -a` for a
region. Save into `jupyter-book/images/` using the filenames listed in
`jupyter-book/images/README.md`.

If you are on a headless server or WSL without a GUI, use `curl` output as
evidence instead and say so in the reflection — but the Neo4j Browser graph view
is worth the trouble of getting a desktop session, because it is the clearest
single image of what the pipeline produced.

---

## WSL2

Everything above works under WSL2 with two caveats:

- Use Docker Desktop's WSL2 backend, or install Docker Engine inside the WSL
  distribution. Do not mix both.
- Keep the project on the Linux filesystem (`~/lab04`), not under `/mnt/c/`.
  Filesystem performance across the Windows boundary is roughly an order of
  magnitude worse, and the parser writes tens of thousands of events.
- `localhost` forwarding from Windows to WSL2 works for the browser UIs, so
  Neo4j Browser and Compass on Windows can reach the containers.

---

## Ubuntu-specific troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `permission denied ... docker.sock` | not in docker group yet | log out/in, or `newgrp docker` |
| `externally-managed-environment` | PEP 668 | use the venv |
| `No module named 'kafka.vendor.six.moves'` | Python 3.12 | `pip install kafka-python-ng` |
| Container exits with code 137 | out of memory | raise Docker memory, or run Spark separately |
| Neo4j fails to start, mmap errors | `vm.max_map_count` | `sudo sysctl -w vm.max_map_count=262144` |
| `spark-submit: command not found` | venv not active | `source .venv/bin/activate` |
| `IllegalAccessError` from Spark | Java 21 | switch `JAVA_HOME` to JDK 17 |
| Port 9092/7474/27017 already in use | a local service is bound | `sudo ss -tlnp \| grep 9092` then stop it |

For failures that are not Ubuntu-specific — connector tasks failing, duplicate
nodes after replay, Spark package mismatches — see `TROUBLESHOOTING.md`.
