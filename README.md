# PAC1 — Supervisor de processos 

## Descripció

Aplicació Java que actua com a **supervisor de processos**. Crea processos fills amb `ProcessBuilder`, n'obté el PID i el PPID, comunica dades per `stdin`, `stdout` i `stderr`, controla el temps d'execució i gestiona la cancel·lació. També executa tres workers amb concurrència limitada i implementa una comunicació IPC entre productor i consumidor **sense utilitzar una pipe del shell**.

## Estructura del projecte

```text
ProgramacioProcessos_pt1/
├── pom.xml
├── practica01.service
├── src/
│   ├── main/java/
│   │   ├── CreacioProcessos.java
│   │   ├── ProcessRunner.java
│   │   └── ProcessExecutionResult.java
│   └── test/java/
│       └── ProcessRunnerTest.java
└── target/                         # Generat per Maven
```

- **`CreacioProcessos.java`:** conté `main()`, el supervisor, els rols ECHO/WORKER/PRODUCER/CONSUMER i les demostracions.
- **`ProcessRunner.java`:** construeix les ordres, crea processos, captura els fluxos i gestiona timeout i cancel·lació.
- **`ProcessExecutionResult.java`:** emmagatzema els resultats de cada procés.
- **`ProcessRunnerTest.java`:** bateria de nou proves automatitzades amb JUnit 5.

Les classes del projecte es troben al *package* per defecte; per tant, la classe principal s'executa com a `CreacioProcessos`.

## Compilació i execució

Des de l'arrel del projecte:

```bash
mvn clean compile
java -cp target/classes CreacioProcessos
```

Sense arguments, el programa executa la demostració completa: **ECHO → WORKERS → TIMEOUT → PIPE → CODI 7 → ORDRE INEXISTENT**.

Per guardar les sortides en fitxers independents:

```bash
java -cp target/classes CreacioProcessos > prova_completa.out 2> prova_completa.err
```

`stdout` recull la sortida normal del supervisor i `stderr` els seus diagnòstics. Alguns missatges capturats del `stderr` dels fills també apareixen al resum escrit pel supervisor a `stdout`; això no vol dir necessàriament que hi hagi errors.

## Proves principals realitzades

### 1. ECHO — Entrada estàndard i EOF

```bash
printf 'Ebre\nprocessos\nFI\n' | java -cp target/classes CreacioProcessos echo
```

**Resultat esperat:** `EBRE` i `PROCESSOS` per `stdout`; el procés s'atura amb la marca `FI` o en rebre EOF. El supervisor també registra els missatges d'inici i finalització del fill.

### 2. Tres workers amb un màxim de dos simultanis

A `parallelDemo()`, `Executors.newFixedThreadPool(2)` limita a dues les tasques de supervisió en execució simultània. Els workers tenen PID propis i comparteixen el PPID del supervisor.

| Worker | Entrada | Resultat esperat |
|---|---|---|
| ALPHA | Ada, Pol | `COUNT=2 CHARS=6` |
| BETA | Joana, Nil | `COUNT=2 CHARS=8` |
| GAMMA | Marc | `COUNT=1 CHARS=4` |
| **Total** | **5 noms** | **`COUNT=5 CHARS=18`** |

Per consultar el resum i les marques temporals:

```bash
grep -E 'WORKER-ALPHA|WORKER-BETA|WORKER-GAMMA|TOTAL WORKERS' prova_completa.out
```

**Comprovació feta:** ALPHA i BETA apareixen actius simultàniament; GAMMA comença quan un dels dos fils del pool queda lliure. `waitFor()` espera un procés fill i `Future.get()` recupera el resultat de la tasca de supervisió.

### 3. Mapa de processos amb `ps`

Per observar PID, PPID i estat dels processos mentre funcionen:

```bash
java -cp target/classes CreacioProcessos > prova_mapa.out 2> prova_mapa.err &
PID=$!
while kill -0 "$PID" 2>/dev/null; do
    ps -eo pid,ppid,stat,etime,cmd | awk -v pare="$PID" '$1==pare || $2==pare'
    sleep 0.1
done | tee mapa_ps.txt
wait "$PID"
```

L'evidència de l'informe mostra ALPHA i BETA vius al mateix instant i GAMMA posteriorment. En aquella captura es va observar l'estat `Sl` (espera interrompible i procés multifil).

**Nota:** els PID i PPID canvien en cada execució. No s'han d'utilitzar identificadors fixos en els tests ni barrejar PID de captures diferents com si corresponguessin a una sola execució.

Per mostrar un arbre de processos durant una prova llarga:

```bash
java -cp target/classes CreacioProcessos --hold-test
# Des d'un altre terminal, mentre el supervisor està actiu:
ps -eo pid,ppid,stat,etime,cmd | grep 'CreacioProcessos'
pstree -p PID_REAL_DEL_SUPERVISOR
```

### 4. Timeout i cancel·lació

El worker `LENT` simula una espera superior al límit configurat: `--sleep 2000` amb un timeout de **500 ms**.

```bash
grep -E 'WORKER-LENT|TIMEOUT|Encara viu' prova_completa.out
grep -i 'cancel' prova_completa.err
```

**Resultat observat:** `timeout=true`, estat `TIMEOUT`, cancel·lació del procés i `Encara viu: false`.

### 5. Pipe IPC — PRODUCER → SUPERVISOR → CONSUMER

```text
PRODUCER (stdout)
        |
        v
SUPERVISOR (fil pont getPont)
        |
        v
CONSUMER (stdin) → COUNT=5 CHARS=18
```

El productor genera **Ada, Pol, Joana, Nil i Marc**; el supervisor copia les línies al `stdin` del consumidor i tanca el canal en acabar, de manera que el consumidor rep EOF.

```bash
grep -E 'PIPE|PRODUCER|CONSUMER|COUNT=5 CHARS=18' prova_completa.out
```

**Resultat observat:** productor i consumidor acaben amb `exit=0`, `timeout=false`, i el consumidor retorna **`COUNT=5 CHARS=18`**. La connexió s'implementa al codi Java, no amb `|` del shell.

### 6. Codi d'error 7 i ordre inexistent

Per provar el codi de sortida d'un worker:

```bash
java -cp target/classes CreacioProcessos worker ERROR --exit-code 7 < /dev/null
echo $?
```

**Resultat esperat:** `7`. En la demostració completa, el supervisor registra `WORKER-ERROR` amb estat `ERROR` i continua amb les altres proves.

La prova d'**ordre inexistent** intenta iniciar `/ruta/inexistent/executable`: `ProcessBuilder.start()` genera una `IOException`, que el supervisor captura i registra.

```bash
grep -i 'ordre inexistent' prova_completa.err
```

### 7. Nou tests automatitzats amb JUnit 5

```bash
mvn test
# Alternativa sense Internet, si les dependències ja són a la memòria cau:
mvn -o test
```

| Test | Comprovació |
|---|---|
| `testEcho()` | Entrada per `stdin`, majúscules i sortida correcta. |
| `testStderr()` | Captura dels diagnòstics de `stderr`. |
| `testWorkers()` | Recompte dels tres workers. |
| `testLimitConcurrencia()` | Limitació a dos workers simultanis. |
| `testTimeout()` | Temps límit i finalització del procés. |
| `testExitCode7()` | Identificació del codi 7 i estat `ERROR`. |
| `testOrdreInexistent()` | Detecció d'una `IOException`. |
| `testPipe()` | Transferència de dades entre productor i consumidor. |
| `testCancelacio()` | Aturada segura del worker. |

**Resultat obtingut amb OpenJDK 25.0.2:**

```text
Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## Execució com a JAR

Després de compilar:

```bash
jar --create --file supervisor.jar --main-class CreacioProcessos -C target/classes .
java -jar supervisor.jar
```

El punt (`.`) al final de la primera ordre és necessari per incloure les classes de `target/classes`.

## Servei `systemd`

El projecte inclou `practica01.service`. Durant les proves documentades s'ha utilitzat l'executable **OpenJDK 25.0.2** de `/home/alumnat/.jdks/openjdk-25.0.2/bin/java`.

Instal·lació del servei, des de l'arrel del projecte:

```bash
sudo cp practica01.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start practica01.service
sudo systemctl status practica01.service --no-pager -l
sudo journalctl -u practica01.service -b --no-pager
```

La unitat utilitza `Type=oneshot`, `User=alumnat`, `TimeoutStartSec=90s`, `TimeoutStopSec=5s`, `KillMode=control-group` i envia `stdout` i `stderr` al journal. Després d'una execució normal, un servei `oneshot` pot quedar com a `inactive (dead)` encara que hagi acabat correctament.

### Prova de parada controlada amb `--hold-test`

1. Afegir temporalment `--hold-test` al final de la línia `ExecStart` de `practica01.service`.
2. Tornar a copiar el fitxer i executar `sudo systemctl daemon-reload`.
3. **Terminal A:** executar `sudo systemctl start practica01.service`.
4. **Terminal B**, abans que venci `TimeoutStartSec`: comprovar `sudo systemctl status practica01.service --no-pager -l` i els processos actius.
5. Des del terminal B, executar `sudo systemctl stop practica01.service` i verificar que no queden processos de l'aplicació.

```bash
ps -eo pid,ppid,stat,etime,cmd | grep -E '[s]upervisor.jar|[C]reacioProcessos'
sudo journalctl -u practica01.service -b --no-pager -n 100
```

**Evidència de l'informe:** durant `--hold-test` es van observar el supervisor (PID **42789**) i el worker SERVICE (PID **42810**, PPID **42789**) dins del mateix grup de control del servei. També es documenta la comprovació de tancament dels processos.

**En acabar:** retirar `--hold-test`, tornar a copiar la unitat i executar `sudo systemctl daemon-reload` per recuperar la configuració normal.


## Criteris i consideracions

- Els processos fills es creen amb `ProcessBuilder` i una llista d'arguments, sense delegar les connexions IPC al shell.
- La captura concurrent de `stdout` i `stderr` redueix el risc de bloqueig per buffers plens.
- `stopTree()` i el *shutdown hook* gestionen cancel·lacions i terminacions; `systemd` proporciona una capa addicional de gestió del grup de processos.
- L'equivalent conceptual a **fork** és `pool.submit()` i `ProcessBuilder.start()`; a **join**, `waitFor()` i `Future.get()`. **No s'utilitza `ForkJoinPool`.**
- Els exemples de PID són evidències històriques, no identificadors constants.

## Documentació

Les captures, les dades concretes de les execucions, el registre d'incidències i l'explicació de les decisions de disseny es troben a l'**informe tècnic de la PAC1**.

### Ús d'IA

s'ha utilitzat IA com a suport per comprendre les API de processos i concurrència, redactar la documentació i preparar el script d'observació amb `ps` i els tests JUnit. 
