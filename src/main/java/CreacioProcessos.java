import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Programa de demostració de creació i supervisió de processos.
 *
 * <p>La mateixa classe s'executa com a supervisor o com a procés fill. El rol
 * s'escull amb el primer argument de la línia d'ordres.</p>
 */
public class CreacioProcessos {

    /** Crea la classe que conté els rols del supervisor i dels processos fills. */
    public CreacioProcessos() {
    }

    /**
     * Selecciona el rol que s'executarà segons els arguments rebuts.
     *
     * @param args arguments de la línia d'ordres; el primer indica el rol
     * @throws Exception si falla alguna operació del rol seleccionat
     */
    public static void main(String[] args) throws Exception {
        String rol = args.length == 0 ? "supervisor" : args[0].toLowerCase(Locale.ROOT);
        switch (rol) {
            case "supervisor" -> runSupervisor();
            case "echo" -> runEcho();
            case "worker" -> runWorker(args);
            case "producer" -> runProducer();
            case "consumer" -> runConsumer();
            case "--hold-test" -> runHoldTest();
            default -> throw new IllegalArgumentException("Rol desconegut: " + rol);
        }
    }

    /** Executa les demostracions de processos i les proves de gestió d'errors. */
    private static void runSupervisor() throws Exception {
        diagnostic("SUPERVISOR", "inici");
        stdinDemo();
        parallelDemo();
        timeoutDemo();
        pipeDemo();
        exitCodeDemo();
        missingCommandDemo();
        diagnostic("SUPERVISOR", "final");
    }

    /** Envia text a un procés echo i mostra les línies que aquest retorna. */
    private static void stdinDemo() throws Exception {
        System.out.println("\n=== ECHO ===");
        // Enviem text al fill; supervise() tanca stdin i recull la resposta.
        printResult(ProcessRunner.supervise(ProcessRunner.javaCommand("echo"),
                "ECHO", Duration.ofSeconds(3), "Ebre\nprocessos\nFI\n"));
    }

    /** Executa tres workers en paral·lel, amb un màxim de dos alhora, i suma els resultats. */
    private static void parallelDemo() throws Exception {
        System.out.println("\n=== WORKERS: TRES AMB MAXIM DOS ALHORA ===");
        String[] noms = {"ALPHA", "BETA", "GAMMA"};
        String[] entrades = {"Ada\nPol\n", "Joana\nNil\n", "Marc\n"};
        try (var pool = Executors.newFixedThreadPool(2)) {
            var tasques = new ArrayList<Future<ProcessExecutionResult>>();
            for (int i = 0; i < noms.length; i++) {
                String nom = noms[i], entrada = entrades[i];
                // Cada tasca crea un PROCÉS worker i espera que acabi.
                tasques.add(pool.submit(() -> ProcessRunner.supervise(
                        ProcessRunner.javaCommand("worker", nom),
                        "WORKER-" + nom, Duration.ofSeconds(5), entrada)));
            }
            int count = 0, chars = 0;
            for (var tasca : tasques) {
                var resultat = tasca.get(); // Espera i recull el resultat del worker.
                printResult(resultat);
                if (!resultat.status().equals("OK")) throw new IOException("Ha fallat un worker");
                for (String linia : resultat.stdout()) {
                    if (linia.contains("COUNT=")) {
                        // Extraiem COUNT i CHARS de la resposta del worker.
                        String[] nums = linia.substring(linia.indexOf("COUNT=")).split("[ =]");
                        count += Integer.parseInt(nums[1]);
                        chars += Integer.parseInt(nums[3]);
                    }
                }
            }
            System.out.println("TOTAL WORKERS: COUNT=" + count + " CHARS=" + chars);
        }
    }

    /** Comprova que un worker que supera el límit de temps és aturat. */
    private static void timeoutDemo() throws Exception {
        System.out.println("\n=== TIMEOUT ===");
        // Aquest worker tarda més que els 500 ms permesos.
        var r = ProcessRunner.supervise(
                ProcessRunner.javaCommand("worker", "LENT", "--sleep", "2000"),
                "WORKER-LENT", Duration.ofMillis(500), null);
        printResult(r);
        System.out.println("Encara viu: " + ProcessHandle.of(r.pid())
                .map(ProcessHandle::isAlive).orElse(false));
    }

    /**
     * Connecta la sortida del productor amb l'entrada del consumidor mitjançant un fil pont.
     *
     * <p>També controla els temps límit i els codis de finalització dels dos processos.</p>
     */
    private static void pipeDemo() throws Exception {
        System.out.println("\n=== PIPE: PRODUCTOR -> SUPERVISOR -> CONSUMIDOR ===");
        List<String> cmdP = ProcessRunner.javaCommand("producer");
        List<String> cmdC = ProcessRunner.javaCommand("consumer");
        Process p = null, c = null;
        try {
            long iniciC = System.nanoTime();
            c = ProcessRunner.startProcess(cmdC); // El consumidor espera stdin.
            long iniciP = System.nanoTime();
            p = ProcessRunner.startProcess(cmdP); // El productor escriu stdout.

            List<String> dades = new ArrayList<>(), errP = new ArrayList<>();
            List<String> outC = new ArrayList<>(), errC = new ArrayList<>();
            // Llegim stderr del productor i stdout/stderr del consumidor.
            Thread t1 = ProcessRunner.drain(p.getErrorStream(), errP, "stderr");
            Thread t2 = ProcessRunner.drain(c.getInputStream(), outC, "stdout");
            Thread t3 = ProcessRunner.drain(c.getErrorStream(), errC, "stderr");

            Thread pont = getPont(p, c, dades); // crida a getPont

            boolean timeoutP = !p.waitFor(5, TimeUnit.SECONDS);
            if (timeoutP) ProcessRunner.stopTree(p, "timeout productor");
            if (!timeoutP && p.exitValue() != 0) ProcessRunner.stopTree(c, "error productor");
            boolean timeoutC = !c.waitFor(5, TimeUnit.SECONDS);
            if (timeoutC) ProcessRunner.stopTree(c, "timeout consumidor");

            pont.join(); t1.join(); t2.join(); t3.join(); // Acaben els fils de lectura.
            printResult(ProcessRunner.resultat(p, cmdP, "PRODUCER", iniciP, dades, errP, timeoutP));
            printResult(ProcessRunner.resultat(c, cmdC, "CONSUMER", iniciC, outC, errC, timeoutC));
            if (dades.stream().anyMatch(s -> s.contains("[pipe-error]")))
                throw new IOException("Error al pont de la pipe");
        } finally {
            // També en cas d'error, netegem tots dos processos.
            if (p != null) {
                if (p.isAlive()) ProcessRunner.stopTree(p, "neteja pipe");
                ProcessRunner.finished(p);
            }
            if (c != null) {
                if (c.isAlive()) ProcessRunner.stopTree(c, "neteja pipe");
                ProcessRunner.finished(c);
            }
        }
    }

    /**
     * Crea el fil que copia les línies de stdout del productor a stdin del consumidor.
     *
     * @param p procés productor
     * @param c procés consumidor
     * @param dades llista on es desen les línies rebudes del productor
     * @return fil pont iniciat
     */
    private static Thread getPont(Process p, Process c, List<String> dades) {
        Process productor = p, consumidor = c;
        // Un sol fil llegeix stdout del productor i el reenvia a stdin del consumidor.
        Thread pont = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(
                    productor.getInputStream(), StandardCharsets.UTF_8));
                 BufferedWriter w = consumidor.outputWriter(StandardCharsets.UTF_8)) {
                String linia;
                while ((linia = r.readLine()) != null) {
                    dades.add(ProcessRunner.logLine("stdout", linia));
                    w.write(linia);
                    w.newLine();
                    w.flush();
                }
                // En tancar w, el consumidor rep EOF.
            } catch (IOException e) {
                dades.add(ProcessRunner.logLine("pipe-error", e.toString()));
            }
        }, "io-pipe");
        pont.start();
        return pont;
    }

    /** Mostra el resultat d'un worker que acaba amb un codi de sortida diferent de zero. */
    private static void exitCodeDemo() throws Exception {
        System.out.println("\n=== CODI 7 ===");
        printResult(ProcessRunner.supervise(
                ProcessRunner.javaCommand("worker", "ERROR", "--exit-code", "7"),
                "WORKER-ERROR", Duration.ofSeconds(3), null));
    }

    /** Comprova que un executable inexistent es comunica com un error d'entrada/sortida. */
    private static void missingCommandDemo() throws Exception {
        System.out.println("\n=== ORDRE INEXISTENT ===");
        try {
            ProcessRunner.supervise(List.of("/ruta/inexistent/executable"),
                    "MISSING", Duration.ofSeconds(2), null);
        } catch (IOException e) {
            diagnostic("SUPERVISOR", "ordre inexistent: " + e.getMessage());
        }
    }

    /**
     * Executa el rol echo: transforma cada línia de stdin a majúscules fins a FI o EOF.
     */
    private static void runEcho() throws IOException {
        diagnostic("ECHO", "inici");
        try (BufferedReader r = new BufferedReader(new InputStreamReader(
                System.in, StandardCharsets.UTF_8))) {
            String linia;
            while ((linia = r.readLine()) != null && !linia.equals("FI"))
                System.out.println(linia.toUpperCase(Locale.ROOT));
        }
        diagnostic("ECHO", "final");
    }

    /**
     * Executa un worker: compta les línies rebudes, simula tres passos i mostra el recompte.
     *
     * @param args arguments amb el nom del worker i, opcionalment, opcions de prova
     * @throws Exception si les opcions no són vàlides o s'interromp la pausa
     */
    private static void runWorker(String[] args) throws Exception {
        if (args.length < 2) throw new IllegalArgumentException("worker <nom> [opcions]");
        String nom = args[1];
        long pausa = 250;
        int exit = 0;
        for (int i = 2; i + 1 < args.length; i += 2) {
            if (args[i].equals("--sleep")) pausa = Long.parseLong(args[i + 1]);
            else if (args[i].equals("--exit-code")) exit = Integer.parseInt(args[i + 1]);
        }
        int[] dades = comptarEntrada();
        for (int pas = 1; pas <= 3; pas++) {
            diagnostic("WORKER-" + nom, "pas " + pas + "/3");
            Thread.sleep(pausa);
        }
        System.out.println("COUNT=" + dades[0] + " CHARS=" + dades[1]);
        if (exit != 0) System.exit(exit);
    }

    /** Executa el productor, que envia les dades per stdout i els diagnòstics per stderr. */
    private static void runProducer() {
        diagnostic("PRODUCER", "inici");
        for (String nom : List.of("Ada", "Pol", "Joana", "Nil", "Marc"))
            System.out.println(nom);
        diagnostic("PRODUCER", "final");
    }

    /** Executa el consumidor, que espera EOF, compta les dades rebudes i mostra el total. */
    private static void runConsumer() throws IOException {
        diagnostic("CONSUMER", "inici");
        int[] dades = comptarEntrada();
        System.out.println("COUNT=" + dades[0] + " CHARS=" + dades[1]);
        diagnostic("CONSUMER", "final");
    }

    /**
     * Compta les línies no buides i els caràcters rebuts per stdin.
     *
     * @return parell d'enters: nombre de línies i nombre total de caràcters
     */
    private static int[] comptarEntrada() throws IOException {
        int count = 0, chars = 0;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(
                System.in, StandardCharsets.UTF_8))) {
            String linia;
            while ((linia = r.readLine()) != null) {
                if (!linia.isEmpty()) {
                    count++; chars += linia.length();
                }
            }
        }
        return new int[]{count, chars};
    }

    /** Manté un worker actiu durant prou temps per provar l'aturada del servei. */
    private static void runHoldTest() throws Exception {
        diagnostic("SUPERVISOR", "prova de servei llarga");
        ProcessRunner.supervise(ProcessRunner.javaCommand("worker", "SERVICE", "--sleep", "60000"),
                "WORKER-SERVICE", Duration.ofMinutes(4), null);
    }

    /**
     * Escriu un diagnòstic amb el rol, els identificadors de procés i el fil actual.
     *
     * @param rol nom del rol que genera el missatge
     * @param missatge informació que es vol registrar
     */
    private static void diagnostic(String rol, String missatge) {
        ProcessHandle p = ProcessHandle.current();
        System.err.printf("[%s pid=%d ppid=%d thread=%s] %s%n", rol, p.pid(),
                p.parent().map(ProcessHandle::pid).orElse(-1L),
                Thread.currentThread().getName(), missatge);
    }

    /**
     * Mostra el resum d'un procés i totes les línies capturades de stdout i stderr.
     *
     * @param r resultat del procés supervisat
     */
    private static void printResult(ProcessExecutionResult r) {
        System.out.println(r);
        r.stdout().forEach(System.out::println);
        r.stderr().forEach(System.out::println);
    }
}