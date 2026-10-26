import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * NO es comparen PID FIXOS: Linux assigna nous PID a cada execució.
 * Els tests creen processos reals i no necessiten Internet.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
public class ProcessRunnerTest {

    private static ProcessExecutionResult executar(String rol, Duration limit,
                                                   String entrada, String... args) throws Exception {
        return ProcessRunner.supervise(ProcessRunner.javaCommand(args), rol, limit, entrada);
    }

    private static boolean teLinia(List<String> linies, String fragment) {
        return linies.stream().anyMatch(linia -> linia.contains(fragment));
    }

    private static int[] llegirRecompte(ProcessExecutionResult resultat) {
        Pattern patro = Pattern.compile("COUNT=(\\d+)\\s+CHARS=(\\d+)");
        for (String linia : resultat.stdout()) {
            Matcher trobat = patro.matcher(linia);
            if (trobat.find()) {
                return new int[]{Integer.parseInt(trobat.group(1)),
                        Integer.parseInt(trobat.group(2))};
            }
        }
        fail("No s'ha trobat COUNT/CHARS a stdout: " + resultat.stdout());
        return new int[]{0, 0}; // Inaccessible si fail() llança AssertionError.
    }

    @Test
    void testEcho() throws Exception {
        ProcessExecutionResult resultat = executar("ECHO", Duration.ofSeconds(5),
                "Ebre\nprocessos\nFI\n", "echo");

        assertEquals(0, resultat.exitCode(), "ECHO ha de sortir amb codi 0");
        assertEquals("OK", resultat.status());
        assertFalse(resultat.timedOut());
        assertTrue(teLinia(resultat.stdout(), "[stdout] EBRE"));
        assertTrue(teLinia(resultat.stdout(), "[stdout] PROCESSOS"));
        assertEquals(ProcessHandle.current().pid(), resultat.ppid(),
                "El PPID de l'ECHO ha de ser el PID del test que l'ha creat");
    }

    @Test
    void testStderr() throws Exception {
        ProcessExecutionResult resultat = executar("ECHO", Duration.ofSeconds(5),
                "Ebre\nFI\n", "echo");

        assertEquals(0, resultat.exitCode());
        assertTrue(teLinia(resultat.stderr(), "[stderr] [ECHO pid="));
        assertTrue(teLinia(resultat.stderr(), "] inici"), "Cal el diagnòstic inicial");
        assertTrue(teLinia(resultat.stderr(), "] final"), "Cal el diagnòstic final");
        assertTrue(resultat.stdout().stream().allMatch(s -> s.contains("[stdout]")),
                "Les línies capturades han d'indicar el canal d'origen");
    }

    @Test
    void testWorkers() throws Exception {
        String[] noms = {"ALPHA", "BETA", "GAMMA"};
        String[] entrades = {"Ada\nPol\n", "Joana\nNil\n", "Marc\n"};
        int[] counts = {2, 2, 1};
        int[] chars = {6, 8, 4};
        int totalCount = 0;
        int totalChars = 0;

        for (int i = 0; i < noms.length; i++) {
            ProcessExecutionResult resultat = executar("WORKER-" + noms[i],
                    Duration.ofSeconds(6), entrades[i], "worker", noms[i]);

            assertEquals(0, resultat.exitCode(), "Error al worker " + noms[i]);
            assertEquals("OK", resultat.status());
            assertFalse(resultat.timedOut());
            assertTrue(resultat.pid() > 0);
            assertEquals(ProcessHandle.current().pid(), resultat.ppid());
            int[] comptatge = llegirRecompte(resultat);
            assertEquals(counts[i], comptatge[0], "COUNT del worker " + noms[i]);
            assertEquals(chars[i], comptatge[1], "CHARS del worker " + noms[i]);
            totalCount += comptatge[0];
            totalChars += comptatge[1];
        }

        assertEquals(5, totalCount, "Total de línies dels tres workers");
        assertEquals(18, totalChars, "Total de caràcters dels tres workers");
    }

    @Test
    void testLimitConcurrencia() throws Exception {
        // El pool té dos fils i tres tasques de supervisió.
        // Comprovem les tasques simultànies i que GAMMA espera el seu torn.
        AtomicInteger enCurs = new AtomicInteger();
        AtomicInteger maximEnCurs = new AtomicInteger();
        Map<String, Long> inici = new ConcurrentHashMap<>();
        Map<String, Long> finalitzacio = new ConcurrentHashMap<>();
        List<Future<ProcessExecutionResult>> futures = new ArrayList<>();
        String[] noms = {"ALPHA", "BETA", "GAMMA"};

        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            for (String nom : noms) {
                futures.add(pool.submit(() -> {
                    inici.put(nom, System.nanoTime());
                    int ara = enCurs.incrementAndGet();
                    maximEnCurs.accumulateAndGet(ara, Math::max);
                    try {
                        return executar("WORKER-" + nom, Duration.ofSeconds(6),
                                nom + "\n", "worker", nom);
                    } finally {
                        finalitzacio.put(nom, System.nanoTime());
                        enCurs.decrementAndGet();
                    }
                }));
            }
            for (Future<ProcessExecutionResult> future : futures) {
                ProcessExecutionResult resultat = future.get(10, TimeUnit.SECONDS);
                assertEquals(0, resultat.exitCode());
                assertFalse(resultat.timedOut());
                assertEquals(1, llegirRecompte(resultat)[0]);
            }
        }

        assertEquals(2, maximEnCurs.get(), "Hi ha d'haver com a màxim dues tasques simultànies");
        assertTrue(inici.get("GAMMA") >= Math.min(finalitzacio.get("ALPHA"),
                        finalitzacio.get("BETA")),
                "GAMMA ha de començar quan una de les dues tasques acaba");
    }

    @Test
    void testTimeout() throws Exception {
        ProcessExecutionResult resultat = executar("WORKER-LENT", Duration.ofMillis(500),
                null, "worker", "LENT", "--sleep", "2000");

        assertTrue(resultat.timedOut(), "El worker lent ha de superar els 500 ms");
        assertEquals("TIMEOUT", resultat.status());
        assertFalse(ProcessHandle.of(resultat.pid())
                        .map(ProcessHandle::isAlive).orElse(false),
                "El procés amb timeout no ha de quedar viu");
        // No fixem exit=143: el codi de terminació pot dependre del sistema.
    }

    @Test
    void testExitCode7() throws Exception {
        ProcessExecutionResult resultat = executar("WORKER-ERROR", Duration.ofSeconds(6),
                "", "worker", "ERROR", "--exit-code", "7");

        assertEquals(7, resultat.exitCode());
        assertEquals("ERROR", resultat.status());
        assertFalse(resultat.timedOut());
    }

    @Test
    void testOrdreInexistent() {
        assertThrows(IOException.class, () -> ProcessRunner.supervise(
                List.of("/ruta/inexistent/executable"), "MISSING",
                Duration.ofSeconds(2), null));
    }

    @Test
    void testPipe() throws Exception {
        // Executem la demostració completa per provar pipeDemo() REAL.
        // No fem servir una pipe del shell, ni reconstruïm una pipe diferent al test.
        ProcessExecutionResult supervisor = executar("SUPERVISOR-TEST",
                Duration.ofSeconds(40), null);

        assertEquals(0, supervisor.exitCode(), "El supervisor complet ha d'acabar bé");
        assertFalse(supervisor.timedOut());

        String sortida = String.join("\n", supervisor.stdout());
        int iniciPipe = sortida.indexOf("=== PIPE:");
        assertTrue(iniciPipe >= 0, "Falta l'apartat PIPE al supervisor");
        int finalPipe = sortida.indexOf("=== CODI 7 ===", iniciPipe);
        String seccioPipe = finalPipe >= 0
                ? sortida.substring(iniciPipe, finalPipe)
                : sortida.substring(iniciPipe);

        assertTrue(Pattern.compile("PRODUCER pid=\\d+ ppid=\\d+ exit=0")
                .matcher(seccioPipe).find(), "El PRODUCTOR ha d'acabar amb exit=0");
        assertTrue(Pattern.compile("CONSUMER pid=\\d+ ppid=\\d+ exit=0")
                .matcher(seccioPipe).find(), "El CONSUMIDOR ha d'acabar amb exit=0");
        assertTrue(seccioPipe.contains("COUNT=5 CHARS=18"),
                "El consumidor ha de comptar cinc noms i divuit caràcters");
        for (String nom : List.of("Ada", "Pol", "Joana", "Nil", "Marc")) {
            assertTrue(seccioPipe.contains("[stdout] " + nom),
                    "El productor ha d'enviar el nom " + nom);
        }
    }

    @Test
    void testCancelacio() throws Exception {
        // Iniciem un fill intencionadament molt lent i el cancel·lem.
        Process p = ProcessRunner.startProcess(ProcessRunner.javaCommand(
                "worker", "CANCEL", "--sleep", "60000"));
        long pid = p.pid();
        try {
            p.getOutputStream().close(); // EOF per al worker.
            assertTrue(p.isAlive(), "El worker ha d'estar viu abans de cancel·lar-lo");
            ProcessRunner.stopTree(p, "prova JUnit de cancel·lació");
            assertFalse(p.isAlive(), "El worker ha d'estar aturat després de stopTree()");
            assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false),
                    "No ha de quedar un procés actiu amb aquest PID");
        } finally {
            if (p.isAlive()) ProcessRunner.stopTree(p, "neteja final del test");
            ProcessRunner.finished(p);
        }
    }
}