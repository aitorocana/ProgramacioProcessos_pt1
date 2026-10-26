import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Utilitats del supervisor per crear, executar, registrar i aturar processos fills.
 *
 * <p>També centralitza la lectura de les sortides dels processos i la construcció
 * del resultat amb el codi de finalització, la durada i les línies capturades.</p>
 */
public class ProcessRunner {
    // Processos iniciats que encara no s'han donat per acabats.
    private static final Set<Process> actius = new HashSet<>();
    // Indica que s'ha iniciat el tancament de la JVM supervisora.
    private static boolean tancant = false;

    // Crea un utilitzador de les operacions estàtiques de supervisió.
    public ProcessRunner() {
    }

    static {
        // Si es tanca la JVM del supervisor, cancel·lem els fills pendents.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            List<Process> pendents;
            synchronized (actius) {
                tancant = true;
                pendents = List.copyOf(actius);
            }
            for (Process p : pendents) stopTree(p, "tancament del supervisor");
        }, "shutdown-hook"));
    }

    /**
     * Construeix l'ordre per iniciar una altra JVM amb la classe del programa.
     *
     * @param args rol i arguments que rebrà el nou procés
     * @return ordre completa, preparada per passar a {@link ProcessBuilder}
     */
    public static List<String> javaCommand(String... args) {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> ordre = new ArrayList<>(List.of(
                java, "-cp", System.getProperty("java.class.path"), "CreacioProcessos"));
        Collections.addAll(ordre, args);
        return ordre;
    }

    /**
     * Inicia un procés i el registra perquè es pugui cancel·lar durant el tancament.
     *
     * @param ordre executable i arguments del procés
     * @return procés iniciat
     * @throws IOException si no es pot iniciar el procés o el supervisor ja es tanca
     */
    public static Process startProcess(List<String> ordre) throws IOException {
        synchronized (actius) {
            if (tancant) throw new IOException("El supervisor s'està tancant");
            Process p = new ProcessBuilder(ordre).start();
            actius.add(p);
            return p;
        }
    }

    /**
     * Treu un procés de la llista de processos que cal netejar en tancar.
     *
     * @param p procés que ja s'ha acabat o s'ha netejat
     */
    public static void finished(Process p) {
        synchronized (actius) {
            actius.remove(p);
        }
    }

    /**
     * Llegeix un canal de sortida en paral·lel i n'afegeix les línies a una llista.
     *
     * @param canal canal de sortida del procés, com ara stdout o stderr
     * @param linies llista on s'emmagatzemen les línies llegides amb marca de temps
     * @param origen etiqueta que identifica el canal als registres
     * @return fil lector, que es pot esperar amb {@link Thread#join()}
     */
    public static Thread drain(InputStream canal, List<String> linies, String origen) {
        Thread fil = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(canal, StandardCharsets.UTF_8))) {
                String linia;
                while ((linia = r.readLine()) != null) linies.add(logLine(origen, linia));
            } catch (IOException e) {
                linies.add(logLine(origen + "-error", e.getMessage()));
            }
        }, "io-" + origen);
        fil.start();
        return fil;
    }

    /**
     * Afegeix a un missatge l'instant de registre i l'etiqueta del canal d'origen.
     *
     * @param origen identificador del procés o canal
     * @param text missatge que s'ha de registrar
     * @return línia de registre amb marca de temps
     */
    public static String logLine(String origen, String text) {
        String hora = LocalTime.now().format(
                DateTimeFormatter.ofPattern("HH:mm:ss.SSS") //formatat l'output
        );

        return hora + " [" + origen + "] " + text;
    }

    /**
     * Executa un procés fill, li envia l'entrada i supervisa el temps i les sortides.
     *
     * <p>stdout i stderr es llegeixen en fils separats per evitar que un canal ple
     * bloquegi el procés. Si s'excedeix el límit, s'atura el procés i els seus fills.</p>
     *
     * @param ordre executable i arguments del procés
     * @param rol nom descriptiu del rol per identificar-lo als resultats i registres
     * @param limit temps màxim d'execució
     * @param entrada text que s'envia a stdin; si és {@code null}, no s'envia cap text
     * @return resultat final amb les sortides i l'estat del procés
     * @throws Exception si falla l'espera, la comunicació o la supervisió del procés
     */
    public static ProcessExecutionResult supervise(List<String> ordre, String rol,
                                                   Duration limit, String entrada) throws Exception {
        long inici = System.nanoTime();
        Process p = startProcess(ordre);
        List<String> stdout = new ArrayList<>(), stderr = new ArrayList<>();
        Thread out = drain(p.getInputStream(), stdout, "stdout");
        Thread err = drain(p.getErrorStream(), stderr, "stderr");
        try {
            // getOutputStream() és l'stdin del fill. En tancar-lo, enviem EOF.
            try (OutputStream stdin = p.getOutputStream()) {
                if (entrada != null) stdin.write(entrada.getBytes(StandardCharsets.UTF_8));
            }
            // El tancament de stdin permet al fill detectar el final de l'entrada (EOF).
            boolean timeout = !p.waitFor(limit.toMillis(), TimeUnit.MILLISECONDS);
            if (timeout) stopTree(p, "timeout de " + rol);
            out.join(); // Espera el fil que llegeix stdout.
            err.join(); // Espera el fil que llegeix stderr.
            return resultat(p, ordre, rol, inici, stdout, stderr, timeout);
        } finally {
            if (p.isAlive()) stopTree(p, "error o interrupció");
            finished(p);
        }
    }

    /**
     * Recull les dades finals d'un procés en un objecte immutable.
     *
     * @param p procés ja acabat
     * @param ordre ordre que l'ha iniciat
     * @param rol identificador del rol del procés
     * @param inici instant de començament en nanosegons
     * @param stdout línies capturades de la sortida estàndard
     * @param stderr línies capturades de la sortida d'error
     * @param timeout indica si s'ha superat el temps màxim
     * @return resultat amb codis, durada i sortides del procés
     */
    public static ProcessExecutionResult resultat(Process p, List<String> ordre, String rol,
                                                  long inici, List<String> stdout, List<String> stderr, boolean timeout) {
        int codi = p.exitValue();
        String estat = timeout ? "TIMEOUT" : codi == 0 ? "OK" : "ERROR";
        return new ProcessExecutionResult(ordre, rol, p.pid(), ProcessHandle.current().pid(),
                codi, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - inici),
                stdout, stderr, timeout, estat);
    }

    /**
     * Atura el procés i els seus descendents: primer ho demana i després ho força.
     *
     * <p>Després d'un període breu de gràcia, envia una ordre d'aturada forçada als
     * processos que continuïn actius i espera que el procés principal acabi.</p>
     *
     * @param p procés principal que s'ha d'aturar
     * @param causa motiu de la cancel·lació, que s'escriu al registre d'error
     */
    public static void stopTree(Process p, String causa) {
        ProcessHandle actual = ProcessHandle.current();
        System.err.printf("[SUPERVISOR pid=%d ppid=%d thread=%s] cancel·lació: %s%n",
                actual.pid(), actual.parent().map(ProcessHandle::pid).orElse(-1L),
                Thread.currentThread().getName(), causa);
        List<ProcessHandle> fills = p.toHandle().descendants().toList();
        // Primer s'ofereix una finalització ordenada tant als fills com al procés principal.
        for (ProcessHandle f : fills) if (f.isAlive()) f.destroy();
        if (p.isAlive()) p.destroy();
        try {
            p.waitFor(300, TimeUnit.MILLISECONDS); // Temps de gràcia.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (ProcessHandle f : fills) if (f.isAlive()) f.destroyForcibly();
        if (p.isAlive()) p.destroyForcibly();
        try {
            p.waitFor(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}