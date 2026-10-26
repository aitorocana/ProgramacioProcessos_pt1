import java.util.List;

/**
 * Resultat immutable de l'execució d'un procés supervisat.
 *
 * @param command executable i arguments amb què s'ha iniciat el procés
 * @param role nom del rol del procés
 * @param pid identificador del procés fill
 * @param ppid identificador del procés pare
 * @param exitCode codi de finalització retornat pel procés
 * @param durationMs durada de l'execució en mil·lisegons
 * @param stdout línies de sortida estàndard amb marca de temps i origen
 * @param stderr línies de sortida d'error amb marca de temps i origen
 * @param timedOut indica si l'execució ha superat el temps límit
 * @param status estat resumit: {@code OK}, {@code ERROR} o {@code TIMEOUT}
 */
public record ProcessExecutionResult(
        List<String> command, String role, long pid, long ppid,
        int exitCode, long durationMs,
        List<String> stdout, List<String> stderr,
        boolean timedOut, String status) {

    /**
     * Crea un resultat i copia les llistes per impedir-ne la modificació externa.
     */
    public ProcessExecutionResult {
        command = List.copyOf(command);
        stdout = List.copyOf(stdout);
        stderr = List.copyOf(stderr);
    }

    /**
     * Retorna un resum textual dels camps principals del resultat.
     *
     * @return resum llegible del procés executat
     */
    @Override
    public String toString() {
        return role + " pid=" + pid + " ppid=" + ppid
                + " exit=" + exitCode + " temps=" + durationMs + "ms"
                + " timeout=" + timedOut + " estat=" + status
                + " ordre=" + command;
    }
}