import java.util.Scanner;

public class Class {
    public static void main() {
        Scanner sc = new Scanner(System.in);

        System.out.println("Introdueix el nom del fitxer a executar:");
        String fileName = sc.nextLine();
    }
}

//process builder defineix executable arguments i configuracio i
//proces representa el procés inicial i permet gestionar fluxos


//fluxos eestàndar
//system.in = entrades/teclat
//system.out = sortida normal de programa
//system.err = missatge ERR-diagnostic
//Exemple: System.out.print("Es un procès")