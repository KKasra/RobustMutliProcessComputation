package os.hw1.worker;


import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;


public class Worker {
    public static int SERVER_PORT;
    public static Socket socket;
    public static Scanner socketScanner;
    public static PrintStream socketWriter;
    public static final List<Process> subProcesses = new ArrayList<>();
    public static int capacity;

     public static ExecutorService executorService;
    public static void main(String[] args) throws IOException {
        setOperationOnExit();
        SERVER_PORT = Integer.parseInt(args[0]);
        capacity = Integer.parseInt(args[1]);
        System.err.println("Worker started");
        System.err.flush();

        socket = new Socket(InetAddress.getLocalHost(),SERVER_PORT);
        socketScanner = new Scanner(socket.getInputStream());
        socketWriter = new PrintStream(socket.getOutputStream());

        executorService = java.util.concurrent.Executors.newFixedThreadPool(capacity);

        while (true){
            String line = socketScanner.nextLine();
            System.err.println("received: " + line);
            String[] request = line.split("#");
            int port = Integer.parseInt(request[0]);
            String value = request[1];
            String processPath = request[2];
            int load = Integer.parseInt(request[3]);

            executorService.execute(() -> calculate(port,value,processPath, load));
        }

    }

    private static void calculate(int port, String value, String processPath, int load) {
        Process process = null;
        synchronized (subProcesses) {
            try {
                process = Runtime.getRuntime().exec(processPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            capacity -= load;
            subProcesses.add(process);
        }
        System.err.println("calculating" + processPath + " "+ value + " capacity=" + capacity);
        PrintStream processWriter = new PrintStream(process.getOutputStream());
        processWriter.println(value);
        processWriter.flush();
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String res = new Scanner(process.getInputStream()).nextLine();

        synchronized (subProcesses) {
            subProcesses.remove(process);
            capacity += load;
            process.destroy();
        }
        synchronized (socketWriter) {
            System.err.println("sending: " + port + "#" + res +"#" + capacity);
            socketWriter.println(port + "#" + res + "#" + capacity);
            socketWriter.flush();
        }
    }

    private static void setOperationOnExit(){
        Runtime.getRuntime().addShutdownHook(new Thread(() -> subProcesses.forEach(Process::destroy)));
    }
}
