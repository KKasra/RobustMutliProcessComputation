package os.hw1.cache;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;

public class CacheServer {
    public static final String NO_ENTRY = "@#$";
    private final Socket socket;
    private final PrintStream printStream;
    private final Scanner scanner;
    public CacheServer(Socket socket) throws IOException {
        this.socket = socket;
        printStream = new PrintStream(socket.getOutputStream());
        scanner = new Scanner(socket.getInputStream());

    }


    public void listenToServer() {
        while (true){
            String line = scanner.nextLine();
            String[] args = line.split("#");
            String command = args[0];
            if (Objects.equals(command, "put")){
                int programID = Integer.parseInt(args[1]);
                int input = Integer.parseInt(args[2]);
                String result = args[3];
                Task task = new Task(programID, input);
                add(task, result);
            }
            else if (Objects.equals(command, "get")){
                int programID = Integer.parseInt(args[1]);
                int input = Integer.parseInt(args[2]);
                Task task = new Task(programID, input);
                String result = get(task);
                printStream.println(result);
            }

        }
    }

    public static void main(String[] args) throws IOException {
        int mainServerPort = Integer.parseInt(args[0]);
        Socket socket = new Socket(InetAddress.getLocalHost(), mainServerPort);

        CacheServer server = new CacheServer(socket);
        server.listenToServer();

    }
    public static class Task{
        public final int programID;
        public final int input;

        public Task(int programID, int input) {
            this.programID = programID;
            this.input = input;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Task task = (Task) o;
            return programID == task.programID && input == task.input;
        }

        @Override
        public int hashCode() {
            return Objects.hash(programID, input);
        }
    }

    private Map<Task, String> cache = new HashMap<>();

    public void add(Task task, String result){
        cache.put(task, result);
    }

    public String get(Task task){
        return cache.getOrDefault(task, NO_ENTRY);
    }
}
