package os.hw1.master;

import java.net.Socket;
import java.util.Queue;

public class TaskChain {
    private static int numberOfCreatedTasks = 0;
    private int id;
    private Socket socket;
    private Queue<Integer> ProgramIDs;
    private String input;
    public TaskChain(Socket socket, Queue<Integer> ProgramIDs, String input){
        this.socket = socket;
        this.ProgramIDs = ProgramIDs;
        this.input = input;
        this.id = numberOfCreatedTasks++;
    }

    public String getInput(){
        return input;
    }
    public int getProgramID(){
        return ProgramIDs.peek();
    }
    public void update(String newValue){
        this.input = newValue;
        ProgramIDs.poll();
    }

    public Socket getSocket(){
        return socket;
    }

    public boolean isFinished(){
        return ProgramIDs.isEmpty();
    }

    @Override
    public String toString() {
        return "TaskChain{" +
                "socket=" + socket.getPort() +
                ", ProgramIDs=" + ProgramIDs +
                ", input=" + input +
                '}';
    }

    public int getId() {
        return id;
    }
}
