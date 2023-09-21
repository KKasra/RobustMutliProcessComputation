package os.hw1.master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class WorkerHandler {
    public static final String WORKER_PATH = "os.hw1.worker.Worker";
    private int numberOfWorkers;
    private int port;
    private int W;
    public ServerSocket workersServerSocket;
    public Socket[] workersSockets;

    private Process[] workers;
    private TaskChainHandler taskChainHandler;
    public static int[] workerCapacity;

    private Set<String> inProgressTasks = new HashSet<>();

    public WorkerHandler(int numberOfWorkers, int workersServerPort, int W, TaskChainHandler taskChainHandler) {
        this.numberOfWorkers = numberOfWorkers;
        this.port = workersServerPort;
        this.W= W;
        setTaskChainHandler(taskChainHandler);
        workerCapacity = new int[numberOfWorkers];
        try {
            initWorkers();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void initWorkers() throws IOException {
        MasterMain.logState("Initializing workers");

        workers = new Process[numberOfWorkers];
        workersServerSocket = new ServerSocket(port);
        workersSockets = new Socket[numberOfWorkers];

        for (int i = 0; i < numberOfWorkers; i++)
            createWorker(i);
    }

    public final Object createWorkerLock = new Object();
    private int numberOfTasksOfDeadWorker = 0;

    private void waitForDistributionOfTasks(){
        while (numberOfTasksOfDeadWorker > 0)
            try {
                synchronized (createWorkerLock) {
                    createWorkerLock.wait();
                }
            }catch (InterruptedException e) {
                e.printStackTrace();
            }
    }
    private void updateNumberOfTasksOfDeadWorker(boolean otherWorkersAreFull){
        if (otherWorkersAreFull)
            numberOfTasksOfDeadWorker = 0;
        else
            numberOfTasksOfDeadWorker--;
        try {
            synchronized (createWorkerLock) {
                createWorkerLock.notifyAll();
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void sendTask(TaskChain taskChain, int workerIndex){
        inProgressTasks.add(singleTaskToString(taskChain));
        updateNumberOfTasksOfDeadWorker(false);
        workerCapacity[workerIndex] -= MasterMain.programWeight[taskChain.getProgramID()];
        try {
            String message = taskChain.getSocket().getPort() + "#" + taskChain.getInput()
                    + "#" + String.join(" ",MasterMain.commonArgs) + " " + MasterMain.programs[taskChain.getProgramID()]
                    +"#" + MasterMain.programWeight[taskChain.getProgramID()];

            message += "\n";
            workersSockets[workerIndex].getOutputStream().write(message.getBytes(StandardCharsets.UTF_8));
            workersSockets[workerIndex].getOutputStream().flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized void createWorker(int index) throws IOException {
        workers[index] = createWorkerProcess();
        Socket socket = workersServerSocket.accept();

        workersSockets[index] = socket;
        workerCapacity[index] = W;

        new Thread(()->handleWorkerSocket(socket, index)).start();
        new Thread(()->{
            Scanner scanner = new Scanner(workers[index].getErrorStream());
            while (scanner.hasNextLine()) {
                MasterMain.logState("worker " +index+ ": " +scanner.nextLine());
            }
        }).start();
        long pid = ProcessIDFinder.getInstance().getSubProcessID(workers[index]);

        MasterMain.logState("Worker " + index + " started with pid " + pid);
        Logger.getInstance().logProcessAction(Logger.ProcessType.WORKER, index, Logger.ProcessAction.START,
                pid,workersSockets[index].getPort());

        notifyAll();
    }

    private Process createWorkerProcess() {
        String command = String.join(" ", MasterMain.commonArgs);
        command += " " + WORKER_PATH + " " + port + " " + W;
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(command);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return process;
    }


    public void handleWorkerSocket(Socket workerSocket ,int index){
        try {
            readResponses(workerSocket, index);
        } catch (Exception e) {
            e.printStackTrace();
            MasterMain.logState("Worker " + index + " died");
            replaceWorker(index);
        }
    }

    public void replaceWorker(int index){
        Logger.getInstance().logProcessAction(Logger.ProcessType.WORKER, index, Logger.ProcessAction.STOP,
                ProcessIDFinder.getInstance().getSubProcessID(workers[index]), workersSockets[index].getPort());

        synchronized (this){
            workerCapacity[index] = -1;

        }
        List<TaskChain> taskChains = taskChainHandler.returnWorkersTaskChainsToQueue(index);
        taskChains.forEach(taskChain -> inProgressTasks.remove(singleTaskToString(taskChain)));
        numberOfTasksOfDeadWorker = taskChains.size();

        synchronized (this){
            notifyAll();
        }


        try {
            waitForDistributionOfTasks();
            MasterMain.logState("creating new worker " + index);
            createWorker(index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readResponses(Socket socket, int index) throws Exception {
        Scanner scanner = new Scanner(socket.getInputStream());
        while(true){
            String[] output = scanner.nextLine().split("#");
            int port = Integer.parseInt(output[0]);
            String value = output[1];


            TaskChain taskChain = taskChainHandler.getTaskChainOfPort(port);
            MasterMain.updateCacheServer(taskChain.getProgramID(), taskChain.getInput(), value);
            synchronized (this) {
                inProgressTasks.remove(singleTaskToString(taskChain));
            }
            taskChainHandler.updateTaskChain(port, value);

            synchronized (this) {
                workerCapacity[index] = Integer.parseInt(output[2]);
                notifyAll();
            }
        }
    }

    private String singleTaskToString(TaskChain taskChain){
        return taskChain.getProgramID() + " " + taskChain.getInput();
    }

    public void destroyWorkers(){
        Arrays.stream(workers).forEach(Process::destroy);
    }

    public int findWorker(TaskChain taskChain){
        int taskIndex = taskChain.getProgramID();
        String input = taskChain.getInput();

        int weight = MasterMain.programWeight[taskIndex];
        int workerIndex;
        synchronized (this) {
            if (inProgressTasks.contains(singleTaskToString(taskChain))) {
                return -1;
            }

            workerIndex = 0;
            for (int i = 0; i < numberOfWorkers; i++) {
                if (workerCapacity[i] >= workerCapacity[workerIndex])
                    workerIndex = i;
            }

        }
        if (weight <= workerCapacity[workerIndex]) {
            sendTask(taskChain, workerIndex);
            return workerIndex;
        }
        updateNumberOfTasksOfDeadWorker(true);
        return -1;
    }

    public void setTaskChainHandler(TaskChainHandler taskChainHandler) {
        this.taskChainHandler = taskChainHandler;
    }
}
