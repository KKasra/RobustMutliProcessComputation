package os.hw1.master;



import os.hw1.cache.CacheServer;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class MasterMain {
    public static final int WORKER_SERVER_PORT = 1670;
    public static final int CACHE_SERVER_LISTENER_PORT = 1671;
    public static final String CACHE_SERVER_PATH = "os.hw1.cache.CacheServer";
    public static int portNumber;
    public static int numberOfWorkers;
    public static int W;
    public static int numberOfCommonArgs;
    public static String[] commonArgs;
    public static int numberOfPrograms;
    public static String[] programs;
    public static int[] programWeight;
    public static ServerSocket serverSocket;

    public static WorkerHandler workerHandler;
    public static TaskChainHandler taskChainHandler;

    public static ServerSocket cacheServerListenerSocket;
    public static Socket cacheServerSocket;
    public static PrintStream cacheServerOut;
    public static Scanner cacheServerIn;
    private static Process cacheServerProcess;

    public static boolean logState = true;

    public static void logState(String state){
        if (logState)
            System.err.println(state);
    }

    public static void main(String[] args) throws IOException {
        Logger.getInstance().
                logProcessAction(Logger.ProcessType.MASTER,-1, Logger.ProcessAction.START,
                        ProcessIDFinder.getInstance().getCurrentProcessPID(), portNumber);
        setOperationOnExit();
        getArgs();
        makeServerSocket(portNumber);

        taskChainHandler = new TaskChainHandler();
        workerHandler = new WorkerHandler(numberOfWorkers, WORKER_SERVER_PORT, W, taskChainHandler);
        initCacheServer();
        Logger.getInstance().setSendLogs(true);
        logState("Master started");

        new Thread(MasterMain::processTasks).start();


        receiveRequests();
    }

    private static void initCacheServer(){
        try {
            cacheServerListenerSocket = new ServerSocket(CACHE_SERVER_LISTENER_PORT);
            createCacheServerProcess();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createCacheServerProcess() throws IOException {
        String command = String.join(" ", commonArgs);
        command += " " + CACHE_SERVER_PATH;
        command += " " + CACHE_SERVER_LISTENER_PORT;
        cacheServerProcess = Runtime.getRuntime().exec(command);
        cacheServerSocket = cacheServerListenerSocket.accept();
        cacheServerOut = new PrintStream(cacheServerSocket.getOutputStream());
        cacheServerIn = new Scanner(cacheServerSocket.getInputStream());
        Logger.getInstance().logProcessAction(Logger.ProcessType.CACHE_SERVER,
                0, Logger.ProcessAction.START, ProcessIDFinder.getInstance().getSubProcessID(cacheServerProcess),
                cacheServerSocket.getPort());
    }

    private static void setOperationOnExit(){
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cacheServerProcess.destroy();
            workerHandler.destroyWorkers();
            try {
                serverSocket.close();
                cacheServerListenerSocket.close();
                cacheServerSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Logger.getInstance().
                    logProcessAction(Logger.ProcessType.MASTER,-1, Logger.ProcessAction.STOP,
                            ProcessIDFinder.getInstance().getCurrentProcessPID(), portNumber);
        }));

    }

    private static void getArgs(){
        Scanner scanner = new Scanner(System.in);

        portNumber = Integer.parseInt(scanner.nextLine());


        numberOfWorkers = Integer.parseInt(scanner.nextLine());

        W = Integer.parseInt(scanner.nextLine());
        numberOfCommonArgs = Integer.parseInt(scanner.nextLine());
        
        commonArgs = new String[numberOfCommonArgs];
        for (int i = 0; i < numberOfCommonArgs; i++) {
            commonArgs[i] = scanner.nextLine();
        }

        numberOfPrograms = Integer.parseInt(scanner.nextLine());
        programs = new String[numberOfPrograms];
        programWeight = new int[numberOfPrograms];
        for (int i = 0; i < numberOfPrograms; i++) {
            String[] input = scanner.nextLine().split(" ");
            programs[i] = input[0];
            programWeight[i] = Integer.parseInt(input[1]);
        }

        scanner.close();
    }

    private static void makeServerSocket(int portNumber) throws IOException {
        serverSocket = new ServerSocket(portNumber);
    }

    private static void receiveRequests() throws IOException {
        while (true){
            Socket socket = serverSocket.accept();
            Scanner scanner = new Scanner(socket.getInputStream());
            String request = scanner.nextLine();
            logState("Master received request: " + request);
            String[] requestParts = request.split(" ");
            String inputValue = requestParts[1];
            LinkedList<Integer> taskQueue = new LinkedList<>();
            Arrays.stream(requestParts[0].split("\\|")).map(s ->Integer.parseInt(s)-1).forEach(taskQueue::addFirst);
            TaskChain taskChain = new TaskChain(socket, taskQueue, inputValue);


            taskChainHandler.addNewTaskChain(taskChain,socket.getPort());


        }
    }

    private static void processTasks() {
        while(true){
            logState("Processing tasks");
            TaskChain taskChain = taskChainHandler.getNextTaskChain();
            if (taskChain.isFinished()){
                taskChainHandler.getNextTaskChain();
                taskChainHandler.removeTaskChain(taskChain);
                respond(taskChain);
                continue;
            }


            String cachedValue = getCachedValue(taskChain.getProgramID(), taskChain.getInput());
            if (!cachedValue.equals(CacheServer.NO_ENTRY)){
                taskChain.update(cachedValue);
                continue;
            }

            int index = workerHandler.findWorker(taskChain);

            if (index == -1){
                waitForWorkHandler();
                continue;
            }

            taskChainHandler.setTaskChainProcessor(taskChain, index);

        }
    }
    private static void waitForWorkHandler(){
        synchronized (workerHandler){
            try {
                workerHandler.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static String getCachedValue(int programID, String input){
        try {
            cacheServerOut.println("get#"+programID+"#"+input);
            cacheServerOut.flush();
            String response = cacheServerIn.nextLine();
            return response;
        } catch (Exception e){
            e.printStackTrace();
            Logger.getInstance().logProcessAction(Logger.ProcessType.CACHE_SERVER,-1,
                    Logger.ProcessAction.STOP,
                    ProcessIDFinder.getInstance().getSubProcessID(cacheServerProcess),
                    portNumber);

            try {
                createCacheServerProcess();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            return CacheServer.NO_ENTRY;
        }

    }
    public static void updateCacheServer(int programID, String input, String value){
        cacheServerOut.println("put#"+programID+"#"+input+"#"+value);
        cacheServerOut.flush();
    }
    public static void respond(TaskChain taskChain){
        try {
            taskChain.getSocket().getOutputStream().write((taskChain.getInput()+"\n").getBytes());
            taskChain.getSocket().getOutputStream().flush();
            taskChain.getSocket().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
