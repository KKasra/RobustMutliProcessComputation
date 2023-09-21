package os.hw1.master;

import java.util.*;

public class TaskChainHandler {
    private int numberOfGivenTaskChains = 0;
    private TreeSet<TaskChain> taskChainQueue = new TreeSet<>(Comparator.comparingInt(TaskChain::getId));

    private Map<Integer, TaskChain> clientTaskChain = new HashMap<>();
    private Map<TaskChain, Integer> taskChainToWorkerMap = new HashMap<>();

    public synchronized void updateTaskChain(int port, String value){
        TaskChain taskChain = clientTaskChain.get(port);
        taskChain.update(value);
        taskChainToWorkerMap.remove(taskChain);
        addTaskChainToQueue(taskChain);
    }

    public TaskChain getTaskChainOfPort(int port){
        return clientTaskChain.get(port);
    }

    public synchronized void addNewTaskChain(TaskChain taskChain, int port){
        clientTaskChain.put(port, taskChain);
        addTaskChainToQueue(taskChain);
    }

    private void addTaskChainToQueue(TaskChain taskChain) {
        taskChainQueue.add(taskChain);
        notifyAll();
    }
    public synchronized TaskChain getNextTaskChain(){
        if (taskChainQueue.isEmpty()) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

            return taskChainQueue.first();
    }
    public synchronized void setTaskChainProcessor(TaskChain taskChain, int workerID){
//        System.err.println("Setting task chain processor for task chain " + taskChain + " to worker " + workerID);
        taskChainToWorkerMap.put(taskChain, workerID);
        taskChainQueue.remove(taskChain);
    }
    public synchronized void removeTaskChain(TaskChain taskChain){
        taskChainQueue.remove(taskChain);
        taskChainToWorkerMap.remove(taskChain);
    }

    public synchronized List<TaskChain> returnWorkersTaskChainsToQueue(int index) {

        List<TaskChain> taskChains = new ArrayList<>();
        for (Map.Entry<TaskChain, Integer> entry : taskChainToWorkerMap.entrySet()) {
            if (entry.getValue() == index) {
                taskChains.add(entry.getKey());
            }
        }

        for (TaskChain taskChain : taskChains) {

            taskChainToWorkerMap.remove(taskChain);
            addTaskChainToQueue(taskChain);

        }
        return taskChains;
    }
}
