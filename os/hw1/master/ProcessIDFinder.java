package os.hw1.master;

import java.lang.management.ManagementFactory;

public class ProcessIDFinder {

    private static ProcessIDFinder instance = new ProcessIDFinder();
    private ProcessIDFinder() {
    }
    public static ProcessIDFinder getInstance() {
        return instance;
    }
    public long getCurrentProcessPID(){
        return ManagementFactory.getRuntimeMXBean().getPid();
    }
    public long getSubProcessID(Process process){
        return process.pid();

    }
}
