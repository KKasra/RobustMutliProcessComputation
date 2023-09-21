package os.hw1.master;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.Scanner;

public class Logger {
    private static Logger instance = null;

    private PrintStream logStream;

    private boolean sendLogs;

    public static Logger getInstance() {
        if (instance == null) {
            instance = new Logger();
        }
        return instance;
    }

    private Logger() {
        logStream = System.out;
    }
    public synchronized void setSendLogs(boolean sendLogs) {
        this.sendLogs = sendLogs;
        notifyAll();
    }

    public void log(String message) {
        logStream.println(message);
        logStream.flush();
    }

    public enum ProcessAction{
        START{
            @Override
            public String toString() {
                return "start";
            }
        }, STOP{
            @Override
            public String toString() {
                return "stop";
            }
        }
    }
    public enum ProcessType{
        MASTER{
            @Override
            public String toString() {
                return "master";
            }
        }, WORKER{
            @Override
            public String toString() {
                return "worker";
            }
        }, CACHE_SERVER{
            @Override
            public String toString() {
                return "cache";
            }
        }
    }



    public void logProcessAction(ProcessType type, int workerId, ProcessAction action, long pid, int port) {
        String message = type + " " + (type == ProcessType.WORKER ? workerId+" ":"") + action + " " + pid + " " + port;
        log(message);
    }
}
