package KafkaUniform.tsdb;

import java.util.HashMap;

/**
 * Created by Eddie on 2017/8/2.
 */
public class StateCollection {
    public static final HashMap<String, Integer> containerStateMap = new HashMap<String, Integer>(){
        {
            put("NEW", 1);
            put("LOCALIZING", 2);
            put("LOCALIZED", 3);
            put("RUNNING", 4);
            put("KILLING", 5);
            put("EXITED_WITH_SUCCESS", 6);
            put("DONE", 7);
            put("LOCALIZATION_FAILED", 8);
            put("CONTAINER_CLEANEDUP_AFTER_KILL", 9);
            put("EXITED_WITH_FAILURE", 10);
        }
    };

    public static final HashMap<String, Integer> RMAppAttemptStateMap = new HashMap<String, Integer>() {
        {
            put("NEW", 1);
            put("SUBMITTED", 2);
            put("SCHEDULED", 3);
            put("ALLOCATED", 4);
            put("LAUNCHED", 5);
            put("FAILED", 6);
            put("RUNNING", 7);
            put("FINISHING", 8);
            put("FINISHED", 9);
            put("KILLED", 10);
            put("ALLOCATED_SAVING", 11);
            put("LAUNCHED_UNMANAGED_SAVING", 12);
            put("FINAL_SAVING", 13);
        }
    };

    public static final HashMap<String, Integer> RMAppState = new HashMap<String, Integer>() {
        {
            put("NEW", 1);
            put("NEW_SAVING", 2);
            put("SUBMITTED", 3);
            put("ACCEPTED", 4);
            put("RUNNING", 5);
            put("FINAL_SAVING", 6);
            put("FINISHING", 7);
            put("FINISHED", 8);
            put("FAILED", 9);
            put("KILLING", 10);
            put("KILLED", 11);
        }
    };

    public static final HashMap<String, Integer> RMContainerState = new HashMap<String, Integer>() {
        {
            put("NEW", 1);
            put("RESERVED", 2);
            put("ALLOCATED", 3);
            put("ACQUIRED", 4);
            put("RUNNING", 5);
            put("COMPLETED", 6);
            put("EXPIRED", 7);
            put("RELEASED", 8);
            put("KILLED", 9);
        }
    };
}
