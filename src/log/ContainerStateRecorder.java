package log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Eddie on 2017/6/26.
 */
public class ContainerStateRecorder {
    private static final ContainerStateRecorder instance = new ContainerStateRecorder();

    private ContainerStateRecorder(){}

    public static ContainerStateRecorder getInstance() {
        return instance;
    }

    private ConcurrentMap<String, List<StateWithTimestamp>> stateMap = new ConcurrentHashMap<>();


    public void putState(String containerId, String state, Long timestamp) {
        List<StateWithTimestamp> swtList =
                stateMap.getOrDefault(containerId, null);
        if (swtList == null) {
            swtList = new ArrayList<>();
            stateMap.put(containerId, swtList);
        }
        swtList.add(new StateWithTimestamp(state, timestamp));
    }

    public String getState(String containerId, Long timestamp, boolean needDelete) {
        List<StateWithTimestamp> swtList =
                stateMap.getOrDefault(containerId, null);
        if (swtList == null || swtList.size() == 0) {
            return "NEW";
        }
        int index;
        for(index = 0; index < swtList.size(); index++) {
            StateWithTimestamp swt = swtList.get(index);
            if(swt.timestamp > timestamp) {
                break;
            }
        }
        String state = swtList.get(index - 1).state;

        if(state.equals("DONE") && needDelete) {
            stateMap.remove(containerId);
        }

        return state;
    }

    private class StateWithTimestamp {
        String state;
        Long timestamp;

        public StateWithTimestamp(String state, Long timestamp) {
            this.state = state;
            this.timestamp = timestamp;
        }
    }
}
