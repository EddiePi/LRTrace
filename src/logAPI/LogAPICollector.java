package logAPI;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eddie on 2017/7/3.
 */
public class LogAPICollector {
    public List<MessageMark> containerRuleMarkList = new ArrayList<>();
    public List<MessageMark> managerRuleMarkList = new ArrayList<>();

    private LogAPICollector(){}

    private static final LogAPICollector instance = new LogAPICollector();

    public static LogAPICollector getInstance() {
        return instance;
    }

    public void registerContainerRules(AbstractLogAPI api) {
        containerRuleMarkList.addAll(api.messageMarkList);
    }

    public void registerManagerRules(AbstractLogAPI api) {
        managerRuleMarkList.addAll(api.messageMarkList);
    }

    public void clearAllAPI() {
        containerRuleMarkList.clear();
    }
}
