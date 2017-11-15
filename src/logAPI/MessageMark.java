package logAPI;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eddie on 2017/7/3.
 */
public class MessageMark {
    public String regex;
    public Long dateOffset = 0L;
    public List<Group> groups = new ArrayList<>();

    public class Group {
        public String name;
        public Boolean isFinish;
        public List<String> tags = new ArrayList<>();
        public String value;
        // event or state
        public String type;
    }

    public Group createNewGroup() {
        return new Group();
    }
}
