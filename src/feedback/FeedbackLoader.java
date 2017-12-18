package feedback;

import server.TracerConf;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eddie on 2017/12/12.
 */
public class FeedbackLoader {
    TracerConf conf = TracerConf.getInstance();
    FeedbackManager feedbackManager = FeedbackManager.getInstance();
    List<FeedbackInfo> feedbackInfoList;

    public FeedbackLoader() {
        feedbackInfoList = new ArrayList<>();
        parseURL();
    }

    private void parseURL() {
        String[] pairs = conf.getStringOrDefault("tracer.feedback.paths", "./").split(",");
        File feedbackFile;
        try {
            for (String pair: pairs) {
                String[] strInfo = pair.split(":");
                if (strInfo.length < 3) {
                    continue;
                }
                feedbackFile = new File(strInfo[0].trim());
                if (!feedbackFile.isDirectory() && feedbackFile.getPath().matches(".*\\.jar")) {
                    feedbackInfoList.add(new FeedbackInfo(feedbackFile.toURI().toURL(), strInfo[1].trim(), Integer.valueOf(strInfo[2].trim())));

                }
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public void load() {
        ClassLoader loader;
        Class<AbstractFeedback> clazz;
        Constructor<AbstractFeedback> constructor;
        AbstractFeedback feedback;
        try {
            for (FeedbackInfo info : feedbackInfoList) {
                loader = URLClassLoader.newInstance(new URL[]{info.path});
                clazz = (Class<AbstractFeedback>) loader.loadClass(info.className);
                constructor = clazz.getConstructor(String.class, Integer.class);
                feedback = constructor.newInstance(info.className, info.interval);
                feedbackManager.registerFeedback(feedback);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
