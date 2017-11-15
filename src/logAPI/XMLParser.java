package logAPI;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eddie on 2017/7/17.
 */
public class XMLParser {
    private static Document doc;

    public static List<MessageMark> parse(String path) {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        List<MessageMark> messageMarkList = new ArrayList<>();
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();

            doc = db.parse(path);

            Element root = doc.getDocumentElement();
            if (!root.getNodeName().equals("rules")) {
                System.out.printf("unrecognized root element: %s\n", root.getNodeName());
            }

            NodeList ruleList = root.getElementsByTagName("rule");
            int rlLength = ruleList.getLength();

            if (rlLength == 0) {
                System.out.printf("no rule is specified in file: %s\n", path);
            }

            for (int i = 0; i < rlLength; i++) {
                Element rule = (Element) ruleList.item(i);
                String regex = getTextValue(rule, "regex");
                Long dateOffset = getLongValueOrDefault(rule, "dateOffset", 0L);
                NodeList groupList = rule.getElementsByTagName("group");
                MessageMark mm = new MessageMark();
                mm.regex = regex;
                mm.dateOffset = dateOffset;
                for(int j = 0; j < groupList.getLength(); j++) {
                    Element groupElem = (Element) groupList.item(j);
                    if(!hasSubTag(groupElem, "name")) {
                        System.out.printf("lack of required field. parse next group\n");
                        continue;
                    }
                    MessageMark.Group group = mm.createNewGroup();
                    String name = getTextValue(groupElem, "name");
                    String type = getTextValue(groupElem, "type");
                    Boolean isFinish = getBooleanValueOrDefault(groupElem, "isFinish", false);
                    NodeList tagList = groupElem.getElementsByTagName("tag");
                    List<String> tags = new ArrayList<>();
                    for (int k = 0; k < tagList.getLength(); k++) {
                        Element tag = (Element) tagList.item(k);
                        String tagValue = tag.getFirstChild().getNodeValue();
                        tags.add(tagValue);
                    }
                    String value = getTextValue(groupElem, "value");

                    // TEST

                    group.name = name;
                    group.isFinish = isFinish;
                    group.type = type;
                    group.tags = tags;
                    group.value = value;
                    if(name != null) {
                        mm.groups.add(group);
                    }
                }
                if(checkSanity(mm)) {
                    messageMarkList.add(mm);
                }
            }

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (SAXException se) {
            se.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return messageMarkList;
    }

    private static String getTextValue(Element element, String tagName){
        String textValue;
        NodeList nl = element.getElementsByTagName(tagName);
        if(!hasSubTag(element, tagName)) {
            return null;
        }
        Element el = (Element)nl.item(0);
        textValue = el.getFirstChild().getNodeValue();
        if(textValue.matches("\\s*")) {
            return null;
        }
        return textValue;
    }

    private static String getTextValueOrDefault(Element element, String tagName, String defaultValue) {
        String res = getTextValue(element, tagName);
        if (res == null) {
            res = defaultValue;
        }
        return res;
    }

    private static Long getLongValueOrDefault(Element element, String tagName, Long defaultValue) {
        String res = getTextValue(element, tagName);
        if(res == null) {
            return defaultValue;
        } else {
            return Long.valueOf(res);
        }
    }

    private static Boolean getBooleanValue(Element element, String tagName){
        String boolStr = getTextValue(element, tagName);
        return Boolean.valueOf(boolStr);
    }

    private static boolean getBooleanValueOrDefault(Element element, String tagName, boolean defaultValue) {
        String resStr = getTextValue(element, tagName);
        boolean res;
        if (resStr == null) {
            res = defaultValue;
        } else {
            res = Boolean.valueOf(resStr);
        }
        return res;
    }

    private static boolean hasSubTag(Element element, String tagName) {
        NodeList nl = element.getElementsByTagName(tagName);
        if(nl.getLength() == 0) {
            return false;
        }
        return true;
    }

    private static boolean checkSanity(MessageMark mm) {
        if(mm.regex != null && mm.groups != null) {
            return true;
        }
        return false;
    }
}
