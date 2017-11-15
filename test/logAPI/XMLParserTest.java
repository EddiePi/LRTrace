package logAPI;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Eddie on 2017/7/18.
 */
public class XMLParserTest {
    @Test
    public void parse() throws Exception {
        XMLParser.parse("/Users/Eddie/gitRepo/tracing-server-2.0/conf/Testing-api.xml");
    }

}