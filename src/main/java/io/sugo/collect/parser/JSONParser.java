package io.sugo.collect.parser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sugo.collect.Configure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class JSONParser extends AbstractParser{
    private final Logger logger = LoggerFactory.getLogger(JSONParser.class);
    private final Gson gson = new GsonBuilder().create();

    public JSONParser(Configure conf) {
        super(conf);
    }


    @Override
    public Map<String, Object> parse(String line) throws Exception {
        Map<String,Object> mapOld = gson.fromJson(line,Map.class);
        Map<String,Object> mapParsed = gson.fromJson(line,Map.class);
        String dataTimeStr;
        for (String key : mapOld.keySet()) {
            Object value = mapOld.get(key);
            if (value == null) {
                mapParsed.put(key,"");
            } else if (key.equals("DataTime")) {
                dataTimeStr = value.toString();
                mapParsed.put(key, Long.parseLong(dataTimeStr.substring(dataTimeStr.indexOf('(') + 1, dataTimeStr.indexOf('+'))));
            }else {
                mapParsed.put(key,value);
            }
        }
        return mapParsed;
    }
}
