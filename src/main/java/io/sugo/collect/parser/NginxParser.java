package io.sugo.collect.parser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sugo.collect.Configure;
import io.sugo.collect.util.IpConverter;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;

/**
 * Created by fengxj on 6/10/17.
 */
public class NginxParser extends AbstractParser {
  private final Logger logger = LoggerFactory.getLogger(NginxParser.class);
  private static final Gson gson = new GsonBuilder().create();
  private static Map<String,Object> map = new HashMap<>();

  public NginxParser(Configure conf) {
    super(conf);

  }


//  @Override
//  public Map<String, Object> parse(String line) throws Exception {
//    String s1 = line.replaceAll("\\\\x", "%");
//    String decodeStr = URLDecoder.decode(s1, "utf-8");
//    return gson.fromJson(decodeStr,map.getClass());
//  }

  @Override
  public Map<String, Object> parse(String line) {
    Map<String, Object> gmMap = gson.fromJson(line, Map.class);
    for (String key: gmMap.keySet()) {
      Object value = gmMap.get(key);
      if (key.equals("agent") && value != null){
        if (value.toString().startsWith("xE5xB9")){
          gmMap.put(key,value.toString().replace("xE5xB9xBFxE5xB7x9ExE5x9CxB0xE9x93x81","广州地铁"));
        }
        return gmMap;
      }
    }

    return gmMap;
  }

}
