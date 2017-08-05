package io.sugo.collect.parser;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.sugo.collect.Configure;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by fengxj on 6/10/17.
 */
public class CSVParser extends AbstractParser {
  private final Logger logger = LoggerFactory.getLogger(CSVParser.class);
  private static final Gson gson = new GsonBuilder().create();
  public static final String FILE_READER_CSV_SEPARATOR = "file.reader.csv.separator";
  public static final String FILE_READER_CSV_DIMPATH = "file.reader.csv.dimPath";
  private List<Dimension> dimensionList;
  private String separator;

  public CSVParser(Configure conf) {
    super(conf);
    separator = conf.getProperty(FILE_READER_CSV_SEPARATOR, ",");
    if (separator.equals("space"))
      separator = " ";
    String dimPath = conf.getProperty(FILE_READER_CSV_DIMPATH);
    if (dimPath == null) {
      dimPath = conf.getProperty(Configure.USER_DIR) + "conf/dimension";
    } else if (!dimPath.startsWith("/")) {
      dimPath = conf.getProperty(Configure.USER_DIR) + "/" + dimPath;
    }
    try {
      String dimJson = FileUtils.readFileToString(new File(dimPath), "UTF-8");
      JsonArray jsonArray = new JsonParser().parse(dimJson).getAsJsonArray();
      dimensionList = new ArrayList<>();
      for (JsonElement user : jsonArray) {
        Dimension dimension = gson.fromJson(user, new TypeToken<Dimension>() {
        }.getType());
        dimensionList.add(dimension);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Map<String, Object> parse(String line) {
    String[] fields = line.split(separator, dimensionList.size());
    Map<String, Object> result = new HashMap<>();
    for (int i = 0; i < dimensionList.size(); i++) {
      Dimension dim = dimensionList.get(i);
      String value = null;
      if (fields.length > i)
        value = fields[i];
      try {
        Object readValue = dim.getValue(value);
        if (readValue == null)
          continue;
        result.put(dim.getName(), readValue);
      } catch (ParseException e) {
        if (logger.isDebugEnabled()) {
          logger.error("", e);
        }
        continue;
      }
    }
    return result;
  }

  private class Dimension {
    private String name;
    private String type;
    private String format;
    private String defaultValue;
    private boolean firstFetchName;
    private static final String STRING = "string";
    private static final String INT = "int";
    private static final String LONG = "long";
    private static final String FLOAT = "float";
    private static final String DATE = "date";
    private SimpleDateFormat sdf;

    public String getName() {
//      if (!firstFetchName) {
//        switch (type) {
//          case STRING:
//            break;
//          case INT:
//            name = "i|" + name;
//            break;
//          case LONG:
//            name = "l|" + name;
//            break;
//          case FLOAT:
//            name = "f|" + name;
//            break;
//          case DATE:
//            name = "d|" + name;
//            break;
//        }
//        firstFetchName = true;
//      }
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getFormat() {
      return format;
    }

    public void setFormat(String format) {
      this.format = format;
    }

    public Object getValue(String value) throws ParseException {
      if (StringUtils.isBlank(value)){
        if (defaultValue != null)
          return defaultValue;

        return null;
      }

      switch (type) {
        case STRING:
          return value;
        case INT:
          return Integer.parseInt(value);
        case LONG:
          return Long.parseLong(value);
        case FLOAT:
          return Float.parseFloat(value);
        case DATE: {
          if (format.equals("millis"))
            return Long.parseLong(value);

          if (format.equals("posix"))
            return Long.parseLong(value + "000");

          if (sdf == null) {
            this.sdf = new SimpleDateFormat(this.format);
          }
          return sdf.parse(value.toString()).getTime();
        }
      }

      return value;
    }
  }
}
