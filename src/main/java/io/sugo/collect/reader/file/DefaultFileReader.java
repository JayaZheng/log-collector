package io.sugo.collect.reader.file;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sugo.collect.Configure;
import io.sugo.collect.metrics.ReaderMetrics;
import io.sugo.collect.parser.IgnorableException;
import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.util.HttpUtil;
import io.sugo.collect.writer.AbstractWriter;
import io.sugo.collect.writer.WriterFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by fengxj on 4/8/17.
 */
public class DefaultFileReader extends AbstractReader {
  private static final String UTF8 = "UTF-8";
  private final Logger logger = LoggerFactory.getLogger(DefaultFileReader.class);
  private Map<String, Reader> readerMap;
  public static final String FILE_READER_LOG_DIR = "file.reader.log.dir";
  public static final String COLLECT_OFFSET = ".collect_offset";
  public static final String FINISH_FILE = ".finish";
  public static final String FILE_READER_LOG_REGEX = "file.reader.log.regex";
  public static final String FILE_READER_SCAN_TIMERANGE = "file.reader.scan.timerange";
  public static final String FILE_READER_SCAN_INTERVAL = "file.reader.scan.interval";
  public static final String FILE_READER_THREADPOOL_SIZE = "file.reader.threadpool.size";
  public static final String FILE_READER_HOST = "file.reader.host";
  public static final String FILE_READER_LOG_TYPE = "file.reader.log.type";

  private int lineSeparatorSize;
  private String metaBaseDir;
  ExecutorService fixedThreadPool;
  private String errMsgCollectorUrl;
  private boolean isSeparate;

  private int maxSize;
  protected AbstractWriter writer;

  public DefaultFileReader(Configure conf, WriterFactory writerFactory) {
    super(conf, writerFactory);
    try {
      writer = writerFactory.createWriter();
    } catch (Exception e) {
      e.printStackTrace();
    }
    host = conf.getProperty(FILE_READER_HOST);
    String logType = conf.getProperty(FILE_READER_LOG_TYPE, "separate");
    isSeparate = logType.equals("separate");
    if (StringUtils.isBlank(host)){
      try {
        InetAddress addr = InetAddress.getLocalHost();
        host=addr.getHostAddress();
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
    String lineSeparator = conf.getProperty(Configure.FILE_READER_LINE_SEPARATOR, "\n");
    lineSeparatorSize = lineSeparator.length();
    readerMap = new HashMap<String, Reader>();
    int threadSize = conf.getInt(FILE_READER_THREADPOOL_SIZE);
    fixedThreadPool = Executors.newFixedThreadPool(threadSize);
    maxSize = conf.getInt(Configure.READER_MESSAGE_MAX_SIZE_BYTES, Configure.DEFAULT_READER_MESSAGE_MAX_SIZE_BYTES);
    errMsgCollectorUrl = conf.getProperty(Configure.ERROR_MESSAGE_HTTP_COLLECTOR_URL);
  }

  @Override
  public void stop(){
    super.stop();
    int time = 0;
    while (!fixedThreadPool.isTerminated()){
      if (time >= 10)
        break;
      time ++;

      int readerSize = readerMap.size();

      if (readerSize == 0)
        break;
      logger.info("Waiting for the reader to complete , still " + readerSize);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void read() {
    super.read();
    metaBaseDir = conf.getProperty(Configure.USER_DIR) + "/meta/";
    logger.info("DefaultFileReader started");
    int diffMin = conf.getInt(FILE_READER_SCAN_TIMERANGE);
    int inteval = conf.getInt(FILE_READER_SCAN_INTERVAL);
    long diffTs = diffMin * 60l * 1000l;
    File directory = new File(conf.getProperty(FILE_READER_LOG_DIR));
    while (running) {
      addReader(directory);
      File[] files = directory.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
      long currentTime = System.currentTimeMillis();
      for (File subdir : files) {
        File metaDir = new File(metaBaseDir + "/" + subdir.getName());
        metaDir.mkdirs();
        long lastModTime = subdir.lastModified();
        //忽略过期目录
        if (currentTime - lastModTime > diffTs) {
          File finishFile = new File(metaDir, FINISH_FILE);
          try {
            if (!finishFile.exists())
              finishFile.createNewFile();
          } catch (IOException e) {
            logger.error("create file failed :" + FINISH_FILE, e);
          }
          continue;
        }

        File finishFile = new File(metaDir, FINISH_FILE);
        //忽略已完成目录
        if (!finishFile.exists())
          addReader(subdir);

      }

      try {
        Thread.sleep(inteval);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void addReader(File directory) {
    String directoryName = directory.getAbsolutePath();
    if (readerMap.containsKey(directoryName))
      return;

    if (!readMetricMap.containsKey(directoryName))
      readMetricMap.put(directoryName, new ReaderMetrics());

    Reader reader = new Reader(directory);
    readerMap.put(directoryName, reader);
    fixedThreadPool.execute(reader);
  }

  private class Reader implements Runnable {
    private final File directory;
    private final Gson gson = new GsonBuilder().create();
    public Reader(File directory) {
      this.directory = directory;
    }

    @Override
    public void run() {
      String dirPath = directory.getAbsolutePath();
      if (!running){
        readerMap.remove(dirPath);
        return;
      }
      ReaderMetrics readerMetrics= readMetricMap.get(dirPath);

      logger.info("reading directory:" + dirPath);
      try {
        int batchSize = conf.getInt(Configure.FILE_READER_BATCH_SIZE);
        File metaDir = new File(metaBaseDir + "/" + directory.getName());
        metaDir.mkdirs();
        File offsetFile = new File(metaDir + "/" + COLLECT_OFFSET);
        long lastByteOffset = 0;
        String lastFileName = null;
        String offsetStr = null;
        if (offsetFile.exists()) {
          offsetStr = FileUtils.readFileToString(offsetFile);
          if(StringUtils.isBlank(offsetStr))
            logger.error(offsetFile.getAbsolutePath() + " is empty!!");
          String[] fields = StringUtils.split(offsetStr.trim(), ':');
          lastFileName = fields[0];
          if (fields.length == 3)
            lastByteOffset = Long.parseLong(fields[2]);
          else
            lastByteOffset = Long.parseLong(fields[1]);
        }

        long currentByteOffset = lastByteOffset;
        Collection<File> files = FileUtils.listFiles(directory, new SugoFileFilter(conf.getProperty(FILE_READER_LOG_REGEX), lastFileName), null);
        List<File> sortFiles = new ArrayList<>(files);
        Collections.sort(sortFiles, new Comparator<File>() {
          @Override
          public int compare(File o1, File o2) {
            return o1.getPath().compareTo(o2.getPath());
          }
        });
        long current = System.currentTimeMillis();
        for (File file : sortFiles) {
          if (!running){
            readerMap.remove(dirPath);
            return;
          }
          String fileName = file.getName();

          if (lastFileName != null && !lastFileName.equals(fileName)) {
            currentByteOffset = 0;
          }
          long fileLength = file.length();
          //如果offset大于文件长度，从0开始读
          if (!isSeparate && currentByteOffset > 0 && fileLength < currentByteOffset) {
            currentByteOffset = 0;
          }

          String fileAbsolutePath = file.getAbsolutePath();
          logger.info("handle file:" + fileAbsolutePath);

          FileInputStream fis = new FileInputStream(file);
          fis.skip(currentByteOffset);
          BufferedReader br = new BufferedReader(
                  new InputStreamReader(fis, Charset.forName(UTF8)));

          String tempString = null;
          int line = 0;
          int error = 0;
          List<String> messages = new ArrayList<>();
          do {
            tempString = br.readLine();

            //文件结尾处理
            if (tempString == null) {
              lastFileName = fileName;
              if (messages.size() > 0) {
                write(messages);
              }
              //记录消费位点
              FileUtils.writeStringToFile(offsetFile, fileName + ":" + currentByteOffset);

              currentByteOffset = 0;
              StringBuffer logbuf = new StringBuffer();
              logbuf.append("file:").append(fileName).append(" handle finished, total lines:").append(line).append(" error:").append(error);
              logger.info(logbuf.toString());
              fis.close();
              break;
            }
            int tmpSize = tempString.getBytes(UTF8).length ;
            if (tmpSize >= maxSize){
              error ++;
              readerMetrics.incrementError();
              logger.error(host + " " + fileAbsolutePath, new Exception("record too large, size: " + tmpSize));
              if (StringUtils.isNotBlank(errMsgCollectorUrl)){
                try{
                  HttpUtil.post(errMsgCollectorUrl, tempString);
                } catch (IOException e) {
                  logger.error("send large message fail", e);
                }
              }

            }

            if (StringUtils.isNotBlank(tempString) && tmpSize < maxSize) {
              if (parser == null){
                messages.add(tempString);
              }else {
                try {
                  Map<String, Object> gmMap = parser.parse(tempString);

                  if (gmMap.size() > 0){
                    gmMap.put("directory", dirPath);
                    gmMap.put("host", host);
                    gmMap.put("filename", fileName);
                    messages.add(gson.toJson(gmMap));
                    if (shouldMetricSuccessProcessed) {
                      readerMetrics.incrementSuccess();
                    } else {
                      readerMetrics.incrementSuccess((long)gmMap.get(metricDimensionTime));
                    }
                  }else {
                    error ++;
                    readerMetrics.incrementError();
                    if (logger.isDebugEnabled())
                      logger.debug(tempString);
                  }
                } catch (Exception e) {
                  if (!(e instanceof IgnorableException)){
                    StringBuffer logbuf = new StringBuffer();
                    logbuf.append("file:").append(fileAbsolutePath).append("currentByteOffset:").
                            append(currentByteOffset).append(" failed to parse:").append(tempString);
                    logger.error(logbuf.toString(), e);
                  }
                  //if (StringUtils.isNotBlank(errMsgCollectorUrl))
                  //  HttpUtil.post(errMsgCollectorUrl, tempString);
                }
              }
            }

            currentByteOffset += (tmpSize + lineSeparatorSize);
            line++;
            //分批写入
            if (line % batchSize == 0) {
              write(messages);
              FileUtils.writeStringToFile(offsetFile, fileName + ":" + currentByteOffset);
              messages = new ArrayList<>();
            }

            if (line % 100000 == 0) {
              long now = System.currentTimeMillis();
              long diff = now - current;
              current = now;
              if (logger.isDebugEnabled()) {
                StringBuffer logbuf = new StringBuffer("file:").append(fileAbsolutePath).append(" current line:")
                        .append(line).append(" time:").append(diff).append(" percent:").append((int) ((double) currentByteOffset / (double) fileLength * 100)).append("%");
                logger.info(logbuf.toString());
                logger.info("error:" + error);
                logger.info("handle:" + line);
              }
            }
          } while (running);
        }
      } catch (Exception e) {
        logger.error("reader terminated abnormally ", e);
      } finally {
        readerMap.remove(dirPath);
    }
    }

    private boolean write(List<String> messages) throws InterruptedException {
      boolean res = writer.write(messages);
      if (!res) {
        logger.warn("写入失败，1秒后重试!!!");
        Thread.sleep(1000);
        return writer.write(messages);
      }
      return false;
    }
  }
}
