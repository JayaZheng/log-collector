package io.sugo.collect.writer.hdfs;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by zhengjy on 19-3-7 上午11:26
 **/
public class HDFSWriter extends AbstractWriter {
    private final Logger logger = LoggerFactory.getLogger(HDFSWriter.class);
    private static final String HDFS_OUTPUT_DIR = "hdfs.output.dir";
    private static final String HDFS_AUTO_MKDIR = "hdfs.auto.mkdir";
    private static final String HDFS_CLUSTER_NAME = "hdfs.cluster.name";
    private static final String HDFS_NAMENODE1 = "hdfs.namenode1";
    private static final String HDFS_NAMENODE2 = "hdfs.namenode2";
    private static final String HDFS_OUTPRUT_FILE_PRE = "hdfs.output.file.pre";
    private static final String HDFS_OUTPRUT_FILE_LINES = "hdfs.output.file.lines";

    private static Configuration hdfsConf;

    private static String clusterName;
    private static String HADOOP_URL;

    private FileSystem fs;
    private static String outputDir;
    private static String outputFile;
    private static String filePre;

    private static int HDFS_WRITER_OFFSET;
    private static int HDFS_WRITER_TOTAL_LINES;

    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private static FSDataOutputStream fsDataOutputStream;

    public HDFSWriter(Configure conf) {
        super(conf);
        clusterName = conf.getProperty(HDFS_CLUSTER_NAME);
        HADOOP_URL = "hdfs://" + clusterName;
        hdfsConf = new Configuration();
        hdfsConf.set("fs.defaultFS", HADOOP_URL);
        hdfsConf.set("dfs.nameservices", clusterName);
        hdfsConf.set("dfs.ha.namenodes." + clusterName, "nn1,nn2");
        hdfsConf.set("dfs.namenode.rpc-address." + clusterName + ".nn1", conf.getProperty(HDFS_NAMENODE1));
        hdfsConf.set("dfs.namenode.rpc-address." + clusterName + ".nn2", conf.getProperty(HDFS_NAMENODE2));
        hdfsConf.set("dfs.client.failover.proxy.provider." + clusterName,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        filePre = conf.getProperty(HDFS_OUTPRUT_FILE_PRE,"collect_file");

        initOutputDir(conf);

    }

    private void initOutputDir(Configure conf){
        HDFS_WRITER_TOTAL_LINES = Integer.parseInt(conf.getProperty(HDFS_OUTPRUT_FILE_LINES,"100000"));
        HDFS_WRITER_OFFSET = 0;

        try {
            fs = FileSystem.get(URI.create(HADOOP_URL),hdfsConf);
            outputDir = conf.getProperty(HDFS_OUTPUT_DIR);
            Path path = new Path(outputDir);
            if (fs.exists(path)) {
                logger.info("output directory is " + path);
            }else {
                logger.info(outputDir + " is not exist");
                if (conf.getProperty(HDFS_AUTO_MKDIR,"false").equals("true")){
                    logger.info("try to create dir : " + outputDir);
                    fs.mkdirs(path);
                }
            }

            if (!fs.isDirectory(path)){
                logger.warn(outputDir + " is not a directory, please check it ...");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getOutputFile(){
        if (StringUtils.isBlank(outputDir)){
            logger.warn("HDFS directory could not be null");
        }

        if (HDFS_WRITER_OFFSET >= HDFS_WRITER_TOTAL_LINES){
            HDFS_WRITER_OFFSET = 0;
            return filePre+ "_" + formatter.format(new Date());

        }

        if (StringUtils.isBlank(outputFile)){
            return filePre+ "_" + formatter.format(new Date());
        }

        return outputFile;
    }

    private void updateLineOffset(List<String> messages){
        HDFS_WRITER_OFFSET += messages.size();
    }

    @Override
    public boolean write(List<String> messages) {
        outputFile = getOutputFile();
        Path path = new Path(outputDir + "/" + outputFile);
        try {

            if (fs.exists(path)){
                fsDataOutputStream = fs.append(path);
            }else {
                fsDataOutputStream = fs.create(path);
            }

            if (fs.isFile(path)){
                for (String message : messages) {
                    fsDataOutputStream.write((message+"\n").getBytes("UTF-8"));
                }
                updateLineOffset(messages);
                fsDataOutputStream.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            IOUtils.closeStream(fsDataOutputStream);
        }
        return true;
    }

}
