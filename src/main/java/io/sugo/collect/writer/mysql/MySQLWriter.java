package io.sugo.collect.writer.mysql;

import io.sugo.collect.Configure;
import io.sugo.collect.util.MySQLDBUtil;
import io.sugo.collect.writer.AbstractWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by sugo on 18-11-12 下午2:54
 **/
public class MySQLWriter extends AbstractWriter {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static Connection connection;
    private static PreparedStatement pstmt ;
    private static ResultSet rs = null;

    public MySQLWriter(Configure conf) {
        super(conf);
        connection = MySQLDBUtil.getConnection();
    }

    @Override
    public boolean write(List<String> messages) {
        for (String message : messages){
            System.out.println(message);
        }
        try {
            pstmt = connection.prepareStatement("");
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }
}
