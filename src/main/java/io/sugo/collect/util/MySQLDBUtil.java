package io.sugo.collect.util;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

/**
 * Created by sugo on 18-10-24 上午10:44
 **/
public class MySQLDBUtil {
    //四个参数
    private static String driver = "com.mysql.jdbc.Driver";
    private static String url = null;
    private static String username = null;
    private static String password = null;

    private static String propertiesDir = null;

    //使用静态代码块的特性进行配置文件的读取，并复制给上面的静态变量
    static {
        //读取配置文件
        Properties properties = new Properties();
        File file = new File("src/main/resources/mysql.properties");
        if (file.exists()){
            propertiesDir = "src/main/resources/mysql.properties";
        }else {
            propertiesDir = "conf/mysql.properties";
        }
        try {

            properties.load(new FileInputStream(propertiesDir));
            url = properties.getProperty("url");
            username = properties.getProperty("username");
            password = properties.getProperty("password");
            //加载驱动类
            Class.forName(driver);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //获得连接
    public static Connection getConnection() {
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void  query(String sql) throws SQLException {
        PreparedStatement pstmt;
        pstmt = getConnection().prepareStatement(sql);
        pstmt.execute();
        pstmt.close();
    }

    //关闭连接
    public static void close(Connection connection, PreparedStatement pstamt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        if (pstamt != null) {
            try {
                pstamt.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        Connection conn = MySQLDBUtil.getConnection();
        PreparedStatement pstmt ;
        ResultSet rs = null;

        try {
            String sql = "select count(1) from USER_INFO";
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()){
                System.out.println(rs.getInt(1));
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
