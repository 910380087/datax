package com.alibaba.datax.plugin.reader.gbase.util;

import com.gbase.jdbc.Driver;

import java.sql.*;

public class ConnectionUtil {


    public static Connection getConnection(String url, String username, String password){
        try{
//            Driver
            //加载MySql的驱动类
            Class.forName("com.gbase.jdbc.Driver");
//            String username = "gbase" ;
//            String password = "gbase20110531" ;
//            String url = "jdbc:gbase://10.45.50.145:5258/ztesoft" ;
            Connection con = DriverManager.getConnection(url,username,password);
            Statement statement = con.createStatement();
            ResultSet rs = statement.executeQuery("select * from person");
            rs.absolute(100000);
            return con;
        }catch (SQLException e1) {
            System.out.println("获取链接错误！");
        }
        catch(ClassNotFoundException e){
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace() ;
        }finally {

        }
        return null;
    }






}
