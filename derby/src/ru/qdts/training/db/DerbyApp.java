package ru.qdts.training.db;

import ru.qdts.training.sample.data.SalesRec;
import ru.qdts.training.sample.data.SalesRecLoader;
import static ru.qdts.java.util.Util.*;

import java.sql.*;
import java.util.Enumeration;
import java.util.List;

public class DerbyApp {
    private static final String url = "jdbc:derby://localhost:1527/testDB2";

    public static void main(String[] args) {
        Connection conn;
        DatabaseMetaData dbmd;
         //DriverManager.registerDriver(
        try {
            Class.forName("org.apache.derby.client.ClientAutoloadedDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        listDrivers();
        loadSampleData();

        try {
            conn = DriverManager.getConnection(url + ";create=true");
            dbmd = conn.getMetaData();
            System.out.println(dbmd.getDatabaseProductName());
            System.out.println(dbmd.getDatabaseProductVersion());
            System.out.println(dbmd.supportsGroupBy());

        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 1. Вывести в консоль все страны из Азии, где имели место продажи

        // 2. Проссумировать приходы от продаж по регионам, результат вывести в консоль

        // 3. Используя позиционированные изменения, установить TOTAL_REVENUE заказов,
        // чье исполнение заняло более 15 дней в значение -1
    }
    private static void listDrivers() {
        Enumeration<Driver> driverList = DriverManager.getDrivers();
        System.out.println("\nList of drivers:");
        while (driverList.hasMoreElements()) {
            Driver driverClass = (Driver) driverList.nextElement();
            System.out.println("   "+driverClass.getClass().getName());
        }
    }

    private static void loadSampleData() {
        List<SalesRec> data = SalesRecLoader.load();
        try(Connection conn = DriverManager.getConnection(url + ";create=true")) {
            DatabaseMetaData dbmd = conn.getMetaData();
            if(dbmd.getTables(null, null, "SALES", null).next()) {
                conn.createStatement().executeUpdate("DROP TABLE SALES");
                println("SALES table wad dropped");
            }

            conn.createStatement().executeUpdate("CREATE TABLE SALES(ID INT, REGION VARCHAR(50)," +
                    "COUNTRY VARCHAR(50), ORDER_DATE DATE, SHIP_DATE DATE, TOTAL_REVENUE REAL)");
            println("SALES table wad created");

            //println("Load time is: " + stopwatch(DerbyApp::insertDataCommit500, conn, data)/1e9);
            //println("Load time is: " + stopwatch(DerbyApp::insertDataAutoCommit, conn, data)/1e9);
            //println("Load time is: " + stopwatch(DerbyApp::insertDataNoCommit, conn, data)/1e9);
            println("Load time is: " + stopwatch(DerbyApp::insertDataNotPrepared, conn, data)/1e9);
        }
        catch (SQLException e) {
            println("Sample data load error:");
            println(e.getMessage());
            println(e.getSQLState());
        }
    }

    private static void insertDataCommit500(Connection conn, List<SalesRec> data) {
       try(PreparedStatement ins = conn.prepareStatement("INSERT INTO SALES VALUES(?,?,?,?,?,?)")) {
           conn.setAutoCommit(false);
           int commitCounter = 0;
           println("Start loading data to table");
           for (SalesRec sr : data) {
               ins.setInt(1, sr.getOrderID());
               ins.setString(2, sr.getRegion());
               ins.setString(3, sr.getCountry());
               ins.setDate(4, java.sql.Date.valueOf(sr.getOrderDate()));
               ins.setDate(5, java.sql.Date.valueOf(sr.getShipDate()));
               ins.setFloat(6, sr.getTotalRevenue());
               ins.executeUpdate();
               if (++commitCounter == 500) {
                   conn.commit();
                   println("500 row inserts committed");
                   commitCounter = 0;
               }
           }
           conn.commit();
           conn.setAutoCommit(true);
       } catch(SQLException e) {
           println(e.getMessage());
           println(e.getSQLState());
       }
        println("Data was successfully loaded");
    }

    private static void insertDataAutoCommit(Connection conn, List<SalesRec> data) {
        try(PreparedStatement ins = conn.prepareStatement("INSERT INTO SALES VALUES(?,?,?,?,?,?)")) {
            println("Start loading data to table");
            for (SalesRec sr : data) {
                ins.setInt(1, sr.getOrderID());
                ins.setString(2, sr.getRegion());
                ins.setString(3, sr.getCountry());
                ins.setDate(4, java.sql.Date.valueOf(sr.getOrderDate()));
                ins.setDate(5, java.sql.Date.valueOf(sr.getShipDate()));
                ins.setFloat(6, sr.getTotalRevenue());
                ins.executeUpdate();
            }
        } catch(SQLException e) {
            println(e.getMessage());
            println(e.getSQLState());
        }
        println("Data was successfully loaded");
    }

    private static void insertDataNoCommit(Connection conn, List<SalesRec> data) {
        try(PreparedStatement ins = conn.prepareStatement("INSERT INTO SALES VALUES(?,?,?,?,?,?)")) {
            conn.setAutoCommit(false);
            println("Start loading data to table");
            for (SalesRec sr : data) {
                ins.setInt(1, sr.getOrderID());
                ins.setString(2, sr.getRegion());
                ins.setString(3, sr.getCountry());
                ins.setDate(4, java.sql.Date.valueOf(sr.getOrderDate()));
                ins.setDate(5, java.sql.Date.valueOf(sr.getShipDate()));
                ins.setFloat(6, sr.getTotalRevenue());
                ins.executeUpdate();
            }
            conn.commit();
            conn.setAutoCommit(true);
        } catch(SQLException e) {
            println(e.getMessage());
            println(e.getSQLState());
        }
        println("Data was successfully loaded");
    }

    private static void insertDataNotPrepared(Connection conn, List<SalesRec> data) {

        try(Statement ins = conn.createStatement()) {
            conn.setAutoCommit(false);
            println("Start loading data to table");
            for (SalesRec sr : data) {
                StringBuilder sql = new StringBuilder();
                sql.append("INSERT INTO SALES VALUES(").
                        append(sr.getOrderID()).append(",'").
                        append(sr.getRegion()).append("','").
                        append(sr.getCountry().replace('\'', '_')).append("','").
                        append(java.sql.Date.valueOf(sr.getOrderDate())).append("','").
                        append(java.sql.Date.valueOf(sr.getShipDate())).append("',").
                        append(sr.getTotalRevenue()).append(")");
                //println(sql.toString());
                ins.executeUpdate(sql.toString());
            }
            conn.commit();
            conn.setAutoCommit(true);
        } catch(SQLException e) {
            println(e.getMessage());
            println(e.getSQLState());
        }
        println("Data was successfully loaded");
    }
}
