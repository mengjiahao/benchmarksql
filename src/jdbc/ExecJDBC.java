/*
 * ExecJDBC - Command line program to process SQL DDL statements, from
 *             a text input file, to any JDBC Data Source
 *
 * Copyright (C) 2004-2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 *  单个JDBC 客户端测试.
 */
public class ExecJDBC {

    /**
     mysql-connector-java-5.1.45 ( Revision: 9131eefa398531c7dc98776e8a3fe839e544c5
     b2 ) SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@characte
    r_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_s
    erver, @@collation_server AS collation_server, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS
    license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_buffer_length AS net_buff
    er_length, @@net_write_timeout AS net_write_timeout, @@query_cache_size AS query_cache_size, @@query_cache_type AS query_cache_type, @@sq
    l_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@tx_isolation AS tx_isolation, @@wait_timeout AS w
    ait_timeout;
     SHOW WARNINGS
     SET NAMES utf8mb4
     SET character_set_results = NULL
     SET autocommit=1
     SET sql_mode='NO_ENGINE_SUBSTITUTION,STRICT_TRANS_TABLES'
     */

    /**
     mysql-connector-java-8.0.27 (Revision: e920b979015ae7117d60d72bcc8f077a839cd79
     1) SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_
    set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_ser
    ver, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactiv
    e_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_a
    llowed_packet, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@query_cache_size AS query_cache_si
    ze, @@query_cache_type AS query_cache_type, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@t
    ransaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout
     SHOW WARNINGS
     SET character_set_results = NULL
     SET autocommit=1
     SET sql_mode='NO_ENGINE_SUBSTITUTION,STRICT_TRANS_TABLES'
     */
  public static void main(String[] args) {

    Connection conn = null;
    Statement stmt = null;
    String rLine = null;
    StringBuffer sql = new StringBuffer();

    try {

    Properties ini = new Properties();
    ini.load( new FileInputStream(System.getProperty("prop")));

    // Register jdbcDriver
    // 仍然是通过 Class.forName + DriverManager.getConnection 获取连接.
    Class.forName(ini.getProperty( "driver" ));

    // make connection
    // 只使用1个连接.
    conn = DriverManager.getConnection(ini.getProperty("conn"),
      ini.getProperty("user"),ini.getProperty("password"));
    conn.setAutoCommit(true);

    // Create Statement
    stmt = conn.createStatement();

      // Open inputFile
      BufferedReader in = new BufferedReader
        (new FileReader(jTPCCUtil.getSysProp("commandFile",null)));

      // loop thru input file and concatenate SQL statement fragments
      while((rLine = in.readLine()) != null) {

         String line = rLine.trim();

         if (line.length() != 0) {
           if (line.startsWith("--")) {
              System.out.println(line);  // print comment line
           } else {
           // line结尾为什么会有\;?
	       if (line.endsWith("\\;"))
	       {
	         sql.append(line.replaceAll("\\\\;", ";"));
		 sql.append("\n");
	       }
	       else
	       {
		   sql.append(line.replaceAll("\\\\;", ";"));
		   if (line.endsWith(";")) {
		      String query = sql.toString();

		      execJDBC(stmt, query.substring(0, query.length() - 1));
		      sql = new StringBuffer();
		   } else {
		     sql.append("\n");
		   }
	       }
           }

         } //end if

      } //end while

      in.close();

    } catch(IOException ie) {
        System.out.println(ie.getMessage());

    } catch(SQLException se) {
        System.out.println(se.getMessage());

    } catch(Exception e) {
        e.printStackTrace();

    //exit Cleanly
    } finally {
      try {
        if (conn !=null)
           conn.close();
      } catch(SQLException se) {
        se.printStackTrace();
      } // end finally

    } // end try

  } // end main


  static void execJDBC(Statement stmt, String query) {

    System.out.println(query + ";");

    try {
      stmt.execute(query);
    }catch(SQLException se) {
      System.out.println(se.getMessage());
    } // end try

  } // end execJDBCCommand

} // end ExecJDBC Class
