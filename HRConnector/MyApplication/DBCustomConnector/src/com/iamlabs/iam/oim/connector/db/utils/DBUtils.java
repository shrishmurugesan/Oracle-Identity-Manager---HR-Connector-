package com.iamlabs.iam.oim.connector.db.utils;

import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcFormNotFoundException;
import Thor.API.Exceptions.tcITResourceNotFoundException;
import Thor.API.Exceptions.tcNotAtomicProcessException;
import Thor.API.Exceptions.tcProcessNotFoundException;

import java.util.HashMap;
import java.util.Map;

import oracle.core.ojdl.logging.ODLLogger;

import oracle.iam.platform.Platform;

import Thor.API.Operations.tcFormInstanceOperationsIntf;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;
import Thor.API.tcResultSet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.sql.SQLException;

import java.sql.Statement;

import java.util.Hashtable;
import java.util.logging.Level;

import oracle.iam.platform.OIMClient;

public class DBUtils {
    private static ODLLogger logger =
        ODLLogger.getODLLogger(DBUtils.class.getName());
   private OIMClient oimClient = null;

    public DBUtils() {
        super();
        try {
            oimClient=getOIMClient("wls","weblogic","/oracle/products/middleware/products/identity/iam/designconsole/config/authwl.conf","weblogic.jndi.WLInitialContextFactory","t3://vm.iamlabs.com:14000","xelsysadm","C0mput3r");
        } catch (Exception e) {
            logger.log(Level.WARNING,"Error occured : "+e.getMessage(),e);
        }
    }


    public Map<String, String> getProcessFormData(long processInstanceKey) throws tcNotAtomicProcessException,
                                                                                         tcFormNotFoundException,
                                                                                         tcAPIException,
                                                                                         tcProcessNotFoundException,
                                                                                         tcAPIException {
        tcFormInstanceOperationsIntf formInstanceIntf =
            Platform.getService(tcFormInstanceOperationsIntf.class);
        Map<String, String> retMap = new HashMap();
        tcResultSet trs =
            formInstanceIntf.getProcessFormData(processInstanceKey);
        int count = trs.getRowCount();
        for (int i = 0; i < count; i++) {
            trs.goToRow(i);

            String columnNames[] = trs.getColumnNames();
            for (String string : columnNames) {
                try {
                    retMap.put(string, trs.getStringValue(string));
                } catch (Exception e) {
                    logger.log(Level.WARNING,
                               "Error occured while retrieving process form data : " +
                               e.getMessage(), e);

                }
            }

        }
        return retMap;
    }

    public String executeStmt(Long itResKey, String sqlQuery) throws SQLException, Exception {
        String retVal = "FAILED";
        Connection con = null;
        Statement stmt = null;
        try {
            logger.info("SQL Query : "+sqlQuery);
            con=getDBConnectionFromITResource(itResKey);
            stmt = con.createStatement();
                stmt.execute(sqlQuery);
            logger.info("SQL Query is successfully executed.");
                retVal = "SUCCESS";
            
        } catch (SQLException e) {
            logger.log(Level.WARNING, "EXCEPTION", e);
            throw new SQLException(e);
        }

        finally {
            
            closeStatement(stmt);
            closeConnection(con);
        }
        return retVal;
    }

    public void closeResultSet(ResultSet rs) {
        //logger.entering("DBUtil", "closeResultSet");
        try {
            if (rs != null)
                rs.close();
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Exception closing result set", e);
        }
        //logger.exiting("DBUtils", "closeResultSet");
    }


    /**
     * Closes the connection
     *
     * @param con - connecton object to be closed.
     */
    public void closeConnection(Connection con) {
        //logger.entering("DBUtils", "closeConnection");
        try {
            if (con != null)
                con.close();
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Exception closing connection", e);
        }
        //logger.exiting("DBUtils", "closeConnection");
    }

    /**
     * Closes prepared statement.
     *
     * @param ps - preparedstatement to be closed.
     */
    public void closeStatement(Statement stmt) {
        //logger.entering("DBUtils", "closeStatement");
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Exception closing statement", e);
        }
        //logger.exiting("DBUtils", "closeStatement");
    }

    /**
     * Closes prepared statement.
     *
     * @param ps - preparedstatement to be closed.
     */
    public void closePreparedStatement(PreparedStatement ps) {
        //logger.entering("DBUtils", "closePreparedStatement");
        try {
            if (ps != null)
                ps.close();
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Exception closing prepared statement",
                       e);
        }
        //logger.exiting("DBUtils", "closePreparedStatement");
    }



    /**
     * @param itResKey
     * @return
     * @throws tcITResourceNotFoundException
     * @throws tcAPIException
     * @throws tcColumnNotFoundException
     * @throws ClientManagerException
     */
    public Map<String, String> getITResourceMap(Long itResKey) throws tcITResourceNotFoundException,
                                                                             tcAPIException,
                                                                             tcColumnNotFoundException,
                                                                             Exception {
        tcITResourceInstanceOperationsIntf itResInsOpsIntf =
            getService(tcITResourceInstanceOperationsIntf.class);

        if ((itResKey != null) && (itResKey != 0)) {
            tcResultSet itResParams =
                itResInsOpsIntf.getITResourceInstanceParameters(Long.valueOf(itResKey));

            return constructHashMapFromITResource(itResKey + " IT Resource",
                                                  itResParams);
        } else {
            logger.warning("Unable to find an IT resource with the resource key : " +
                           itResKey);

            throw new tcITResourceNotFoundException();
        }
    }
    
    public OIMClient getOIMClient(String OIM_WLS_NAME,
                                         String OIM_APPSERVER_TYPE,
                                         String OIM_AUTH_CONF,
                                         String OIM_JAVA_NAMING_FACTORY_INITIAL,
                                         String OIM_SERVER_URL,
                                         String OIM_SVCACC_USER,
                                         String OIM_SVCACC_PWD) throws Exception {


        OIMClient oimClientLocal = null;

        try {
            System.out.println("Entering into Login");
            System.setProperty("java.security.auth.login.config",
                               OIM_AUTH_CONF);
            System.setProperty("APPSERVER_TYPE", OIM_APPSERVER_TYPE);
            System.setProperty("weblogic.Name", OIM_WLS_NAME);

            Hashtable<String, String> env = new Hashtable<String, String>();

            env.put(OIMClient.JAVA_NAMING_FACTORY_INITIAL,
                    OIM_JAVA_NAMING_FACTORY_INITIAL);
            env.put(OIMClient.JAVA_NAMING_PROVIDER_URL, OIM_SERVER_URL);
            oimClientLocal = new OIMClient(env);
            oimClientLocal.login(OIM_SVCACC_USER,
                                 OIM_SVCACC_PWD.toCharArray());
        } catch (Exception e) {
            throw new Exception(e);
        }


        return oimClientLocal;
    }
    
    public Connection getDBConnectionFromITResource(Long itResourceKey) throws 
                                                                                     Exception {

        Connection con = null;
        try {
            Map<String, String> itResMap =
                getITResourceMap(itResourceKey);

            // Available Parameters - DatabaseName, Driver, Password, URL, UserID
            con =
    DriverManager.getConnection(itResMap.get("URL"), itResMap.get("UserID"),
                            itResMap.get("Password"));
        } catch (Exception e) {
            throw new Exception(e);
        }

        return con;
    }


    public Map<String, String> constructHashMapFromITResource(String resultSetName,
                                                                     tcResultSet resultSet) throws tcAPIException,
                                                                                                   
                                                                                                   tcColumnNotFoundException {
        Map<String, String> itResMap = new HashMap<String, String>();

        if (!resultSet.isEmpty()) {
            String[] columnNames = resultSet.getColumnNames();

            for (int i = 0; i < resultSet.getRowCount(); i++) {
                resultSet.goToRow(i);

                String parName = "";
                String parValue = "";

                for (int columnNo = 0; columnNo < columnNames.length;
                     columnNo++) {

                    // logger.finest(resultSetName + " => [" + columnNo + "] " + columnNames[columnNo] + " -> "
                    // + resultSet.getStringValueFromColumn(columnNo));
                    if (columnNames[columnNo].equalsIgnoreCase("IT Resources Type Parameter.Name")) {
                        parName = resultSet.getStringValueFromColumn(columnNo);
                    }

                    if (columnNames[columnNo].equalsIgnoreCase("IT Resource.Parameter.Value")) {
                        parValue =
                                resultSet.getStringValueFromColumn(columnNo);
                    }
                }

                if (isNotEmpty(parName)) {

                    // logger.finest("Adding -> " + parName + " = " + parValue);
                    itResMap.put(parName, parValue);
                    parName = "";
                    parValue = "";
                } else {

                    // logger.warning("Skipping -> " + parName + " = " + parValue);
                    parName = "";
                    parValue = "";
                }
            }
        }

        // logger.fine(CommonUtils.getHashMapInLine(logger, resultSetName, itResMap, ""));
        return itResMap;
    }

    public boolean isNotEmpty(String str) {
        if ((str == null) || str.trim().equals("") || str.equals("null") ||
            (str.length() == 0) || str.equals("N/A") ||
            str.equalsIgnoreCase("[NONE]")) {
            return false;
        } else {
            return true;
        }
    }
    
    public <T> T getService(final Class<T> serviceClass) throws Exception {
        try {
            if (oimClient == null)
                return Platform.getService(serviceClass);
           else
               return oimClient.getService(serviceClass);
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Error occured while retrieving service class [ " +
                       serviceClass + "] : " + e.getMessage(), e);
            throw new Exception(e);
        }
    }
    
    public static void main(String[] args) {
        try {
            DBUtils dbUtils = new DBUtils();
            //logger.info(" Details : "+dbUtils.getITResourceMap(Long.valueOf("44")));            
            dbUtils.executeStmt(Long.valueOf("44"), "INSERT INTO \"HR\".\"ACCOUNT\" (USER_ID, PASSWORD, FIRST_NAME, LAST_NAME, EMAIL, STATUS) VALUES ('test', '1', 'FN', 'LN', 'EM', 'ST')");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
