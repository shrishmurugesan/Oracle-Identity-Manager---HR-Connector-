package com.iamlabs.iam.oim.connector.db;

import com.iamlabs.iam.oim.connector.db.utils.DBUtils;

import java.util.Map;
import java.util.logging.Level;

import oracle.core.ojdl.logging.ODLLogger;

public class DBProvisioningTasks {
    private static ODLLogger logger =
        ODLLogger.getODLLogger(DBProvisioningTasks.class.getName());
    DBUtils dbUtils = null;

    public DBProvisioningTasks() {
        super();
        dbUtils=new DBUtils();
    }

    public static void main(String[] args) {
        logger.info("This is a test...");
    }

    public String createUser(Long processInstanceKey,
                             String itResourceFieldName) {
        String retVal = "FAILED";
        try {

            Map<String, String> processFormData =
                dbUtils.getProcessFormData(processInstanceKey);
            String sqlQuery =
                "INSERT INTO ACCOUNT (USER_ID, PASSWORD, FIRST_NAME, LAST_NAME, EMAIL, STATUS) VALUES ('" +
                processFormData.get("UD_DBUSER_USERID") + "', '" +
                processFormData.get("UD_DBUSER_PASSWORD") + "', '" +
                processFormData.get("UD_DBUSER_FIRST_NAME") + "', '" +
                processFormData.get("UD_DBUSER_LAST_NAME") + "', '" +
                processFormData.get("UD_DBUSER_EMAIL") + "', 'ACTIVE')";
            logger.info("SQL Query : " + sqlQuery);
            dbUtils.executeStmt(Long.valueOf(processFormData.get(itResourceFieldName)),
                                sqlQuery);
            retVal = "SUCCESS";
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Error occured while creating the user : " +
                       e.getMessage(), e);
        }
        return retVal;
    }

    public String updateUser(Long processInstanceKey,
                             String itResourceFieldName,
                             String updatedOIMFieldName) {
        String retVal = "FAILED";
        try {
            Map<String, String> processFormData =
                dbUtils.getProcessFormData(processInstanceKey);
            String sqlQuery = "";
            String updatedTableFieldName = "";
            if (updatedOIMFieldName.equalsIgnoreCase("UD_DBUSER_EMAIL"))
                updatedTableFieldName = "EMAIL";
            else if (updatedOIMFieldName.equalsIgnoreCase("UD_DBUSER_PASSWORD"))
                updatedTableFieldName = "PASSWORD";
            else if (updatedOIMFieldName.equalsIgnoreCase("ENABLED") ||
                     updatedOIMFieldName.equalsIgnoreCase("DISABLED"))
                updatedTableFieldName = "STATUS";
            else {
                logger.warning("Unknown Field Name : " + updatedOIMFieldName);
            }
            if (!updatedTableFieldName.isEmpty()) {
                if (updatedOIMFieldName.equalsIgnoreCase("ENABLED")) {
                    sqlQuery =
                            "UPDATE ACCOUNT SET "+updatedTableFieldName+"='ACTIVE' where USER_ID='" +
                            processFormData.get("UD_DBUSER_USERID") + "'";
                } else if (updatedOIMFieldName.equalsIgnoreCase("DISABLED")) {
                    sqlQuery =
                            "UPDATE ACCOUNT SET "+updatedTableFieldName+"='INACTIVE' where USER_ID='" +
                            processFormData.get("UD_DBUSER_USERID") + "'";
                } else {
                    sqlQuery =
                            "UPDATE ACCOUNT SET " + updatedTableFieldName + "='" +
                            processFormData.get(updatedOIMFieldName) +
                            "' where USER_ID='" +
                            processFormData.get("UD_DBUSER_USERID") + "'";
                }

                logger.info("SQL Query : " + sqlQuery);
                dbUtils.executeStmt(Long.valueOf(processFormData.get(itResourceFieldName)),
                                    sqlQuery);
                retVal = "SUCCESS";
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Error occured while updating the user : " +
                       e.getMessage(), e);
        }
        return retVal;
    }

    public String deleteUser(Long processInstanceKey,
                             String itResourceFieldName) {
        String retVal = "FAILED";
        try {
            Map<String, String> processFormData =
                dbUtils.getProcessFormData(processInstanceKey);
            String sqlQuery =
                "DELETE FROM ACCOUNT WHERE USER_ID='" + processFormData.get("UD_DBUSER_USERID") +
                "'";
            logger.info("SQL Query : " + sqlQuery);
            dbUtils.executeStmt(Long.valueOf(processFormData.get(itResourceFieldName)),
                                sqlQuery);
            retVal = "SUCCESS";
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Error occured while deleting the user : " +
                       e.getMessage(), e);
        }
        return retVal;
    }
}
