package com.iamlabs.iam.oim.connector.db;

import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcAttributeNotFoundException;
import Thor.API.Exceptions.tcEventDataReceivedException;
import Thor.API.Exceptions.tcEventNotFoundException;

import com.iamlabs.iam.oim.connector.db.utils.DBUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import java.util.HashMap;
import java.util.Iterator;

import oracle.core.ojdl.logging.ODLLogger;

import oracle.iam.reconciliation.api.ChangeType;
import oracle.iam.reconciliation.api.EventAttributes;
import oracle.iam.reconciliation.api.ReconOperationsService;


public class DBReconciliationTasks {
    private static ODLLogger logger =
        ODLLogger.getODLLogger(DBReconciliationTasks.class.getName());
    private DBUtils dbUtils = null;

    public DBReconciliationTasks() {
        super();
        dbUtils = new DBUtils();
    }

    public void execute(HashMap hashMap) {
        System.out.println("*************************   STARTED SCHEDULED JOB ************************");
        logger.info("*************************   STARTED SCHEDULED JOB ************************");
        Connection con = null;
        Statement stmt = null;
        ResultSet reSet = null;
        try {
            ReconOperationsService reconService =
                dbUtils.getService(ReconOperationsService.class);
            con = dbUtils.getDBConnectionFromITResource(Long.valueOf("44"));
            stmt = con.createStatement();
            stmt.setMaxRows(100);
            reSet =
stmt.executeQuery("select user_id as \"UserID\",password as \"Password\",first_name as \"FirstName\",last_name as \"LastName\",email as \"Email\",'44' as \"Server\",status from account");

            while (reSet.next()) {
                HashMap<String, Object> dataMap =
                    new HashMap<String, Object>();

                dataMap.put("UserID", reSet.getNString(1));
                dataMap.put("FirstName", reSet.getNString(2));
                dataMap.put("LastName", reSet.getNString(3));
                dataMap.put("Email", reSet.getNString(4));
                dataMap.put("Server", "44");
                reconcileEvent(reconService, "DB User", dataMap, null);
                logger.info("Data : "+dataMap);

            }
            callingEndOfJobAPI(reconService);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dbUtils.closeResultSet(reSet);
            dbUtils.closeStatement(stmt);
            dbUtils.closeConnection(con);
        }
        System.out.println("*************************   END SCHEDULED JOB ************************");
        logger.info("*************************   END SCHEDULED JOB ************************");
    }

    public HashMap getAttributes() {
        return null;
    }

    public void setAttributes() {
    }

    public static void callingEndOfJobAPI(ReconOperationsService reconService) {
        try {
            reconService.callingEndOfJobAPI();
        } catch (tcAPIException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        }
    }

    public static long reconcileEvent(ReconOperationsService reconService,
                                      String resourceName,
                                      HashMap<String, Object> reconUserData,
                                      HashMap<String, Object> reconChildData) {
        EventAttributes eventAttributes = new EventAttributes();
        eventAttributes.setDateFormat("yyyy-mm-dd hh:mm:ss");
        eventAttributes.setChangeType(ChangeType.CHANGELOG);

        //logger.info("Creating Reconciliation Event...");
        long reconKey = 0L;
        if (reconChildData != null) {
            try {
                eventAttributes.setEventFinished(false);
                reconKey =
                        reconService.createReconciliationEvent(resourceName, reconUserData,
                                                               eventAttributes);
                logger.fine("Created Reconciliation Event with Key : " +
                            reconKey);
                Iterator roleTypeIterator = reconChildData.keySet().iterator();
                while (roleTypeIterator.hasNext()) {
                    String roleType = roleTypeIterator.next().toString();
                    HashMap<String, Object> roleTypeChildData =
                        (HashMap<String, Object>)reconChildData.get(roleType);
                    if (roleTypeChildData != null) {
                        reconService.providingAllMultiAttributeData(reconKey,
                                                                    roleType,
                                                                    true);
                        Iterator entitlementChildDataIterator =
                            roleTypeChildData.keySet().iterator();
                        while (entitlementChildDataIterator.hasNext()) {
                            String entitlementKey =
                                entitlementChildDataIterator.next().toString();
                            HashMap<String, String> entitlementChildFormValues =
                                (HashMap<String, String>)roleTypeChildData.get(entitlementKey);
                            if (entitlementChildFormValues != null) {
                                //CommonUtils.printHashMapInLine("Adding" + " - "
                                //              + roleType + " - " + entitlementKey,
                                //              entitlementChildFormValues, "ALL");

                                reconService.addMultiAttributeData(reconKey,
                                                                   roleType,
                                                                   entitlementChildFormValues);

                            }
                        }
                    }
                }
                reconService.finishReconciliationEvent(reconKey);
            } catch (tcAPIException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (tcEventNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (tcEventDataReceivedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (tcAttributeNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            eventAttributes.setEventFinished(false);
            reconKey =
                    reconService.createReconciliationEvent(resourceName, reconUserData,
                                                           eventAttributes);
            logger.fine("Returned Reconciliation Event ID: " + reconKey);
            try {
                reconService.finishReconciliationEvent(reconKey);
            } catch (tcEventDataReceivedException e) {
                e.printStackTrace();
            } catch (tcEventNotFoundException e) {
                e.printStackTrace();
            } catch (tcAPIException e) {
                e.printStackTrace();
            }
        }

        return reconKey;

    }

    public static void main(String[] argv) {
        DBReconciliationTasks dbTasks = new DBReconciliationTasks();
        dbTasks.execute(null);


    }
}
