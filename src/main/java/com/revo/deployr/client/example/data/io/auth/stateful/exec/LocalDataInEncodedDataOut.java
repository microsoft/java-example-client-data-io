/*
 * LocalDataInEncodedDataOut.java
 *
 * Copyright (C) 2010-2015 by Revolution Analytics Inc.
 *
 * This program is licensed to you under the terms of Version 2.0 of the
 * Apache License. This program is distributed WITHOUT
 * ANY EXPRESS OR IMPLIED WARRANTY, INCLUDING THOSE OF NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE. Please refer to the
 * Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) for more details.
 *
 */
package com.revo.deployr.client.example.data.io.auth.stateful.exec;

import com.revo.deployr.client.*;
import com.revo.deployr.client.data.*;
import com.revo.deployr.client.factory.*;
import com.revo.deployr.client.params.*;
import com.revo.deployr.client.auth.RAuthentication;
import com.revo.deployr.client.auth.basic.RBasicAuthentication;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.commons.io.IOUtils;

import org.apache.log4j.Logger;

public class LocalDataInEncodedDataOut {

    private static Logger log = Logger.getLogger(LocalDataInEncodedDataOut.class);
    /*
     * Hipparcos star dataset URL endpoint.
     */
    private static String HIP_DAT_URL =
        "http://astrostatistics.psu.edu/datasets/HIP_star.dat";

    public static void main(String args[]) throws Exception {

        RClient rClient = null;
        RProject rProject = null;

        try {

            /*
             * Determine DeployR server endpoint.
             */
            String endpoint = System.getProperty("endpoint");
            log.info("[ CONFIGURATION  ] Using endpoint=" + endpoint);

            /*
             * Establish RClient connection to DeployR server.
             *
             * An RClient connection is the mandatory starting
             * point for any application using the client library.
             */
            rClient = RClientFactory.createClient(endpoint);

            log.info("[   CONNECTION   ] Established anonymous " +
                    "connection [ RClient ].");

            /*
             * Build a basic authentication token.
             */
            RAuthentication rAuth =
                    new RBasicAuthentication(System.getProperty("username"),
                            System.getProperty("password"));

            /*
             * Establish an authenticated handle with the DeployR
             * server, rUser. Following this call the rClient 
             * connection is operating as an authenticated connection
             * and all calls on rClient inherit the access permissions
             * of the authenticated user, rUser.
             */
            RUser rUser = rClient.login(rAuth);
            log.info("[ AUTHENTICATION ] Upgraded to authenticated " +
                    "connection [ RUser ].");
            
            /*
             * Create a temporary project (R session).
             *
             * Optionally:
             * ProjectCreationOptions options =
             * new ProjectCreationOptions();
             *
             * Populate options as needed, then:
             *
             * rProject = rUser.createProject(options);
             */
            rProject = rUser.createProject();

            log.info("[  GO STATEFUL   ] Created stateful temporary " +
                    "R session [ RProject ].");

            /* 
             * Upload a data file from local disk to the working
             * directory of your temporary project (R session.)
             */
            File upFile = new File("analytics/hipStar.dat");
            InputStream is = new FileInputStream(upFile);
            DirectoryUploadOptions upOpts = new DirectoryUploadOptions();
            upOpts.filename = "hipStar.dat";
            upOpts.overwrite = true;
            RProjectFile projFile = rProject.uploadFile(is, upOpts);

            log.info("[  DATA UPLOAD   ] Uploaded data file input " +
                "to working directory, [ RProjectFile ].");

            /*
             * Create a ProjectExecutionOptions instance
             * to specify data inputs and output to the
             * execution of the repository-managed R script.
             *
             * This options object can be used to pass standard
             * execution model parameters on execution calls. All
             * fields are optional.
             *
             * See the Standard Execution Model chapter in the
             * Client Library Tutorial on the DeployR website for
             * further details.
             */
            ProjectExecutionOptions options =
                new ProjectExecutionOptions();


            /*
             * Request the retrieval of the "hip" data.frame and
             * two vector objects from the workspace following the
             * execution. The corresponding R objects are named as
             * follows:
             * 'hip', hipDim', 'hipNames'.
             */
            options.routputs =
                Arrays.asList("hip", "hipDim", "hipNames");

            log.info("[  EXEC OPTION   ] DeployR-encoded R object request " +
                "set on execution [ ProjectExecutionOptions.routputs ].");

            /*
             * Execute a public analytics Web service as an authenticated
             * user based on a repository-managed R script:
             * /testuser/example-data-io/dataIO.R
             */
            RProjectExecution exec =
                    rProject.executeScript("dataIO.R",
                            "example-data-io", "testuser", null, options);

            log.info("[   EXECUTION    ] Stateful R script " +
                    "execution completed [ RProjectExecution ].");

            /*
             * Retrieve the requested R object data encodings from
             * the results of the script execution. 
             *
             * See the R Object Data Decoding chapter in the
             * Client Library Tutorial on the DeployR website for
             * further details.
             */
            List<RData> objects = exec.about().workspaceObjects;

            for(RData rData : objects) {
                if(rData instanceof RDataFrame) {
                    log.info("[  DATA OUTPUT   ] Retrieved DeployR-encoded R " +
                        "object output " + rData.getName() + " [ RDataFrame ].");
                    List<RData> hipSubsetVal =
                        ((RDataFrame) rData).getValue();
                } else
                if(rData instanceof RNumericVector) {
                    log.info("[  DATA OUTPUT   ] Retrieved DeployR-encoded R " +
                        "object output " + rData.getName() + " [ RNumericVector ].");
                    List<Double> hipDimVal =
                        ((RNumericVector) rData).getValue();
                    log.info("[  DATA OUTPUT   ] Retrieved DeployR-encoded R " +
                        "object " + rData.getName() +
                        " value=" + hipDimVal);
                } else
                if(rData instanceof RStringVector) {
                    log.info("[  DATA OUTPUT   ] Retrieved DeployR-encoded R " +
                        "object output " + rData.getName() + " [ RStringVector ].");
                    List<String> hipNamesVal =
                        ((RStringVector) rData).getValue();
                    log.info("[  DATA OUTPUT   ] Retrieved DeployR-encoded R " +
                        "object " + rData.getName() +
                        " value=" + hipNamesVal);
                } else {
                    log.info("Unexpected DeployR-encoded R object returned, " +
                        "object name=" + rData.getName() + ", encoding=" +
                                                        rData.getClass());
                }
            }

        } catch (Exception ex) {
            log.warn("Unexpected runtime exception=" + ex);
        } finally {
            try {
                if (rProject != null) {
                    /*
                     * Close rProject before application exits.
                     */
                    rProject.close();
                }
            } catch (Exception fex) { }
            try {
                if (rClient != null) {
                    /*
                     * Release rClient connection before application exits.
                     */
                    rClient.release();
                }
            } catch (Exception fex) {
            }
        }

    }

}
