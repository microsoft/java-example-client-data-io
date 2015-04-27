/*
 * EncodedDataInBinaryFileOut.java
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

public class EncodedDataInBinaryFileOut {

    private static Logger log = Logger.getLogger(EncodedDataInBinaryFileOut.class);
    /*
     * Hipparcos star dataset URL endpoint.
     */

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
             * Simulate application generated data. This data
             * is first encoded using the RDataFactory before
             * being passed as an input on the execution.
             *
             * This encoded R input is automatically converted
             * into a workspace object before script execution.
             */
            RData generatedData = simulateGeneratedData();
            if(generatedData != null) {
                List<RData> rinputs = Arrays.asList(generatedData);
                options.rinputs = rinputs;
            }

            log.info("[   DATA INPUT   ] DeployR-encoded R input set on execution, " +
                                "[ ProjectExecutionOptions.rinputs ].");

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
             * Retrieve the working directory file (artifact) called
             * hip.rData that was generated by the execution.
             *
             * Outputs generated by an execution can be used in any
             * number of ways by client applications, including:
             *
             * 1. Use output data to perform further calculations.
             * 2. Display output data to an end-user.
             * 3. Write output data to a database.
             * 4. Pass output data along to another Web service.
             * 5. etc.
             */
            List<RProjectFile> wdFiles = exec.about().artifacts;

            for(RProjectFile wdFile : wdFiles) {
                if(wdFile.about().filename.equals("hip.rData")) {
                    log.info("[  DATA OUTPUT   ] Retrieved working directory " +
                        "file output " + wdFile.about().filename +
                        " [ RProjectFile ].");
                    InputStream fis = null;
                    try { fis = wdFile.download(); } catch(Exception ex) {
                        log.warn("Working directory binary file download " + ex);
                    } finally {
                        IOUtils.closeQuietly(fis);
                    }
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

    /*
     * simulateGeneratedData
     *
     * This method is used to generate sample data within the
     * application. In a real-world application this data may 
     * originate from any number of sources, for example:
     *
     * - Data read from a file
     * - Data read from a database
     * - Data read from an external Web service
     * - Data read from direct user input
     * - Data generated in real time by the application itself
     *
     * This data is encoded using the RDataFactory and then
     * passed as an input the execution.
     *
     */
    private static RData simulateGeneratedData() {

        RData df = null;
        try {

            URL url =
                new URL("http://astrostatistics.psu.edu/datasets/HIP_star.dat");
            InputStream is = url.openStream();
            RDataTable table = RDataFactory.createDataTable(is, "\\s+", true, true);
            df = table.asDataFrame("hip");

        } catch(Exception ex) {
            log.warn("Simulate generated data failed, ex=" + ex);
        } finally {
            return df;
        }

    }
}
