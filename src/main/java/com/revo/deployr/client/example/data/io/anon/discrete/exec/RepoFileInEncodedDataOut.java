/*
 * RepoFileInEncodedDataOut.java
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
package com.revo.deployr.client.example.data.io.anon.discrete.exec;

import com.revo.deployr.client.*;
import com.revo.deployr.client.data.*;
import com.revo.deployr.client.factory.*;
import com.revo.deployr.client.params.*;
import java.util.*;

import org.apache.log4j.Logger;

public class RepoFileInEncodedDataOut {

    private static Logger log = Logger.getLogger(RepoFileInEncodedDataOut.class);

    public static void main(String args[]) throws Exception {

        RClient rClient = null;

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
             * Create the AnonymousProjectExecutionOptions object·
             * to specify data inputs and output to the script.
             *
             * This options object can be used to pass standard
             * execution model parameters on execution calls. All
             * fields are optional.
             *
             * See the Standard Execution Model chapter in the
             * Client Library Tutorial on the DeployR website for
             * further details.
             */
            AnonymousProjectExecutionOptions options =
                    new AnonymousProjectExecutionOptions();

            /* 
             * Preload from the DeployR repository the following
             * binary R object input file:
             * /testuser/example-data-io/hipStar.rData
             *
             * As this is an anonymous operation "hipStar.rData"
             * must have it's repository-managed access controls
             * set to "public".
             */
            ProjectPreloadOptions preloadWorkspace =
                                new ProjectPreloadOptions();
            preloadWorkspace.filename = "hipStar.rData";
            preloadWorkspace.directory = "example-data-io";
            preloadWorkspace.author = "testuser";
            options.preloadWorkspace = preloadWorkspace;

            log.info("[   DATA INPUT   ] Repository binary file input " +
                "set on execution, [ ProjectExecutionOptions.preloadWorkspace ].");

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
             * Execute a public analytics Web service as an anonymous
             * user based on a repository-managed R script:
             * /testuser/example-data-io/dataIO.R
             */
            RScriptExecution exec =
                    rClient.executeScript("dataIO.R",
                            "example-data-io", "testuser", null, options);

            log.info("[   EXECUTION    ] Discrete R script " +
                    "execution completed [ RScriptExecution ].");

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
                    /*
                     * Optionally convert RDataFrame to RTableData to
                     * simplify working with data values within the object.
                     */
                    RDataTable table =
                        RDataFactory.createDataTable(rData);

                } else
                if(rData instanceof RNumericVector) {
                    log.info("[  DATA OUTPUT   ] Retrieved DeployR-encoded R " +
                        "object output " + rData.getName() + " [ RNumericVector ].");
                    List<Double> hipDimVal =
                        ((RNumericVector) rData).getValue();
                    log.info("[  DATA OUTPUT   ] Retrieved DeployR-encoded R " +
                        "object " + rData.getName() +
                        " value=" + hipDimVal);
                    /*
                     * Optionally convert RDataFrame to RTableData to
                     * simplify working with data values within the object.
                     */
                    RDataTable table =
                        RDataFactory.createDataTable(rData);

                } else
                if(rData instanceof RStringVector) {
                    log.info("[  DATA OUTPUT   ] Retrieved DeployR-encoded R " +
                        "object output " + rData.getName() + " [ RStringVector ].");
                    List<String> hipNamesVal =
                        ((RStringVector) rData).getValue();
                    log.info("[  DATA OUTPUT   ] Retrieved DeployR-encoded R " +
                        "object " + rData.getName() +
                        " value=" + hipNamesVal);
                    /*
                     * Optionally convert RDataFrame to RTableData to
                     * simplify working with data values within the object.
                     */
                    RDataTable table =
                        RDataFactory.createDataTable(rData);
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
