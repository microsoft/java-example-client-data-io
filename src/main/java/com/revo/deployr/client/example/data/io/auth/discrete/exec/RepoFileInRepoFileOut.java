/*
 * RepoFileInRepoFileOut.java
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
package com.revo.deployr.client.example.data.io.auth.discrete.exec;

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

public class RepoFileInRepoFileOut {

    private static Logger log = Logger.getLogger(RepoFileInRepoFileOut.class);

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
             * data input file:
             * /testuser/example-data-io/hipStar.dat
             */
            ProjectPreloadOptions preloadDirectory =
                                new ProjectPreloadOptions();
            preloadDirectory.filename = "hipStar.dat";
            preloadDirectory.directory = "example-data-io";
            preloadDirectory.author = "testuser";
            options.preloadDirectory = preloadDirectory;

            log.info("[   DATA INPUT   ] Repository data file input " +
                "set on execution, [ ProjectExecutionOptions.preloadDirectory ].");

            /* 
             * Request storage of entire workspace as a
             * binary rData file to the DeployR-repository
             * following the execution.
             *
             * Alternatively, you could use storageOptions.objects
             * to store individual objects from the workspace.
             */
            ProjectStorageOptions storageOptions =
                                new ProjectStorageOptions();
            // Use random file name for this example.
            storageOptions.workspace =
                Long.toHexString(Double.doubleToLongBits(Math.random()));
            storageOptions.directory = "example-data-io";
            options.storageOptions = storageOptions;

            log.info("[  EXEC OPTION   ] Repository storage request " +
                "set on execution [ ProjectExecutionOptions.storageOptions ].");

            /*
             * Execute an analytics Web service as an authenticated
             * user based on a repository-managed R script:
             * /testuser/example-data-io/dataIO.R
             */
            RScriptExecution exec =
                    rClient.executeScript("dataIO.R",
                            "example-data-io", "testuser", null, options);

            log.info("[   EXECUTION    ] Discrete R script " +
                    "execution completed [ RScriptExecution ].");

            /*
             * Retrieve repository-managed file(s) that were 
             * generated by the execution per ProjectStorageOptions.
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
            List<RRepositoryFile> repoFiles = exec.about().repositoryFiles;

            for(RRepositoryFile repoFile : repoFiles) {
                log.info("[  DATA OUTPUT   ] Retrieved repository " +
                    "file output " + repoFile.about().filename +
                    " [ RRepositoryFile ].");
                InputStream fis = null;
                try { fis = repoFile.download(); } catch(Exception ex) {
                    log.warn("Repository-managed file download " + ex);
                } finally {
                    IOUtils.closeQuietly(fis);
                    try { // Clean-up after example.
                        repoFile.delete();
                    } catch(Exception dex) {
                        log.warn("Repository-managed file delete " + dex);
                    }
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
