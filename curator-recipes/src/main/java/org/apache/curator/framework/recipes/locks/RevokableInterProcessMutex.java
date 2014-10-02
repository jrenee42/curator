/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.locks;

import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

/**
 * This class is an InterProcessMutex (a lock) that can be revoked, from anywhere.
 * <P>
 * the client that revokes this lock *does not* have to be the same client that created this lock.
 * <P>
 * The user still has to make it able to be revoked, though.
 * 
 * To revoke this from anywhere, the other client may not have the full lockpath; ie:<br>
 * <code> /a/b/c/foo_atatatat_lock_0000_abatat </code>
 * <P>
 * only the parent path is needed: <br>
 * <code> /a/b/c </code>
 * <p>
 * 
 * @author usingji
 * @date Oct 2, 2014
 */

public class RevokableInterProcessMutex extends InterProcessMutex {

    private static final Logger logger = Logger.getLogger(RevokableInterProcessMutex.class);

    private static final String REVOKE_ANYWHERE = "__REVOKE_FROM_ANYWHERE";
    private static byte[] REVOKE_BYTES;
    private static final String PATH_SEPARATOR = "/";
    static {
        REVOKE_BYTES = REVOKE_ANYWHERE.getBytes();
    }

    /**
     * @param client
     * @param path
     */
    public RevokableInterProcessMutex(CuratorFramework client, String path) {
        super(client, path);
    }

    public void makeRevocableFromAnywhere(CuratorFramework client) {
        try {
            client.setData().forPath(basePath, REVOKE_BYTES);
        } catch (Exception e) {
            logger.error("exception thrown while making this revocable: " + e);
        }
    }

    public static void revoke(CuratorFramework client, String path) {
        try {
            byte[] data = client.getData().forPath(path);

            String actualData = new String(data);
            if (REVOKE_ANYWHERE.equals(actualData)) {
                // do the revocation

                List<String> children = client.getChildren().forPath(path);

                if (children.size() != 1) {
                    return;
                }

                String fullPath = path + PATH_SEPARATOR + children.get(0);
                client.delete().guaranteed().forPath(fullPath);

                // unset the revoke_anywhere data:
                client.setData().forPath(path);
            }

        } catch (Exception e) {
            logger.error("exception thrown while revoking this path: " + path + ": " + e);
        }

    }
}
