/**
 * 
 */

package org.apache.curator.framework.recipes.locks;

import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author usingji
 * @date Oct 2, 2014
 */

public class TestRevokableLock extends TestInterProcessMutexBase {

    private static final Logger logger = Logger.getLogger(TestRevokableLock.class);

    private static final String LOCK_PATH = "/locks/our-lock2";

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.curator.framework.recipes.locks.TestInterProcessMutexBase#makeLock(org.apache.curator.framework.
     * CuratorFramework)
     */
    @Override
    protected InterProcessLock makeLock(CuratorFramework client) {
        return new RevokableInterProcessMutex(client, LOCK_PATH);
    }

    @Test
    public void testRevokingFromAnotherClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        String zookeeperConnectionString = "127.0.0.1:2181";

        final CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
        final CuratorFramework client2 = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);

        try {
            client.start();

            RevokableInterProcessMutex ipMutex = new RevokableInterProcessMutex(client, LOCK_PATH);

            boolean gotLock = ipMutex.acquire(3, TimeUnit.SECONDS);

            ipMutex.makeRevocableFromAnywhere(client);

            Assert.assertTrue(gotLock);

            client2.start();

            RevokableInterProcessMutex.revoke(client2, LOCK_PATH);

            RevokableInterProcessMutex ipMutex2 = new RevokableInterProcessMutex(client2, LOCK_PATH);

            gotLock = ipMutex2.acquire(3, TimeUnit.SECONDS);

            Assert.assertTrue(gotLock);

        } catch (Exception e) {
            logger.debug("caught exception : " + e);
            Assert.fail();
        }

    }

}
