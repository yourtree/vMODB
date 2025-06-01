package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.modb.integration.IoUringMigrationIntegrationTest;
import dk.ku.di.dms.vms.modb.storage.IoUringStorageFactoryTest;
import dk.ku.di.dms.vms.modb.utils.StorageUtilsIntegrationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for IoUring migration functionality in modb.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Storage layer tests
    IoUringStorageFactoryTest.class,
    
    // Integration tests
    StorageUtilsIntegrationTest.class,
    IoUringMigrationIntegrationTest.class
})
public class IoUringMigrationTestSuite {
    // This class serves as a holder for the test suite annotations
} 