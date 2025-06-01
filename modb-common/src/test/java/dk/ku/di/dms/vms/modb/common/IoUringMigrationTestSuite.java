package dk.ku.di.dms.vms.modb.common;

import dk.ku.di.dms.vms.modb.common.config.IoUringBootstrapTest;
import dk.ku.di.dms.vms.modb.common.config.IoUringConfigurationFactoryTest;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilderTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for IoUring migration functionality in modb-common.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Configuration layer tests
    IoUringConfigurationFactoryTest.class,
    IoUringBootstrapTest.class,
    
    // Logging layer tests
    LoggingHandlerBuilderTest.class
})
public class IoUringMigrationTestSuite {
    // This class serves as a holder for the test suite annotations
} 