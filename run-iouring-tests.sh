#!/bin/bash

# IoUring Migration Tests Runner (Maven version)
# This script runs all the IoUring migration tests and provides detailed output

set -e

echo "=== IoUring Migration Test Runner (Maven) ==="
echo "Running comprehensive tests for IoUring migration functionality"
echo

# Check for Maven
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed or not in PATH"
    exit 1
fi

echo "1. Running Configuration Tests..."
echo "   Testing IoUringConfigurationFactory..."
mvn test -Dtest="dk.ku.di.dms.vms.modb.common.config.IoUringConfigurationFactoryTest" -pl modb-common -q || true

echo "   Testing IoUringBootstrap..."
mvn test -Dtest="dk.ku.di.dms.vms.modb.common.config.IoUringBootstrapTest" -pl modb-common -q || true

echo

echo "2. Running Storage Tests..."
echo "   Testing IoUringStorageFactory..."
mvn test -Dtest="dk.ku.di.dms.vms.modb.storage.IoUringStorageFactoryTest" -pl modb -q || true

echo

echo "3. Running Logging Tests..."
echo "   Testing LoggingHandlerBuilder..."
mvn test -Dtest="dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilderTest" -pl modb-common -q || true

echo

echo "4. Running Storage Utils Integration Tests..."
mvn test -Dtest="dk.ku.di.dms.vms.modb.utils.StorageUtilsIntegrationTest" -pl modb -q || true

echo

echo "5. Running End-to-End Integration Tests..."
mvn test -Dtest="dk.ku.di.dms.vms.modb.integration.IoUringMigrationIntegrationTest" -pl modb -q || true

echo

echo "6. Running Complete Test Suite..."
mvn test -Dtest="dk.ku.di.dms.vms.modb.IoUringMigrationTestSuite" -pl modb -q || true

echo

echo "=== Test Summary ==="
echo "All IoUring migration tests have been executed."
echo "Check the output above for any failures or issues."
echo
echo "📁 Test files created:"
echo "   - IoUringConfigurationFactoryTest.java (modb-common)"
echo "   - IoUringBootstrapTest.java (modb-common)"
echo "   - IoUringStorageFactoryTest.java (modb)"
echo "   - LoggingHandlerBuilderTest.java (modb-common)"
echo "   - StorageUtilsIntegrationTest.java (modb)"
echo "   - IoUringMigrationIntegrationTest.java (modb)"
echo "   - IoUringMigrationTestSuite.java (modb)"
echo
echo "🧪 Test Coverage:"
echo "   ✓ Configuration factory and bootstrap functionality"
echo "   ✓ Storage factory and fallback mechanisms"
echo "   ✓ Logging handler builder integration"
echo "   ✓ Storage utilities with IoUring support"
echo "   ✓ End-to-end transaction manager integration"
echo "   ✓ System properties and configuration transitions"
echo
echo "💡 Key Test Features:"
echo "   ✓ Automatic fallback testing when IoUring is unavailable"
echo "   ✓ System property isolation and restoration"
echo "   ✓ Temporary directory usage for clean test environment"
echo "   ✓ Configuration state verification"
echo "   ✓ Integration with existing TransactionManager"
echo
echo "=== Test Execution Complete ===" 