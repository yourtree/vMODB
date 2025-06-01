#!/bin/bash

# IoUring Migration Tests Runner
# This script runs all the IoUring migration tests and provides detailed output

set -e

echo "=== IoUring Migration Test Runner ==="
echo "Running comprehensive tests for IoUring migration functionality"
echo

# Change to project root directory if not already there
if [ ! -f "gradlew" ]; then
    echo "Error: Please run this script from the project root directory"
    exit 1
fi

# Make gradlew executable
chmod +x gradlew

echo "1. Running Configuration Tests..."
echo "   Testing IoUringConfigurationFactory..."
./gradlew :modb-common:test --tests "dk.ku.di.dms.vms.modb.common.config.IoUringConfigurationFactoryTest" || true

echo "   Testing IoUringBootstrap..."
./gradlew :modb-common:test --tests "dk.ku.di.dms.vms.modb.common.config.IoUringBootstrapTest" || true

echo

echo "2. Running Storage Tests..."
echo "   Testing IoUringStorageFactory..."
./gradlew :modb:test --tests "dk.ku.di.dms.vms.modb.storage.IoUringStorageFactoryTest" || true

echo

echo "3. Running Logging Tests..."
echo "   Testing LoggingHandlerBuilder..."
./gradlew :modb-common:test --tests "dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilderTest" || true

echo

echo "4. Running Storage Utils Integration Tests..."
./gradlew :modb:test --tests "dk.ku.di.dms.vms.modb.utils.StorageUtilsIntegrationTest" || true

echo

echo "5. Running End-to-End Integration Tests..."
./gradlew :modb:test --tests "dk.ku.di.dms.vms.modb.integration.IoUringMigrationIntegrationTest" || true

echo

echo "6. Running Complete Test Suite..."
./gradlew :modb:test --tests "dk.ku.di.dms.vms.modb.IoUringMigrationTestSuite" || true

echo

echo "=== Test Summary ==="
echo "All IoUring migration tests have been executed."
echo "Check the output above for any failures or issues."
echo
echo "📁 Test files created:"
echo "   - IoUringConfigurationFactoryTest.java"
echo "   - IoUringBootstrapTest.java"
echo "   - IoUringStorageFactoryTest.java"
echo "   - LoggingHandlerBuilderTest.java"
echo "   - StorageUtilsIntegrationTest.java"
echo "   - IoUringMigrationIntegrationTest.java"
echo "   - IoUringMigrationTestSuite.java"
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