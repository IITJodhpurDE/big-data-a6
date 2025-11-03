#!/bin/bash

# Gradle wrapper script for Unix-based systems
# This script will download and use the correct Gradle version

set -e

# Define Gradle version
GRADLE_VERSION=8.4

# Define paths
WRAPPER_DIR="$(dirname "$0")"
GRADLE_USER_HOME="${GRADLE_USER_HOME:-$HOME/.gradle}"
GRADLE_DIST_DIR="$GRADLE_USER_HOME/wrapper/dists"

echo "Setting up Gradle wrapper..."

# Download Gradle wrapper JAR if it doesn't exist
if [ ! -f "$WRAPPER_DIR/gradle/wrapper/gradle-wrapper.jar" ]; then
    mkdir -p "$WRAPPER_DIR/gradle/wrapper"
    
    echo "Downloading Gradle wrapper JAR..."
    curl -L -o "$WRAPPER_DIR/gradle/wrapper/gradle-wrapper.jar" \
        "https://raw.githubusercontent.com/gradle/gradle/master/gradle/wrapper/gradle-wrapper.jar"
fi

# Create gradle-wrapper.properties if it doesn't exist
if [ ! -f "$WRAPPER_DIR/gradle/wrapper/gradle-wrapper.properties" ]; then
    cat > "$WRAPPER_DIR/gradle/wrapper/gradle-wrapper.properties" << EOF
distributionBase=GRADLE_USER_HOME
distributionPath=wrapper/dists
distributionUrl=https\\://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip
zipStoreBase=GRADLE_USER_HOME
zipStorePath=wrapper/dists
EOF
fi

# Make gradlew executable
chmod +x "$WRAPPER_DIR/gradlew"

echo "Gradle wrapper setup complete!"
echo "You can now run: ./gradlew build"
