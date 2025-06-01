CC=gcc
# Detect JAVA_HOME automatically or use provided value
JAVA_HOME ?= /home/calvin/.sdkman/candidates/java/current
CFLAGS=-I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -fPIC -Wall -Wextra -shared
LDFLAGS=-luring

# Find all .c files in the current directory
SOURCES=$(wildcard *.c)
OBJECTS=$(SOURCES:.c=.o)
TARGET_LIB_DIR=lib
TARGET_LIB=$(TARGET_LIB_DIR)/liburing_provider.so

# Default target
all: $(TARGET_LIB)

$(TARGET_LIB): $(OBJECTS)
	@mkdir -p $(TARGET_LIB_DIR)
	$(CC) $(CFLAGS) -o $@ $(OBJECTS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET_LIB)

.PHONY: all clean 