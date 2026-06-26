# DataWeave C wrapper — hand-rolled Makefile.
#
# Targets (matches what the README documents):
#   make            — alias for `make all` (lib + test + examples)
#   make lib        — build static + shared libraries under build/lib/
#   make test       — build and run the test binary
#   make examples   — build the example programs
#   make clean      — remove build/
#   make install    — install lib + headers to $(PREFIX) (default /usr/local)
#   make uninstall  — remove the installed files
#   make help       — list these targets
#
# The wrapper depends on the dataweave native shared library (`dwlib`).
# Build it first from the repo root:
#   ./gradlew :native-lib:nativeCompile
# It lands at native-lib/build/native/nativeCompile/dwlib.{dylib,so}.

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
SHLIB_EXT := dylib
RPATH_FLAG := -Wl,-rpath,$(abspath ../build/native/nativeCompile)
else
SHLIB_EXT := so
RPATH_FLAG := -Wl,-rpath,$(abspath ../build/native/nativeCompile)
endif

# Paths
NATIVE_DIR  := ../build/native/nativeCompile
NATIVE_LIB  := $(NATIVE_DIR)/dwlib.$(SHLIB_EXT)

INCLUDE_DIR := include
SRC_DIR     := src
TEST_DIR    := tests
EX_DIR      := examples

BUILD_DIR   := build
OBJ_DIR     := $(BUILD_DIR)/obj
LIB_DIR     := $(BUILD_DIR)/lib
TEST_BIN_DIR := $(BUILD_DIR)/test
EX_BIN_DIR  := $(BUILD_DIR)/examples

# Tools / flags
CC      ?= cc
AR      ?= ar
CSTD    ?= -std=c11
CFLAGS  ?= -O2 -g -Wall -Wextra -Wpedantic -fPIC $(CSTD) -I$(INCLUDE_DIR)
LDFLAGS ?=
LDLIBS  ?= -lpthread

# The wrapper static lib
STATIC_LIB := $(LIB_DIR)/libdataweave.a
SHARED_LIB := $(LIB_DIR)/libdataweave.$(SHLIB_EXT)

# Wrapper sources
LIB_SOURCES := $(SRC_DIR)/dataweave.c
LIB_OBJECTS := $(OBJ_DIR)/dataweave.o

# Test
TEST_BIN := $(TEST_BIN_DIR)/test_dataweave

# Examples
EX_SIMPLE    := $(EX_BIN_DIR)/simple
EX_STREAMING := $(EX_BIN_DIR)/streaming

# Install prefix
PREFIX     ?= /usr/local
INC_INSTALL := $(PREFIX)/include
LIB_INSTALL := $(PREFIX)/lib

.PHONY: all lib test examples clean install uninstall help

all: lib test examples

lib: $(STATIC_LIB) $(SHARED_LIB)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(OBJ_DIR):
	@mkdir -p $@

$(LIB_DIR):
	@mkdir -p $@

$(TEST_BIN_DIR):
	@mkdir -p $@

$(EX_BIN_DIR):
	@mkdir -p $@

$(STATIC_LIB): $(LIB_OBJECTS) | $(LIB_DIR)
	$(AR) rcs $@ $^

$(SHARED_LIB): $(LIB_OBJECTS) | $(LIB_DIR)
	$(CC) -shared -o $@ $^ $(LDLIBS)

# Test binary links the wrapper (static, simpler) and the dataweave native dylib.
$(TEST_BIN): $(STATIC_LIB) $(TEST_DIR)/test_dataweave.c | $(TEST_BIN_DIR)
	$(CC) $(CFLAGS) $(TEST_DIR)/test_dataweave.c $(STATIC_LIB) \
	    $(NATIVE_LIB) $(RPATH_FLAG) $(LDLIBS) -o $@

test: $(TEST_BIN)
	@echo "==> Running test suite from $(CURDIR)"
	@DATAWEAVE_NATIVE_LIB="$(abspath $(NATIVE_LIB))" \
	    DYLD_LIBRARY_PATH="$(abspath $(NATIVE_DIR))" \
	    LD_LIBRARY_PATH="$(abspath $(NATIVE_DIR))" \
	    $(TEST_BIN)

examples: $(EX_SIMPLE) $(EX_STREAMING)

$(EX_SIMPLE): $(EX_DIR)/simple.c $(STATIC_LIB) | $(EX_BIN_DIR)
	$(CC) $(CFLAGS) $< $(STATIC_LIB) $(NATIVE_LIB) $(RPATH_FLAG) $(LDLIBS) -o $@

$(EX_STREAMING): $(EX_DIR)/streaming.c $(STATIC_LIB) | $(EX_BIN_DIR)
	$(CC) $(CFLAGS) $< $(STATIC_LIB) $(NATIVE_LIB) $(RPATH_FLAG) $(LDLIBS) -o $@

clean:
	rm -rf $(BUILD_DIR)

install: lib
	@mkdir -p $(INC_INSTALL) $(LIB_INSTALL)
	cp $(INCLUDE_DIR)/dataweave.h $(INC_INSTALL)/
	cp $(STATIC_LIB) $(LIB_INSTALL)/
	cp $(SHARED_LIB) $(LIB_INSTALL)/

uninstall:
	rm -f $(INC_INSTALL)/dataweave.h
	rm -f $(LIB_INSTALL)/libdataweave.a
	rm -f $(LIB_INSTALL)/libdataweave.$(SHLIB_EXT)

help:
	@echo "DataWeave C wrapper — Makefile targets:"
	@echo "  make            build everything (lib + test + examples)"
	@echo "  make lib        build static + shared libraries"
	@echo "  make test       build and run the test suite"
	@echo "  make examples   build example programs"
	@echo "  make clean      remove build artifacts"
	@echo "  make install    install to PREFIX (default /usr/local)"
	@echo "  make uninstall  remove installed files"
	@echo "  make help       this help"
	@echo ""
	@echo "Prerequisite: build the native library first:"
	@echo "  cd ../.. && ./gradlew :native-lib:nativeCompile"
