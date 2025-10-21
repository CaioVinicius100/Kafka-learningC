CXX ?= g++
CXXFLAGS ?= -std=c++14 -O2 -Wall -Wextra -Iinclude
LDFLAGS ?= -lcppkafka -lrdkafka -lpthread

SRC := $(wildcard src/*.cpp)
BIN := fmt_broker_demo

all: $(BIN)

$(BIN): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(BIN)

.PHONY: all clean
