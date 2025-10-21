CPPFLAGS = $(SW_DEBUG) -DPKGVER="$(PACKAGE)-$(VERSION)"

lib_LTLIBRARIES = libFmtBroker.la

libFmtBroker_la_SOURCES = FmtBroker.cpp

AM_CPPFLAGS = \
    -DWITH_OPENSSL -DWITH_GZIP -DWITH_NONAMESPACES -DPROTOTYPE -Dunix -DLINUX -DLinux -DOBJ64 \
    -D_XOPEN_SOURCE=600 -D_POSIX_C_SOURCE=199506L -D_DEFAULT_SOURCE -g -DBOOL_H -DOC_USE_CPP_TYPENAME \
    -I$(BOOST_ROOT)/include -I$(BOOST_ROOT)/include/boost/asio \
    $(CPPKAFKA_CFLAGS) -I$(KAFKA_HOME)/include -I$(KAFKA_HOME)/include/cppkafka \
   -I$(KAFKA_HOME)/lib4

CXXFLAGS = -std=c++14

libFmtBroker_la_LIBADD =  -losyslg -lotrace -lotaks -loottrace -lostring \
    -loshc -loshcsdk -lMailboxBaseBR -lUtility \
    -loc_na_factory -locatsig -lorcmapping \
    -loll -loserverCB -lombox -lofmt -loseq -loshc \
    -lUtils50 -lcppkafka -lrdkafka++ -lrdkafka

libFmtBroker_la_LDFLAGS = -version-info 1:0:0
