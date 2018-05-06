TEMPLATE = app
CONFIG += console c++11
CONFIG -= app_bundle
CONFIG -= qt

SOURCES += \
        main.cpp

INCLUDEPATH += /usr/local/include/mongocxx/v_noabi
INCLUDEPATH += /usr/local/include/bsoncxx/v_noabi

LIBS += -lboost_system -lboost_program_options -lpthread
LIBS += -lmongocxx -lbsoncxx -L/usr/local/lib

HEADERS += \
    csv.h
