# mongo_csvimport
Fast import large CSV documents into mongodb

# usage example
General usage: mongo_csvimport -d < count of document for "insert_many" > -u < mongodb connection URI > -b < database name > -c < collection name > -i < CSV file name > -n < comma separated columns names > -t < comma separated columns types >

For advanced usage see: mongo_csvimport --help
  
# target platform
Any platform. 
Project file for QT Crater provided.

# dependencies
- pthread
- boost_system
- boost_program_options
- mongocxx
- bsoncxx
