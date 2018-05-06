#include <iostream>
#include <map>
#include <vector>

#include <boost/program_options.hpp>
#include <boost/tokenizer.hpp>
#include <boost/foreach.hpp>

#include "csv.h"

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

///////////////////////////////////////////////////////////////////////////////

using namespace std;
using namespace boost::program_options;
using bsoncxx::builder::basic::kvp;

typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

int main(int ac, char ** av) {

    try {

        string
                connection_uri,
                database_name,
                collection_name,
                input_file,
                header_str,
                types_str;

        char csv_separator;

        size_t document_step;

        int64_t lines_for_skip;

        options_description desc("Allowed options");
        desc.add_options()
                ("help,h",                                                    "print usage message"             )
                ("uri,u",        value(&connection_uri )->required(),         "database connection URI"         )
                ("database,b",   value(&database_name  )->required(),         "database name"                   )
                ("collection,c", value(&collection_name)->required(),         "database collection name"        )
                ("input,i",      value(&input_file     )->required(),         "input csv file"                  )
                ("separator,s",  value(&csv_separator  )->default_value(';'), "CSV column separator (';', ',' or TAB)")
                ("lines,l",      value(&lines_for_skip )->default_value(0),   "number of lines for skip"        )
                ("names,n",      value(&header_str     )->required(),         "comma separated column names"    )
                ("types,t",      value(&types_str      )->required(),         "comma separated types of columns (i - int, S - string, F - float, I - int64)")
                ("doc,d",        value(&document_step  )->default_value(100), "document insertion step")
                ;

        variables_map vm;
        store(parse_command_line(ac, av, desc), vm);

        if (vm.count("help")) {
            cout << desc << endl;
        } else {
            notify(vm);

            vector<string> columns;
            vector<char>   types;
            boost::char_separator<char> sep(",");

            tokenizer tokens(header_str, sep);
            BOOST_FOREACH(std::string const& token, tokens) {
                columns.push_back(token);
            }

            tokens = tokenizer(types_str, sep);
            BOOST_FOREACH(std::string const& token, tokens) {
                types.push_back(token[0]);
            }

            if (columns.size() != types.size()) {
                throw runtime_error("count of columns and column types mismatch!");
            }

            io::LineReader lr(input_file);

            for(int64_t i(0); (i < lines_for_skip) && lr.next_line(); ++i) {}

            vector<char*> cells;
            cells.resize(columns.size());

            vector<int> col_order;
            for(size_t i(0); i < columns.size(); ++i) {
                col_order.push_back((int)i);
            }

            mongocxx::instance instance{};

            mongocxx::uri uri(connection_uri);
            mongocxx::client client(uri);
            mongocxx::database db = client[database_name];
            mongocxx::collection collection = db[collection_name];

            std::vector<bsoncxx::document::value> documents;

            while (char* line = lr.next_line()) {

                char** pcells = &cells[0];

                switch (csv_separator) {
                case ',':
                    io::detail::parse_line<
                            io::trim_chars<' ', '\t'>,
                            io::no_quote_escape<','>
                            >(line, pcells, col_order);
                    break;
                case ';':
                    io::detail::parse_line<
                            io::trim_chars<' ', '\t'>,
                            io::no_quote_escape<';'>
                            >(line, pcells, col_order);
                    break;
                case '\t':
                    io::detail::parse_line<
                            io::trim_chars<' ', '\t'>,
                            io::no_quote_escape<'\t'>
                            >(line, pcells, col_order);
                    break;
                default:
                    throw runtime_error("CSV separator does not support!");
                    break;
                }

                bsoncxx::builder::basic::document builder{};

                int row_index(0);
                std::for_each(cells.begin(), cells.end(), [&row_index, &types, &builder, &columns](char* row_value) {
                    switch (types[row_index]) {
                    case 'i': // int
                    {
                        int value;
                        io::detail::parse<io::throw_on_overflow>(row_value, value);
                        builder.append(kvp(columns[row_index], value));
                    }
                        break;
                    case 'I': // int64
                    {
                        int64_t value;
                        io::detail::parse<io::throw_on_overflow>(row_value, value);
                        builder.append(kvp(columns[row_index], value));
                    }
                        break;
                    case 'F': // float
                    {
                        float value;
                        io::detail::parse<io::throw_on_overflow>(row_value, value);
                        builder.append(kvp(columns[row_index], value));
                    }
                        break;
                    case 'S': // string
                    {
                        string value;
                        io::detail::parse<io::throw_on_overflow>(row_value, value);
                        builder.append(kvp(columns[row_index], value));
                    }
                        break;
                    default:
                        throw runtime_error("ROW type does not support!");
                        break;
                    }
                    ++row_index;
                });

                documents.push_back(builder.extract());

                if (documents.size() >= document_step) {
                    collection.insert_many(documents);
                    cout << "Insert " << collection.count( bsoncxx::document::view_or_value{} ) << " done.\r" << flush;
                    documents.clear();
                }
            }

            if (documents.size()) {
                collection.insert_many(documents);
                cout << "Insert " << collection.count( bsoncxx::document::view_or_value{} ) << " done." << endl << flush;
                documents.clear();
            }

        }

    } catch (exception &e) {
        cout << e.what() << endl << endl;
    }

    return 0;
}
