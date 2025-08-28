#include <cassert>
#include <iostream>

#include "kuzu.hpp"

using namespace kuzu::main;
using namespace std;

unique_ptr<QueryResult> runQuery(const string_view& query, unique_ptr<Connection>& connection) {
    auto result = connection->query(query);
    if (!result->isSuccess()) {
        throw std::runtime_error(result->getErrorMessage());
    }
    return result;
}

int main() {
    // Remove example.kuzu
    remove("example.kuzu");
    remove("example.kuzu.wal");

    // Create an empty on-disk database and connect to it
    SystemConfig systemConfig;
    auto database = make_unique<Database>("example.kuzu", systemConfig);

    // Connect to the database.
    auto connection = make_unique<Connection>(database.get());

    // Create the schema.
    runQuery("CREATE NODE TABLE User(name STRING PRIMARY KEY, age INT64)", connection);
    runQuery("CREATE NODE TABLE City(name STRING PRIMARY KEY, population INT64)", connection);
    runQuery("CREATE REL TABLE Follows(FROM User TO User, since INT64)", connection);
    runQuery("CREATE REL TABLE LivesIn(FROM User TO City)", connection);

    // Load data.
    runQuery("COPY User FROM \"data/user.csv\"", connection);
    runQuery("COPY City FROM \"data/city.csv\"", connection);
    runQuery("COPY Follows FROM \"data/follows.csv\"", connection);
    runQuery("COPY LivesIn FROM \"data/lives-in.csv\"", connection);

    // Execute a simple query.
    auto result =
        runQuery("MATCH (a:User)-[f:Follows]->(b:User) RETURN a.name, f.since, b.name;", connection);

    // Output query result.
    while (result->hasNext()) {
        auto row = result->getNext();
        std::cout << row->getValue(0)->getValue<string>() << " "
                  << row->getValue(1)->getValue<int64_t>() << " "
                  << row->getValue(2)->getValue<string>() << std::endl;
    }
    return 0;
}
