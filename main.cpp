#include <iostream>

#include "kuzu.hpp"

using namespace kuzu::main;
using namespace std;

int main() {
    // Create an empty on-disk database and connect to it
    SystemConfig systemConfig;
    auto database = make_unique<Database>("example.kuzu", systemConfig);

    // Connect to the database.
    auto connection = make_unique<Connection>(database.get());

    std::cout << "Starting\n";
    // Create the schema.
    connection->query("CREATE NODE TABLE User(name STRING PRIMARY KEY, age INT64)");
    connection->query("CREATE NODE TABLE City(name STRING PRIMARY KEY, population INT64)");
    connection->query("CREATE REL TABLE Follows(FROM User TO User, since INT64)");
    connection->query("CREATE REL TABLE LivesIn(FROM User TO City)");

    std::cout << "Starting\n";
    // Load data.
    connection->query("COPY User FROM \"data/user.csv\"");
    connection->query("COPY City FROM \"data/city.csv\"");
    connection->query("COPY Follows FROM \"data/follows.csv\"");
    connection->query("COPY LivesIn FROM \"data/lives-in.csv\"");

    std::cout << "Starting\n";
    // Execute a simple query.
    auto result =
        connection->query("MATCH (a:User)-[f:Follows]->(b:User) RETURN a.name, f.since, b.name;");

    // Output query result.
    while (result->hasNext()) {
        auto row = result->getNext();
        std::cout << row->getValue(0)->getValue<string>() << " "
                  << row->getValue(1)->getValue<int64_t>() << " "
                  << row->getValue(2)->getValue<string>() << std::endl;
    }
    return 0;
}
