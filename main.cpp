#include <cassert>
#include <iostream>
#include <ranges>

#include "kuzu.hpp"

using namespace kuzu::main;
using namespace std;

unique_ptr<QueryResult> runQuery(const string_view &query, unique_ptr<Connection> &connection) {
    auto results = connection->query(query);
    if (!results->isSuccess()) {
        throw std::runtime_error(results->getErrorMessage());
    }
    return results;
}

unique_ptr<QueryResult>
runPreparedQuery(const string_view &query,
                 std::unordered_map<std::string, std::unique_ptr<kuzu::common::Value>> inputParams,
                 const unique_ptr<Connection> &connection) {
    auto prepared_stmt = connection->prepare(query);
    if (!prepared_stmt->isSuccess()) {
        throw std::runtime_error(prepared_stmt->getErrorMessage());
    }

    auto results = connection->executeWithParams(prepared_stmt.get(), std::move(inputParams));
    if (!results->isSuccess()) {
        throw std::runtime_error(results->getErrorMessage());
    }

    return results;
}

int main() {
    // Remove example.kuzu
    remove("example.kuzu");
    remove("example.kuzu.wal");

    SystemConfig systemConfig;
    auto database = make_unique<Database>("example.kuzu", systemConfig);
    auto connection = make_unique<Connection>(database.get());

    runQuery("CREATE NODE TABLE N(id SERIAL PRIMARY KEY, name STRING, embedding FLOAT[])", connection);

    std::unordered_map<std::string, unique_ptr<kuzu::common::Value>> args;

    args.emplace("name", make_unique<kuzu::common::Value>("hello world"));

    auto type = kuzu::common::LogicalType::LIST(kuzu::common::LogicalType::FLOAT());
    vector<unique_ptr<kuzu::common::Value>> data;
    data.emplace_back(make_unique<kuzu::common::Value>(1.0));
    data.emplace_back(make_unique<kuzu::common::Value>(2.0));
    data.emplace_back(make_unique<kuzu::common::Value>(3.0));
    args.emplace("embedding", make_unique<kuzu::common::Value>(std::move(type), std::move(data)));

    runPreparedQuery("CREATE (:N {name: $name, embedding: $embedding});", std::move(args), connection);

    auto result = runQuery("MATCH (n) RETURN n.name, n.embedding;", connection);

    auto columns = result->getColumnNames();
    for (auto i = 0u; i < columns.size(); ++i) {
        if (i != 0) {
            cout << " | ";
        }
        cout << columns[i];
    }
    cout << "\n";
    while (result->hasNext()) {
        auto row = result->getNext();

        auto value_name = row->getValue(0);
        KU_ASSERT(value_name->getDataType().getLogicalTypeID() == kuzu::common::LogicalTypeID::STRING);
        string name = value_name->getValue<string>();
        cout << name << " | ";

        auto value_embedding = row->getValue(1);
        KU_ASSERT(value_name->getDataType().getLogicalTypeID() == kuzu::common::LogicalTypeID::LIST);
        cout << value_embedding->toString();
    }
    return 0;
}
