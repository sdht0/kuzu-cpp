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

    runQuery("CREATE NODE TABLE N(id SERIAL PRIMARY KEY, name STRING, age INT64, embedding FLOAT[])", connection);

    std::unordered_map<std::string, unique_ptr<kuzu::common::Value>> args;

    args.emplace("name", make_unique<kuzu::common::Value>("Bob"));

    uint64_t age = 30;
    args.emplace("age", make_unique<kuzu::common::Value>(age));

    auto type = kuzu::common::LogicalType::LIST(kuzu::common::LogicalType::FLOAT());
    vector<unique_ptr<kuzu::common::Value>> data;
    data.push_back(make_unique<kuzu::common::Value>((float)40.0));
    data.push_back(make_unique<kuzu::common::Value>((float)50.0));
    data.push_back(make_unique<kuzu::common::Value>((float)60.0));
    args.emplace("embedding", make_unique<kuzu::common::Value>(std::move(type), std::move(data)));

    runQuery("CREATE (:N {name: 'Alice', embedding: [10.0, 20.0, 30.0], age: 25});", connection);
    runPreparedQuery("CREATE (:N {name: $name, age: $age, embedding: $embedding});", std::move(args), connection);

    auto result = runQuery("MATCH (n) RETURN n.name, n.age, n.embedding;", connection);

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
        KU_ASSERT_UNCONDITIONAL(value_name->getDataType().getLogicalTypeID() == kuzu::common::LogicalTypeID::STRING);
        cout << value_name->getValue<string>() << " | ";

        auto value_int64 = row->getValue(1);
        KU_ASSERT_UNCONDITIONAL(value_int64->getDataType().getLogicalTypeID() == kuzu::common::LogicalTypeID::INT64);
        cout << value_int64->getValue<int64_t>() << " | ";

        auto value_embedding = row->getValue(2);
        KU_ASSERT_UNCONDITIONAL(value_embedding->getDataType().getLogicalTypeID() == kuzu::common::LogicalTypeID::LIST);
        auto length = value_embedding->getChildrenSize();
        vector<float> embedding;
        for (auto i = 0u; i < length; ++i) {
            auto element = kuzu::common::NestedVal::getChildVal(value_embedding, i);
            KU_ASSERT_UNCONDITIONAL(element->getDataType().getLogicalTypeID() == kuzu::common::LogicalTypeID::FLOAT);
            embedding.push_back(element->getValue<float>());
        }
        auto join = [](const std::vector<float>& vec, const std::string& sep) {
            std::ostringstream oss;
            for (size_t i = 0; i < vec.size(); i++) {
                if (i > 0) oss << sep;
                oss << vec[i];
            }
            return oss.str();
        };
        cout << value_embedding->toString();
        cout << "[" << join(embedding, ", ") << "]\n";
    }
    return 0;
}
