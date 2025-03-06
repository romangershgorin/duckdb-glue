#define DUCKDB_EXTENSION_MAIN

#include "glue_reader_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/glue/GlueClient.h>
#include <aws/glue/model/GetTablesRequest.h>
#include <aws/glue/model/Table.h>
#include <aws/glue/model/StorageDescriptor.h>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include <iostream>
#include <sstream>
#include <fstream>

using namespace Aws;
using namespace Aws::Auth;

namespace duckdb {
/*
string CheckS3() {
	Aws::SDKOptions options;
	// Optionally change the log level for debugging.
	//   options.loggingOptions.logLevel = Utils::Logging::LogLevel::Debug;
	Aws::InitAPI(options); // Should only be called once.
	std::stringstream ss;
	int result = 0;
	{
		Aws::Client::ClientConfiguration clientConfig;
		clientConfig.region = "eu-west-2";

		// You don't normally have to test that you are authenticated. But the S3 service permits anonymous requests, thus the s3Client will return "success" and 0 buckets even if you are unauthenticated, which can be confusing to a new user.
		auto provider = Aws::MakeShared<DefaultAWSCredentialsProviderChain>("alloc-tag");
		auto creds = provider->GetAWSCredentials();
		if (creds.IsEmpty()) {
			ss << "Failed authentication" << std::endl;
		}

		Aws::S3::S3Client s3Client(clientConfig);
		auto outcome = s3Client.ListBuckets();

		if (!outcome.IsSuccess()) {
			ss << "Failed with error: " << outcome.GetError() << std::endl;
			result = 1;
		} else {
			ss << "Found " << outcome.GetResult().GetBuckets().size()
					  << " buckets\n";
			for (auto &bucket: outcome.GetResult().GetBuckets()) {
				ss << bucket.GetName() << std::endl;
			}
		}
	}

	Aws::ShutdownAPI(options);

	return ss.str();
}
*/

std::string GetS3Path(std::string databaseName, std::string tableName) {
	Aws::SDKOptions options;
	Aws::InitAPI(options);
	
	std::string location;

	std::ofstream out("/Users/rgershgorin/gitlab/duckdb-dev/extension-template/log.txt");

	Aws::Client::ClientConfiguration clientConfig;
	clientConfig.region = "eu-west-2";

	Aws::Glue::GlueClient glueClient(clientConfig);
	Aws::Glue::Model::GetTablesRequest request;
	request.SetDatabaseName(databaseName);

	Aws::String nextToken;
	do {
		Aws::Glue::Model::GetTablesOutcome outcome = glueClient.GetTables(request);
		if (outcome.IsSuccess()) {
			for (const auto& table: outcome.GetResult().GetTableList()) {
				if (table.GetName() == tableName) {
					location = table.GetStorageDescriptor().GetLocation();
				}
			}

			nextToken = outcome.GetResult().GetNextToken();
		}
		else {
			stringstream(location) << "Error getting the tables. " << outcome.GetError().GetMessage() << std::endl;
			break;
		}
	} while (!nextToken.empty());

	Aws::ShutdownAPI(options);

	return location;
}

//select glue_reader('testdatabase', 'newnamestable');
inline void GlueReaderScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &databaseName = args.data[0];
	auto &tableName = args.data[1];
    BinaryExecutor::Execute<string_t, string_t, string_t>(
	    databaseName, tableName, result, args.size(),
	    [&](string_t database, string_t table) {
			return StringVector::AddString(result, GetS3Path(database.GetString(), table.GetString()));
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto glue_reader_scalar_function = ScalarFunction("glue_reader", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, GlueReaderScalarFun);
    ExtensionUtil::RegisterFunction(instance, glue_reader_scalar_function);
}

void GlueReaderExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string GlueReaderExtension::Name() {
	return "glue_reader";
}

std::string GlueReaderExtension::Version() const {
#ifdef EXT_VERSION_GLUE_READER
	return EXT_VERSION_GLUE_READER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void glue_reader_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::GlueReaderExtension>();
}

DUCKDB_EXTENSION_API const char *glue_reader_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
