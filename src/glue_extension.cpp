#define DUCKDB_EXTENSION_MAIN

#include "glue_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/glue/GlueClient.h>
#include <aws/glue/model/GetTablesRequest.h>
#include <aws/glue/model/Table.h>
#include <aws/glue/model/StorageDescriptor.h>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include <nlohmann/json.hpp>

#include <iostream>
#include <sstream>
#include <fstream>
#include <filesystem>

using namespace Aws;
using namespace Aws::Auth;

namespace duckdb {

/*
        class AWS_CORE_API AWSCredentialsProvider
        {
        public:
            AWSCredentialsProvider() : m_lastLoadedMs(0)
            {
            }
 
            virtual ~AWSCredentialsProvider() = default;
 
            virtual AWSCredentials GetAWSCredentials() = 0;
 
        protected:
            virtual bool IsTimeToRefresh(long reloadFrequency);
            virtual void Reload();
            mutable Aws::Utils::Threading::ReaderWriterLock m_reloadLock;
        private:
            long long m_lastLoadedMs;
        };

		{
*/

class VaultCredentialsProvider : public AWSCredentialsProvider {
public:
	VaultCredentialsProvider(std::string domain, std::string account, std::string role) 
		: m_vaultSecretFile("/var/run/secrets/" + domain + "/arn_aws_iam__" + account + "_role_" + role + ".json") {
	}

public:
	AWSCredentials GetAWSCredentials() override {
		if (!std::filesystem::exists(m_vaultSecretFile)) {
			return AWSCredentials();
		}

		std::ifstream secretFile(m_vaultSecretFile);
		nlohmann::json secret;
		secretFile >> secret;

		return AWSCredentials(
			secret["access_key"],
			secret["secret_key"],
			secret["security_token"]
		);
	}
private:
	std::filesystem::path m_vaultSecretFile;
};

std::string GetS3Path(std::string databaseName, std::string tableName) {
	Aws::SDKOptions options;
	Aws::InitAPI(options);
	
	std::string location;

	Aws::Client::ClientConfiguration clientConfig;
	clientConfig.region = "eu-west-2";

	const auto credentialsProvider = std::make_unique<VaultCredentialsProvider>("aws", "123456789012", "glue_role");
	Aws::Glue::GlueClient glueClient(*credentialsProvider, clientConfig);
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

inline void GlueScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
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
    auto glue_reader_scalar_function = ScalarFunction("get_s3_location", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, GlueScalarFun);
    ExtensionUtil::RegisterFunction(instance, glue_reader_scalar_function);
}

void GlueExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string GlueExtension::Name() {
	return "glue";
}

std::string GlueExtension::Version() const {
#ifdef EXT_VERSION_GLUE
	return EXT_VERSION_GLUE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void glue_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::GlueExtension>();
}

DUCKDB_EXTENSION_API const char *glue_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
