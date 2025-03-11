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
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/Table.h>
#include <aws/glue/model/StorageDescriptor.h>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include <nlohmann/json.hpp>

#include <iostream>
#include <sstream>
#include <fstream>

using namespace Aws;
using namespace Aws::Auth;

namespace duckdb {

const char* LOG_PATH = "/tmp/ducklog.txt";

class VaultCredentialsProvider : public AWSCredentialsProvider {
public:
	VaultCredentialsProvider(std::string domain, std::string account, std::string role) 
		: m_vaultSecretFile("/var/run/secrets/" + domain + "/arn_aws_iam__" + account + "_role_" + role + ".json")
	{
		m_credentials = RefreshCredentials();
	}

public:
	AWSCredentials GetAWSCredentials() override {
		Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
		if (m_credentials.GetExpiration() < Aws::Utils::DateTime::Now()) {
			guard.UpgradeToWriterLock();
			m_credentials = RefreshCredentials();
		}

		return m_credentials;
	}

private:
	AWSCredentials RefreshCredentials() {
		std::ifstream secretFile(m_vaultSecretFile);
		if (!secretFile.good()) {
			throw std::runtime_error("Secret was not materialised the expected location: " + m_vaultSecretFile);
		}

		nlohmann::json secret;
		secretFile >> secret;
		std::ofstream(LOG_PATH, std::ios::app) << "access key is " << secret["access_key"] << std::endl;
		return AWSCredentials(
			secret["access_key"],
			secret["secret_key"],
			secret["session_token"],
			Aws::Utils::DateTime(secret["expiration_time"], Aws::Utils::DateFormat::ISO_8601)
		);
	}

private:
	mutable Aws::Utils::Threading::ReaderWriterLock m_reloadLock;
	Aws::Auth::AWSCredentials m_credentials;
	std::string m_vaultSecretFile;
};

std::string GetS3Path(std::string databaseName, std::string tableName) {
	std::ofstream log(LOG_PATH, std::ios::app);
	log << "getting s3 path for " << databaseName << "." << tableName << std::endl;

	Aws::SDKOptions options;
	Aws::InitAPI(options);
	
	std::string location;

	Aws::Client::ClientConfiguration clientConfig;
	clientConfig.region = "eu-west-2";

	char *domain = getenv("DOMAIN");
	char *account = getenv("AWS_ACCOUNT");
	char *role = getenv("AWS_ROLE");
	char *catalogId = getenv("CATALOG_ID");
	if (domain == nullptr || account == nullptr || role == nullptr || catalogId == nullptr) {
		throw std::runtime_error("Environment variables DOMAIN, AWS_ACCOUNT, AWS_ROLE and CATALOG_ID must be set");
	}
	
	const auto credentialsProvider = 
		std::make_shared<VaultCredentialsProvider>(domain, account, role);

	log << "creating glue client" << std::endl;
	Aws::Glue::GlueClient glueClient(credentialsProvider, nullptr, clientConfig);
	
	log << "setting db" << std::endl;
	Aws::Glue::Model::GetTableRequest request;
	request.SetDatabaseName(databaseName);
	request.SetName(tableName);
	request.SetCatalogId(catalogId);

	Aws::Glue::Model::GetTableOutcome outcome = glueClient.GetTable(request);
	if (outcome.IsSuccess()) {
		location = outcome.GetResult().GetTable().GetStorageDescriptor().GetLocation();
	}
	else {
		throw std::runtime_error("Error getting the tables. " + outcome.GetError().GetMessage());
	}

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
