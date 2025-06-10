using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;

namespace Azunt.BannedCustomerManagement
{
    public class BannedTypesTableBuilder
    {
        private readonly string _masterConnectionString;
        private readonly ILogger<BannedTypesTableBuilder> _logger;

        public BannedTypesTableBuilder(string masterConnectionString, ILogger<BannedTypesTableBuilder> logger)
        {
            _masterConnectionString = masterConnectionString;
            _logger = logger;
        }

        public void BuildTenantDatabases()
        {
            var tenantConnectionStrings = GetTenantConnectionStrings();

            foreach (var connStr in tenantConnectionStrings)
            {
                try
                {
                    EnsureBannedTypesTable(connStr);
                    _logger.LogInformation($"BannedTypes table processed (tenant DB): {connStr}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"[{connStr}] Error processing tenant DB");
                }
            }
        }

        public void BuildMasterDatabase()
        {
            try
            {
                EnsureBannedTypesTable(_masterConnectionString);
                _logger.LogInformation("BannedTypes table processed (master DB)");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing master DB");
            }
        }

        private List<string> GetTenantConnectionStrings()
        {
            var result = new List<string>();

            using (var connection = new SqlConnection(_masterConnectionString))
            {
                connection.Open();
                var cmd = new SqlCommand("SELECT ConnectionString FROM dbo.Tenants", connection);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var connectionString = reader["ConnectionString"]?.ToString();
                        if (!string.IsNullOrEmpty(connectionString))
                        {
                            result.Add(connectionString);
                        }
                    }
                }
            }

            return result;
        }

        private void EnsureBannedTypesTable(string connectionString)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var cmdCheck = new SqlCommand(@"
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = 'BannedTypes'", connection);

                int tableCount = (int)cmdCheck.ExecuteScalar();

                if (tableCount == 0)
                {
                    var cmdCreate = new SqlCommand(@"
                        CREATE TABLE [dbo].[BannedTypes] (
                            [Id] BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
                            [Active] BIT NOT NULL DEFAULT((1)),
                            [CreatedAt] DATETIMEOFFSET(7) NOT NULL,
                            [CreatedBy] NVARCHAR(255) NULL,
                            [Name] NVARCHAR(MAX) NULL
                        )", connection);

                    cmdCreate.ExecuteNonQuery();
                    _logger.LogInformation("BannedTypes table created.");
                }

                // Insert default rows if empty
                var cmdCount = new SqlCommand("SELECT COUNT(*) FROM [dbo].[BannedTypes]", connection);
                int rowCount = (int)cmdCount.ExecuteScalar();

                if (rowCount == 0)
                {
                    var cmdInsert = new SqlCommand(@"
                        INSERT INTO [dbo].[BannedTypes] (Active, CreatedAt, CreatedBy, Name)
                        VALUES
                            (1, SYSDATETIMEOFFSET(), 'System', 'Abusive Language'),
                            (1, SYSDATETIMEOFFSET(), 'System', 'Malicious Spam')", connection);

                    int inserted = cmdInsert.ExecuteNonQuery();
                    _logger.LogInformation($"Default BannedTypes inserted: {inserted} rows.");
                }
            }
        }

        public static void Run(IServiceProvider services, bool forMaster, string? optionalConnectionString = null)
        {
            try
            {
                var logger = services.GetRequiredService<ILogger<BannedTypesTableBuilder>>();
                var config = services.GetRequiredService<IConfiguration>();

                string connectionString = !string.IsNullOrWhiteSpace(optionalConnectionString)
                    ? optionalConnectionString
                    : config.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("DefaultConnection is not configured.");

                var builder = new BannedTypesTableBuilder(connectionString, logger);

                if (forMaster)
                    builder.BuildMasterDatabase();
                else
                    builder.BuildTenantDatabases();
            }
            catch (Exception ex)
            {
                var fallbackLogger = services.GetService<ILogger<BannedTypesTableBuilder>>();
                fallbackLogger?.LogError(ex, "Error while processing BannedTypes table.");
            }
        }
    }
}
