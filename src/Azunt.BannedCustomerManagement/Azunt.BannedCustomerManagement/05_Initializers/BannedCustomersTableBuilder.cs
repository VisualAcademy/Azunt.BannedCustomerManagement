using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;

namespace Azunt.BannedCustomerManagement
{
    public class BannedCustomersTableBuilder
    {
        private readonly string _masterConnectionString;
        private readonly ILogger<BannedCustomersTableBuilder> _logger;

        public BannedCustomersTableBuilder(string masterConnectionString, ILogger<BannedCustomersTableBuilder> logger)
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
                    EnsureBannedCustomersTable(connStr);
                    _logger.LogInformation($"BannedCustomers table processed (tenant DB): {connStr}");
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
                EnsureBannedCustomersTable(_masterConnectionString);
                _logger.LogInformation("BannedCustomers table processed (master DB)");
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

        private void EnsureBannedCustomersTable(string connectionString)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var cmdCheck = new SqlCommand(@"
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = 'BannedCustomers'", connection);

                int tableCount = (int)cmdCheck.ExecuteScalar();

                if (tableCount == 0)
                {
                    // Create 'BannedCustomers' table if it doesn't exist
                    var cmdCreate = new SqlCommand(@"
                        CREATE TABLE [dbo].[BannedCustomers] (
                            [Id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,                     -- 고유 ID
                            [Created] DATETIME NOT NULL DEFAULT(GETDATE()),                    -- 생성일
                            [CreatedBy] NVARCHAR(255) NULL,                                    -- 작성자
                            [Name] NVARCHAR(255) NULL,                                         -- 이름
                            [Title] NVARCHAR(150) NULL,                                        -- 제목
                            [Content] NTEXT NULL,                                              -- 내용
                            [Category] NVARCHAR(20) NULL DEFAULT('Free'),                      -- 카테고리
                            [ReadCount] INT NULL DEFAULT 0,                                    -- 조회수
                            [CommentCount] INT NULL DEFAULT 0,                                 -- 댓글수
                            [IsPinned] BIT NULL DEFAULT 0,                                     -- 상단 고정
                            [FileName] NVARCHAR(255) NULL,                                     -- 파일명
                            [FileSize] INT NULL DEFAULT 0,                                     -- 파일크기
                            [DownCount] INT NULL DEFAULT 0,                                    -- 다운로드수
                            [Ref] INT NOT NULL,                                                -- 참조
                            [Step] INT NOT NULL DEFAULT 0,                                     -- 단계
                            [RefOrder] INT NOT NULL DEFAULT 0,                                 -- 순서
                            [AnswerNum] INT NOT NULL DEFAULT 0,                                -- 답변 수
                            [ParentNum] INT NOT NULL DEFAULT 0                                 -- 부모글 번호
                        )", connection);

                    cmdCreate.ExecuteNonQuery();
                    _logger.LogInformation("BannedCustomers table created.");
                }


                var expectedColumns = new Dictionary<string, string>
                {
                    ["ParentId"] = "INT NULL",
                    ["ParentKey"] = "NVARCHAR(255) NULL",
                    ["CreatedBy"] = "NVARCHAR(255) NULL",
                    ["Created"] = "DATETIME DEFAULT(GETDATE()) NULL",
                    ["ModifiedBy"] = "NVARCHAR(255) NULL",
                    ["Modified"] = "DATETIME NULL",
                    ["Name"] = "NVARCHAR(255) NULL",
                    ["PostDate"] = "DATETIME DEFAULT GETDATE() NULL",
                    ["PostIp"] = "NVARCHAR(15) NULL",
                    ["Title"] = "NVARCHAR(150) NULL",
                    ["Content"] = "NTEXT NULL",
                    ["Category"] = "NVARCHAR(20) DEFAULT('Free') NULL",
                    ["Email"] = "NVARCHAR(100) NULL",
                    ["Password"] = "NVARCHAR(255) NULL",
                    ["ReadCount"] = "INT DEFAULT 0",
                    ["Encoding"] = "NVARCHAR(20) NULL",
                    ["Homepage"] = "NVARCHAR(100) NULL",
                    ["ModifyDate"] = "DATETIME NULL",
                    ["ModifyIp"] = "NVARCHAR(15) NULL",
                    ["CommentCount"] = "INT DEFAULT 0",
                    ["IsPinned"] = "BIT DEFAULT 0 NULL",
                    ["FileName"] = "NVARCHAR(255) NULL",
                    ["FileSize"] = "INT DEFAULT 0",
                    ["DownCount"] = "INT DEFAULT 0",
                    ["Ref"] = "INT NOT NULL",
                    ["Step"] = "INT NOT NULL DEFAULT 0",
                    ["RefOrder"] = "INT NOT NULL DEFAULT 0",
                    ["AnswerNum"] = "INT NOT NULL DEFAULT 0",
                    ["ParentNum"] = "INT NOT NULL DEFAULT 0",
                    ["ReportID"] = "INT NULL",
                    ["Employee"] = "BIT NULL",
                    ["ParticipantID"] = "INT NULL",
                    ["DailyLogID"] = "INT NULL",
                    ["Banned"] = "BIT NULL",
                    ["AuditID"] = "INT NULL",
                    ["PlayerAnalysisID"] = "INT NULL",
                    ["POI"] = "NVARCHAR(100) NULL",
                    ["POIRole"] = "NVARCHAR(50) NULL",
                    ["POIType"] = "NVARCHAR(50) NULL",
                    ["BannedTypeId"] = "INT NULL",
                    ["BannedType"] = "NVARCHAR(255) NULL",
                    ["PoiTypeID"] = "INT NULL",
                    ["PoiRoleID"] = "INT NULL",
                    ["FirstName"] = "NVARCHAR(MAX) NULL",
                    ["LastName"] = "NVARCHAR(MAX) NULL",
                    ["IsEmployee"] = "BIT NULL",
                    ["EmployeeId"] = "BIGINT NULL",
                    ["Address"] = "NVARCHAR(70) NULL",
                    ["City"] = "NVARCHAR(70) NULL",
                    ["State"] = "NVARCHAR(255) NULL",
                    ["PostalCode"] = "NVARCHAR(35) NULL",
                    ["BirthCity"] = "NVARCHAR(70) NULL",
                    ["BirthState"] = "NVARCHAR(2) NULL",
                    ["BirthCountry"] = "NVARCHAR(70) NULL",
                    ["DriverLicenseNumber"] = "NVARCHAR(35) NULL",
                    ["DriverLicenseState"] = "NVARCHAR(2) NULL",
                    ["Photo"] = "NVARCHAR(MAX) NULL",
                    ["PrimaryPhone"] = "NVARCHAR(35) NULL",
                    ["StartDate"] = "DATETIME NULL",
                    ["EndDate"] = "DATETIME NULL",
                    ["HowLong"] = "NVARCHAR(255) NULL"
                };

                foreach (var (columnName, columnDefinition) in expectedColumns)
                {
                    var cmdColCheck = new SqlCommand(@"
                        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = 'BannedCustomers' AND COLUMN_NAME = @ColumnName", connection);
                    cmdColCheck.Parameters.AddWithValue("@ColumnName", columnName);

                    int columnExists = (int)cmdColCheck.ExecuteScalar();

                    if (columnExists == 0)
                    {
                        var cmdAlter = new SqlCommand($"ALTER TABLE [dbo].[BannedCustomers] ADD [{columnName}] {columnDefinition}", connection);
                        cmdAlter.ExecuteNonQuery();
                        _logger.LogInformation($"Column added to BannedCustomers: {columnName} ({columnDefinition})");
                    }
                }
            }
        }

        public static void Run(IServiceProvider services, bool forMaster, string? optionalConnectionString = null)
        {
            try
            {
                var logger = services.GetRequiredService<ILogger<BannedCustomersTableBuilder>>();
                var config = services.GetRequiredService<IConfiguration>();

                string connectionString = !string.IsNullOrWhiteSpace(optionalConnectionString)
                    ? optionalConnectionString
                    : config.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("DefaultConnection is not configured in appsettings.json.");

                var builder = new BannedCustomersTableBuilder(connectionString, logger);

                if (forMaster)
                    builder.BuildMasterDatabase();
                else
                    builder.BuildTenantDatabases();
            }
            catch (Exception ex)
            {
                var fallbackLogger = services.GetService<ILogger<BannedCustomersTableBuilder>>();
                fallbackLogger?.LogError(ex, "Error while processing BannedCustomers table.");
            }
        }
    }
}
