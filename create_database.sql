-- Create a new database (if it doesn't exist)
USE master;
GO
IF NOT EXISTS (
    SELECT [name]
    FROM sys.databases
    WHERE [name] = N'FDB2'
)
CREATE DATABASE FDB2;
GO

-- Create a new login if it doesn't exist
IF NOT EXISTS (
    SELECT name
    FROM sys.server_principals
    WHERE name = N'FormulaUser2'
)
BEGIN
    CREATE LOGIN FormulaUser2 WITH PASSWORD = 'temp';
END
GO

-- Create a new user if it doesn't exist
USE FDB2;
GO
IF NOT EXISTS (
    SELECT name
    FROM sys.database_principals
    WHERE name = N'FormulaUser2'
)
BEGIN
    CREATE USER FormulaUser2 FOR LOGIN FormulaUser2;
END
GO

-- Grant necessary permissions
ALTER ROLE db_owner ADD MEMBER FormulaUser2;
GO