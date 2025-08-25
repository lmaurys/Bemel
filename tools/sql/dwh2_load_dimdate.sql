-- Load procedure for Dwh2.DimDate in Azure SQL
-- Generates a contiguous calendar between @StartDate and @EndDate

IF OBJECT_ID('Dwh2.SpLoadDimDate', 'P') IS NOT NULL
  DROP PROCEDURE Dwh2.SpLoadDimDate;
GO

CREATE PROCEDURE Dwh2.SpLoadDimDate
  @StartDate DATE,
  @EndDate   DATE,
  @DeleteExisting BIT = 0
AS
BEGIN
  SET NOCOUNT ON;

  IF @StartDate IS NULL OR @EndDate IS NULL OR @EndDate < @StartDate
  BEGIN
    THROW 50000, 'Invalid date range', 1;
  END

  IF @DeleteExisting = 1
  BEGIN
    DELETE FROM Dwh2.DimDate
    WHERE [Date] BETWEEN @StartDate AND @EndDate;
  END

  ;WITH Dates AS (
    SELECT @StartDate AS [Date]
    UNION ALL
    SELECT DATEADD(DAY, 1, [Date])
    FROM Dates
    WHERE [Date] < @EndDate
  )
  INSERT INTO Dwh2.DimDate (DateKey, [Date], [Year], [Quarter], [Month], DayOfMonth, DayOfWeek, MonthName, DayName, UpdatedAt)
  SELECT
    CONVERT(INT, CONVERT(CHAR(8), [Date], 112))           AS DateKey,
    [Date],
    DATEPART(YEAR, [Date])                                 AS [Year],
    DATEPART(QUARTER, [Date])                              AS [Quarter],
    DATEPART(MONTH, [Date])                                AS [Month],
    DATEPART(DAY, [Date])                                  AS DayOfMonth,
    ((DATEPART(WEEKDAY, [Date]) + @@DATEFIRST - 1 + 6) % 7) + 1 AS DayOfWeek, -- 1=Monday, 7=Sunday
    DATENAME(MONTH, [Date])                                AS MonthName,
    DATENAME(WEEKDAY, [Date])                              AS DayName,
    SYSUTCDATETIME()                                       AS UpdatedAt
  FROM Dates d
  WHERE NOT EXISTS (SELECT 1 FROM Dwh2.DimDate x WHERE x.[Date] = d.[Date])
  OPTION (MAXRECURSION 0);
END
GO

-- Example load for 2000-01-01 to 2040-12-31
-- EXEC Dwh2.SpLoadDimDate @StartDate = '2000-01-01', @EndDate = '2040-12-31', @DeleteExisting = 0;
GO
