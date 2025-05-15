# Load Data into a Warehouse using T-SQL in Microsoft Fabric

This lab demonstrates how to load data into a Microsoft Fabric data warehouse using T-SQL. A data warehouse in Fabric provides a relational database for large-scale analytics with full SQL semantics, allowing for data insertion, updating, and deletion, unlike the read-only SQL endpoint of a lakehouse.

**Estimated Completion Time:** 30 minutes

**Note:** A Microsoft Fabric trial is required to complete this exercise.

## Prerequisites

* A Microsoft Fabric account with a trial enabled.

## Steps

### 1. Create a Workspace

1.  Navigate to the [Microsoft Fabric home page](https://app.fabric.microsoft.com/home?experience=fabric) in your browser and sign in with your Fabric credentials.
2.  In the left menu bar, select **Workspaces** (the icon looks like 🗇).
3.  Create a **New workspace** with a name of your choice. Ensure you select a licensing mode that includes Fabric capacity (Trial, Premium, or Fabric).
4.  Once the workspace is created, it should be empty.

### 2. Create a Lakehouse and Upload Files

1.  Select **+ New item** and create a new **Lakehouse** with a name of your choice.
2.  After a short time, an empty lakehouse will be created. Download the `sales.csv` file from [https://github.com/MicrosoftLearning/dp-data/raw/main/sales.csv](https://github.com/MicrosoftLearning/dp-data/raw/main/sales.csv) to your local machine.
3.  Return to your lakehouse in the browser. In the **...** menu for the **Files** folder in the **Explorer** pane, select **Upload** and then **Upload files**.
4.  Upload the `sales.csv` file from your local computer to the lakehouse.
5.  After uploading, select **Files** and verify that `sales.csv` is visible.

### 3. Create a Table in the Lakehouse

1.  In the **...** menu for the `sales.csv` file in the **Explorer** pane, select **Load to tables**, and then **New table**.
2.  In the **Load file to new table** dialog, provide the following information:
    * **New table name:** `staging_sales`
    * **Use header for columns names:** Selected
    * **Separator:** `,`
3.  Select **Load**.

### 4. Create a Warehouse

1.  On the left menu bar, select **Create**.
2.  Under the **Data Warehouse** section on the **New** page, select **Warehouse**.
3.  Give your new warehouse a unique name of your choice.
    * **Note:** If the **Create** option is not visible, click the ellipsis (**...**) first.
4.  After a minute or so, your new warehouse will be created.

### 5. Create Fact Table, Dimensions, and View

1.  From your workspace, select the warehouse you created.
2.  In the warehouse toolbar, select **New SQL query**.
3.  Copy and run the following T-SQL query to create the `Sales` schema, `Fact_Sales`, `Dim_Customer`, and `Dim_Item` tables:

    ```sql
    CREATE SCHEMA [Sales]
    GO

    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Fact_Sales' AND SCHEMA_NAME(schema_id)='Sales')
    	CREATE TABLE Sales.Fact_Sales (
    		CustomerID VARCHAR(255) NOT NULL,
    		ItemID VARCHAR(255) NOT NULL,
    		SalesOrderNumber VARCHAR(30),
    		SalesOrderLineNumber INT,
    		OrderDate DATE,
    		Quantity INT,
    		TaxAmount FLOAT,
    		UnitPrice FLOAT
    	);

    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Dim_Customer' AND SCHEMA_NAME(schema_id)='Sales')
        CREATE TABLE Sales.Dim_Customer (
            CustomerID VARCHAR(255) NOT NULL,
            CustomerName VARCHAR(255) NOT NULL,
            EmailAddress VARCHAR(255) NOT NULL
        );

    ALTER TABLE Sales.Dim_Customer add CONSTRAINT PK_Dim_Customer PRIMARY KEY NONCLUSTERED (CustomerID) NOT ENFORCED
    GO

    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Dim_Item' AND SCHEMA_NAME(schema_id)='Sales')
        CREATE TABLE Sales.Dim_Item (
            ItemID VARCHAR(255) NOT NULL,
            ItemName VARCHAR(255) NOT NULL
        );

    ALTER TABLE Sales.Dim_Item add CONSTRAINT PK_Dim_Item PRIMARY KEY NONCLUSTERED (ItemID) NOT ENFORCED
    GO
    ```

    **Important:** Foreign key constraints are often omitted in data warehouses to optimize ETL performance.

4.  In the **Explorer** pane, navigate to **Schemas** » **Sales** » **Tables** to see the newly created tables. If they are not visible, refresh the **Tables** node.
5.  Open a new **New SQL query** editor.
6.  Copy and run the following query, replacing `<your lakehouse name>` with the actual name of your lakehouse, to create a view pointing to the staging table in the lakehouse:

    ```sql
    CREATE VIEW Sales.Staging_Sales
    AS
    SELECT * FROM [<your lakehouse name>].[dbo].[staging_sales];
    ```

7.  In the **Explorer** pane, navigate to **Schemas** » **Sales** » **Views** to see the `Staging_Sales` view.

### 6. Load Data to the Warehouse

1.  Create a new **New SQL query** editor.
2.  Copy and run the following T-SQL stored procedure to load data from the lakehouse into the warehouse tables:

    ```sql
    CREATE OR ALTER PROCEDURE Sales.LoadDataFromStaging (@OrderYear INT)
    AS
    BEGIN
    	-- Load data into the Customer dimension table
        INSERT INTO Sales.Dim_Customer (CustomerID, CustomerName, EmailAddress)
        SELECT DISTINCT CustomerName, CustomerName, EmailAddress
        FROM [Sales].[Staging_Sales]
        WHERE YEAR(OrderDate) = @OrderYear
        AND NOT EXISTS (
            SELECT 1
            FROM Sales.Dim_Customer
            WHERE Sales.Dim_Customer.CustomerName = Sales.Staging_Sales.CustomerName
            AND Sales.Dim_Customer.EmailAddress = Sales.Staging_Sales.EmailAddress
        );

        -- Load data into the Item dimension table
        INSERT INTO Sales.Dim_Item (ItemID, ItemName)
        SELECT DISTINCT Item, Item
        FROM [Sales].[Staging_Sales]
        WHERE YEAR(OrderDate) = @OrderYear
        AND NOT EXISTS (
            SELECT 1
            FROM Sales.Dim_Item
            WHERE Sales.Dim_Item.ItemName = Sales.Staging_Sales.Item
        );

        -- Load data into the Sales fact table
        INSERT INTO Sales.Fact_Sales (CustomerID, ItemID, SalesOrderNumber, SalesOrderLineNumber, OrderDate, Quantity, TaxAmount, UnitPrice)
        SELECT CustomerName, Item, SalesOrderNumber, CAST(SalesOrderLineNumber AS INT), CAST(OrderDate AS DATE), CAST(Quantity AS INT), CAST(TaxAmount AS FLOAT), CAST(UnitPrice AS FLOAT)
        FROM [Sales].[Staging_Sales]
        WHERE YEAR(OrderDate) = @OrderYear;
    END
    ```

3.  Create another new **New SQL query** editor.
4.  Copy and run the following query to execute the stored procedure and load data for the year 2021:

    ```sql
    EXEC Sales.LoadDataFromStaging 2021
    ```

    **Note:** You can modify the year parameter to load data from other years if available in the `sales.csv` file.

### 7. Run Analytical Queries

1.  On the top menu, select **New SQL query**.
2.  Copy and run the following query to see total sales by customer for 2021:

    ```sql
    SELECT c.CustomerName, SUM(s.UnitPrice * s.Quantity) AS TotalSales
    FROM Sales.Fact_Sales s
    JOIN Sales.Dim_Customer c
    ON s.CustomerID = c.CustomerID
    WHERE YEAR(s.OrderDate) = 2021
    GROUP BY c.CustomerName
    ORDER BY TotalSales DESC;
    ```

    **Note:** The top customer by total sales for 2021 is Jordan Turner with total sales of 14686.69.

3.  Open a new **New SQL query** or reuse the same editor and run the following query to see the top-selling items by total sales for 2021:

    ```sql
    SELECT i.ItemName, SUM(s.UnitPrice * s.Quantity) AS TotalSales
    FROM Sales.Fact_Sales s
    JOIN Sales.Dim_Item i
    ON s.ItemID = i.ItemID
    WHERE YEAR(s.OrderDate) = 2021
    GROUP BY i.ItemName
    ORDER BY TotalSales DESC;
    ```

    **Note:** The Mountain-200 bike model in black and silver were the top-selling items in 2021.

4.  Open a new **New SQL query** or reuse the same editor and run the following query to find the top customer for each product category (Bike, Helmet, Gloves) based on total sales in 2021:

    ```sql
    WITH CategorizedSales AS (
        SELECT
            CASE
                WHEN i.ItemName LIKE '%Helmet%' THEN 'Helmet'
                WHEN i.ItemName LIKE '%Bike%' THEN 'Bike'
                WHEN i.ItemName LIKE '%Gloves%' THEN 'Gloves'
                ELSE 'Other'
            END AS Category,
            c.CustomerName,
            s.UnitPrice * s.Quantity AS Sales
        FROM Sales.Fact_Sales s
        JOIN Sales.Dim_Customer c
        ON s.CustomerID = c.CustomerID
        JOIN Sales.Dim_Item i
        ON s.ItemID = i.ItemID
        WHERE YEAR(s.OrderDate) = 2021
    ),
    RankedSales AS (
        SELECT
            Category,
            CustomerName,
            SUM(Sales) AS TotalSales,
            ROW_NUMBER() OVER (PARTITION BY Category ORDER BY SUM(Sales) DESC) AS SalesRank
        FROM CategorizedSales
        WHERE Category IN ('Helmet', 'Bike', 'Gloves')
        GROUP BY Category, CustomerName
    )
    SELECT Category, CustomerName, TotalSales
    FROM RankedSales
    WHERE SalesRank = 1
    ORDER BY TotalSales DESC;
    ```

    **Note:** This query categorizes items based on their name and finds the top customer for each category. The category information is derived from the `ItemName` column.

## 8. Clean Up Resources

If you have finished exploring the data warehouse, you can delete the workspace created for this exercise.

1.  In the left bar, select the icon for your workspace.
2.  Select **Workspace settings**.
3.  In the **General** section, scroll down and select **Remove this workspace**.
4.  Select **Delete** to confirm.
