# Amazon Redshift Assignment

This project implements the TPC-H database operations on Amazon Redshift using Java and Gradle.

## Prerequisites

- Java 11 or higher
- Gradle 7.x or higher
- Amazon Redshift cluster running on AWS
- TPC-H DDL files in the `DDL data` directory

## Configuration

Before running the project, update the database connection details in `AmazonRedshift.java`:

```java
private String url = "jdbc:redshift://YOUR_CLUSTER_ENDPOINT:5439/dev";
private String uid = "admin";  // Your admin username
private String pw = "YourPassword123";  // Your admin password
```

Replace:
- `YOUR_CLUSTER_ENDPOINT` with your actual Redshift cluster endpoint
- `admin` with your database username
- `YourPassword123` with your database password

## Project Structure

```
redshift-assignment/
├── build.gradle                 # Gradle build configuration
├── settings.gradle              # Gradle settings
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/iitj/bigdata/
│   │   │       └── AmazonRedshift.java    # Main implementation
│   │   └── resources/
│   └── test/
│       └── java/
│           └── com/iitj/bigdata/
│               └── AmazonRedshiftTest.java # JUnit tests
└── README.md
```

## Building the Project

```bash
cd redshift-assignment
./gradlew build
```

## Running the Application

### Run with Gradle:
```bash
./gradlew run
```

### Run the compiled JAR:
```bash
java -jar build/libs/redshift-assignment-1.0-SNAPSHOT.jar
```

## Running Tests

```bash
./gradlew test
```

## Implementation Details

### Task 1: Database Connection (5 points)
- Establishes JDBC connection to Amazon Redshift
- Loads the Redshift JDBC driver
- Returns connection object

### Task 2: Close Connection (5 points)
- Properly closes database connection
- Handles exceptions gracefully

### Task 3: Drop Tables (5 points)
- Drops all TPC-H tables from the `dev` schema
- Uses CASCADE to handle foreign key dependencies
- Drops in proper order: lineitem, orders, partsupp, part, customer, supplier, nation, region

### Task 4: Create Tables (5 points)
- Creates the `dev` schema if it doesn't exist
- Reads and executes the TPC-H create script
- Creates all 8 TPC-H tables with proper schema prefix

### Task 5: Insert TPC-H Data (10 points)
- Loads data from DDL files in correct order
- Inserts data in batches for performance
- Commits every 100 records to avoid memory issues
- Order: region → nation → customer → supplier → part → partsupp → orders → lineitem

### Task 6: Query 1 (5 points)
Returns the **most recent top 10 orders** with:
- Order key
- Order date
- Total sale (calculated from lineitem prices)
- For customers in AMERICA region only

### Task 7: Query 2 (10 points)
Returns:
- Customer key and total spent (descending)
- For URGENT orders only
- Excludes FAILED orders
- Customers outside EUROPE
- Belonging to the largest market segment (determined by CTE)

### Task 8: Query 3 (10 points)
Returns:
- Count of line items ordered between April 1, 1997 and April 1, 2003 (6 years)
- Grouped by order priority
- Sorted by order priority (ascending)

## Dependencies

- **Redshift JDBC Driver**: `com.amazon.redshift:redshift-jdbc42:2.1.0.29`
- **PostgreSQL JDBC Driver**: `org.postgresql:postgresql:42.7.0`
- **JUnit**: `junit:junit:4.13.2`

## Network Configuration

Make sure your AWS Redshift cluster's security group allows inbound traffic:
- Protocol: TCP
- Port: 5439
- Source: Your IP address or 0.0.0.0/0 (for testing only)

## VPC Configuration

Ensure your Redshift cluster is:
- Publicly accessible (for external connections)
- Has proper VPC routing table configuration
- Has internet gateway if in public subnet

## Troubleshooting

### Connection Issues
1. Verify cluster endpoint and port
2. Check security group inbound rules
3. Ensure cluster is publicly accessible
4. Verify VPC and subnet configurations

### Data Loading Issues
1. Ensure DDL files are in the correct path (`../DDL data/`)
2. Check file encoding (UTF-8 recommended)
3. Verify data files follow TPC-H format

### Query Performance
- Redshift may take time for initial queries (cold start)
- Consider adding SORTKEY and DISTKEY for better performance
- Analyze tables after data load: `ANALYZE dev.tablename;`

## Notes

- The database schema name is `dev` (default Redshift database)
- All tables are created with `dev.` prefix
- Auto-commit is disabled for batch inserts
- Results are limited to 10 rows for display purposes

## Assignment Submission

Include in your PDF:
1. Screenshots of successful connection
2. Screenshots of insert() output showing data loaded
3. Screenshots of all three query results
4. Complete code listing
5. Ensure screenshots are uniquely identifiable (show your cluster name/timestamp)
