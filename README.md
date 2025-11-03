# Assignment 6: Amazon Redshift - TPC-H Data Analysis

Implementation of Assignment 6 for Big Data Analytics course, demonstrating Amazon Redshift database operations with TPC-H benchmark data.

## Features

- Connection to AWS Redshift Serverless
- Schema and table management (drop/create)
- Efficient data loading using multi-row INSERT (5000 rows/batch)
- Complex analytical queries on TPC-H dataset
- Transaction management and error handling

## Dataset

TPC-H benchmark data with 86,635 records across 8 tables:
- Region (5 records)
- Nation (25 records)
- Customer (1,500 records)
- Supplier (100 records)
- Part (2,000 records)
- PartSupp (8,000 records)
- Orders (15,000 records)
- LineItem (60,005 records)

## Technologies

- **Database:** Amazon Redshift Serverless (AWS ap-south-1)
- **Language:** Java 11+
- **Build Tool:** Gradle 8.4
- **JDBC Driver:** Amazon Redshift JDBC42 2.1.0.29

## Setup

### 1. Configure Redshift Connection

Copy the example config and add your credentials:

```bash
cd redshift-assignment/src/main/resources
cp config.properties.example config.properties
```

Edit `config.properties`:
```properties
redshift.url=jdbc:redshift://your-workgroup.region.redshift-serverless.amazonaws.com:5439/dev
redshift.username=admin
redshift.password=YourPassword
```

### 2. Copy SQL Data Files

```bash
cd /path/to/a6
cp -R "DDL data/." "redshift-assignment/src/main/resources/data/"
```

### 3. Build and Run

```bash
cd redshift-assignment
./gradlew run
```

Or run tests:
```bash
./gradlew test
```

## Implementation Highlights

### Multi-Row INSERT Optimization

The application uses Redshift-optimized multi-row INSERT statements:

```java
INSERT INTO table VALUES 
  (row1_data), 
  (row2_data), 
  ..., 
  (row5000_data);
```

**Performance:**
- Batches 5000 rows per INSERT statement
- Reduces 86,635 individual statements to ~18 multi-row statements
- Significantly faster than traditional row-by-row insertion

### Query Implementations

**Query 1:** Most recent top 10 orders in America region
- Joins: orders → lineitem → customer → nation → region
- Aggregates sales with discount calculation
- Orders by date descending

**Query 2:** Customer spending analysis
- Filters: urgent orders, non-failed, outside Europe, largest market segment
- Uses CTE to identify largest market segment
- Aggregates total spending per customer

**Query 3:** Line item counts by order priority
- Date range: April 1, 1997 - April 1, 2003 (6 years)
- Groups by order priority
- Counts line items per priority level

## Execution Results

```
Total execution time: 2m 54s
- Data loading: ~170 seconds (497 records/sec)
- Query execution: < 1 second (all 3 queries)

Query 1: 10 results (most recent orders)
Query 2: 166 results (customers with urgent orders)
Query 3: 5 results (priority levels)
```

## Key Learnings

1. **Multi-row INSERT:** Dramatic performance improvement for bulk data loading
2. **Transaction Management:** Proper auto-commit handling crucial for batch operations
3. **Redshift Specifics:** Public schema default, VPC configuration required
4. **Resource Management:** Classpath loading provides portability

## AWS Configuration

- Redshift Serverless workgroup
- VPC security group: Allow port 5439
- Publicly accessible endpoint (for development)
- Database: `dev`

## Assignment Completion

All 8 tasks completed (55/55 points):
- connect() - Database connection
- close() - Connection cleanup
- drop() - Drop all tables
- create() - Create TPC-H schema
- insert() - Load data with multi-row INSERT
- query1() - Top orders in America
- query2() - Customer spending analysis
- query3() - Line items by priority

## Documentation

See `Assignment-6-Report.md` for:
- Complete code listings
- Detailed implementation explanations
- Execution screenshots
- Performance analysis
- Query result interpretation

## License

Academic project for IIT Jodhpur - Big Data Analytics course.
