## Quick Start Guide

### Step 1: Set Up Redshift Cluster
1. Login to AWS Console
2. Navigate to Amazon Redshift
3. Create a new cluster (use free trial)
4. Note down the cluster endpoint
5. Configure security group to allow your IP on port 5439

### Step 2: Configure Connection
Edit `src/main/java/com/iitj/bigdata/AmazonRedshift.java`:

```java
private String url = "jdbc:redshift://YOUR_CLUSTER_ENDPOINT:5439/dev";
private String uid = "admin";
private String pw = "YourPassword123";
```

### Step 3: Ensure DDL Files Are Present
Make sure the following files exist in `../DDL data/`:
- tpch_create.sql
- region.sql
- nation.sql
- customer.sql
- supplier.sql
- part.sql
- partsupp.sql
- orders.sql
- lineitem.sql

### Step 4: Build the Project
```bash
cd redshift-assignment

# If you have Gradle installed:
gradle wrapper
./gradlew build

# If you don't have Gradle:
# Install Gradle first or use the IDE's built-in Gradle
```

### Step 5: Run the Application
```bash
./gradlew run
```

### Step 6: Run Tests
```bash
./gradlew test
```

## Expected Output

When you run the application, you should see:

1. **Connection**
   ```
   Connecting to database.
   Successfully connected to Redshift database!
   ```

2. **Drop Tables**
   ```
   Dropping all the tables
   Dropped table: lineitem
   Dropped table: orders
   ...
   ```

3. **Create Tables**
   ```
   Creating Tables
   Schema 'dev' created/verified.
   All tables created successfully.
   ```

4. **Insert Data**
   ```
   Loading TPC-H Data
   Loading data from: region.sql
   Completed loading region.sql (5 records)
   ...
   ```

5. **Query Results**
   - Query 1: Top 10 recent orders in America
   - Query 2: Customer spending for urgent orders
   - Query 3: Line items count by order priority

## Troubleshooting

### "Connection refused"
- Check security group allows port 5439
- Verify cluster is publicly accessible
- Check VPC settings

### "Class not found: com.amazon.redshift.jdbc42.Driver"
- Run `./gradlew build` to download dependencies
- Check build.gradle has the correct driver dependency

### "File not found" for DDL files
- Ensure DDL data directory exists at `../DDL data/`
- Check file names match exactly (case-sensitive)

### Gradle wrapper not found
```bash
# Install Gradle wrapper manually:
gradle wrapper --gradle-version 8.4

# Or on macOS with Homebrew:
brew install gradle
gradle wrapper
```

## Taking Screenshots for Submission

1. **Connection Screenshot**: Show successful connection message
2. **Data Insert Screenshot**: Show loading progress with record counts
3. **Query 1 Screenshot**: Show full output with column headers and data
4. **Query 2 Screenshot**: Show full output 
5. **Query 3 Screenshot**: Show full output with priority counts

Include your cluster name or timestamp in screenshots to make them unique!
