# Building a Data Pipeline

## Part 1: AWS S3 & Sourcing Datasets
In this step, we copy data from website to S3 bucket directly using `boto3`'s `putObject` function.

When using using `requests`, we need to add a `User-Agent` to the header displaying our email and application name (this combination seems to work), per Hint #3 and pass it along via `requests.get(url, headers=headers..)` .

We'll be adding the data to the s3 bucket perfix `inbound/`. The uploads are conditional based on whether data has been updated at the source. This will have a different md5 from the data that is currently sitting in our S3 bucket, and hence would overwrite our S3 contents. If upon trying an upload and the data has NOT been updated at the source, nothing happens.

We won't be able to add data to the s3 bucket unless we have permissions set up appropriately. That is, my role needs to have `putObject` permissions to the s3 bucket.

Finally, it is possible to run the script standalone (via the shebang / `__main__`) using 
the syntax `./sync_to_s3.py`.

## Part 2: APIs
Very similar to Step 1, using `requests` to copy data from `https://api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_5&drilldowns=State,Year&measures=Population` to the s3 bucket `inbound/` folder. 

## Part 3: Data Analysis
We will use PySpark to load the data into dataframes and do some analysis. In order to do this, we need to install pyspark and provide some configuration options that enable us to read data from our s3 bucket.
- `spark.hadoop.fs.s3a.aws.credentials.provider`: use AWS credentials set up through the cli
- `spark.hadoop.fs.s3a.impl`: use AWS' s3a protocol to interact with s3
- `spark.jars.packages`: integration and dependencies needed to work with AWS services

### Permissions to S3 bucket
To access, we'll need to have these permissions:
- on bucket contents, `"s3:PutObject","s3:GetObject"`
- on (just) bucket, `"s3:ListBucket","s3:GetBucketLocation"`

### Reading the data
Note that there are some "modes" defined in the notebook.
- `test` is a "unit-test" like mode to check our queries on the BLS datasets using the sample dataset provided in the instructions
- `local` uses locally-downloaded files from the BLS and datausa datasets; also used to test the queries using "real" data but to not incur AWS costs
- `prod` actually uses the S3 locations

Use `spark.read.. csv` with header, tabs as separators to read the bls dataset. Use `spark.read..json` selecting on the `data` column to read dataa from the datausa dataset.

We note that reading the BLS dataset, the column names are padded with whitespace
```
root
 |-- series_id        : string (nullable = true)
 |-- year: integer (nullable = true)
 |-- period: string (nullable = true)
 |--        value: double (nullable = true)
 |-- footnote_codes: string (nullable = true)
```
Need to trim. Use this syntax to rename columns
```
new_cols = [x.strip() for x in bls_df.columns]
bls_renamed = bls_df.toDF(*new_cols)
```
## Part 4: Infrastructure as Code & Data Pipeline with AWS CloudFormation

### Step 1 
For this part, we read in the population json. The population is given in terms of states; we want the population of all states, hence we group by states and collect the total sum. Finally, we use the built-in Spark `summary` function to get the mean and standard deviation for the populations recorded across 2013 to 2018.

### Step 2
For every `series_id`, find the best year: the year with the max/largest sum of `value` for all quarters in that year. This broken into two queries. The first query groups by `series_id` and `year` and sums up the values across quarters. The second query takes this input and orders value in descending order (via a window function), while keeping `series_id`, `year` and `value` columns, then selecting the highest `value` (rank of 1) with its associated `year`. In the end then, we have the entire set of unique `series_id` with their associated best `year` and `value`

### Step 3
For this part, to my understanding, unrelated to Part 2, we inner join the BLS dataset and the population dataset on `year`, filtering on a particular `series_id` and `period`. We need to trim whitespace just in case. Noting there is an issue with the filter on `series_id`: the exact match `==` doesn't appear to work whereas the like with wildcard at the end returns values, that is using `.like('PRS30006032%')` appears to work.

### Step 4
We'll use CloudFormation for this, which is used to deploy AWS infrastructure and resources in a programmatic, version-controlled way using yaml templates. I will lean heavily on an LLM here since the notation can be very fiddly, though, happy to explain concepts. Generally, I would ask it to do something like "I would like to create a lambda that does x, y" per instructions and it would generate a file. I would then ask it specifically what such-and-such block does. My intereaction with the LLM did assume that I knew my way around the AWS ecosystem, so it wasn't just vibe coding.

#### Task 1
*The deployment should include a Lambda function that executes Part 1 and Part 2 (you can combine both in 1 lambda function). The lambda function will be scheduled to run daily.* 

Lambda is AWS' function-as-a-service designed to run a single function, in this case `sync_file_to_s3`. 

This code is added to the `Code..ZipFile` portion of the Cloudformation *.yaml template file. The `__main__` portion from the sync_to_s3.py becomes part of the `def handler(event, context)` function; this is the top level executable run by Lambda.

Under `Resources..LambdaRole`, we have the permissions required to run Lambda and have it drop files in the S3 bucket. 
- `ManagedPolicyArns` default permissions to run Lambda
- `Policies` user-defined policy with permissions to my S3 bucket

To run the lambda daily, we set values under `Parameters..ScheduleExpression` and use 
`DailyRule` resource with `ScheduleExpression` state set to `ENABLED`, this is what actually triggers the Lambda to run daily.

To deploy, we can run commands in the CLI
```
aws cloudformation deploy   --stack-name rearc-quest   --template-file rearc-quest-part4.yaml   --capabilities CAPABILITY_IAM   --region us-east-2
```

#### Task 2
*The deployment should include an SQS queue that will be populated every time the JSON file is written to S3.*

This is configured in the file notifications.json. This tells S3 to send an event to the SQS queue (`rearc-quest-population-events`) whenever a new file is written to `inbound/datausa_population.json`. To deploy this notification to the Lambda, we run this in the CLI
```
aws s3api put-bucket-notification-configuration   --bucket rearc-demo-dk   --notification-configuration file://notification.json   --region us-east-2
```
Whenever Lambda writes a fresh datausa_population.json to S3, this config ensures a message lands in that SQS queue.

#### Task 3
*Every time a message arrives on the queue, execute a Lambda function to generate the report from Part 3 (join BLS + population data, log the results).*

This is deployed as a separate CloudFormation stack in `rearc-quest-part4-step3.yaml`. The template creates:
- A Lambda function that reads both datasets from S3, filters BLS for `series_id == PRS30006032` and `period == Q01`, joins with population data on `year`, and logs the resulting table to CloudWatch.
- An `AWS::Lambda::EventSourceMapping` that wires the SQS queue as a trigger, so the Lambda runs automatically whenever a new message arrives.
- The code that does the "heavy lifting" is added to the `def handler(event, context)` block 
  - because Spark is too heavy-weight for Lambda (requires JVM, would easily exceed 256mb limit), the code had to be re-written in terms of dictionary lookups
  ```
  for row in bls_rows:
    if not sid.startswith(SERIES_ID): continue
    if row.get('period', '') != PERIOD: continue
    pop = pop_by_year.get(year)
    if pop is not None:
        results.append({...})
  ```

To deploy:
```
aws cloudformation deploy   --stack-name rearc-quest-analysis   --template-file rearc-quest-part4-step3.yaml   --capabilities CAPABILITY_IAM   --region us-east-2
```
Once that's deployed, verify it's working by checking CloudWatch Logs after the next daily run, or by sending a test message to the queue now:
```
aws sqs send-message \
  --queue-url https://sqs.us-east-2.amazonaws.com/075550799614/rearc-quest-population-events \
  --message-body '{"test": true}' \
  --region us-east-2
```

We can view the output (6 rows) by accessing logs using the command (subject to having appropriate permssions set up to be able to view logs)
```
aws logs tail /aws/lambda/rearc-quest-analysis-analysis \
  --region us-east-2 \
  --since 1h
```
The output table is displayed as lines in the logfile below:
```
2026-04-12T15:27:40.278000+00:00 2026/04/12/[$LATEST]5104422568b4443bbfe4b5db342f32c0 [INFO]   2026-04-12T15:27:40.278Z a31d0f6f-4c4c-4dfd-bd22-fad8bd519d33    year   series_id      period   value    Population
2026-04-12T15:27:40.278000+00:00 2026/04/12/[$LATEST]5104422568b4443bbfe4b5db342f32c0 [INFO]   2026-04-12T15:27:40.278Z a31d0f6f-4c4c-4dfd-bd22-fad8bd519d33    2013   PRS30006032    Q01      0.5      315219560.0
2026-04-12T15:27:40.278000+00:00 2026/04/12/[$LATEST]5104422568b4443bbfe4b5db342f32c0 [INFO]   2026-04-12T15:27:40.278Z a31d0f6f-4c4c-4dfd-bd22-fad8bd519d33    2014   PRS30006032    Q01      -0.1     317746049.0
2026-04-12T15:27:40.278000+00:00 2026/04/12/[$LATEST]5104422568b4443bbfe4b5db342f32c0 [INFO]   2026-04-12T15:27:40.278Z a31d0f6f-4c4c-4dfd-bd22-fad8bd519d33    2015   PRS30006032    Q01      -1.7     320098094.0
2026-04-12T15:27:40.278000+00:00 2026/04/12/[$LATEST]5104422568b4443bbfe4b5db342f32c0 [INFO]   2026-04-12T15:27:40.278Z a31d0f6f-4c4c-4dfd-bd22-fad8bd519d33    2016   PRS30006032    Q01      -1.4     322087547.0
2026-04-12T15:27:40.278000+00:00 2026/04/12/[$LATEST]5104422568b4443bbfe4b5db342f32c0 [INFO]   2026-04-12T15:27:40.278Z a31d0f6f-4c4c-4dfd-bd22-fad8bd519d33    2017   PRS30006032    Q01      0.9      324473370.0
2026-04-12T15:27:40.278000+00:00 2026/04/12/[$LATEST]5104422568b4443bbfe4b5db342f32c0 [INFO]   2026-04-12T15:27:40.278Z a31d0f6f-4c4c-4dfd-bd22-fad8bd519d33    2018   PRS30006032    Q01      0.5      326289971.0
2026-04-12T15:27:40.278000+00:00 2026/04/12/[$LATEST]5104422568b4443bbfe4b5db342f32c0 [INFO]   2026-04-12T15:27:40.278Z a31d0f6f-4c4c-4dfd-bd22-fad8bd519d33    === End of report (6 rows) ===
```

## Cleanup Steps
To delete the stacks in case of errors to deploy or at the end of this process, just to clean up, use:
```
aws cloudformation delete-stack --stack-name rearc-quest-analysis --region us-east-2 \
aws cloudformation delete-stack --stack-name rearc-quest --region us-east-2 
```