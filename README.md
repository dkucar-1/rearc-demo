## Step 1: Data Collection ##
In this step, we copy data from website to S3 bucket directly using `boto3`. Since boto's `putObject` function can't handle multiple files in a path, we have to collect the whole set of files in that directory. 

We can use the BeautifulSoup Python package to extract web elements via tags such as `<a href="some-filename</a>` see code starting with `links = `

When using using `requests`, we need to add a `User-Agent` header specifying the kind of machine programatically accessing the bls website, per Hint #3 and pass it along via `requests.get(url, headers=headers)` 

Finally, we'll be adding the data to the s3 bucket perfix `inbound/<date>` to partition uploads by date as they come in, in anticipation of collecting filenames with the same name on future days, but keeping them distinct with their date-stamped prefix.

We won't be able to add data to the s3 bucket unless we have permissions set up appropriately. That is, my role needs to have `putObject` permissions to the s3 bucket.

## Step 2: APIs
Very similar to Step 1, using `requests` to copy data from `https://api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_5&drilldowns=State,Year&measures=Population` to the s3 bucket `inbound` folder. This is more straighforward as it is just one file. 

## Step 3: Data Analysis
We will use PySpark to load the data into dataframes and do some analysis. In order to do this, we need to install pyspark and provide some configuration options that enable us to read data from our s3 bucket.
- `spark.hadoop.fs.s3a.aws.credentials.provider`: use AWS credentials set up through the cli
- `spark.hadoop.fs.s3a.impl`: use AWS' s3a protocol to interact with s3
- `spark.jars.packages`: integration and dependencies needed to work with AWS services

In order to access, we also need `s3:ListBucket` permissions on the bucket. This in addition to `"s3:PutObject", "s3:GetObject"`permissions on the contents of the bucket.

### Reading the data
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

