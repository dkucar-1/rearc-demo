## Step 1: Data Collection ##
In this step, we copy data from website to S3 bucket directly using `boto3`. Since boto's `putObject` function can't handle multiple files in a path, we have to collect the whole set of files in that directory. 

We can use the BeautifulSoup Python package to extract web elements via tags such as `<a href="/pub/time.series/pr/pr.contacts">pr.contacts</a>` see code starting with `links = `

When using using `requests`, we need to add a `User-Agent` header specifying the kind of machine programatically accessing the bls website, per Hint #3 and pass it along via `requests.get(url, headers=headers)` 

Finally, we'll be adding the data to the s3 bucket perfix `inbound/<date>` to partition uploads by date as they come in, in anticipation of collecting filenames with the same name on future days, but keeping them distinct with their date-stamped prefix.