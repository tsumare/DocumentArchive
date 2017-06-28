Your CLI application is DocumentArchive (requires python3)

Set up a lambda with the (python3.6) code from lambda_function.py to handle all Create and Delete events from S3.
Set environment variable "SDB_DOMAIN" to your simpledb domain.
Apply the policy from iam_lambda_role_policy.json to this lambda.

Apply the policy from s3_bucket_policy.json to the chosen bucket.

Apply the policy from iam_user_policy.json to any IAM users who should have access to the document archive

Configure ~/.documentarchive.conf from the example for all CLI users.
pastebucket is optional and may be deleted.
If pastebucket is supplied, it will be used to create empty objects with redirect locatiaon metadata to let geturl return shortlinks.
