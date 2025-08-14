create storage integration aws_data
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'arn:aws:iam::638335486592:role/snowflake-aws-role'
    enabled = true
    storage_allowed_locations = ('s3://datasus-sih');



desc integration aws_data;