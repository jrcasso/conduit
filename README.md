# Conduit

## Summary 
Conduit is a lightweight Go framework for handling asynchronous, concurrent blob ETL processing in AWS using SQS, SNS, and S3.

## Usage
Consumers implement this package by constructing a conduit with a *transform* method, and then invoking `Run`. 
Each conduit undergoes the following lifecycle loop when run:

1. **Receive** an SQS message containing the blob path in S3
1. **Extract** the data from the S3 ingress bucket using the blob path
1. **Transform** the data using an implementer-defined function
1. **Upload** the new data to an egress S3 bucket
1. **Delete** the SQS message

Example:
```go
func myTransform(t conduit.Transformable) conduit.Upload {
    // Receive a Transformable interface with data and metadata
    // properties. Deserialize the data, mutate the data, etc.
    t.Data = fmt.Sprintf("new data %v", t.Data)

    return conduit.Upload{
        Key:           fmt.Sprintf("transformed-%v", t.Record.S3.Object.Key),
        Transformable: t,
    }
}

func main() {
    // Create an AWS session
    ctx, cancel := context.WithCancel(context.Background())
    defer func() { cancel() }()
    sess := session.Must(session.NewSession())

    // Create the conduit with my transform method and run
    c := conduit.NewConduit(*sess, myTransform).Run(ctx)
}
```

# Configuration

The application utilizes the following environment variables:

|Environment|Default|
|-|-|
|`AWS_SECRET_ACCESS_KEY`|*None*|
|`AWS_ACCESS_KEY_ID`|*None*|
|`AWS_REGION`|`us-east-1`|
|`CONDUIT_S3_EGRESS_BUCKET`|*None*|
|`CONDUIT_QUEUE_URL`|*None*|
|`CONDUIT_BATCH_SIZE`|`10`|
|`CONDUIT_POLL_FREQUENCY` (milliseconds) |`3000`|
|`CONDUIT_VISIBILITY_TIMEOUT` (seconds)|`10`|
