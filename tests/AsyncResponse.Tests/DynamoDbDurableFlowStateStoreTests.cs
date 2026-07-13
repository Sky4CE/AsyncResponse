using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using AsyncResponse.DurableFlows.DynamoDB;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class DynamoDbDurableFlowStateStoreTests
{
    [Fact]
    public async Task Provisioning_CreatesMissingTableAndEnablesTtl()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .SetupSequence(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ResourceNotFoundException("missing"))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidTable() });
        client
            .Setup(database => database.CreateTableAsync(It.IsAny<CreateTableRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new CreateTableResponse());
        client
            .Setup(database => database.DescribeTimeToLiveAsync(It.IsAny<DescribeTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Ttl(TimeToLiveStatus.DISABLED));
        client
            .Setup(database => database.UpdateTimeToLiveAsync(It.IsAny<UpdateTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateTimeToLiveResponse());
        client
            .Setup(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetItemResponse { Item = [] });
        using var store = CreateStore(client);

        Assert.Null(await store.LoadAsync("flow"));
        client.Verify(database => database.CreateTableAsync(
            It.Is<CreateTableRequest>(request => request.BillingMode == BillingMode.PAY_PER_REQUEST),
            It.IsAny<CancellationToken>()));
        client.Verify(database => database.UpdateTimeToLiveAsync(
            It.Is<UpdateTimeToLiveRequest>(request =>
                request.TimeToLiveSpecification.AttributeName == "expires_at" &&
                request.TimeToLiveSpecification.Enabled == true),
            It.IsAny<CancellationToken>()));
    }

    [Fact]
    public async Task Provisioning_AcceptsConcurrentCreateAndTtlRaces()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .SetupSequence(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ResourceNotFoundException("missing"))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidTable(TableStatus.CREATING) })
            .ReturnsAsync(new DescribeTableResponse { Table = ValidTable() });
        client
            .Setup(database => database.CreateTableAsync(It.IsAny<CreateTableRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ResourceInUseException("race"));
        client
            .SetupSequence(database => database.DescribeTimeToLiveAsync(It.IsAny<DescribeTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Ttl(TimeToLiveStatus.DISABLED))
            .ReturnsAsync(Ttl(TimeToLiveStatus.ENABLED));
        client
            .Setup(database => database.UpdateTimeToLiveAsync(It.IsAny<UpdateTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonDynamoDBException("race") { ErrorCode = "ValidationException" });
        client
            .Setup(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetItemResponse { Item = [] });
        using var store = CreateStore(client);

        Assert.Null(await store.LoadAsync("flow"));
    }

    [Fact]
    public async Task Provisioning_RejectsMissingTableDisabledTtlAndUnresolvedTtlRace()
    {
        var missingClient = new Mock<IAmazonDynamoDB>();
        missingClient
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ResourceNotFoundException("missing"));
        using var missingStore = CreateStore(missingClient, autoCreate: false);
        await Assert.ThrowsAsync<InvalidOperationException>(() => missingStore.LoadAsync("flow"));

        var disabledClient = new Mock<IAmazonDynamoDB>();
        disabledClient
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidTable() });
        disabledClient
            .Setup(database => database.DescribeTimeToLiveAsync(It.IsAny<DescribeTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Ttl(TimeToLiveStatus.DISABLED));
        using var disabledStore = CreateStore(disabledClient, autoCreate: false);
        await Assert.ThrowsAsync<InvalidOperationException>(() => disabledStore.LoadAsync("flow"));

        var racedClient = new Mock<IAmazonDynamoDB>();
        racedClient
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidTable() });
        racedClient
            .SetupSequence(database => database.DescribeTimeToLiveAsync(It.IsAny<DescribeTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Ttl(TimeToLiveStatus.DISABLED))
            .ReturnsAsync(Ttl(TimeToLiveStatus.DISABLED));
        racedClient
            .Setup(database => database.UpdateTimeToLiveAsync(It.IsAny<UpdateTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonDynamoDBException("not the safe race") { ErrorCode = "ValidationException" });
        using var racedStore = CreateStore(racedClient);
        await Assert.ThrowsAsync<AmazonDynamoDBException>(() => racedStore.LoadAsync("flow"));
    }

    [Fact]
    public async Task Store_RejectsConditionalLeaseAndUnreadableRevision_AndDisposesOwnedClient()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidTable() });
        var state = CreateState("flow");
        client
            .Setup(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetItemResponse
            {
                Item = new Dictionary<string, AttributeValue>
                {
                    ["expires_at"] = new() { N = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeSeconds().ToString() },
                    ["state_json"] = new() { S = JsonSerializer.Serialize(state) }
                }
            });
        client
            .Setup(database => database.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ConditionalCheckFailedException("held"));
        var store = CreateStore(client, enableTtl: false, ownsClient: true);

        Assert.Null(await store.LoadAsync("flow"));
        Assert.False(await store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        store.Dispose();
        client.Verify(database => database.Dispose());
    }

    [Fact]
    public async Task Load_RejectsMalformedItemsAndReturnsMatchingRevision()
    {
        var client = ReadyClient();
        var state = CreateState("flow");
        var futureExpiry = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeSeconds().ToString();
        var expired = DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeSeconds().ToString();
        var json = JsonSerializer.Serialize(state);
        client
            .SetupSequence(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Item(("state_json", new AttributeValue { S = json })))
            .ReturnsAsync(Item(
                ("expires_at", new AttributeValue { N = "not-a-number" }),
                ("state_json", new AttributeValue { S = json })))
            .ReturnsAsync(Item(
                ("expires_at", new AttributeValue { N = expired }),
                ("state_json", new AttributeValue { S = json })))
            .ReturnsAsync(Item(("expires_at", new AttributeValue { N = futureExpiry })))
            .ReturnsAsync(Item(
                ("expires_at", new AttributeValue { N = futureExpiry }),
                ("state_json", new AttributeValue { S = "" })))
            .ReturnsAsync(Item(
                ("expires_at", new AttributeValue { N = futureExpiry }),
                ("state_json", new AttributeValue { S = json })))
            .ReturnsAsync(Item(
                ("expires_at", new AttributeValue { N = futureExpiry }),
                ("state_json", new AttributeValue { S = json }),
                ("revision", new AttributeValue { N = "bad" })))
            .ReturnsAsync(Item(
                ("expires_at", new AttributeValue { N = futureExpiry }),
                ("state_json", new AttributeValue { S = json }),
                ("revision", new AttributeValue { N = "1" })))
            .ReturnsAsync(Item(
                ("expires_at", new AttributeValue { N = futureExpiry }),
                ("state_json", new AttributeValue { S = json }),
                ("revision", new AttributeValue { N = "0" })));
        using var store = CreateStore(client, enableTtl: false);

        for (var index = 0; index < 8; index++)
            Assert.Null(await store.LoadAsync("flow"));

        Assert.Equal("flow", (await store.LoadAsync("flow"))?.FlowId);
    }

    [Fact]
    public async Task Provisioning_RejectsInvalidSchemaAndMismatchedTtlAttribute()
    {
        TableDescription[] invalidTables =
        [
            new()
            {
                TableStatus = TableStatus.ACTIVE,
                KeySchema = [],
                AttributeDefinitions = []
            },
            new()
            {
                TableStatus = TableStatus.ACTIVE,
                KeySchema = [new KeySchemaElement("other", KeyType.HASH)],
                AttributeDefinitions = [new AttributeDefinition("flow_id", ScalarAttributeType.S)]
            },
            new()
            {
                TableStatus = TableStatus.ACTIVE,
                KeySchema = [new KeySchemaElement("flow_id", KeyType.HASH)],
                AttributeDefinitions = [new AttributeDefinition("flow_id", ScalarAttributeType.N)]
            }
        ];

        foreach (var table in invalidTables)
        {
            var invalidClient = new Mock<IAmazonDynamoDB>();
            invalidClient
                .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DescribeTableResponse { Table = table });
            using var invalidStore = CreateStore(invalidClient, enableTtl: false);
            await Assert.ThrowsAsync<InvalidOperationException>(() => invalidStore.LoadAsync("flow"));
        }

        var ttlClient = ReadyClient();
        ttlClient
            .Setup(database => database.DescribeTimeToLiveAsync(
                It.IsAny<DescribeTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTimeToLiveResponse
            {
                TimeToLiveDescription = new TimeToLiveDescription
                {
                    TimeToLiveStatus = TimeToLiveStatus.ENABLED,
                    AttributeName = "wrong"
                }
            });
        using var ttlStore = CreateStore(ttlClient);
        await Assert.ThrowsAsync<InvalidOperationException>(() => ttlStore.LoadAsync("flow"));

        var enablingClient = ReadyClient();
        enablingClient
            .Setup(database => database.DescribeTimeToLiveAsync(
                It.IsAny<DescribeTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Ttl(TimeToLiveStatus.ENABLING));
        enablingClient
            .Setup(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetItemResponse { Item = [] });
        using var enablingStore = CreateStore(enablingClient);
        Assert.Null(await enablingStore.LoadAsync("flow"));
        enablingClient.Verify(
            database => database.UpdateTimeToLiveAsync(
                It.IsAny<UpdateTimeToLiveRequest>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Store_HandlesSuccessfulAndConditionalWriteOutcomes()
    {
        var client = ReadyClient();
        client
            .SetupSequence(database => database.PutItemAsync(It.IsAny<PutItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PutItemResponse())
            .ThrowsAsync(new ConditionalCheckFailedException("exists"));
        client
            .SetupSequence(database => database.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateItemResponse())
            .ThrowsAsync(new ConditionalCheckFailedException("stale"))
            .ReturnsAsync(new UpdateItemResponse())
            .ReturnsAsync(new UpdateItemResponse())
            .ReturnsAsync(new UpdateItemResponse())
            .ThrowsAsync(new ConditionalCheckFailedException("released"));
        client
            .SetupSequence(database => database.DeleteItemAsync(It.IsAny<DeleteItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteItemResponse { Attributes = Item(("flow_id", new AttributeValue { S = "flow" })).Item })
            .ReturnsAsync(new DeleteItemResponse { Attributes = [] });
        using var store = CreateStore(client, enableTtl: false);
        var state = CreateState("flow");

        Assert.True(await store.TryCreateAsync("flow", state, TimeSpan.FromSeconds(1)));
        Assert.False(await store.TryCreateAsync("flow", state, TimeSpan.FromSeconds(1)));

        state.Revision = 1;
        Assert.True(await store.TryUpdateAsync("flow", state, expectedRevision: 0, TimeSpan.FromSeconds(1), "owner"));
        Assert.False(await store.TryUpdateAsync("flow", state, expectedRevision: 0, TimeSpan.FromSeconds(1)));
        Assert.True(await store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromSeconds(1)));
        Assert.True(await store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromSeconds(1)));
        await store.ReleaseLeaseAsync("flow", "owner");
        await store.ReleaseLeaseAsync("flow", "owner");
        Assert.True(await store.TryDeleteAsync("flow"));
        Assert.False(await store.TryDeleteAsync("flow"));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.Zero));
    }

    [Fact]
    public async Task Provisioning_WaitsForAnExistingCreatingTableAndHandlesMissingTtlDescription()
    {
        var creatingClient = new Mock<IAmazonDynamoDB>();
        creatingClient
            .SetupSequence(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidTable(TableStatus.CREATING) })
            .ReturnsAsync(new DescribeTableResponse { Table = ValidTable() });
        creatingClient
            .Setup(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetItemResponse { Item = [] });
        using (var creatingStore = CreateStore(creatingClient, enableTtl: false))
            Assert.Null(await creatingStore.LoadAsync("flow"));

        var ttlClient = ReadyClient();
        ttlClient
            .Setup(database => database.DescribeTimeToLiveAsync(
                It.IsAny<DescribeTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTimeToLiveResponse());
        ttlClient
            .Setup(database => database.UpdateTimeToLiveAsync(
                It.IsAny<UpdateTimeToLiveRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateTimeToLiveResponse());
        ttlClient
            .Setup(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetItemResponse { Item = [] });
        using var ttlStore = CreateStore(ttlClient);
        Assert.Null(await ttlStore.LoadAsync("flow"));
    }

    private static DynamoDbFlowStateStore CreateStore(
        Mock<IAmazonDynamoDB> client,
        bool autoCreate = true,
        bool enableTtl = true,
        bool ownsClient = false)
        => new(client.Object, Options.Create(new DynamoDbDurableFlowOptions
        {
            TableName = "flows",
            AutoCreateTable = autoCreate,
            EnableTimeToLive = enableTtl,
            TimeToLiveAttributeName = "expires_at"
        }), ownsClient);

    private static Mock<IAmazonDynamoDB> ReadyClient()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidTable() });
        return client;
    }

    private static GetItemResponse Item(params (string Name, AttributeValue Value)[] attributes)
        => new()
        {
            Item = attributes.ToDictionary(attribute => attribute.Name, attribute => attribute.Value)
        };

    private static TableDescription ValidTable(string? status = null) => new()
    {
        TableStatus = status ?? TableStatus.ACTIVE,
        KeySchema = [new KeySchemaElement("flow_id", KeyType.HASH)],
        AttributeDefinitions = [new AttributeDefinition("flow_id", ScalarAttributeType.S)]
    };

    private static DescribeTimeToLiveResponse Ttl(string status) => new()
    {
        TimeToLiveDescription = new TimeToLiveDescription
        {
            AttributeName = "expires_at",
            TimeToLiveStatus = status
        }
    };

    private static FlowState CreateState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(TestOnboardingFlow).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };
}
