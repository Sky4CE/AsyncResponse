# Summary - AsyncResponse (Release / net8.0+net10.0 / unit+integration)
<details open><summary>Summary</summary>

|||
|:---|:---|
| Generated on: | 09/02/2026 - 15:15:30 |
| Coverage date: | 09/02/2026 - 15:01:27 - 09/02/2026 - 15:13:04 |
| Parser: | MultiReport (16x Cobertura) |
| Assemblies: | 27 |
| Classes: | 456 |
| Files: | 225 |
| **Line coverage:** | 95.1% (25901 of 27208) |
| Covered lines: | 25901 |
| Uncovered lines: | 1307 |
| Coverable lines: | 27208 |
| Total lines: | 52346 |
| **Branch coverage:** | 89% (8839 of 9926) |
| Covered branches: | 8839 |
| Total branches: | 9926 |
| **Method coverage:** | [Feature is only available for sponsors](https://reportgenerator.io/pro) |

</details>

## Coverage
<details><summary>AsyncResponse.Abstractions - 97.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Abstractions**|**97.9%**|**100%**|
|AsyncResponse.AsyncResponseContext|100%|100%|
|AsyncResponse.AsyncResponseDomainFailureException|100%||
|AsyncResponse.AsyncResponseIndeterminateDeliveryException|100%||
|AsyncResponse.AsyncResponsePayloadReflection|88.2%|100%|
|AsyncResponse.AsyncResponseReplyTarget|100%||
|AsyncResponse.AsyncResponseRequestContext|100%||
|AsyncResponse.CallbackParam|100%||
|AsyncResponse.DurableFlowFailedException|100%||
|AsyncResponse.DurableFlowIdConflictException|100%||
|AsyncResponse.DurableFlowNotDispatchedException|100%||
|AsyncResponse.DurableFlowRunEvent|66.6%||
|AsyncResponse.DurableFlowStepEvent|100%||
|AsyncResponse.FlowState|100%||
|AsyncResponse.FlowStateSchema|100%||
|AsyncResponse.FlowStateUnreadableException|100%||
|AsyncResponse.FlowStepState|100%||
|AsyncResponse.IAsyncResponseIngress|100%||
|AsyncResponse.IAsyncResponsePayload|100%||
|AsyncResponse.IDurableFlowExecutionObserver|80%||
|AsyncResponse.Placeholder|100%||
|AsyncResponse.RecoveryState|100%||
|AsyncResponse.RecoveryStateSchema|100%||
|AsyncResponse.RecoveryStateUnreadableException|100%||
|AsyncResponse.ReflectionCallDto|100%||
|AsyncResponse.ReflectionInvocationDto|100%||
|AsyncResponse.WorkerJobEnvelope|100%||
|AsyncResponse.WorkerJobEnvelopeSchema|100%||

</details>
<details><summary>AsyncResponse.Channels.MongoDB - 95.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.MongoDB**|**95.7%**|**90.3%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|95.3%|92.3%|
|AsyncResponse.Channels.MongoDB.MongoChannelMessageDocument|100%||
|AsyncResponse.Channels.MongoDB.MongoChannelSubscriberDocument|0%||
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannel|68.8%|70%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannelOptions|100%|95.8%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.MongoDB.MongoDbChannelMessage|100%||
|AsyncResponse.Channels.MongoDB.MongoDbChannelStore|99.2%|82.8%|
|AsyncResponse.Channels.MongoDB.MongoDbRecoveryStateStore|100%|90%|
|AsyncResponse.Channels.MongoDB.MongoRecoveryStateDocument|0%||
|AsyncResponse.Internal.MongoNamespaceRegistry|100%|100%|
|AsyncResponse.Internal.MongoOwnershipLedger|100%|85.7%|
|AsyncResponse.Internal.MongoTransientFaults|100%|100%|
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseChannelService<br/>CollectionExtensions|100%|91.6%|

</details>
<details><summary>AsyncResponse.Channels.NATS - 98.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.NATS**|**98.3%**|**94%**|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannel|97.6%|92.1%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.NATS.NatsConsumeLoopException|100%||
|AsyncResponse.Channels.NATS.NatsInboundResponse|100%||
|AsyncResponse.Channels.NATS.NatsKvEntry|100%||
|AsyncResponse.Channels.NATS.NatsKvStoreAdapter|100%|100%|
|AsyncResponse.Channels.NATS.NatsRawRequester|100%||
|AsyncResponse.Channels.NATS.NatsRecoveryStateStore|98.3%|95%|
|AsyncResponse.Channels.NATS.NatsResponseChannelClient|100%|100%|
|AsyncResponse.Channels.NATS.NatsSubjectSchema|100%|100%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseChannelServiceCol<br/>lectionExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.PostgreSQL - 94.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.PostgreSQL**|**94.6%**|**85.7%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|93.2%|89.7%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannel|94.2%|75%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelMessage|100%||
|AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql|98.7%|88.7%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlRecoveryStateStore|100%|90%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|85%|70.3%|
|AsyncResponse.Internal.PostgreSqlTransientFaults|100%|100%|
|AsyncResponse.Internal.RelationalNamePlan|73.3%|83.3%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseChannelServ<br/>iceCollectionExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.Redis - 96.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.Redis**|**96.5%**|**89.2%**|
|AsyncResponse.Channels.Redis.RedisAsyncResponseChannel|96.3%|92.5%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseOptions|100%|100%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.Redis.RedisChannelMessageQueueSubscriber|100%||
|AsyncResponse.Channels.Redis.RedisKeySchema|100%|100%|
|AsyncResponse.Channels.Redis.RedisRecoveryStateStore|95.7%|83.3%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseServiceCollectio<br/>nExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.SqlServer - 95.2%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.SqlServer**|**95.2%**|**84.6%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|93.4%|85.7%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannel|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.SqlServer.SqlServerChannelMessage|100%||
|AsyncResponse.Channels.SqlServer.SqlServerChannelSql|98.5%|88.9%|
|AsyncResponse.Channels.SqlServer.SqlServerRecoveryStateStore|100%|90%|
|AsyncResponse.Internal.RelationalNamePlan|73.3%|83.3%|
|AsyncResponse.Internal.SqlServerRelationVerifier|91.3%|78.3%|
|AsyncResponse.Internal.SqlServerTransientFaults|100%|100%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseChannelServi<br/>ceCollectionExtensions|100%|50%|

</details>
<details><summary>AsyncResponse.Core - 95.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Core**|**95.9%**|**91.2%**|
|AsyncResponse.AsyncResponseBuilder|100%||
|AsyncResponse.AsyncResponseBuilder`1|100%|100%|
|AsyncResponse.AsyncResponseBuilderBase|94.2%|95%|
|AsyncResponse.AsyncResponseChannelMarker|100%||
|AsyncResponse.AsyncResponseChannelOptions|100%|100%|
|AsyncResponse.AsyncResponseContextPropagation|100%|93%|
|AsyncResponse.AsyncResponseDiagnostics|97.6%|93.1%|
|AsyncResponse.AsyncResponseDurableFlowStoreMarker|95.2%|57.1%|
|AsyncResponse.AsyncResponseEnvelope`1|100%||
|AsyncResponse.AsyncResponseEnvelopeConverter`1|100%|98.9%|
|AsyncResponse.AsyncResponseEnvelopeJson|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeOptions`1|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeSchema|100%||
|AsyncResponse.AsyncResponseIngress|99%|89.4%|
|AsyncResponse.AsyncResponseJson|78.6%|100%|
|AsyncResponse.AsyncResponseJsonSerialization|100%||
|AsyncResponse.AsyncResponseOptions|100%||
|AsyncResponse.AsyncResponseRecoveryHealthCheck|100%|97%|
|AsyncResponse.AsyncResponseRecoveryStats|100%||
|AsyncResponse.AsyncResponseRetry|100%|100%|
|AsyncResponse.AsyncResponseStaleRecoveryEntry|100%||
|AsyncResponse.AsyncResponseStartupValidator|98.1%|95%|
|AsyncResponse.AsyncResponseTransportMarker|100%||
|AsyncResponse.AsyncResponseTypeResolution|98.3%|92.8%|
|AsyncResponse.AsyncResponseWatchdog|97.5%|93.5%|
|AsyncResponse.AsyncResponseWatchdogOptions|100%|100%|
|AsyncResponse.AsyncResponseWatchdogReport|100%|100%|
|AsyncResponse.AsyncResponseWatchdogSnapshot|100%||
|AsyncResponse.AsyncResponseWatchdogState|100%|66.6%|
|AsyncResponse.CallbackExpressionConverter|98.2%|88.4%|
|AsyncResponse.CallbackTargetUnresolvableException|100%||
|AsyncResponse.ChannelSerialExecutor|100%|95.8%|
|AsyncResponse.CorrelationIdGuard|96.2%|95%|
|AsyncResponse.CronSchedule|96.8%|95.8%|
|AsyncResponse.DurableAsyncResponseChannelOptions|100%||
|AsyncResponse.DurableFlowContext|89.3%|83.8%|
|AsyncResponse.DurableFlowExecutor|98.3%|94%|
|AsyncResponse.DurableFlowObserverLifetimeAudit|60%|28.5%|
|AsyncResponse.DurableFlowOptions|100%||
|AsyncResponse.DurableFlowRegistration|100%||
|AsyncResponse.DurableFlowService|100%|94.4%|
|AsyncResponse.DurableFlowSuspendedException|100%||
|AsyncResponse.FlowExecutionLease|96.2%|88.8%|
|AsyncResponse.FlowStateConcurrency|100%|100%|
|AsyncResponse.FlowStateJson|100%|100%|
|AsyncResponse.InMemoryAsyncResponseChannel|94.5%|91.4%|
|AsyncResponse.InMemoryAsyncResponseOptions|100%|100%|
|AsyncResponse.InMemoryAsyncResponseWaiter`1|100%||
|AsyncResponse.InMemoryFlowStateStore|100%|90.4%|
|AsyncResponse.InMemoryRecoveryStateStore|96.7%|90.4%|
|AsyncResponse.InMemoryWorkerHost|93.4%|88.2%|
|AsyncResponse.InMemoryWorkerTransport|89.1%|81.4%|
|AsyncResponse.InMemoryWorkerTransportOptions|100%|100%|
|AsyncResponse.JsonSafety|100%|71.4%|
|AsyncResponse.LostSubscriberCallbackDispatcher|98.6%|90.7%|
|AsyncResponse.LostSubscriberDispatchResult|100%||
|AsyncResponse.PayloadRecoveryClassifier|100%|96.4%|
|AsyncResponse.PortableText|100%|100%|
|AsyncResponse.RawJsonResponse|100%|100%|
|AsyncResponse.RecoverableAsyncResponseBuilder|100%||
|AsyncResponse.RecoverableAsyncResponseBuilder`1|100%|100%|
|AsyncResponse.RecoveryClassification|100%||
|AsyncResponse.RecoveryStateObservation|100%||
|AsyncResponse.ReflectionExtensions|100%|98%|
|AsyncResponse.RemoteStackTrace|100%|100%|
|AsyncResponse.ScheduledFlowOptions|100%||
|AsyncResponse.ScheduledFlowRegistration|100%||
|AsyncResponse.ScheduledFlowService|69.5%|61.5%|
|AsyncResponse.SerialExecutorRegistry|93.1%|94.8%|
|AsyncResponse.ShutdownBudgetValidator|100%|100%|
|AsyncResponse.UnresolvableTypeNames|95%|75%|
|AsyncResponse.WorkerJobExecutor|95.9%|80.9%|
|AsyncResponse.WorkerJobSkewScope|93.3%|87.5%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAllowList|100%|83.3%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAuthorization<br/>Extensions|100%||
|Microsoft.Extensions.DependencyInjection.AsyncResponseCoreServiceCollection<br/>Extensions|100%|94.7%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseHealthCheckExtensions|100%||
|Microsoft.Extensions.DependencyInjection.AsyncResponseRegistrationBuilder|100%||
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.Cosmos - 93.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Cosmos**|**93.6%**|**92.6%**|
|AsyncResponse.DurableFlows.Cosmos.CosmosDurableFlowOptions|82.6%|81.2%|
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateDocument|100%||
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateStore|96.1%|92.6%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|88.1%|95.1%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|Microsoft.Extensions.DependencyInjection.CosmosDurableFlowServiceCollection<br/>Extensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.DynamoDB - 95.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.DynamoDB**|**95.4%**|**87.2%**|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbDurableFlowOptions|77.2%|61.1%|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbFlowStateStore|99.2%|87.7%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|88.1%|95.1%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|Microsoft.Extensions.DependencyInjection.DynamoDbDurableFlowServiceCollecti<br/>onExtensions|100%|50%|

</details>
<details><summary>AsyncResponse.DurableFlows.EFCore - 96.1%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.EFCore**|**96.1%**|**97%**|
|AsyncResponse.DurableFlows.EFCore.DurableFlowStateRecord|71.4%||
|AsyncResponse.DurableFlows.EFCore.EFCoreDurableFlowModelBuilderExtensions|100%|100%|
|AsyncResponse.DurableFlows.EFCore.EFCoreDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.EFCore.EFCoreFlowStateStore`1|99.4%|100%|
|AsyncResponse.DurableFlows.EFCore.FlowIdCollationRules|100%|100%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|89.2%|95.1%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|Microsoft.Extensions.DependencyInjection.EFCoreDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.MongoDB - 93.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.MongoDB**|**93.8%**|**85.1%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|88.1%|95.1%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.MongoDB.MongoDbDurableFlowOptions|88.2%|83.3%|
|AsyncResponse.DurableFlows.MongoDB.MongoDbFlowStateStore|98.9%|90.9%|
|AsyncResponse.DurableFlows.MongoDB.MongoFlowStateDocument|100%||
|AsyncResponse.Internal.MongoNamespaceRegistry|75%|75%|
|AsyncResponse.Internal.MongoOwnershipLedger|89.1%|53.5%|
|Microsoft.Extensions.DependencyInjection.MongoDurableFlowServiceCollectionE<br/>xtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.MySql - 99.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.MySql**|**99.7%**|**92.9%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|98.3%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.MySql.MySqlDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.MySql.MySqlFlowStateStore|99.6%|89.3%|
|Microsoft.Extensions.DependencyInjection.MySqlDurableFlowServiceCollectionE<br/>xtensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.Oracle - 96.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Oracle**|**96.6%**|**90.8%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|98.3%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.Oracle.OracleDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.Oracle.OracleFlowStateStore|95.6%|86.3%|
|Microsoft.Extensions.DependencyInjection.OracleDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.PostgreSQL - 89.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.PostgreSQL**|**89.5%**|**78.5%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|89.2%|95.1%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlFlowStateStore|98.1%|83.3%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|77.3%|67.7%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlDurableFlowServiceCollec<br/>tionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.Sqlite - 99.2%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Sqlite**|**99.2%**|**91.1%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|98.3%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.Sqlite.SqliteDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.Sqlite.SqliteFlowStateStore|98.9%|86.4%|
|Microsoft.Extensions.DependencyInjection.SqliteDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.SqlServer - 79.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.SqlServer**|**79.8%**|**74.6%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|98.3%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.SqlServer.SqlServerDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.SqlServer.SqlServerFlowStateStore|87.5%|80%|
|AsyncResponse.Internal.SqlServerRelationVerifier|66.6%|67.5%|
|Microsoft.Extensions.DependencyInjection.SqlServerDurableFlowServiceCollect<br/>ionExtensions|100%||

</details>
<details><summary>AsyncResponse.Testing - 89.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Testing**|**89.3%**|**83.6%**|
|AsyncResponse.Testing.AsyncResponseTestHarness|93.3%|84.2%|
|AsyncResponse.Testing.AsyncResponseTestHarnessOptions|100%||
|AsyncResponse.Testing.FlowProbe|85.8%|80.2%|
|AsyncResponse.Testing.FlowProbeEvent|100%||
|AsyncResponse.Testing.FlowRunHandle|88.4%|75%|
|AsyncResponse.Testing.FlowTestHarness|100%|50%|
|AsyncResponse.Testing.SimulatedCrashException|100%|100%|
|AsyncResponse.Testing.VirtualTimeProvider|84.1%|92.5%|

</details>
<details><summary>AsyncResponse.Transports.AzureServiceBus - 96.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.AzureServiceBus**|**96.6%**|**92.8%**|
|AsyncResponse.Transports.AzureServiceBus.AwaitingAzureServiceBusMessageDisp<br/>atcher|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusAsyncResponseOption<br/>s|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusBackgroundFailureCo<br/>ntext|90%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientResolver|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusCorrelationIdExtrac<br/>tor|90.9%|90.9%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusMessageDispatcher|98.8%|97%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOptionsValidator|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOutboundMessage|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReceiverAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetOptions|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusResponseIngressSubs<br/>criber|100%|50%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSenderAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberOptions|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberService|85%|90%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusTransportDelivery|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerSubscriber|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerTransport|100%|83.3%|
|AsyncResponse.Transports.AzureServiceBus.QueuedAzureServiceBusMessageDispat<br/>cher|93.7%|91.6%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|93.1%|
|AsyncResponse.Transports.SubscriberSupervisor|90.9%|50%|
|Microsoft.Extensions.DependencyInjection.AzureServiceBusAsyncResponseServic<br/>eCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.GooglePubSub - 98.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.GooglePubSub**|**98.6%**|**96.5%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|93.1%|
|AsyncResponse.Transports.GooglePubSub.AwaitingGooglePubSubMessageDispatcher|75%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubAsyncResponseOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubBackgroundFailureContext|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubMessageDispatcher|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubOptionsValidator|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubPublisherClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberOptions|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberService|93.6%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerSubscriber|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerTransport|100%|100%|
|AsyncResponse.Transports.GooglePubSub.QueuedGooglePubSubMessageDispatcher|100%|100%|
|AsyncResponse.Transports.SubscriberSupervisor|90.9%|50%|
|Microsoft.Extensions.DependencyInjection.GooglePubSubAsyncResponseServiceCo<br/>llectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.Kafka - 98.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Kafka**|**98.8%**|**95.8%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|93.1%|
|AsyncResponse.Transports.Kafka.AwaitingKafkaMessageDispatcher|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaBackgroundFailureContext|95.6%||
|AsyncResponse.Transports.Kafka.KafkaConsumerClientAdapter|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaConsumerClientFactory|100%|83.3%|
|AsyncResponse.Transports.Kafka.KafkaCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaDelivery|100%||
|AsyncResponse.Transports.Kafka.KafkaIncomingMessage|100%||
|AsyncResponse.Transports.Kafka.KafkaMessageDispatcher|99.5%|94.4%|
|AsyncResponse.Transports.Kafka.KafkaProducerClientAdapter|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaPublishResult|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetProvider|100%|93.7%|
|AsyncResponse.Transports.Kafka.KafkaResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaSubscriberService|99.1%|92.8%|
|AsyncResponse.Transports.Kafka.KafkaTransportClientDefaults|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportHeader|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportOptionsValidator|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportRetry|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportTopicSchema|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaWorkerSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaWorkerTransport|100%|100%|
|AsyncResponse.Transports.Kafka.QueuedKafkaMessageDispatcher|94%|100%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|50%|
|Microsoft.Extensions.DependencyInjection.KafkaAsyncResponseTransportService<br/>CollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.MongoDB - 92.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.MongoDB**|**92.3%**|**84%**|
|AsyncResponse.Internal.MongoNamespaceRegistry|75%|75%|
|AsyncResponse.Internal.MongoOwnershipLedger|67.5%|50%|
|AsyncResponse.Internal.MongoTransientFaults|100%|68.7%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|93.1%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|88.1%|98.2%|
|AsyncResponse.Transports.DbTransportHeaders|0%|0%|
|AsyncResponse.Transports.MongoDB.LenientTransportHeaderSerializer|94.4%|93.3%|
|AsyncResponse.Transports.MongoDB.MongoDbAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.MongoDB.MongoDbBackgroundFailureContext|91.6%||
|AsyncResponse.Transports.MongoDB.MongoDbCorrelationIdExtractor|100%||
|AsyncResponse.Transports.MongoDB.MongoDbMessageDispatcher|100%||
|AsyncResponse.Transports.MongoDB.MongoDbReplyTargetOptions|100%||
|AsyncResponse.Transports.MongoDB.MongoDbReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.MongoDB.MongoDbSubscriberOptions|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbSubscriberService|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbTransportDelivery|100%||
|AsyncResponse.Transports.MongoDB.MongoDbTransportOptionsValidator|96.9%|92.8%|
|AsyncResponse.Transports.MongoDB.MongoDbTransportRetry|100%||
|AsyncResponse.Transports.MongoDB.MongoDbTransportStore|95.5%|83.3%|
|AsyncResponse.Transports.MongoDB.MongoDbWorkerSubscriber|100%||
|AsyncResponse.Transports.MongoDB.MongoDbWorkerTransport|100%|64.2%|
|AsyncResponse.Transports.MongoDB.MongoTransportMessageDocument|100%||
|AsyncResponse.Transports.SubscriberSupervisor|100%|50%|
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseTransportServi<br/>ceCollectionExtensions|97.9%|87.5%|

</details>
<details><summary>AsyncResponse.Transports.NATS - 97.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.NATS**|**97.5%**|**95.3%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|93.1%|
|AsyncResponse.Transports.NATS.INatsJetStreamTransport|0%||
|AsyncResponse.Transports.NATS.NatsAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.NATS.NatsBackgroundFailureContext|85%||
|AsyncResponse.Transports.NATS.NatsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.NATS.NatsJetStreamTransportAdapter|100%|96.1%|
|AsyncResponse.Transports.NATS.NatsJobDelivery|100%||
|AsyncResponse.Transports.NATS.NatsMessageDispatcher|96.6%|97.7%|
|AsyncResponse.Transports.NATS.NatsReplyTargetOptions|100%||
|AsyncResponse.Transports.NATS.NatsReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.NATS.NatsResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.NATS.NatsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.NATS.NatsSubscriberService|90.4%|95.4%|
|AsyncResponse.Transports.NATS.NatsTransportOptionsValidator|100%|100%|
|AsyncResponse.Transports.NATS.NatsTransportRetry|100%|100%|
|AsyncResponse.Transports.NATS.NatsTransportSubjectSchema|100%|100%|
|AsyncResponse.Transports.NATS.NatsWorkerSubscriber|100%||
|AsyncResponse.Transports.NATS.NatsWorkerTransport|100%|75%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|100%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseTransportServiceC<br/>ollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.PostgreSQL - 92.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.PostgreSQL**|**92.7%**|**87.5%**|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|80.6%|68.6%|
|AsyncResponse.Internal.PostgreSqlTransientFaults|100%|100%|
|AsyncResponse.Internal.RelationalNamePlan|100%|91.6%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|93.1%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|88.1%|98.2%|
|AsyncResponse.Transports.DbTransportHeaders|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlBackgroundFailureContext|91.6%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlCorrelationIdExtractor|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlMessageDispatcher|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberOptions|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberService|93.9%|95%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportDelivery|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportOptionsValidator|100%|95.8%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportRetry|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportStore|94.7%|94.6%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerSubscriber|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerTransport|95.8%|85.7%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|50%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseTransportSe<br/>rviceCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.RabbitMQ - 95.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.RabbitMQ**|**95.6%**|**89.2%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|93.1%|
|AsyncResponse.Transports.RabbitMQ.AwaitingRabbitMqMessageDispatcher|75%|86.3%|
|AsyncResponse.Transports.RabbitMQ.QueuedRabbitMqMessageDispatcher|88.1%|83.3%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqAsyncResponseOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqBackgroundFailureContext|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqChannelAdapter|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionAdapter|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionFactoryAdapter|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqConsumer|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqCorrelationIdExtractor|95.2%|95.4%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqDelivery|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqMessageDispatcher|100%|86.4%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqOptionsValidator|100%|83.3%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetProvider|100%|89.2%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberOptions|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberService|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqTopology|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerSubscriber|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerTransport|96.9%|85.7%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|50%|
|Microsoft.Extensions.DependencyInjection.RabbitMqAsyncResponseServiceCollec<br/>tionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.Redis - 96.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Redis**|**96.9%**|**95.5%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|94.8%|
|AsyncResponse.Transports.Redis.AwaitingRedisMessageDispatcher|100%|100%|
|AsyncResponse.Transports.Redis.IRedisStreamDatabase|0%||
|AsyncResponse.Transports.Redis.QueuedRedisMessageDispatcher|95.2%|100%|
|AsyncResponse.Transports.Redis.RedisAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Redis.RedisBackgroundFailureContext|85%||
|AsyncResponse.Transports.Redis.RedisCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Redis.RedisMessageDispatcher|100%|96%|
|AsyncResponse.Transports.Redis.RedisReplyTargetOptions|100%||
|AsyncResponse.Transports.Redis.RedisReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.Redis.RedisResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisStreamDatabaseAdapter|100%|75%|
|AsyncResponse.Transports.Redis.RedisStreamDelivery|100%||
|AsyncResponse.Transports.Redis.RedisSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Redis.RedisSubscriberService|92.3%|93.1%|
|AsyncResponse.Transports.Redis.RedisTransportKeySchema|100%|100%|
|AsyncResponse.Transports.Redis.RedisTransportOptionsValidator|92.3%|96.1%|
|AsyncResponse.Transports.Redis.RedisTransportRetry|100%|100%|
|AsyncResponse.Transports.Redis.RedisWorkerSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisWorkerTransport|100%|100%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|50%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseTransportService<br/>CollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.SqlServer - 91.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SqlServer**|**91.4%**|**84.4%**|
|AsyncResponse.Internal.RelationalNamePlan|53.3%|41.6%|
|AsyncResponse.Internal.SqlServerRelationVerifier|80.6%|72.9%|
|AsyncResponse.Internal.SqlServerTransientFaults|100%|100%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|93.1%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|88.1%|98.2%|
|AsyncResponse.Transports.DbTransportHeaders|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerBackgroundFailureContext|91.6%||
|AsyncResponse.Transports.SqlServer.SqlServerCorrelationIdExtractor|100%||
|AsyncResponse.Transports.SqlServer.SqlServerMessageDispatcher|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberService|98.3%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportDelivery|100%||
|AsyncResponse.Transports.SqlServer.SqlServerTransportOptionsValidator|100%|96.2%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportRetry|100%||
|AsyncResponse.Transports.SqlServer.SqlServerTransportStore|94.6%|93.3%|
|AsyncResponse.Transports.SqlServer.SqlServerWorkerSubscriber|100%||
|AsyncResponse.Transports.SqlServer.SqlServerWorkerTransport|95.8%|64.2%|
|AsyncResponse.Transports.SubscriberSupervisor|63.6%|50%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseTransportSer<br/>viceCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.SQS - 99.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SQS**|**99.3%**|**90.8%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99%|93.1%|
|AsyncResponse.Transports.SQS.AwaitingSqsMessageDispatcher|100%|100%|
|AsyncResponse.Transports.SQS.QueuedSqsMessageDispatcher|99.1%|92.8%|
|AsyncResponse.Transports.SQS.SqsAsyncResponseOptions|100%||
|AsyncResponse.Transports.SQS.SqsBackgroundFailureContext|100%||
|AsyncResponse.Transports.SQS.SqsClientAdapter|100%|97.5%|
|AsyncResponse.Transports.SQS.SqsClientFactory|100%|70%|
|AsyncResponse.Transports.SQS.SqsClientResolver|100%|100%|
|AsyncResponse.Transports.SQS.SqsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.SQS.SqsMessageDispatcher|98.9%|64.2%|
|AsyncResponse.Transports.SQS.SqsOptionsValidator|100%|96.9%|
|AsyncResponse.Transports.SQS.SqsOutboundMessage|100%||
|AsyncResponse.Transports.SQS.SqsQueueAddress|100%|87.5%|
|AsyncResponse.Transports.SQS.SqsQueueProvisioningService|98.3%|100%|
|AsyncResponse.Transports.SQS.SqsReceiveRequest|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetOptions|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetProvider|100%|91.6%|
|AsyncResponse.Transports.SQS.SqsResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.SQS.SqsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SQS.SqsSubscriberService|100%|96.4%|
|AsyncResponse.Transports.SQS.SqsTransportDelivery|100%||
|AsyncResponse.Transports.SQS.SqsWorkerSubscriber|100%||
|AsyncResponse.Transports.SQS.SqsWorkerTransport|96.7%|88%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|50%|
|Microsoft.Extensions.DependencyInjection.SqsAsyncResponseServiceCollectionE<br/>xtensions|100%||

</details>
