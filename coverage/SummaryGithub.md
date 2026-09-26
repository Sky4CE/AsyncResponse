# Summary - AsyncResponse (Release / net8.0+net10.0 / unit+integration)
<details open><summary>Summary</summary>

|||
|:---|:---|
| Generated on: | 09/26/2026 - 00:19:06 |
| Coverage date: | 09/26/2026 - 00:02:29 - 09/26/2026 - 00:16:20 |
| Parser: | MultiReport (16x Cobertura) |
| Assemblies: | 27 |
| Classes: | 487 |
| Files: | 245 |
| **Line coverage:** | 94.8% (31932 of 33667) |
| Covered lines: | 31932 |
| Uncovered lines: | 1735 |
| Coverable lines: | 33667 |
| Total lines: | 68078 |
| **Branch coverage:** | 88.8% (12072 of 13580) |
| Covered branches: | 12072 |
| Total branches: | 13580 |
| **Method coverage:** | [Feature is only available for sponsors](https://reportgenerator.io/pro) |

</details>

## Coverage
<details><summary>AsyncResponse.Abstractions - 98.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Abstractions**|**98.7%**|**100%**|
|AsyncResponse.AsyncResponseContext|100%|100%|
|AsyncResponse.AsyncResponseDomainFailureException|100%||
|AsyncResponse.AsyncResponseIndeterminateDeliveryException|100%||
|AsyncResponse.AsyncResponsePayloadReflection|88.2%|100%|
|AsyncResponse.AsyncResponseReplyTarget|100%||
|AsyncResponse.AsyncResponseRequestContext|100%||
|AsyncResponse.CallbackParam|100%||
|AsyncResponse.DurableFlowFailedException|100%||
|AsyncResponse.DurableFlowIdConflictException|100%||
|AsyncResponse.DurableFlowInterruptedException|100%||
|AsyncResponse.DurableFlowLeaseContendedException|100%||
|AsyncResponse.DurableFlowNotDispatchedException|100%||
|AsyncResponse.DurableFlowRunEvent|66.6%||
|AsyncResponse.DurableFlowStepEvent|100%||
|AsyncResponse.FlowLeaseObservation|100%||
|AsyncResponse.FlowState|100%||
|AsyncResponse.FlowStateSchema|100%||
|AsyncResponse.FlowStateTooLargeException|100%||
|AsyncResponse.FlowStateUnreadableException|100%||
|AsyncResponse.FlowStepState|100%||
|AsyncResponse.IAsyncResponseIngress|100%||
|AsyncResponse.IAsyncResponsePayload|100%||
|AsyncResponse.IDurableFlowExecutionObserver|100%||
|AsyncResponse.IFlowStateStore|100%||
|AsyncResponse.Placeholder|100%||
|AsyncResponse.RecoveryState|100%||
|AsyncResponse.RecoveryStateSchema|100%||
|AsyncResponse.RecoveryStateUnreadableException|100%||
|AsyncResponse.ReflectionCallDto|100%||
|AsyncResponse.ReflectionInvocationDto|100%||
|AsyncResponse.WorkerJobEnvelope|100%||
|AsyncResponse.WorkerJobEnvelopeSchema|100%||
|AsyncResponse.WorkerJobTooLargeException|100%||

</details>
<details><summary>AsyncResponse.Channels.MongoDB - 97.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.MongoDB**|**97.4%**|**91.8%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|96.4%|93%|
|AsyncResponse.Channels.DbRecoveryStateStoreBase|100%|90%|
|AsyncResponse.Channels.MongoDB.MongoChannelMessageDocument|100%||
|AsyncResponse.Channels.MongoDB.MongoChannelSubscriberDocument|60%||
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannel|95.3%|93.7%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannelOptions|100%|95.8%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.MongoDB.MongoDbChannelMessage|100%||
|AsyncResponse.Channels.MongoDB.MongoDbChannelStore|99.1%|83.7%|
|AsyncResponse.Channels.MongoDB.MongoDbRecoveryStateStore|100%||
|AsyncResponse.Channels.MongoDB.MongoRecoveryStateDocument|50%||
|AsyncResponse.Internal.MongoNamespaceRegistry|100%|100%|
|AsyncResponse.Internal.MongoOwnershipLedger|100%|85.7%|
|AsyncResponse.Internal.MongoTransientFaults|100%|100%|
|AsyncResponse.Internal.MongoWriteConcerns|100%|100%|
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseChannelService<br/>CollectionExtensions|100%|91.6%|

</details>
<details><summary>AsyncResponse.Channels.NATS - 97.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.NATS**|**97.4%**|**94.3%**|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannel|96.7%|93.1%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.NATS.NatsConsumeLoopException|100%||
|AsyncResponse.Channels.NATS.NatsInboundResponse|100%||
|AsyncResponse.Channels.NATS.NatsKvEntry|100%||
|AsyncResponse.Channels.NATS.NatsKvStoreAdapter|96.4%|93.3%|
|AsyncResponse.Channels.NATS.NatsRawRequester|100%||
|AsyncResponse.Channels.NATS.NatsRecoveryStateStore|98.1%|95.1%|
|AsyncResponse.Channels.NATS.NatsResponseChannelClient|100%|100%|
|AsyncResponse.Channels.NATS.NatsSubjectSchema|100%|100%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseChannelServiceCol<br/>lectionExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.PostgreSQL - 93.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.PostgreSQL**|**93.6%**|**83.8%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|90.3%|82.9%|
|AsyncResponse.Channels.DbRecoveryStateStoreBase|100%|90%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannel|96%|80%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelMessage|100%||
|AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql|98.7%|88.7%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlRecoveryStateStore|100%||
|AsyncResponse.Internal.OpportunisticPrune|100%|85%|
|AsyncResponse.Internal.PostgreSqlListenConnection|92%|100%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|88.7%|77.4%|
|AsyncResponse.Internal.PostgreSqlTransientFaults|100%|100%|
|AsyncResponse.Internal.RelationalNamePlan|73.3%|83.3%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseChannelServ<br/>iceCollectionExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.Redis - 95.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.Redis**|**95.7%**|**89%**|
|AsyncResponse.Channels.Redis.RedisAsyncResponseChannel|95.3%|89.8%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseOptions|100%|100%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.Redis.RedisChannelMessageQueueSubscriber|100%||
|AsyncResponse.Channels.Redis.RedisClusterNodeTable|100%|97.2%|
|AsyncResponse.Channels.Redis.RedisKeySchema|100%|100%|
|AsyncResponse.Channels.Redis.RedisRecoveryStateStore|94.5%|83.3%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseServiceCollectio<br/>nExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.SqlServer - 93.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.SqlServer**|**93.7%**|**83.5%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|90.1%|82.3%|
|AsyncResponse.Channels.DbRecoveryStateStoreBase|100%|90%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannel|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.SqlServer.SqlServerChannelMessage|100%||
|AsyncResponse.Channels.SqlServer.SqlServerChannelSql|98.6%|89.8%|
|AsyncResponse.Channels.SqlServer.SqlServerRecoveryStateStore|100%||
|AsyncResponse.Internal.OpportunisticPrune|100%|85%|
|AsyncResponse.Internal.RelationalNamePlan|73.3%|83.3%|
|AsyncResponse.Internal.SqlServerRelationVerifier|91.8%|80%|
|AsyncResponse.Internal.SqlServerTransientFaults|100%|100%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseChannelServi<br/>ceCollectionExtensions|100%|50%|

</details>
<details><summary>AsyncResponse.Core - 95.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Core**|**95.3%**|**91.4%**|
|AsyncResponse.AsyncResponseBuilder|100%||
|AsyncResponse.AsyncResponseBuilder`1|100%|100%|
|AsyncResponse.AsyncResponseBuilderBase|96.3%|100%|
|AsyncResponse.AsyncResponseChannelMarker|100%||
|AsyncResponse.AsyncResponseChannelOptions|100%|100%|
|AsyncResponse.AsyncResponseContextPropagation|100%|93%|
|AsyncResponse.AsyncResponseDiagnostics|98%|89.5%|
|AsyncResponse.AsyncResponseDurableFlowStoreMarker|95.2%|57.1%|
|AsyncResponse.AsyncResponseEnvelope`1|100%||
|AsyncResponse.AsyncResponseEnvelopeConverter`1|100%|98.9%|
|AsyncResponse.AsyncResponseEnvelopeJson|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeOptions`1|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeSchema|100%||
|AsyncResponse.AsyncResponseIngress|99.1%|91.3%|
|AsyncResponse.AsyncResponseJson|94.2%|90%|
|AsyncResponse.AsyncResponseJsonSerialization|100%||
|AsyncResponse.AsyncResponseOptions|100%||
|AsyncResponse.AsyncResponsePackageVersionGate|100%||
|AsyncResponse.AsyncResponsePackageVersions|100%|87.9%|
|AsyncResponse.AsyncResponseRecoveryHealthCheck|100%|97.2%|
|AsyncResponse.AsyncResponseRecoveryStats|33.3%||
|AsyncResponse.AsyncResponseRetry|100%|100%|
|AsyncResponse.AsyncResponseStaleRecoveryEntry|50%||
|AsyncResponse.AsyncResponseStartupValidator|98.3%|95.7%|
|AsyncResponse.AsyncResponseTransportMarker|100%||
|AsyncResponse.AsyncResponseTypeResolution|96.7%|89.5%|
|AsyncResponse.AsyncResponseWatchdog|97.6%|93.5%|
|AsyncResponse.AsyncResponseWatchdogOptions|100%|100%|
|AsyncResponse.AsyncResponseWatchdogReport|100%|100%|
|AsyncResponse.AsyncResponseWatchdogSnapshot|100%||
|AsyncResponse.AsyncResponseWatchdogState|100%|66.6%|
|AsyncResponse.CallbackExpressionConverter|98.3%|88.4%|
|AsyncResponse.CallbackTargetUnresolvableException|100%||
|AsyncResponse.ChannelSerialExecutor|100%|100%|
|AsyncResponse.CorrelationIdGuard|96.2%|95%|
|AsyncResponse.CronSchedule|96.8%|95.8%|
|AsyncResponse.CurrentReadFlowStateStore|27.2%||
|AsyncResponse.DiagnosticText|93.9%|96%|
|AsyncResponse.DurableAsyncResponseChannelOptions|100%||
|AsyncResponse.DurableFlowContext|92.8%|86.6%|
|AsyncResponse.DurableFlowExecutor|98%|93.7%|
|AsyncResponse.DurableFlowObserverLifetimeAudit|93.7%|62.5%|
|AsyncResponse.DurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlowRegistration|78.5%|66.6%|
|AsyncResponse.DurableFlowService|92.3%|95.8%|
|AsyncResponse.DurableFlowSuspendedException|100%||
|AsyncResponse.FlowExecutionLease|93.6%|83.9%|
|AsyncResponse.FlowLeaseContention|100%|100%|
|AsyncResponse.FlowStateConcurrency|100%|97.1%|
|AsyncResponse.FlowStateJson|98.3%|94.6%|
|AsyncResponse.FlowStateRetention|100%|100%|
|AsyncResponse.InMemoryAsyncResponseChannel|93%|90.7%|
|AsyncResponse.InMemoryAsyncResponseOptions|100%|100%|
|AsyncResponse.InMemoryAsyncResponseWaiter`1|100%||
|AsyncResponse.InMemoryFlowStateStore|99.2%|90.4%|
|AsyncResponse.InMemoryRecoveryStateStore|97.8%|91.4%|
|AsyncResponse.InMemoryWorkerHost|93.7%|89.4%|
|AsyncResponse.InMemoryWorkerTransport|88.8%|83.6%|
|AsyncResponse.InMemoryWorkerTransportOptions|100%|100%|
|AsyncResponse.JsonSafety|96.6%|75%|
|AsyncResponse.LostSubscriberCallbackDispatcher|95.4%|90.8%|
|AsyncResponse.LostSubscriberDispatchResult|100%||
|AsyncResponse.PayloadRecoveryClassifier|100%|96.4%|
|AsyncResponse.PortableText|100%|95.8%|
|AsyncResponse.RawJsonResponse|100%|100%|
|AsyncResponse.RecoverableAsyncResponseBuilder|100%||
|AsyncResponse.RecoverableAsyncResponseBuilder`1|100%||
|AsyncResponse.RecoveryCallbackFailedException|100%||
|AsyncResponse.RecoveryClassification|100%||
|AsyncResponse.RecoveryStateObservation|100%||
|AsyncResponse.ReflectionCallDtoGuard|93.7%|92.8%|
|AsyncResponse.ReflectionExtensions|96.2%|93.6%|
|AsyncResponse.RemoteStackTrace|100%|100%|
|AsyncResponse.SafeLog|100%||
|AsyncResponse.ScheduledFlowOptions|100%||
|AsyncResponse.ScheduledFlowRegistration|100%||
|AsyncResponse.ScheduledFlowService|76.3%|80.4%|
|AsyncResponse.SerialExecutorRegistry|94.1%|95.8%|
|AsyncResponse.ShutdownBudgetValidator|100%|100%|
|AsyncResponse.TypeNameIdentity|100%|94.4%|
|AsyncResponse.UnresolvableTypeNames|95%|75%|
|AsyncResponse.WorkerJobExecutor|91.5%|81%|
|AsyncResponse.WorkerJobScope|90.9%|50%|
|AsyncResponse.WorkerJobSkewScope|95.4%|90%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAllowList|100%|85.7%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAuthorization<br/>Extensions|100%||
|Microsoft.Extensions.DependencyInjection.AsyncResponseCoreServiceCollection<br/>Extensions|100%|95.2%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseHealthCheckExtensions|100%||
|Microsoft.Extensions.DependencyInjection.AsyncResponseRegistrationBuilder|100%||
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.Cosmos - 91.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Cosmos**|**91.5%**|**88.6%**|
|AsyncResponse.DurableFlows.Cosmos.CosmosDurableFlowOptions|82.6%|81.2%|
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateDocument|100%||
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateStore|95.3%|90.4%|
|AsyncResponse.DurableFlows.Cosmos.CosmosLeaseProjection|100%||
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|80.7%|85.7%|
|Microsoft.Extensions.DependencyInjection.CosmosDurableFlowServiceCollection<br/>Extensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.DynamoDB - 93.2%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.DynamoDB**|**93.2%**|**85.2%**|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbDurableFlowOptions|77.2%|61.1%|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbFlowStateStore|99.6%|89.6%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|80.7%|85.7%|
|Microsoft.Extensions.DependencyInjection.DynamoDbDurableFlowServiceCollecti<br/>onExtensions|100%|50%|

</details>
<details><summary>AsyncResponse.DurableFlows.EFCore - 96.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.EFCore**|**96.7%**|**95.5%**|
|AsyncResponse.DurableFlows.EFCore.DurableFlowStateRecord|85.7%||
|AsyncResponse.DurableFlows.EFCore.EFCoreDurableFlowModelBuilderExtensions|100%|100%|
|AsyncResponse.DurableFlows.EFCore.EFCoreDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.EFCore.EFCoreFlowStateStore`1|99%|97.5%|
|AsyncResponse.DurableFlows.EFCore.FlowIdCollationRules|100%|100%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|92.3%|94%|
|Microsoft.Extensions.DependencyInjection.EFCoreDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.MongoDB - 92.1%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.MongoDB**|**92.1%**|**83.6%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|80.7%|85.7%|
|AsyncResponse.DurableFlows.MongoDB.MongoDbDurableFlowOptions|88.2%|83.3%|
|AsyncResponse.DurableFlows.MongoDB.MongoDbFlowStateStore|99.5%|94.2%|
|AsyncResponse.DurableFlows.MongoDB.MongoFlowStateDocument|100%||
|AsyncResponse.DurableFlows.MongoDB.NullableUtcBsonDateSerializer|100%||
|AsyncResponse.DurableFlows.MongoDB.UtcBsonDateSerializer|100%||
|AsyncResponse.Internal.MongoNamespaceRegistry|75%|75%|
|AsyncResponse.Internal.MongoOwnershipLedger|89.1%|53.5%|
|AsyncResponse.Internal.MongoWriteConcerns|100%|100%|
|Microsoft.Extensions.DependencyInjection.MongoDurableFlowServiceCollectionE<br/>xtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.MySql - 100%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.MySql**|**100%**|**93.5%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.4%|
|AsyncResponse.DurableFlows.MySql.MySqlDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.MySql.MySqlFlowStateStore|100%|91.3%|
|Microsoft.Extensions.DependencyInjection.MySqlDurableFlowServiceCollectionE<br/>xtensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.Oracle - 97.1%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Oracle**|**97.1%**|**90.7%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.4%|
|AsyncResponse.DurableFlows.Oracle.OracleDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.Oracle.OracleFlowStateStore|96.1%|86.6%|
|Microsoft.Extensions.DependencyInjection.OracleDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.PostgreSQL - 91.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.PostgreSQL**|**91.9%**|**83.7%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|92.3%|94%|
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlFlowStateStore|98.7%|89.4%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|81.1%|74.1%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlDurableFlowServiceCollec<br/>tionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.Sqlite - 99.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Sqlite**|**99.3%**|**91.5%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.4%|
|AsyncResponse.DurableFlows.Sqlite.SqliteDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.Sqlite.SqliteFlowStateStore|98.9%|87.7%|
|Microsoft.Extensions.DependencyInjection.SqliteDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.SqlServer - 82.1%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.SqlServer**|**82.1%**|**77%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.4%|
|AsyncResponse.DurableFlows.SqlServer.SqlServerDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.SqlServer.SqlServerFlowStateStore|88.7%|86.6%|
|AsyncResponse.Internal.SqlServerRelationVerifier|69.4%|69.5%|
|Microsoft.Extensions.DependencyInjection.SqlServerDurableFlowServiceCollect<br/>ionExtensions|100%||

</details>
<details><summary>AsyncResponse.Testing - 95.1%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Testing**|**95.1%**|**87.3%**|
|AsyncResponse.Testing.AsyncResponseTestHarness|94.6%|89.6%|
|AsyncResponse.Testing.AsyncResponseTestHarnessOptions|100%||
|AsyncResponse.Testing.FlowProbe|90.7%|82%|
|AsyncResponse.Testing.FlowProbeEvent|100%||
|AsyncResponse.Testing.FlowRunHandle|100%|83.3%|
|AsyncResponse.Testing.FlowTestHarness|100%|50%|
|AsyncResponse.Testing.SimulatedCrashException|100%|100%|
|AsyncResponse.Testing.VirtualTimeProvider|99%|97.3%|

</details>
<details><summary>AsyncResponse.Transports.AzureServiceBus - 96%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.AzureServiceBus**|**96%**|**90.7%**|
|AsyncResponse.Transports.AzureServiceBus.AwaitingAzureServiceBusMessageDisp<br/>atcher|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusAsyncResponseOption<br/>s|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusBackgroundFailureCo<br/>ntext|95%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientResolver|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusCorrelationIdExtrac<br/>tor|90.9%|90.9%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusMessageDispatcher|98.8%|76.4%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOptionsValidator|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOutboundMessage|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReceiverAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetOptions|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusResponseIngressSubs<br/>criber|100%|50%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSenderAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberOptions|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberService|88.4%|96.4%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusTransportDelivery|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerSubscriber|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerTransport|100%|83.3%|
|AsyncResponse.Transports.AzureServiceBus.QueuedAzureServiceBusMessageDispat<br/>cher|92.6%|90%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|75%|
|Microsoft.Extensions.DependencyInjection.AzureServiceBusAsyncResponseServic<br/>eCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.GooglePubSub - 98%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.GooglePubSub**|**98%**|**94.8%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.GooglePubSub.AwaitingGooglePubSubMessageDispatcher|93.4%|90%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubAsyncResponseOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubBackgroundFailureContext|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubMessageDispatcher|99.1%|97.8%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubOptionsValidator|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubPublisherClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberOptions|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberService|96.3%|93.7%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerSubscriber|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerTransport|100%|100%|
|AsyncResponse.Transports.GooglePubSub.QueuedGooglePubSubMessageDispatcher|96.9%|95.4%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|75%|
|Microsoft.Extensions.DependencyInjection.GooglePubSubAsyncResponseServiceCo<br/>llectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.Kafka - 96.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Kafka**|**96.8%**|**93.6%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.Kafka.AwaitingKafkaMessageDispatcher|90.7%|91.1%|
|AsyncResponse.Transports.Kafka.KafkaAssignmentGenerations|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaBackgroundFailureContext|95.6%||
|AsyncResponse.Transports.Kafka.KafkaConsumerClientAdapter|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaConsumerClientFactory|97.2%|100%|
|AsyncResponse.Transports.Kafka.KafkaCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaDeadLetterPublishFailedException|100%||
|AsyncResponse.Transports.Kafka.KafkaDelivery|100%||
|AsyncResponse.Transports.Kafka.KafkaIncomingMessage|100%||
|AsyncResponse.Transports.Kafka.KafkaMessageDispatcher|98.6%|93.1%|
|AsyncResponse.Transports.Kafka.KafkaPartitionNotAssignedException|100%||
|AsyncResponse.Transports.Kafka.KafkaProducerClientAdapter|96.5%|100%|
|AsyncResponse.Transports.Kafka.KafkaPublishResult|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetProvider|100%|93.7%|
|AsyncResponse.Transports.Kafka.KafkaResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaSubscriberService|99.3%|95.4%|
|AsyncResponse.Transports.Kafka.KafkaTransportClientDefaults|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportHeader|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportOptionsValidator|98.7%|92.8%|
|AsyncResponse.Transports.Kafka.KafkaTransportRetry|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportTopicSchema|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaWorkerSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaWorkerTransport|100%|100%|
|AsyncResponse.Transports.Kafka.QueuedKafkaMessageDispatcher|95.6%|90.9%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|62.5%|
|Microsoft.Extensions.DependencyInjection.KafkaAsyncResponseTransportService<br/>CollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.MongoDB - 94.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.MongoDB**|**94.5%**|**86.6%**|
|AsyncResponse.Internal.MongoNamespaceRegistry|75%|75%|
|AsyncResponse.Internal.MongoOwnershipLedger|67.5%|50%|
|AsyncResponse.Internal.MongoTransientFaults|100%|68.7%|
|AsyncResponse.Internal.MongoWriteConcerns|100%|100%|
|AsyncResponse.Internal.OpportunisticPrune|100%|80%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbDeadLetterPrune|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|87.2%|96.8%|
|AsyncResponse.Transports.DbTransportHeaders|91.6%|75%|
|AsyncResponse.Transports.MongoDB.LenientTransportHeaderSerializer|100%|96.6%|
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
|AsyncResponse.Transports.MongoDB.MongoDbTransportOptionsValidator|97.3%|93.4%|
|AsyncResponse.Transports.MongoDB.MongoDbTransportRetry|100%||
|AsyncResponse.Transports.MongoDB.MongoDbTransportStore|98.2%|84.5%|
|AsyncResponse.Transports.MongoDB.MongoDbWorkerSubscriber|100%||
|AsyncResponse.Transports.MongoDB.MongoDbWorkerTransport|100%|64.2%|
|AsyncResponse.Transports.MongoDB.MongoTransportMessageDocument|100%||
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|75%|
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseTransportServi<br/>ceCollectionExtensions|97.9%|87.5%|

</details>
<details><summary>AsyncResponse.Transports.NATS - 94.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.NATS**|**94.6%**|**89.8%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.NATS.INatsJetStreamTransport|0%||
|AsyncResponse.Transports.NATS.NatsAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.NATS.NatsBackgroundFailureContext|85%||
|AsyncResponse.Transports.NATS.NatsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.NATS.NatsJetStreamTransportAdapter|91.3%|73.8%|
|AsyncResponse.Transports.NATS.NatsJobDelivery|100%||
|AsyncResponse.Transports.NATS.NatsMessageDispatcher|93.1%|94.4%|
|AsyncResponse.Transports.NATS.NatsReplyTargetOptions|100%||
|AsyncResponse.Transports.NATS.NatsReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.NATS.NatsResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.NATS.NatsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.NATS.NatsSubscriberService|88.9%|95.6%|
|AsyncResponse.Transports.NATS.NatsTransportOptionsValidator|97.7%|92.2%|
|AsyncResponse.Transports.NATS.NatsTransportRetry|100%|100%|
|AsyncResponse.Transports.NATS.NatsTransportSubjectSchema|100%|100%|
|AsyncResponse.Transports.NATS.NatsWorkerSubscriber|100%||
|AsyncResponse.Transports.NATS.NatsWorkerTransport|100%|77.7%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|100%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseTransportServiceC<br/>ollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.PostgreSQL - 92.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.PostgreSQL**|**92.8%**|**86.7%**|
|AsyncResponse.Internal.OpportunisticPrune|100%|80%|
|AsyncResponse.Internal.PostgreSqlListenConnection|92%|100%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|81.1%|70.1%|
|AsyncResponse.Internal.PostgreSqlTransientFaults|100%|100%|
|AsyncResponse.Internal.RelationalNamePlan|100%|91.6%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbDeadLetterPrune|100%|75%|
|AsyncResponse.Transports.DbMessageDispatcherBase|87.2%|96.8%|
|AsyncResponse.Transports.DbTransportHeaders|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlBackgroundFailureContext|91.6%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlCorrelationIdExtractor|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlMessageDispatcher|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberOptions|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberService|91.6%|95%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportDelivery|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportOptionsValidator|100%|96.1%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportRetry|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportStore|96.3%|94%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerSubscriber|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerTransport|95.8%|64.2%|
|AsyncResponse.Transports.SubscriberSupervisor|60%|37.5%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseTransportSe<br/>rviceCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.RabbitMQ - 94.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.RabbitMQ**|**94.4%**|**89.8%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.RabbitMQ.AwaitingRabbitMqMessageDispatcher|89.3%|85.1%|
|AsyncResponse.Transports.RabbitMQ.QueuedRabbitMqMessageDispatcher|82.4%|76%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqAsyncResponseOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqBackgroundFailureContext|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqChannelAdapter|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionAdapter|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionFactoryAdapter|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqConsumer|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqCorrelationIdExtractor|95.2%|95.4%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqDelivery|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqMessageDispatcher|98.5%|96.7%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqOptionsValidator|100%|91.6%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetProvider|100%|88.2%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberOptions|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberService|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqTopology|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerSubscriber|100%|72.2%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerTransport|97.2%|88.4%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|75%|
|Microsoft.Extensions.DependencyInjection.RabbitMqAsyncResponseServiceCollec<br/>tionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.Redis - 95.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Redis**|**95.7%**|**95.4%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|93.5%|
|AsyncResponse.Transports.Redis.AwaitingRedisMessageDispatcher|100%|100%|
|AsyncResponse.Transports.Redis.IRedisStreamDatabase|33.3%||
|AsyncResponse.Transports.Redis.QueuedRedisMessageDispatcher|91.8%|100%|
|AsyncResponse.Transports.Redis.RedisAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Redis.RedisBackgroundFailureContext|85%||
|AsyncResponse.Transports.Redis.RedisCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Redis.RedisMessageDispatcher|100%|96%|
|AsyncResponse.Transports.Redis.RedisReplyTargetOptions|100%||
|AsyncResponse.Transports.Redis.RedisReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.Redis.RedisResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisStreamDatabaseAdapter|100%|100%|
|AsyncResponse.Transports.Redis.RedisStreamDelivery|100%||
|AsyncResponse.Transports.Redis.RedisSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Redis.RedisSubscriberService|90.8%|94.7%|
|AsyncResponse.Transports.Redis.RedisTransportKeySchema|100%|100%|
|AsyncResponse.Transports.Redis.RedisTransportOptionsValidator|92.3%|96.1%|
|AsyncResponse.Transports.Redis.RedisTransportRetry|100%|100%|
|AsyncResponse.Transports.Redis.RedisWorkerSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisWorkerTransport|100%|100%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|62.5%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseTransportService<br/>CollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.SqlServer - 90.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SqlServer**|**90.7%**|**83%**|
|AsyncResponse.Internal.OpportunisticPrune|100%|80%|
|AsyncResponse.Internal.RelationalNamePlan|53.3%|41.6%|
|AsyncResponse.Internal.SqlServerRelationVerifier|81.5%|74.6%|
|AsyncResponse.Internal.SqlServerTransientFaults|100%|100%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbDeadLetterPrune|100%|75%|
|AsyncResponse.Transports.DbMessageDispatcherBase|87.2%|96.8%|
|AsyncResponse.Transports.DbTransportHeaders|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerBackgroundFailureContext|91.6%||
|AsyncResponse.Transports.SqlServer.SqlServerCorrelationIdExtractor|100%||
|AsyncResponse.Transports.SqlServer.SqlServerMessageDispatcher|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberService|96.8%|94.4%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportDelivery|100%||
|AsyncResponse.Transports.SqlServer.SqlServerTransportOptionsValidator|100%|96.5%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportRetry|100%||
|AsyncResponse.Transports.SqlServer.SqlServerTransportStore|91.7%|85.7%|
|AsyncResponse.Transports.SqlServer.SqlServerWorkerSubscriber|100%||
|AsyncResponse.Transports.SqlServer.SqlServerWorkerTransport|95.8%|64.2%|
|AsyncResponse.Transports.SubscriberSupervisor|60%|37.5%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseTransportSer<br/>viceCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.SQS - 97.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SQS**|**97.5%**|**89%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.SQS.AwaitingSqsMessageDispatcher|100%|100%|
|AsyncResponse.Transports.SQS.QueuedSqsMessageDispatcher|96.7%|90.9%|
|AsyncResponse.Transports.SQS.SqsAsyncResponseOptions|100%||
|AsyncResponse.Transports.SQS.SqsBackgroundFailureContext|100%||
|AsyncResponse.Transports.SQS.SqsClientAdapter|100%|97.5%|
|AsyncResponse.Transports.SQS.SqsClientFactory|100%|70%|
|AsyncResponse.Transports.SQS.SqsClientResolver|100%|100%|
|AsyncResponse.Transports.SQS.SqsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.SQS.SqsMessageDispatcher|98.9%|68.7%|
|AsyncResponse.Transports.SQS.SqsOptionsValidator|98.4%|93.4%|
|AsyncResponse.Transports.SQS.SqsOutboundMessage|100%||
|AsyncResponse.Transports.SQS.SqsQueueAddress|100%|87.5%|
|AsyncResponse.Transports.SQS.SqsQueueProvisioningService|98.5%|91.6%|
|AsyncResponse.Transports.SQS.SqsReceiveRequest|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetOptions|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetProvider|100%|91.6%|
|AsyncResponse.Transports.SQS.SqsResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.SQS.SqsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SQS.SqsSubscriberService|92.7%|93%|
|AsyncResponse.Transports.SQS.SqsTransportDelivery|100%||
|AsyncResponse.Transports.SQS.SqsWorkerSubscriber|100%|81.2%|
|AsyncResponse.Transports.SQS.SqsWorkerTransport|97.4%|85.4%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|75%|
|Microsoft.Extensions.DependencyInjection.SqsAsyncResponseServiceCollectionE<br/>xtensions|100%||

</details>
