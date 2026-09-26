# Summary - AsyncResponse (Release / net8.0+net10.0 / unit+integration)
<details open><summary>Summary</summary>

|||
|:---|:---|
| Generated on: | 09/26/2026 - 15:15:25 |
| Coverage date: | 09/26/2026 - 14:57:51 - 09/26/2026 - 15:14:38 |
| Parser: | MultiReport (16x Cobertura) |
| Assemblies: | 27 |
| Classes: | 506 |
| Files: | 249 |
| **Line coverage:** | 94.6% (35170 of 37166) |
| Covered lines: | 35170 |
| Uncovered lines: | 1996 |
| Coverable lines: | 37166 |
| Total lines: | 74829 |
| **Branch coverage:** | 89% (13577 of 15239) |
| Covered branches: | 13577 |
| Total branches: | 15239 |
| **Method coverage:** | [Feature is only available for sponsors](https://reportgenerator.io/pro) |

</details>

## Coverage
<details><summary>AsyncResponse.Abstractions - 98.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Abstractions**|**98.8%**|**100%**|
|AsyncResponse.AsyncResponseContext|100%|100%|
|AsyncResponse.AsyncResponseDomainFailureException|100%||
|AsyncResponse.AsyncResponseIndeterminateDeliveryException|100%||
|AsyncResponse.AsyncResponsePayloadReflection|88.2%|100%|
|AsyncResponse.AsyncResponseReplyTarget|100%|100%|
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
|AsyncResponse.RecoveryStateScanUnreadableException|100%||
|AsyncResponse.RecoveryStateSchema|100%||
|AsyncResponse.RecoveryStateUnreadableException|100%||
|AsyncResponse.ReflectionCallDto|100%||
|AsyncResponse.ReflectionInvocationDto|100%||
|AsyncResponse.WorkerJobEnvelope|100%||
|AsyncResponse.WorkerJobEnvelopeSchema|100%||
|AsyncResponse.WorkerJobTooLargeException|100%||

</details>
<details><summary>AsyncResponse.Channels.MongoDB - 97%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.MongoDB**|**97%**|**89.6%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|96.1%|92.9%|
|AsyncResponse.Channels.DbRecoveryStateStoreBase|100%|90.4%|
|AsyncResponse.Channels.MongoDB.MongoChannelMessageDocument|100%||
|AsyncResponse.Channels.MongoDB.MongoChannelSubscriberDocument|60%||
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannel|95.3%|87.5%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannelOptions|100%|95.8%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.MongoDB.MongoDbChannelMessage|100%||
|AsyncResponse.Channels.MongoDB.MongoDbChannelStore|98.8%|82%|
|AsyncResponse.Channels.MongoDB.MongoDbRecoveryStateStore|100%||
|AsyncResponse.Channels.MongoDB.MongoRecoveryStateDocument|50%||
|AsyncResponse.Internal.MongoIndexes|92.6%|75%|
|AsyncResponse.Internal.MongoNamespaceRegistry|100%|100%|
|AsyncResponse.Internal.MongoOwnershipLedger|97.6%|85.7%|
|AsyncResponse.Internal.MongoTransientFaults|100%|100%|
|AsyncResponse.Internal.MongoWriteConcerns|91.6%|73.9%|
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseChannelService<br/>CollectionExtensions|100%|91.6%|

</details>
<details><summary>AsyncResponse.Channels.NATS - 96.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.NATS**|**96.9%**|**92.5%**|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannel|96.8%|91.2%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.NATS.NatsConsumeLoopException|100%||
|AsyncResponse.Channels.NATS.NatsInboundResponse|100%||
|AsyncResponse.Channels.NATS.NatsKvEntry|100%||
|AsyncResponse.Channels.NATS.NatsKvStoreAdapter|94.5%|88.6%|
|AsyncResponse.Channels.NATS.NatsRawRequester|93.1%|100%|
|AsyncResponse.Channels.NATS.NatsRecoveryStateStore|97.4%|95.8%|
|AsyncResponse.Channels.NATS.NatsResponseChannelClient|100%|81.2%|
|AsyncResponse.Channels.NATS.NatsSubjectSchema|100%|100%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseChannelServiceCol<br/>lectionExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.PostgreSQL - 92.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.PostgreSQL**|**92.3%**|**84.4%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|89.2%|83.8%|
|AsyncResponse.Channels.DbRecoveryStateStoreBase|98.7%|88%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannel|96.2%|90%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelMessage|100%||
|AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql|98.2%|87.6%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlRecoveryStateStore|100%||
|AsyncResponse.Internal.OpportunisticPrune|100%|85%|
|AsyncResponse.Internal.PostgreSqlDdlGuard|81%|85%|
|AsyncResponse.Internal.PostgreSqlListenConnection|89.6%|84.6%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|90.4%|79.5%|
|AsyncResponse.Internal.PostgreSqlTransientFaults|100%|100%|
|AsyncResponse.Internal.RelationalNamePlan|73.3%|83.3%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseChannelServ<br/>iceCollectionExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.Redis - 94.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.Redis**|**94.7%**|**88%**|
|AsyncResponse.Channels.Redis.IRedisChannelSubscriber|0%||
|AsyncResponse.Channels.Redis.RedisAsyncResponseChannel|93.7%|87.7%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseOptions|100%|100%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.Redis.RedisChannelMessageQueueSubscriber|100%||
|AsyncResponse.Channels.Redis.RedisClusterNodeTable|100%|97.2%|
|AsyncResponse.Channels.Redis.RedisKeySchema|100%|100%|
|AsyncResponse.Channels.Redis.RedisRecoveryStateStore|94.9%|83.3%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseServiceCollectio<br/>nExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.SqlServer - 93.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.SqlServer**|**93.3%**|**83.3%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|89.7%|81.6%|
|AsyncResponse.Channels.DbRecoveryStateStoreBase|98.7%|88%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannel|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.SqlServer.SqlServerChannelMessage|100%||
|AsyncResponse.Channels.SqlServer.SqlServerChannelSql|98.6%|89.8%|
|AsyncResponse.Channels.SqlServer.SqlServerRecoveryStateStore|100%||
|AsyncResponse.Internal.OpportunisticPrune|100%|85%|
|AsyncResponse.Internal.RelationalNamePlan|73.3%|83.3%|
|AsyncResponse.Internal.SqlServerRelationVerifier|91.8%|80.8%|
|AsyncResponse.Internal.SqlServerTransientFaults|96.2%|100%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseChannelServi<br/>ceCollectionExtensions|100%|50%|

</details>
<details><summary>AsyncResponse.Core - 95.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Core**|**95.7%**|**91.6%**|
|AsyncResponse.AsyncResponseBuilder|100%||
|AsyncResponse.AsyncResponseBuilder`1|100%|100%|
|AsyncResponse.AsyncResponseBuilderBase|96.3%|100%|
|AsyncResponse.AsyncResponseChannelMarker|100%||
|AsyncResponse.AsyncResponseChannelOptions|100%|100%|
|AsyncResponse.AsyncResponseContextPropagation|100%|93%|
|AsyncResponse.AsyncResponseDiagnostics|98%|88.6%|
|AsyncResponse.AsyncResponseDurableFlowStoreMarker|95.2%|57.1%|
|AsyncResponse.AsyncResponseEnvelope`1|100%||
|AsyncResponse.AsyncResponseEnvelopeConverter`1|100%|98.9%|
|AsyncResponse.AsyncResponseEnvelopeJson|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeOptions`1|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeSchema|100%||
|AsyncResponse.AsyncResponseIngress|100%|93.4%|
|AsyncResponse.AsyncResponseJson|97.1%|90%|
|AsyncResponse.AsyncResponseJsonSerialization|100%||
|AsyncResponse.AsyncResponseOptions|100%||
|AsyncResponse.AsyncResponsePackageVersionGate|100%||
|AsyncResponse.AsyncResponsePackageVersions|94.8%|87.5%|
|AsyncResponse.AsyncResponseRecoveryHealthCheck|100%|97.3%|
|AsyncResponse.AsyncResponseRecoveryStats|100%||
|AsyncResponse.AsyncResponseRetry|100%|100%|
|AsyncResponse.AsyncResponseStaleRecoveryEntry|100%||
|AsyncResponse.AsyncResponseStartupValidator|98.4%|95.5%|
|AsyncResponse.AsyncResponseTransportMarker|100%||
|AsyncResponse.AsyncResponseTypeResolution|96.9%|89.5%|
|AsyncResponse.AsyncResponseWatchdog|97.9%|91.8%|
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
|AsyncResponse.DurableFlowContext|92.9%|86.9%|
|AsyncResponse.DurableFlowExecutor|97.9%|93.8%|
|AsyncResponse.DurableFlowObserverLifetimeAudit|93.7%|62.5%|
|AsyncResponse.DurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlowRegistration|78.5%|66.6%|
|AsyncResponse.DurableFlowService|92.8%|96.6%|
|AsyncResponse.DurableFlowSuspendedException|100%||
|AsyncResponse.FlowExecutionLease|94.5%|85.3%|
|AsyncResponse.FlowLeaseContention|100%|100%|
|AsyncResponse.FlowStateConcurrency|100%|97.1%|
|AsyncResponse.FlowStateJson|98.3%|94.6%|
|AsyncResponse.FlowStateRetention|100%|100%|
|AsyncResponse.InMemoryAsyncResponseChannel|93.2%|90.3%|
|AsyncResponse.InMemoryAsyncResponseOptions|100%|100%|
|AsyncResponse.InMemoryAsyncResponseWaiter`1|100%||
|AsyncResponse.InMemoryFlowStateStore|99.2%|90.4%|
|AsyncResponse.InMemoryRecoveryStateStore|97.8%|92.5%|
|AsyncResponse.InMemoryWorkerHost|94%|90.9%|
|AsyncResponse.InMemoryWorkerTransport|91.6%|84.8%|
|AsyncResponse.InMemoryWorkerTransportOptions|100%|100%|
|AsyncResponse.JsonSafety|96.6%|75%|
|AsyncResponse.LostSubscriberCallbackDispatcher|95.6%|90.4%|
|AsyncResponse.LostSubscriberDispatchResult|100%||
|AsyncResponse.PayloadRecoveryClassifier|100%|96.4%|
|AsyncResponse.PortableText|100%|100%|
|AsyncResponse.RawJsonResponse|100%|100%|
|AsyncResponse.RecoverableAsyncResponseBuilder|100%||
|AsyncResponse.RecoverableAsyncResponseBuilder`1|100%|100%|
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
|AsyncResponse.SerialExecutorRegistry|99.3%|97.3%|
|AsyncResponse.ShutdownBudgetValidator|100%|100%|
|AsyncResponse.TypeNameIdentity|100%|95.6%|
|AsyncResponse.UnresolvableTypeNames|95%|75%|
|AsyncResponse.WorkerJobExecutor|92.9%|85%|
|AsyncResponse.WorkerJobScope|90.9%|50%|
|AsyncResponse.WorkerJobSkewScope|95.4%|90%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAllowList|100%|87.5%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAuthorization<br/>Extensions|100%||
|Microsoft.Extensions.DependencyInjection.AsyncResponseCoreServiceCollection<br/>Extensions|100%|95.4%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseHealthCheckExtensions|100%||
|Microsoft.Extensions.DependencyInjection.AsyncResponseRegistrationBuilder|100%||
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.Cosmos - 93.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Cosmos**|**93.5%**|**90.1%**|
|AsyncResponse.DurableFlows.Cosmos.CosmosDurableFlowOptions|82.6%|81.2%|
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateDocument|100%||
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateStore|95.2%|90%|
|AsyncResponse.DurableFlows.Cosmos.CosmosLeaseProjection|100%||
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|88.4%|91.6%|
|Microsoft.Extensions.DependencyInjection.CosmosDurableFlowServiceCollection<br/>Extensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.DynamoDB - 95.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.DynamoDB**|**95.4%**|**87.6%**|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbDurableFlowOptions|77.2%|61.1%|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbFlowStateStore|99.6%|89.6%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|88.4%|91.6%|
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
<details><summary>AsyncResponse.DurableFlows.MongoDB - 93.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.MongoDB**|**93.4%**|**85.8%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|88.4%|91.6%|
|AsyncResponse.DurableFlows.MongoDB.MongoDbDurableFlowOptions|88.2%|83.3%|
|AsyncResponse.DurableFlows.MongoDB.MongoDbFlowStateStore|98.3%|93.6%|
|AsyncResponse.DurableFlows.MongoDB.MongoFlowStateDocument|100%||
|AsyncResponse.DurableFlows.MongoDB.NullableUtcBsonDateSerializer|100%||
|AsyncResponse.DurableFlows.MongoDB.UtcBsonDateSerializer|100%||
|AsyncResponse.Internal.MongoNamespaceRegistry|75%|75%|
|AsyncResponse.Internal.MongoOwnershipLedger|81.3%|53.5%|
|AsyncResponse.Internal.MongoWriteConcerns|91.6%|73.9%|
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
<details><summary>AsyncResponse.DurableFlows.PostgreSQL - 89.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.PostgreSQL**|**89.7%**|**86.2%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|92.3%|94%|
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlFlowStateStore|96.1%|94%|
|AsyncResponse.Internal.PostgreSqlDdlGuard|78.3%|90%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|83%|76.5%|
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
<details><summary>AsyncResponse.Testing - 96.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Testing**|**96.9%**|**90%**|
|AsyncResponse.Testing.AsyncResponseTestHarness|95.2%|87.8%|
|AsyncResponse.Testing.AsyncResponseTestHarnessOptions|100%||
|AsyncResponse.Testing.FlowProbe|96.8%|91.2%|
|AsyncResponse.Testing.FlowProbeEvent|100%||
|AsyncResponse.Testing.FlowRunHandle|100%|83.3%|
|AsyncResponse.Testing.FlowTestHarness|100%|50%|
|AsyncResponse.Testing.SimulatedCrashException|100%|100%|
|AsyncResponse.Testing.VirtualTimeProvider|99.1%|97.7%|

</details>
<details><summary>AsyncResponse.Transports.AzureServiceBus - 95.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.AzureServiceBus**|**95.7%**|**91.5%**|
|AsyncResponse.Transports.AzureServiceBus.AwaitingAzureServiceBusMessageDisp<br/>atcher|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusAsyncResponseOption<br/>s|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusBackgroundFailureCo<br/>ntext|95%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientResolver|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusCorrelationIdExtrac<br/>tor|90.9%|90.9%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusMessageDispatcher|98.9%|76.4%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOptionsValidator|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOutboundMessage|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReceiverAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetOptions|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusResponseIngressSubs<br/>criber|100%|50%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSenderAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberOptions|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberService|87.8%|97.3%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusTransportDelivery|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerSubscriber|100%|83.3%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerTransport|100%|83.3%|
|AsyncResponse.Transports.AzureServiceBus.QueuedAzureServiceBusMessageDispat<br/>cher|94.1%|90%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|75%|100%|
|Microsoft.Extensions.DependencyInjection.AzureServiceBusAsyncResponseServic<br/>eCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.GooglePubSub - 97.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.GooglePubSub**|**97.3%**|**94.8%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.GooglePubSub.AwaitingGooglePubSubMessageDispatcher|97.6%|90%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubAsyncResponseOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubBackgroundFailureContext|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubMessageDispatcher|97.6%|97.8%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubOptionsValidator|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubPublisherClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberOptions|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberService|95%|94.4%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerSubscriber|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerTransport|100%|100%|
|AsyncResponse.Transports.GooglePubSub.QueuedGooglePubSubMessageDispatcher|94.9%|93.7%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|75%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.GooglePubSubAsyncResponseServiceCo<br/>llectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.Kafka - 96.1%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Kafka**|**96.1%**|**93.4%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.Kafka.AwaitingKafkaMessageDispatcher|92.3%|89.7%|
|AsyncResponse.Transports.Kafka.KafkaAssignmentGenerations|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaBackgroundFailureContext|95.6%||
|AsyncResponse.Transports.Kafka.KafkaConsumerClientAdapter|96.2%|94%|
|AsyncResponse.Transports.Kafka.KafkaConsumerClientFactory|97.8%|87.5%|
|AsyncResponse.Transports.Kafka.KafkaCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaDeadLetterPublishFailedException|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaDeadLetterTooLargeException|100%||
|AsyncResponse.Transports.Kafka.KafkaDelivery|100%||
|AsyncResponse.Transports.Kafka.KafkaIncomingMessage|100%||
|AsyncResponse.Transports.Kafka.KafkaMessageDispatcher|96.9%|94.1%|
|AsyncResponse.Transports.Kafka.KafkaPartitionNotAssignedException|100%||
|AsyncResponse.Transports.Kafka.KafkaProducerClientAdapter|96.5%|100%|
|AsyncResponse.Transports.Kafka.KafkaPublishResult|100%||
|AsyncResponse.Transports.Kafka.KafkaRecordTooLargeException|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetProvider|100%|93.7%|
|AsyncResponse.Transports.Kafka.KafkaResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaSubscriberService|99.3%|96.2%|
|AsyncResponse.Transports.Kafka.KafkaTransportClientDefaults|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportHeader|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportOptionsValidator|98.7%|92.8%|
|AsyncResponse.Transports.Kafka.KafkaTransportRetry|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportTopicSchema|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaWorkerSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaWorkerTransport|100%|100%|
|AsyncResponse.Transports.Kafka.QueuedKafkaMessageDispatcher|91.2%|92.5%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|62.5%|
|AsyncResponse.Transports.WorkerIntakeGate|75%|100%|
|Microsoft.Extensions.DependencyInjection.KafkaAsyncResponseTransportService<br/>CollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.MongoDB - 94%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.MongoDB**|**94%**|**85.1%**|
|AsyncResponse.Internal.MongoIndexes|82.9%|68.7%|
|AsyncResponse.Internal.MongoNamespaceRegistry|75%|75%|
|AsyncResponse.Internal.MongoOwnershipLedger|62.7%|50%|
|AsyncResponse.Internal.MongoTransientFaults|100%|75%|
|AsyncResponse.Internal.MongoWriteConcerns|95.8%|78.2%|
|AsyncResponse.Internal.OpportunisticPrune|100%|80%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbDeadLetterPrune|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|89.9%|92.7%|
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
|AsyncResponse.Transports.MongoDB.MongoDbTransportStore|97.4%|85.7%|
|AsyncResponse.Transports.MongoDB.MongoDbWorkerSubscriber|100%||
|AsyncResponse.Transports.MongoDB.MongoDbWorkerTransport|100%|64.2%|
|AsyncResponse.Transports.MongoDB.MongoTransportMessageDocument|100%||
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|75%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseTransportServi<br/>ceCollectionExtensions|97.9%|87.5%|

</details>
<details><summary>AsyncResponse.Transports.NATS - 95.2%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.NATS**|**95.2%**|**90.8%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.NATS.INatsJetStreamTransport|0%||
|AsyncResponse.Transports.NATS.NatsAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.NATS.NatsBackgroundFailureContext|85%||
|AsyncResponse.Transports.NATS.NatsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.NATS.NatsJetStreamTransportAdapter|92.5%|80.7%|
|AsyncResponse.Transports.NATS.NatsJobDelivery|100%||
|AsyncResponse.Transports.NATS.NatsMessageDispatcher|93.1%|95.8%|
|AsyncResponse.Transports.NATS.NatsReplyTargetOptions|100%||
|AsyncResponse.Transports.NATS.NatsReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.NATS.NatsResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.NATS.NatsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.NATS.NatsSubscriberService|92.9%|92.3%|
|AsyncResponse.Transports.NATS.NatsTransportOptionsValidator|97.7%|92.2%|
|AsyncResponse.Transports.NATS.NatsTransportRetry|100%|100%|
|AsyncResponse.Transports.NATS.NatsTransportSubjectSchema|100%|100%|
|AsyncResponse.Transports.NATS.NatsWorkerSubscriber|100%||
|AsyncResponse.Transports.NATS.NatsWorkerTransport|100%|77.7%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|100%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseTransportServiceC<br/>ollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.PostgreSQL - 92.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.PostgreSQL**|**92.7%**|**88.7%**|
|AsyncResponse.Internal.OpportunisticPrune|100%|80%|
|AsyncResponse.Internal.PostgreSqlDdlGuard|79.2%|90%|
|AsyncResponse.Internal.PostgreSqlListenConnection|89.6%|84.6%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|83%|72.7%|
|AsyncResponse.Internal.PostgreSqlTransientFaults|100%|100%|
|AsyncResponse.Internal.RelationalNamePlan|100%|91.6%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbDeadLetterPrune|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|90.2%|94.5%|
|AsyncResponse.Transports.DbTransportHeaders|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlBackgroundFailureContext|91.6%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlCorrelationIdExtractor|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlMessageDispatcher|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberOptions|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberService|94.7%|93.3%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportDelivery|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportOptionsValidator|100%|96.1%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportRetry|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportStore|95.8%|95.8%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerSubscriber|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerTransport|95.8%|85.7%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|75%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseTransportSe<br/>rviceCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.RabbitMQ - 94.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.RabbitMQ**|**94.9%**|**91.4%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.RabbitMQ.AwaitingRabbitMqMessageDispatcher|88.7%|86.2%|
|AsyncResponse.Transports.RabbitMQ.QueuedRabbitMqMessageDispatcher|85.5%|81.4%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqAsyncResponseOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqBackgroundFailureContext|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqChannelAdapter|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionAdapter|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionFactoryAdapter|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqConsumer|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqCorrelationIdExtractor|95.2%|95.4%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqDelivery|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqMessageDispatcher|98.3%|96.2%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqOptionsValidator|100%|91.6%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetProvider|100%|88.2%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberOptions|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberService|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqTopology|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerSubscriber|100%|75%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerTransport|97.2%|98%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|50%|
|Microsoft.Extensions.DependencyInjection.RabbitMqAsyncResponseServiceCollec<br/>tionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.Redis - 93.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Redis**|**93.9%**|**95.4%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|93.5%|
|AsyncResponse.Transports.Redis.AwaitingRedisMessageDispatcher|100%|100%|
|AsyncResponse.Transports.Redis.IRedisStreamDatabase|33.3%||
|AsyncResponse.Transports.Redis.QueuedRedisMessageDispatcher|87.6%|93.7%|
|AsyncResponse.Transports.Redis.RedisAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Redis.RedisBackgroundFailureContext|85%||
|AsyncResponse.Transports.Redis.RedisCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Redis.RedisMessageDispatcher|92.8%|96.5%|
|AsyncResponse.Transports.Redis.RedisReplyTargetOptions|100%||
|AsyncResponse.Transports.Redis.RedisReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.Redis.RedisResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisStreamDatabaseAdapter|100%|100%|
|AsyncResponse.Transports.Redis.RedisStreamDelivery|100%||
|AsyncResponse.Transports.Redis.RedisSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Redis.RedisSubscriberService|90.9%|95.6%|
|AsyncResponse.Transports.Redis.RedisTransportKeySchema|100%|100%|
|AsyncResponse.Transports.Redis.RedisTransportOptionsValidator|92.3%|96.1%|
|AsyncResponse.Transports.Redis.RedisTransportRetry|100%|100%|
|AsyncResponse.Transports.Redis.RedisWorkerSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisWorkerTransport|100%|100%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|62.5%|
|AsyncResponse.Transports.WorkerIntakeGate|75%|100%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseTransportService<br/>CollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.SqlServer - 90.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SqlServer**|**90.6%**|**84.1%**|
|AsyncResponse.Internal.OpportunisticPrune|100%|80%|
|AsyncResponse.Internal.RelationalNamePlan|53.3%|41.6%|
|AsyncResponse.Internal.SqlServerRelationVerifier|82.1%|75%|
|AsyncResponse.Internal.SqlServerTransientFaults|94.3%|100%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbDeadLetterPrune|100%|75%|
|AsyncResponse.Transports.DbMessageDispatcherBase|89.9%|92.7%|
|AsyncResponse.Transports.DbTransportHeaders|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerBackgroundFailureContext|91.6%||
|AsyncResponse.Transports.SqlServer.SqlServerCorrelationIdExtractor|100%||
|AsyncResponse.Transports.SqlServer.SqlServerMessageDispatcher|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberService|97.3%|92.8%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportDelivery|100%||
|AsyncResponse.Transports.SqlServer.SqlServerTransportOptionsValidator|100%|96.5%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportRetry|100%||
|AsyncResponse.Transports.SqlServer.SqlServerTransportStore|89.2%|88.2%|
|AsyncResponse.Transports.SqlServer.SqlServerWorkerSubscriber|100%||
|AsyncResponse.Transports.SqlServer.SqlServerWorkerTransport|95.8%|64.2%|
|AsyncResponse.Transports.SubscriberSupervisor|60%|37.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseTransportSer<br/>viceCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.SQS - 96.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SQS**|**96.6%**|**90.3%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|98.2%|91.9%|
|AsyncResponse.Transports.SQS.AwaitingSqsMessageDispatcher|100%|100%|
|AsyncResponse.Transports.SQS.QueuedSqsMessageDispatcher|93.8%|90.9%|
|AsyncResponse.Transports.SQS.SqsAsyncResponseOptions|100%||
|AsyncResponse.Transports.SQS.SqsBackgroundFailureContext|100%||
|AsyncResponse.Transports.SQS.SqsClientAdapter|100%|97.5%|
|AsyncResponse.Transports.SQS.SqsClientFactory|100%|70%|
|AsyncResponse.Transports.SQS.SqsClientResolver|100%|100%|
|AsyncResponse.Transports.SQS.SqsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.SQS.SqsMessageDispatcher|99%|73.6%|
|AsyncResponse.Transports.SQS.SqsOptionsValidator|98.5%|94.7%|
|AsyncResponse.Transports.SQS.SqsOutboundMessage|100%||
|AsyncResponse.Transports.SQS.SqsQueueAddress|100%|87.5%|
|AsyncResponse.Transports.SQS.SqsQueueProvisioningService|98.5%|91.6%|
|AsyncResponse.Transports.SQS.SqsReceiveRequest|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetOptions|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetProvider|100%|91.6%|
|AsyncResponse.Transports.SQS.SqsResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.SQS.SqsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SQS.SqsSubscriberService|90.6%|93.7%|
|AsyncResponse.Transports.SQS.SqsTransportDelivery|100%||
|AsyncResponse.Transports.SQS.SqsWorkerSubscriber|100%|91.6%|
|AsyncResponse.Transports.SQS.SqsWorkerTransport|97.6%|87.7%|
|AsyncResponse.Transports.SubscriberSupervisor|93.3%|75%|
|AsyncResponse.Transports.WorkerIntakeGate|75%|100%|
|Microsoft.Extensions.DependencyInjection.SqsAsyncResponseServiceCollectionE<br/>xtensions|100%||

</details>
