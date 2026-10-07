# Summary - AsyncResponse (Release / net8.0+net10.0 / unit+integration)
<details open><summary>Summary</summary>

|||
|:---|:---|
| Generated on: | 10/07/2026 - 22:24:34 |
| Coverage date: | 10/07/2026 - 22:06:48 - 10/07/2026 - 22:21:33 |
| Parser: | MultiReport (16x Cobertura) |
| Assemblies: | 27 |
| Classes: | 520 |
| Files: | 255 |
| **Line coverage:** | 98.8% (38371 of 38835) |
| Covered lines: | 38371 |
| Uncovered lines: | 464 |
| Coverable lines: | 38835 |
| Total lines: | 79157 |
| **Branch coverage:** | 94% (15046 of 15993) |
| Covered branches: | 15046 |
| Total branches: | 15993 |
| **Method coverage:** | [Feature is only available for sponsors](https://reportgenerator.io/pro) |

</details>

## Coverage
<details><summary>AsyncResponse.Abstractions - 100%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Abstractions**|**100%**|**100%**|
|AsyncResponse.AsyncResponseContext|100%|100%|
|AsyncResponse.AsyncResponseDomainFailureException|100%||
|AsyncResponse.AsyncResponseIndeterminateDeliveryException|100%||
|AsyncResponse.AsyncResponsePayloadReflection|100%|100%|
|AsyncResponse.AsyncResponseReplyTarget|100%|100%|
|AsyncResponse.AsyncResponseRequestContext|100%||
|AsyncResponse.CallbackParam|100%||
|AsyncResponse.DurableFlowFailedException|100%||
|AsyncResponse.DurableFlowIdConflictException|100%||
|AsyncResponse.DurableFlowInterruptedException|100%||
|AsyncResponse.DurableFlowLeaseContendedException|100%||
|AsyncResponse.DurableFlowLeaseLostException|100%||
|AsyncResponse.DurableFlowNotDispatchedException|100%||
|AsyncResponse.DurableFlowRunEvent|100%||
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
|AsyncResponse.RecoveryStateUnconfirmedException|100%||
|AsyncResponse.RecoveryStateUnreadableException|100%||
|AsyncResponse.ReflectionCallDto|100%||
|AsyncResponse.ReflectionInvocationDto|100%||
|AsyncResponse.WorkerJobEnvelope|100%||
|AsyncResponse.WorkerJobEnvelopeSchema|100%||
|AsyncResponse.WorkerJobTooLargeException|100%||

</details>
<details><summary>AsyncResponse.Channels.MongoDB - 99.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.MongoDB**|**99.3%**|**92.8%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|98.7%|94.2%|
|AsyncResponse.Channels.DbRecoveryStateStoreBase|100%|92.5%|
|AsyncResponse.Channels.MongoDB.MongoChannelMessageDocument|100%||
|AsyncResponse.Channels.MongoDB.MongoChannelSubscriberDocument|100%||
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannel|100%|87.5%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannelOptions|100%|95.8%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.MongoDB.MongoDbChannelMessage|100%||
|AsyncResponse.Channels.MongoDB.MongoDbChannelStore|100%|87%|
|AsyncResponse.Channels.MongoDB.MongoDbRecoveryStateStore|100%||
|AsyncResponse.Channels.MongoDB.MongoRecoveryStateDocument|100%||
|AsyncResponse.Internal.MongoIndexes|100%|87.5%|
|AsyncResponse.Internal.MongoNamespaceRegistry|100%|100%|
|AsyncResponse.Internal.MongoOwnershipLedger|97.6%|100%|
|AsyncResponse.Internal.MongoTransientFaults|100%|100%|
|AsyncResponse.Internal.MongoWriteConcerns|100%|86.9%|
|AsyncResponse.Internal.NullableUtcBsonDateSerializer|100%||
|AsyncResponse.Internal.UtcBsonDateSerializer|100%||
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseChannelService<br/>CollectionExtensions|100%|91.6%|

</details>
<details><summary>AsyncResponse.Channels.NATS - 98.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.NATS**|**98.6%**|**94.4%**|
|AsyncResponse.Channels.NATS.INatsRawRequester|0%||
|AsyncResponse.Channels.NATS.INatsResponseChannelClient|100%||
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannel|98.5%|91.9%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.NATS.NatsConsumeLoopException|100%||
|AsyncResponse.Channels.NATS.NatsInboundResponse|100%||
|AsyncResponse.Channels.NATS.NatsKvEntry|100%||
|AsyncResponse.Channels.NATS.NatsKvStoreAdapter|100%|100%|
|AsyncResponse.Channels.NATS.NatsRawRequester|86%|50%|
|AsyncResponse.Channels.NATS.NatsRecoveryStateStore|100%|97.2%|
|AsyncResponse.Channels.NATS.NatsResponseChannelClient|100%|81.2%|
|AsyncResponse.Channels.NATS.NatsSubjectSchema|100%|100%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseChannelServiceCol<br/>lectionExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.PostgreSQL - 98.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.PostgreSQL**|**98.9%**|**92.9%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|98.1%|92.6%|
|AsyncResponse.Channels.DbRecoveryStateStoreBase|100%|90%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannel|100%|90%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelMessage|100%||
|AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql|100%|92.3%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlRecoveryStateStore|100%||
|AsyncResponse.Internal.OpportunisticPrune|100%|85%|
|AsyncResponse.Internal.PostgreSqlDdlGuard|98.2%|92.8%|
|AsyncResponse.Internal.PostgreSqlListenConnection|94.8%|96.1%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|100%|95.4%|
|AsyncResponse.Internal.PostgreSqlTransientFaults|100%|100%|
|AsyncResponse.Internal.RelationalNamePlan|100%|100%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseChannelServ<br/>iceCollectionExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.Redis - 99.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.Redis**|**99.5%**|**96.3%**|
|AsyncResponse.Channels.Redis.IRedisChannelSubscriber|100%||
|AsyncResponse.Channels.Redis.RedisAsyncResponseChannel|99.2%|96%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseOptions|100%|100%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.Redis.RedisChannelMessageQueueSubscriber|100%||
|AsyncResponse.Channels.Redis.RedisClusterNodeTable|100%|97.2%|
|AsyncResponse.Channels.Redis.RedisKeySchema|100%|100%|
|AsyncResponse.Channels.Redis.RedisRecoveryStateStore|100%|96.7%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseServiceCollectio<br/>nExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.Channels.SqlServer - 99.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.SqlServer**|**99.3%**|**93%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|98.8%|91.1%|
|AsyncResponse.Channels.DbRecoveryStateStoreBase|100%|90%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannel|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseWaiter`1|100%||
|AsyncResponse.Channels.SqlServer.SqlServerChannelMessage|100%||
|AsyncResponse.Channels.SqlServer.SqlServerChannelSql|100%|93.2%|
|AsyncResponse.Channels.SqlServer.SqlServerRecoveryStateStore|100%||
|AsyncResponse.Internal.OpportunisticPrune|100%|85%|
|AsyncResponse.Internal.RelationalNamePlan|100%|100%|
|AsyncResponse.Internal.SqlServerRelationVerifier|99.3%|97.6%|
|AsyncResponse.Internal.SqlServerTransientFaults|100%|100%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseChannelServi<br/>ceCollectionExtensions|100%|50%|

</details>
<details><summary>AsyncResponse.Core - 98.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Core**|**98.8%**|**94.2%**|
|AsyncResponse.AsyncResponseBuilder|100%||
|AsyncResponse.AsyncResponseBuilder`1|100%|100%|
|AsyncResponse.AsyncResponseBuilderBase|100%|100%|
|AsyncResponse.AsyncResponseChannelMarker|100%||
|AsyncResponse.AsyncResponseChannelOptions|100%|100%|
|AsyncResponse.AsyncResponseContextPropagation|100%|93%|
|AsyncResponse.AsyncResponseDiagnostics|100%|91%|
|AsyncResponse.AsyncResponseDurableFlowStoreMarker|100%|71.4%|
|AsyncResponse.AsyncResponseEnvelope`1|100%||
|AsyncResponse.AsyncResponseEnvelopeConverter`1|100%|98.9%|
|AsyncResponse.AsyncResponseEnvelopeJson|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeOptions`1|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeSchema|100%||
|AsyncResponse.AsyncResponseIngress|100%|93.4%|
|AsyncResponse.AsyncResponseJson|100%|90%|
|AsyncResponse.AsyncResponseJsonSerialization|100%||
|AsyncResponse.AsyncResponseOptions|100%||
|AsyncResponse.AsyncResponsePackageVersionGate|100%||
|AsyncResponse.AsyncResponsePackageVersions|100%|92.1%|
|AsyncResponse.AsyncResponseRecoveryHealthCheck|100%|95.2%|
|AsyncResponse.AsyncResponseRecoveryStats|100%||
|AsyncResponse.AsyncResponseRetry|100%|100%|
|AsyncResponse.AsyncResponseStaleRecoveryEntry|100%||
|AsyncResponse.AsyncResponseStartupValidator|100%|97.1%|
|AsyncResponse.AsyncResponseTransportMarker|100%||
|AsyncResponse.AsyncResponseTypeResolution|98.9%|93.7%|
|AsyncResponse.AsyncResponseWatchdog|100%|97.1%|
|AsyncResponse.AsyncResponseWatchdogOptions|100%|100%|
|AsyncResponse.AsyncResponseWatchdogReport|100%|100%|
|AsyncResponse.AsyncResponseWatchdogSnapshot|100%||
|AsyncResponse.AsyncResponseWatchdogState|100%|66.6%|
|AsyncResponse.CallbackExpressionConverter|100%|90.3%|
|AsyncResponse.CallbackTargetUnresolvableException|100%||
|AsyncResponse.ChannelSerialExecutor|98.6%|94.4%|
|AsyncResponse.CorrelationIdGuard|100%|100%|
|AsyncResponse.CronSchedule|98.7%|97.9%|
|AsyncResponse.CurrentReadFlowStateStore|100%||
|AsyncResponse.DiagnosticText|100%|98%|
|AsyncResponse.DurableAsyncResponseChannelOptions|100%||
|AsyncResponse.DurableFlowContext|98.5%|91.9%|
|AsyncResponse.DurableFlowExecutor|98.9%|93.5%|
|AsyncResponse.DurableFlowObserverLifetimeAudit|100%|93.7%|
|AsyncResponse.DurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlowRegistration|100%|83.3%|
|AsyncResponse.DurableFlowService|97.6%|100%|
|AsyncResponse.DurableFlowSuspendedException|100%||
|AsyncResponse.EngineGuardTimers|100%|100%|
|AsyncResponse.FlowExecutionLease|100%|91.4%|
|AsyncResponse.FlowLeaseContention|100%|100%|
|AsyncResponse.FlowStateConcurrency|100%|97.1%|
|AsyncResponse.FlowStateJson|100%|96.4%|
|AsyncResponse.FlowStateRetention|100%|100%|
|AsyncResponse.FlowStateSize|100%|100%|
|AsyncResponse.InMemoryAsyncResponseChannel|94.9%|91.8%|
|AsyncResponse.InMemoryAsyncResponseOptions|100%|100%|
|AsyncResponse.InMemoryAsyncResponseWaiter`1|100%||
|AsyncResponse.InMemoryFlowStateStore|100%|92.3%|
|AsyncResponse.InMemoryRecoveryStateStore|97.8%|92.5%|
|AsyncResponse.InMemoryWorkerHost|100%|93.1%|
|AsyncResponse.InMemoryWorkerTransport|100%|96.4%|
|AsyncResponse.InMemoryWorkerTransportOptions|100%|100%|
|AsyncResponse.JsonSafety|100%|85%|
|AsyncResponse.LostSubscriberCallbackDispatcher|95.2%|92.5%|
|AsyncResponse.LostSubscriberDispatchResult|100%||
|AsyncResponse.PayloadRecoveryClassifier|96.8%|88.3%|
|AsyncResponse.PortableText|100%|100%|
|AsyncResponse.RawJsonResponse|100%|100%|
|AsyncResponse.RecoverableAsyncResponseBuilder|100%||
|AsyncResponse.RecoverableAsyncResponseBuilder`1|100%|100%|
|AsyncResponse.RecoveryCallbackFailedException|100%||
|AsyncResponse.RecoveryClassification|100%||
|AsyncResponse.RecoveryRegistrationLimit|100%|100%|
|AsyncResponse.RecoveryStateContention|100%|100%|
|AsyncResponse.RecoveryStateObservation|100%||
|AsyncResponse.ReflectionCallDtoGuard|100%|100%|
|AsyncResponse.ReflectionExtensions|100%|94.3%|
|AsyncResponse.RemoteStackTrace|100%|100%|
|AsyncResponse.SafeLog|100%||
|AsyncResponse.ScheduledFlowOptions|100%||
|AsyncResponse.ScheduledFlowRegistration|100%||
|AsyncResponse.ScheduledFlowService|100%|95.6%|
|AsyncResponse.SerialExecutorRegistry|99.3%|97.3%|
|AsyncResponse.ShutdownBudgetValidator|100%|100%|
|AsyncResponse.TypeNameIdentity|100%|95.6%|
|AsyncResponse.UnresolvableTypeNames|95%|75%|
|AsyncResponse.WorkerJobExecutor|100%|95%|
|AsyncResponse.WorkerJobScope|100%|100%|
|AsyncResponse.WorkerJobSkewScope|100%|100%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAllowList|100%|87.5%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAuthorization<br/>Extensions|100%||
|Microsoft.Extensions.DependencyInjection.AsyncResponseCoreServiceCollection<br/>Extensions|100%|95.4%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseHealthCheckExtensions|100%||
|Microsoft.Extensions.DependencyInjection.AsyncResponseRegistrationBuilder|100%||
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.Cosmos - 99.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Cosmos**|**99.8%**|**95%**|
|AsyncResponse.DurableFlows.Cosmos.CosmosDurableFlowOptions|100%|93.7%|
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateDocument|100%||
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateStore|99.7%|94.1%|
|AsyncResponse.DurableFlows.Cosmos.CosmosLeaseProjection|100%||
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.8%|
|Microsoft.Extensions.DependencyInjection.CosmosDurableFlowServiceCollection<br/>Extensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.DynamoDB - 99.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.DynamoDB**|**99.7%**|**93.2%**|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbFlowStateStore|99.6%|89.6%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.8%|
|Microsoft.Extensions.DependencyInjection.DynamoDbDurableFlowServiceCollecti<br/>onExtensions|100%|50%|

</details>
<details><summary>AsyncResponse.DurableFlows.EFCore - 99.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.EFCore**|**99.5%**|**96.2%**|
|AsyncResponse.DurableFlows.EFCore.DurableFlowStateRecord|100%||
|AsyncResponse.DurableFlows.EFCore.EFCoreDurableFlowModelBuilderExtensions|100%|100%|
|AsyncResponse.DurableFlows.EFCore.EFCoreDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.EFCore.EFCoreFlowStateStore`1|99.1%|94.2%|
|AsyncResponse.DurableFlows.EFCore.FlowIdCollationRules|100%|100%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.8%|
|Microsoft.Extensions.DependencyInjection.EFCoreDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.MongoDB - 99.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.MongoDB**|**99.8%**|**94.7%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.8%|
|AsyncResponse.DurableFlows.MongoDB.MongoDbDurableFlowOptions|100%|91.6%|
|AsyncResponse.DurableFlows.MongoDB.MongoDbFlowStateStore|100%|94.4%|
|AsyncResponse.DurableFlows.MongoDB.MongoFlowStateDocument|100%||
|AsyncResponse.Internal.MongoNamespaceRegistry|100%|100%|
|AsyncResponse.Internal.MongoOwnershipLedger|97.6%|100%|
|AsyncResponse.Internal.MongoWriteConcerns|100%|86.9%|
|AsyncResponse.Internal.NullableUtcBsonDateSerializer|100%||
|AsyncResponse.Internal.UtcBsonDateSerializer|100%||
|Microsoft.Extensions.DependencyInjection.MongoDurableFlowServiceCollectionE<br/>xtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.MySql - 97.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.MySql**|**97.8%**|**91.3%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.8%|
|AsyncResponse.DurableFlows.MySql.MySqlDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.MySql.MySqlFlowStateStore|96.8%|87.6%|
|Microsoft.Extensions.DependencyInjection.MySqlDurableFlowServiceCollectionE<br/>xtensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.Oracle - 96.2%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Oracle**|**96.2%**|**91.1%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.8%|
|AsyncResponse.DurableFlows.Oracle.OracleDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.Oracle.OracleFlowStateStore|94.8%|87.1%|
|Microsoft.Extensions.DependencyInjection.OracleDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.PostgreSQL - 98.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.PostgreSQL**|**98.6%**|**96.3%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.8%|
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlFlowStateStore|96.8%|96%|
|AsyncResponse.Internal.PostgreSqlDdlGuard|99.1%|97.6%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|100%|95.4%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlDurableFlowServiceCollec<br/>tionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.Sqlite - 99.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Sqlite**|**99.7%**|**95%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.8%|
|AsyncResponse.DurableFlows.Sqlite.SqliteDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.Sqlite.SqliteFlowStateStore|99.6%|93.3%|
|Microsoft.Extensions.DependencyInjection.SqliteDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.SqlServer - 88.2%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.SqlServer**|**88.2%**|**91.8%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|96.8%|
|AsyncResponse.DurableFlows.SqlServer.SqlServerDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.SqlServer.SqlServerFlowStateStore|88.7%|86.6%|
|AsyncResponse.Internal.SqlServerRelationVerifier|82.2%|90.6%|
|Microsoft.Extensions.DependencyInjection.SqlServerDurableFlowServiceCollect<br/>ionExtensions|100%||

</details>
<details><summary>AsyncResponse.Testing - 99.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Testing**|**99.4%**|**93%**|
|AsyncResponse.Testing.AsyncResponseTestHarness|99.4%|93.1%|
|AsyncResponse.Testing.AsyncResponseTestHarnessOptions|100%||
|AsyncResponse.Testing.FlowProbe|100%|93.8%|
|AsyncResponse.Testing.FlowProbeEvent|100%||
|AsyncResponse.Testing.FlowRunHandle|100%|83.3%|
|AsyncResponse.Testing.FlowTestHarness|100%|50%|
|AsyncResponse.Testing.SimulatedCrashException|100%|100%|
|AsyncResponse.Testing.VirtualTimeProvider|98.5%|94.2%|

</details>
<details><summary>AsyncResponse.Transports.AzureServiceBus - 99.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.AzureServiceBus**|**99.6%**|**95.6%**|
|AsyncResponse.Transports.AzureServiceBus.AwaitingAzureServiceBusMessageDisp<br/>atcher|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusAsyncResponseOption<br/>s|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusBackgroundFailureCo<br/>ntext|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientResolver|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusCorrelationIdExtrac<br/>tor|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusMessageDispatcher|100%|97%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOptionsValidator|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOutboundMessage|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReceiverAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetOptions|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusResponseIngressSubs<br/>criber|100%|50%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSenderAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberOptions|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberService|98.8%|98.6%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusTransportDelivery|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerSubscriber|100%|83.3%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerTransport|100%|83.3%|
|AsyncResponse.Transports.AzureServiceBus.QueuedAzureServiceBusMessageDispat<br/>cher|100%|100%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|93.5%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|100%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.AzureServiceBusAsyncResponseServic<br/>eCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.GooglePubSub - 98.9%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.GooglePubSub**|**98.9%**|**95.4%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|93.5%|
|AsyncResponse.Transports.GooglePubSub.AwaitingGooglePubSubMessageDispatcher|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubAsyncResponseOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubBackgroundFailureContext|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubMessageDispatcher|99.2%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubOptionsValidator|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubPublisherClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberOptions|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberService|95.4%|84.3%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerSubscriber|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerTransport|100%|100%|
|AsyncResponse.Transports.GooglePubSub.QueuedGooglePubSubMessageDispatcher|100%|96.8%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.GooglePubSubAsyncResponseServiceCo<br/>llectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.Kafka - 99.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Kafka**|**99.3%**|**95.8%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|93.5%|
|AsyncResponse.Transports.Kafka.AwaitingKafkaMessageDispatcher|100%|94.8%|
|AsyncResponse.Transports.Kafka.KafkaAssignmentGenerations|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaBackgroundFailureContext|100%||
|AsyncResponse.Transports.Kafka.KafkaConsumerClientAdapter|100%|98%|
|AsyncResponse.Transports.Kafka.KafkaConsumerClientFactory|97.8%|87.5%|
|AsyncResponse.Transports.Kafka.KafkaCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaDeadLetterPublishFailedException|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaDeadLetterTooLargeException|100%||
|AsyncResponse.Transports.Kafka.KafkaDeadLetterUnconfirmedException|100%||
|AsyncResponse.Transports.Kafka.KafkaDelivery|100%||
|AsyncResponse.Transports.Kafka.KafkaIncomingMessage|100%||
|AsyncResponse.Transports.Kafka.KafkaMessageDispatcher|98.1%|93.7%|
|AsyncResponse.Transports.Kafka.KafkaPartitionNotAssignedException|100%||
|AsyncResponse.Transports.Kafka.KafkaProducerClientAdapter|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaPublishResult|100%||
|AsyncResponse.Transports.Kafka.KafkaRecordTooLargeException|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetProvider|100%|93.7%|
|AsyncResponse.Transports.Kafka.KafkaResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaSubscriberService|100%|96.2%|
|AsyncResponse.Transports.Kafka.KafkaTransportClientDefaults|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportHeader|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportOptionsValidator|100%|98.2%|
|AsyncResponse.Transports.Kafka.KafkaTransportRetry|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportTopicSchema|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaWorkerSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaWorkerTransport|100%|100%|
|AsyncResponse.Transports.Kafka.QueuedKafkaMessageDispatcher|98.4%|96%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.KafkaAsyncResponseTransportService<br/>CollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.MongoDB - 99.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.MongoDB**|**99.7%**|**91.7%**|
|AsyncResponse.Internal.MongoIndexes|100%|79.1%|
|AsyncResponse.Internal.MongoNamespaceRegistry|100%|100%|
|AsyncResponse.Internal.MongoOwnershipLedger|97.6%|100%|
|AsyncResponse.Internal.MongoTransientFaults|100%|81.2%|
|AsyncResponse.Internal.MongoWriteConcerns|100%|86.9%|
|AsyncResponse.Internal.NullableUtcBsonDateSerializer|100%||
|AsyncResponse.Internal.OpportunisticPrune|100%|80%|
|AsyncResponse.Internal.UtcBsonDateSerializer|100%||
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|93.5%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbDeadLetterPrune|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|99.6%|97.4%|
|AsyncResponse.Transports.DbTransportHeaders|100%|100%|
|AsyncResponse.Transports.MongoDB.LenientTransportHeaderSerializer|100%|96.6%|
|AsyncResponse.Transports.MongoDB.MongoDbAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.MongoDB.MongoDbBackgroundFailureContext|100%||
|AsyncResponse.Transports.MongoDB.MongoDbCorrelationIdExtractor|100%||
|AsyncResponse.Transports.MongoDB.MongoDbMessageDispatcher|100%||
|AsyncResponse.Transports.MongoDB.MongoDbReplyTargetOptions|100%||
|AsyncResponse.Transports.MongoDB.MongoDbReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.MongoDB.MongoDbSubscriberOptions|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbSubscriberService|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbTransportDelivery|100%||
|AsyncResponse.Transports.MongoDB.MongoDbTransportOptionsValidator|100%|95.6%|
|AsyncResponse.Transports.MongoDB.MongoDbTransportRetry|100%||
|AsyncResponse.Transports.MongoDB.MongoDbTransportStore|100%|88.3%|
|AsyncResponse.Transports.MongoDB.MongoDbWorkerSubscriber|100%||
|AsyncResponse.Transports.MongoDB.MongoDbWorkerTransport|100%|64.2%|
|AsyncResponse.Transports.MongoDB.MongoTransportMessageDocument|100%||
|AsyncResponse.Transports.SubscriberSupervisor|100%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseTransportServi<br/>ceCollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Transports.NATS - 99.3%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.NATS**|**99.3%**|**96.2%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|93.5%|
|AsyncResponse.Transports.NATS.INatsJetStreamTransport|100%||
|AsyncResponse.Transports.NATS.NatsAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.NATS.NatsBackgroundFailureContext|100%||
|AsyncResponse.Transports.NATS.NatsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.NATS.NatsJetStreamTransportAdapter|100%|95.1%|
|AsyncResponse.Transports.NATS.NatsJobDelivery|100%||
|AsyncResponse.Transports.NATS.NatsMessageDispatcher|98.6%|100%|
|AsyncResponse.Transports.NATS.NatsReplyTargetOptions|100%||
|AsyncResponse.Transports.NATS.NatsReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.NATS.NatsResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.NATS.NatsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.NATS.NatsSubscriberService|98.7%|96.1%|
|AsyncResponse.Transports.NATS.NatsTransportOptionsValidator|100%|97.1%|
|AsyncResponse.Transports.NATS.NatsTransportRetry|100%|100%|
|AsyncResponse.Transports.NATS.NatsTransportSubjectSchema|100%|100%|
|AsyncResponse.Transports.NATS.NatsWorkerSubscriber|100%||
|AsyncResponse.Transports.NATS.NatsWorkerTransport|100%|77.7%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|100%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseTransportServiceC<br/>ollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.PostgreSQL - 98.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.PostgreSQL**|**98.7%**|**95.3%**|
|AsyncResponse.Internal.OpportunisticPrune|100%|80%|
|AsyncResponse.Internal.PostgreSqlDdlGuard|99.1%|97.6%|
|AsyncResponse.Internal.PostgreSqlListenConnection|94.8%|96.1%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|100%|95.4%|
|AsyncResponse.Internal.PostgreSqlTransientFaults|100%|100%|
|AsyncResponse.Internal.RelationalNamePlan|100%|100%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|93.5%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbDeadLetterPrune|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|99.6%|97.4%|
|AsyncResponse.Transports.DbTransportHeaders|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlBackgroundFailureContext|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlCorrelationIdExtractor|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlMessageDispatcher|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberOptions|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberService|97.9%|93.3%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportDelivery|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportOptionsValidator|100%|96.1%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportRetry|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportStore|96.2%|95.8%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerSubscriber|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerTransport|100%|85.7%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseTransportSe<br/>rviceCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.RabbitMQ - 99.1%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.RabbitMQ**|**99.1%**|**94.1%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|93.5%|
|AsyncResponse.Transports.RabbitMQ.AwaitingRabbitMqMessageDispatcher|99.4%|91.6%|
|AsyncResponse.Transports.RabbitMQ.QueuedRabbitMqMessageDispatcher|98.8%|97.5%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqAsyncResponseOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqBackgroundFailureContext|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqBoundedClose|77.2%|66.6%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqChannelAdapter|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionAdapter|90.9%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionFactoryAdapter|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqConsumer|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqDelivery|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqMessageDispatcher|99.6%|94.6%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqOptionsValidator|100%|90%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetProvider|100%|88.2%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberOptions|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberService|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqTopology|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerSubscriber|100%|75%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerTransport|100%|98.1%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.RabbitMqAsyncResponseServiceCollec<br/>tionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.Redis - 98.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Redis**|**98.7%**|**96.8%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|95.1%|
|AsyncResponse.Transports.Redis.AwaitingRedisMessageDispatcher|100%|100%|
|AsyncResponse.Transports.Redis.IRedisStreamDatabase|100%||
|AsyncResponse.Transports.Redis.QueuedRedisMessageDispatcher|94.3%|100%|
|AsyncResponse.Transports.Redis.RedisAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Redis.RedisBackgroundFailureContext|100%||
|AsyncResponse.Transports.Redis.RedisCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Redis.RedisMessageDispatcher|100%|95.8%|
|AsyncResponse.Transports.Redis.RedisReplyTargetOptions|100%||
|AsyncResponse.Transports.Redis.RedisReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.Redis.RedisResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisStreamDatabaseAdapter|98.3%|95.8%|
|AsyncResponse.Transports.Redis.RedisStreamDelivery|100%||
|AsyncResponse.Transports.Redis.RedisSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Redis.RedisSubscriberService|98.9%|95.6%|
|AsyncResponse.Transports.Redis.RedisTransportKeySchema|100%|100%|
|AsyncResponse.Transports.Redis.RedisTransportOptionsValidator|100%|100%|
|AsyncResponse.Transports.Redis.RedisTransportRetry|100%|100%|
|AsyncResponse.Transports.Redis.RedisWorkerSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisWorkerTransport|100%|100%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseTransportService<br/>CollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.SqlServer - 97%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SqlServer**|**97%**|**93.7%**|
|AsyncResponse.Internal.OpportunisticPrune|100%|80%|
|AsyncResponse.Internal.RelationalNamePlan|100%|100%|
|AsyncResponse.Internal.SqlServerRelationVerifier|91.5%|93.7%|
|AsyncResponse.Internal.SqlServerTransientFaults|98.1%|100%|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|93.5%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbDeadLetterPrune|100%|75%|
|AsyncResponse.Transports.DbMessageDispatcherBase|99.6%|97.4%|
|AsyncResponse.Transports.DbTransportHeaders|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerBackgroundFailureContext|100%||
|AsyncResponse.Transports.SqlServer.SqlServerCorrelationIdExtractor|100%||
|AsyncResponse.Transports.SqlServer.SqlServerMessageDispatcher|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberService|100%|92.8%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportDelivery|100%||
|AsyncResponse.Transports.SqlServer.SqlServerTransportOptionsValidator|100%|96.5%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportRetry|100%||
|AsyncResponse.Transports.SqlServer.SqlServerTransportStore|94.2%|93.1%|
|AsyncResponse.Transports.SqlServer.SqlServerWorkerSubscriber|100%||
|AsyncResponse.Transports.SqlServer.SqlServerWorkerTransport|100%|64.2%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseTransportSer<br/>viceCollectionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.SQS - 99.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SQS**|**99.4%**|**91.7%**|
|AsyncResponse.Transports.CorrelationIdJsonPaths|99.1%|93.5%|
|AsyncResponse.Transports.SQS.AwaitingSqsMessageDispatcher|100%|100%|
|AsyncResponse.Transports.SQS.QueuedSqsMessageDispatcher|100%|100%|
|AsyncResponse.Transports.SQS.SqsAsyncResponseOptions|100%||
|AsyncResponse.Transports.SQS.SqsBackgroundFailureContext|100%||
|AsyncResponse.Transports.SQS.SqsClientAdapter|100%|97.5%|
|AsyncResponse.Transports.SQS.SqsClientFactory|100%|70%|
|AsyncResponse.Transports.SQS.SqsClientResolver|100%|100%|
|AsyncResponse.Transports.SQS.SqsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.SQS.SqsMessageDispatcher|100%|76.3%|
|AsyncResponse.Transports.SQS.SqsOptionsValidator|100%|97%|
|AsyncResponse.Transports.SQS.SqsOutboundMessage|100%||
|AsyncResponse.Transports.SQS.SqsQueueAddress|100%|87.5%|
|AsyncResponse.Transports.SQS.SqsQueueProvisioningService|98.5%|91.6%|
|AsyncResponse.Transports.SQS.SqsReceiveRequest|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetOptions|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetProvider|100%|91.6%|
|AsyncResponse.Transports.SQS.SqsResponseIngressSubscriber|100%|50%|
|AsyncResponse.Transports.SQS.SqsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SQS.SqsSubscriberService|98%|95%|
|AsyncResponse.Transports.SQS.SqsTransportDelivery|100%||
|AsyncResponse.Transports.SQS.SqsWorkerSubscriber|100%|91.6%|
|AsyncResponse.Transports.SQS.SqsWorkerTransport|100%|87.7%|
|AsyncResponse.Transports.SubscriberSupervisor|100%|87.5%|
|AsyncResponse.Transports.WorkerIntakeGate|100%|100%|
|Microsoft.Extensions.DependencyInjection.SqsAsyncResponseServiceCollectionE<br/>xtensions|100%||

</details>
