# Summary - AsyncResponse (Release / net8.0+net10.0 / unit+integration)
<details open><summary>Summary</summary>

|||
|:---|:---|
| Generated on: | 08/09/2026 - 21:24:57 |
| Parser: | MultiReport (3x Cobertura) |
| Assemblies: | 26 |
| Classes: | 452 |
| Files: | 203 |
| **Line coverage:** | 98.5% (25580 of 25953) |
| Covered lines: | 25580 |
| Uncovered lines: | 373 |
| Coverable lines: | 25953 |
| Total lines: | 38733 |
| **Branch coverage:** | 95.1% (8942 of 9394) |
| Covered branches: | 8942 |
| Total branches: | 9394 |
| **Method coverage:** | [Feature is only available for sponsors](https://reportgenerator.io/pro) |

</details>

## Coverage
<details><summary>AsyncResponse.Abstractions - 100%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Abstractions**|**100%**|**100%**|
|AsyncResponse.AsyncResponseContext|100%|100%|
|AsyncResponse.AsyncResponseContext.ContextScope|100%|100%|
|AsyncResponse.AsyncResponseContext.CorrelationScope|100%|100%|
|AsyncResponse.AsyncResponseDomainFailureException|100%||
|AsyncResponse.AsyncResponseIndeterminateDeliveryException|100%||
|AsyncResponse.AsyncResponsePayloadReflection|100%|100%|
|AsyncResponse.AsyncResponseReplyTarget|100%||
|AsyncResponse.AsyncResponseRequestContext|100%||
|AsyncResponse.CallbackParam|100%||
|AsyncResponse.DurableFlowFailedException|100%||
|AsyncResponse.FlowState|100%||
|AsyncResponse.FlowStateSchema|100%||
|AsyncResponse.IAsyncResponsePayload|100%||
|AsyncResponse.Placeholder|100%||
|AsyncResponse.RecoveryState|100%||
|AsyncResponse.RecoveryStateSchema|100%||
|AsyncResponse.WorkerJobEnvelope|100%||
|AsyncResponse.WorkerJobEnvelopeSchema|100%||

</details>
<details><summary>AsyncResponse.Channels.MongoDB - 99.1%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.MongoDB**|**99.1%**|**99.1%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|99.6%|98.8%|
|AsyncResponse.Channels.DbAsyncResponseChannelBase.DbSubscription<T>|100%|100%|
|AsyncResponse.Channels.DbAsyncResponseChannelBase.DbSubscription<T>|100%|100%|
|AsyncResponse.Channels.DbAsyncResponseChannelBase.PendingConfirmation|100%||
|AsyncResponse.Channels.DbAsyncResponseChannelBase<T>|99.6%|98.8%|
|AsyncResponse.Channels.MongoDB.MongoChannelMessageDocument|100%||
|AsyncResponse.Channels.MongoDB.MongoChannelSubscriberDocument|100%||
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannel|73.8%|100%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseWaiter<T>|100%||
|AsyncResponse.Channels.MongoDB.MongoDbChannelStore|99.7%|100%|
|AsyncResponse.Channels.MongoDB.MongoDbChannelStore<TDocument>|99.7%|100%|
|AsyncResponse.Channels.MongoDB.MongoDbRecoveryStateStore|100%|97.2%|
|AsyncResponse.Channels.MongoDB.MongoRecoveryStateDocument|33.3%||
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseChannelService<br/>CollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Channels.NATS - 99.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.NATS**|**99.5%**|**95.6%**|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannel|99.3%|94.1%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannel<T>|99.3%|94.1%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.NATS.NatsAsyncResponseWaiter<T>|100%||
|AsyncResponse.Channels.NATS.NatsConsumeLoopException|100%||
|AsyncResponse.Channels.NATS.NatsKvStoreAdapter|100%|90%|
|AsyncResponse.Channels.NATS.NatsRawRequester|100%||
|AsyncResponse.Channels.NATS.NatsRecoveryStateStore|100%|98.7%|
|AsyncResponse.Channels.NATS.NatsResponseChannelClient|100%|100%|
|AsyncResponse.Channels.NATS.NatsResponseChannelClient.NatsChannelSubscripti<br/>on|100%|100%|
|AsyncResponse.Channels.NATS.NatsSubjectSchema|100%|100%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseChannelServiceCol<br/>lectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Channels.PostgreSQL - 99.1%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.PostgreSQL**|**99.1%**|**96.5%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|99.2%|98.4%|
|AsyncResponse.Channels.DbAsyncResponseChannelBase.DbSubscription<T>|100%|100%|
|AsyncResponse.Channels.DbAsyncResponseChannelBase.DbSubscription<T>|100%|100%|
|AsyncResponse.Channels.DbAsyncResponseChannelBase.PendingConfirmation|100%||
|AsyncResponse.Channels.DbAsyncResponseChannelBase<T>|99.2%|98.4%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannel|100%|100%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseWaiter<T>|100%||
|AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql|100%|100%|
|AsyncResponse.Channels.PostgreSQL.PostgreSqlRecoveryStateStore|100%|97.2%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|89%|62.5%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseChannelServ<br/>iceCollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Channels.Redis - 98.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.Redis**|**98.8%**|**94.2%**|
|AsyncResponse.Channels.Redis.RedisAsyncResponseChannel|98.6%|93.2%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseChannel<T>|98.6%|93.2%|
|AsyncResponse.Channels.Redis.RedisAsyncResponseOptions|100%||
|AsyncResponse.Channels.Redis.RedisAsyncResponseWaiter<T>|100%||
|AsyncResponse.Channels.Redis.RedisChannelMessageQueueSubscriber|100%||
|AsyncResponse.Channels.Redis.RedisChannelMessageQueueSubscriber.Subscriptio<br/>n|100%||
|AsyncResponse.Channels.Redis.RedisKeySchema|100%||
|AsyncResponse.Channels.Redis.RedisRecoveryStateStore|100%|98.2%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseServiceCollectio<br/>nExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Channels.SqlServer - 99.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Channels.SqlServer**|**99.5%**|**98.7%**|
|AsyncResponse.Channels.DbAsyncResponseChannelBase|99.3%|98.4%|
|AsyncResponse.Channels.DbAsyncResponseChannelBase.DbSubscription<T>|100%|100%|
|AsyncResponse.Channels.DbAsyncResponseChannelBase.DbSubscription<T>|100%|100%|
|AsyncResponse.Channels.DbAsyncResponseChannelBase.PendingConfirmation|100%||
|AsyncResponse.Channels.DbAsyncResponseChannelBase<T>|99.3%|98.4%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannel|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannelOptions|100%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseWaiter<T>|100%||
|AsyncResponse.Channels.SqlServer.SqlServerChannelSql|99.7%|100%|
|AsyncResponse.Channels.SqlServer.SqlServerRecoveryStateStore|100%|97.2%|
|AsyncResponse.Channels.SqlServer.SqlServerTransientFaults|100%|100%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseChannelServi<br/>ceCollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Core - 97.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Core**|**97.6%**|**92.3%**|
|AsyncResponse.AsyncResponseBuilder|100%|100%|
|AsyncResponse.AsyncResponseBuilder<T>|100%|100%|
|AsyncResponse.AsyncResponseBuilder<T>|100%|100%|
|AsyncResponse.AsyncResponseBuilderBase|100%|100%|
|AsyncResponse.AsyncResponseChannelMarker|100%||
|AsyncResponse.AsyncResponseChannelOptions|100%|100%|
|AsyncResponse.AsyncResponseContextPropagation|100%|96.6%|
|AsyncResponse.AsyncResponseContextPropagation.CompositeScope|100%|100%|
|AsyncResponse.AsyncResponseContextPropagation.CompositeScope2|100%|100%|
|AsyncResponse.AsyncResponseContextPropagation.LazyCapturingCarrier|100%|91.6%|
|AsyncResponse.AsyncResponseContextPropagation.NullScope|100%||
|AsyncResponse.AsyncResponseDiagnostics|98.7%|92.8%|
|AsyncResponse.AsyncResponseDurableFlowStoreMarker|100%|50%|
|AsyncResponse.AsyncResponseEnvelope<T>|100%||
|AsyncResponse.AsyncResponseEnvelopeConverter<T>|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeJson|100%||
|AsyncResponse.AsyncResponseEnvelopeOptions.EnvelopeResolver<T>|100%|100%|
|AsyncResponse.AsyncResponseEnvelopeOptions<T>|100%||
|AsyncResponse.AsyncResponseEnvelopeSchema|100%||
|AsyncResponse.AsyncResponseIngress|100%|100%|
|AsyncResponse.AsyncResponseJson|100%|80%|
|AsyncResponse.AsyncResponseJson.ChainResolver|100%|87.5%|
|AsyncResponse.AsyncResponseJsonSerialization|100%||
|AsyncResponse.AsyncResponseOptions|100%||
|AsyncResponse.AsyncResponseRecoveryHealthCheck|100%|100%|
|AsyncResponse.AsyncResponseRecoveryStats|100%||
|AsyncResponse.AsyncResponseRetry|100%|100%|
|AsyncResponse.AsyncResponseRetry<T>|100%|100%|
|AsyncResponse.AsyncResponseStaleRecoveryEntry|100%||
|AsyncResponse.AsyncResponseStartupValidator|97.1%|92.5%|
|AsyncResponse.AsyncResponseTransportMarker|100%||
|AsyncResponse.AsyncResponseTypeResolution|100%|100%|
|AsyncResponse.AsyncResponseWatchdog|97%|91.6%|
|AsyncResponse.AsyncResponseWatchdogOptions|100%||
|AsyncResponse.AsyncResponseWatchdogReport|100%|100%|
|AsyncResponse.AsyncResponseWatchdogSnapshot|100%||
|AsyncResponse.AsyncResponseWatchdogState|100%||
|AsyncResponse.CallbackExpressionConverter|98.1%|88.6%|
|AsyncResponse.CallbackExpressionConverter.MethodCallGuard|100%|66.6%|
|AsyncResponse.CallbackExpressionConverter.ParameterGuard|100%|100%|
|AsyncResponse.CallbackExpressionConverter<TService>|98.1%|88.6%|
|AsyncResponse.ChannelSerialExecutor|100%|100%|
|AsyncResponse.DurableAsyncResponseChannelOptions|100%||
|AsyncResponse.DurableFlowContext|93.8%|83.8%|
|AsyncResponse.DurableFlowContext<TFlow, TInput>|93.8%|83.8%|
|AsyncResponse.DurableFlowContext<TResponse>|93.8%|83.8%|
|AsyncResponse.DurableFlowContext<TResult>|93.8%|83.8%|
|AsyncResponse.DurableFlowExecutor|98.2%|93.1%|
|AsyncResponse.DurableFlowOptions|100%||
|AsyncResponse.DurableFlowService|100%|100%|
|AsyncResponse.DurableFlowService<TFlow, TInput>|100%|100%|
|AsyncResponse.DurableFlowSuspendedException|100%||
|AsyncResponse.FlowExecutionLease|96.7%|85.7%|
|AsyncResponse.FlowStateConcurrency|100%|100%|
|AsyncResponse.FlowStateJson|100%|100%|
|AsyncResponse.InMemoryAsyncResponseChannel|96.4%|88.2%|
|AsyncResponse.InMemoryAsyncResponseChannel.DeclaredWireSerializer<TDeclared<br/>>|100%||
|AsyncResponse.InMemoryAsyncResponseChannel.DeclaredWireSerializer<TDeclared<br/>>|100%||
|AsyncResponse.InMemoryAsyncResponseChannel.Subscription<T>|100%|100%|
|AsyncResponse.InMemoryAsyncResponseChannel.Subscription<T>|100%|100%|
|AsyncResponse.InMemoryAsyncResponseChannel.SubscriptionBase|98.1%|90.4%|
|AsyncResponse.InMemoryAsyncResponseChannel.SubscriptionBase<TState>|98.1%|90.4%|
|AsyncResponse.InMemoryAsyncResponseChannel.SubscriptionGroup|100%|100%|
|AsyncResponse.InMemoryAsyncResponseChannel.SubscriptionSnapshot|100%|100%|
|AsyncResponse.InMemoryAsyncResponseChannel<T>|96.4%|88.2%|
|AsyncResponse.InMemoryAsyncResponseOptions|100%||
|AsyncResponse.InMemoryAsyncResponseWaiter<T>|100%||
|AsyncResponse.InMemoryFlowStateStore|100%|96.1%|
|AsyncResponse.InMemoryFlowStateStore.Entry|100%||
|AsyncResponse.InMemoryRecoveryStateStore|96.7%|93.6%|
|AsyncResponse.InMemoryRecoveryStateStore.Entry|100%||
|AsyncResponse.InMemoryRecoveryStateStore.EntryBucket|94%|94.1%|
|AsyncResponse.InMemoryWorkerHost|88.4%|87.5%|
|AsyncResponse.InMemoryWorkerTransport|100%|100%|
|AsyncResponse.InMemoryWorkerTransportOptions|100%|100%|
|AsyncResponse.JsonSafety|100%|100%|
|AsyncResponse.LostSubscriberCallbackDispatcher|98.9%|92.3%|
|AsyncResponse.LostSubscriberCallbackDispatcher<T>|98.9%|92.3%|
|AsyncResponse.MongoNamespaceRegistry|100%|100%|
|AsyncResponse.PayloadRecoveryClassifier|100%|100%|
|AsyncResponse.RawJsonResponse|100%|100%|
|AsyncResponse.RecoverableAsyncResponseBuilder|100%||
|AsyncResponse.RecoverableAsyncResponseBuilder<T>|100%|100%|
|AsyncResponse.RecoveryStateObservation|100%||
|AsyncResponse.ReflectionExtensions|100%|98.9%|
|AsyncResponse.ReflectionExtensions.ConversionPlan|100%|95%|
|AsyncResponse.ReflectionExtensions.InvocationPlan|100%|100%|
|AsyncResponse.ReflectionExtensions<T>|100%|98.9%|
|AsyncResponse.RemoteStackTrace|100%|100%|
|AsyncResponse.SerialExecutorRegistry|100%|100%|
|AsyncResponse.SerialExecutorRegistry.ExecutorEntry|100%||
|AsyncResponse.ShutdownBudgetValidator|100%|100%|
|AsyncResponse.UnresolvableTypeNames|95%|90%|
|AsyncResponse.WorkerJobExecutor|100%|100%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAllowList|100%|100%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAllowList.All<br/>owListAuthorizer|100%|100%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCallbackAuthorization<br/>Extensions|100%||
|Microsoft.Extensions.DependencyInjection.AsyncResponseCoreServiceCollection<br/>Extensions|100%|97.5%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCoreServiceCollection<br/>Extensions<TFlow, TInput>|100%|97.5%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseCoreServiceCollection<br/>Extensions<TFlowStateStore, TOptions>|100%|97.5%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseHealthCheckExtensions|100%|100%|
|Microsoft.Extensions.DependencyInjection.AsyncResponseRegistrationBuilder|100%||
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1,<br/> T2, T3, T4, T5>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1,<br/> T2, T3, T4, T5>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1,<br/> T2, T3, T4>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1,<br/> T2, T3, T4>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1,<br/> T2, T3>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1,<br/> T2, T3>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1,<br/> T2>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1,<br/> T2>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0, T1>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0>|100%|100%|
|Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions.LogCache<T0>|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.Cosmos - 97.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Cosmos**|**97.6%**|**95.4%**|
|AsyncResponse.DurableFlows.Cosmos.CosmosDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateDocument|100%||
|AsyncResponse.DurableFlows.Cosmos.CosmosFlowStateStore|96.5%|90.5%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|100%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|Microsoft.Extensions.DependencyInjection.CosmosDurableFlowServiceCollection<br/>Extensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.DynamoDB - 99.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.DynamoDB**|**99.4%**|**92%**|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.DynamoDB.DynamoDbFlowStateStore|99.2%|87.2%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|100%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|Microsoft.Extensions.DependencyInjection.DynamoDbDurableFlowServiceCollecti<br/>onExtensions|100%|75%|

</details>
<details><summary>AsyncResponse.DurableFlows.EFCore - 99.4%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.EFCore**|**99.4%**|**94.3%**|
|AsyncResponse.DurableFlows.EFCore.DurableFlowStateRecord|100%||
|AsyncResponse.DurableFlows.EFCore.EFCoreDurableFlowModelBuilderExtensions|100%||
|AsyncResponse.DurableFlows.EFCore.EFCoreDurableFlowOptions|100%||
|AsyncResponse.DurableFlows.EFCore.EFCoreFlowStateStore.ContextLease<TContex<br/>t>|100%|100%|
|AsyncResponse.DurableFlows.EFCore.EFCoreFlowStateStore.ContextLease<TContex<br/>t>|100%||
|AsyncResponse.DurableFlows.EFCore.EFCoreFlowStateStore.StateRow<TContext>|100%||
|AsyncResponse.DurableFlows.EFCore.EFCoreFlowStateStore<TContext>|100%|100%|
|AsyncResponse.DurableFlows.EFCore.EFCoreFlowStateStore<TContext>|99.2%|85%|
|AsyncResponse.DurableFlows.EFCore.EFCoreFlowStateStore<TContext>|99.2%|83.3%|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|100%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|Microsoft.Extensions.DependencyInjection.EFCoreDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.MongoDB - 100%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.MongoDB**|**100%**|**99.1%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|100%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.MongoDB.MongoDbDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.MongoDB.MongoDbFlowStateStore|100%|97%|
|AsyncResponse.DurableFlows.MongoDB.MongoFlowStateDocument|100%||
|Microsoft.Extensions.DependencyInjection.MongoDurableFlowServiceCollectionE<br/>xtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.MySql - 99.5%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.MySql**|**99.5%**|**96.2%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|100%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.MySql.MySqlDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.MySql.MySqlFlowStateStore|99.3%|81.2%|
|Microsoft.Extensions.DependencyInjection.MySqlDurableFlowServiceCollectionE<br/>xtensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.Oracle - 99.2%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Oracle**|**99.2%**|**96.2%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|100%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.Oracle.OracleDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.Oracle.OracleFlowStateStore|98.8%|81.2%|
|Microsoft.Extensions.DependencyInjection.OracleDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.PostgreSQL - 88.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.PostgreSQL**|**88.8%**|**76.4%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|100%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlFlowStateStore|97.1%|83.3%|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|56%|39.5%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlDurableFlowServiceCollec<br/>tionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.DurableFlows.Sqlite - 100%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.Sqlite**|**100%**|**98.7%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|100%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.Sqlite.SqliteDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.Sqlite.SqliteFlowStateStore|100%|93.7%|
|Microsoft.Extensions.DependencyInjection.SqliteDurableFlowServiceCollection<br/>Extensions|100%||

</details>
<details><summary>AsyncResponse.DurableFlows.SqlServer - 99.6%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.DurableFlows.SqlServer**|**99.6%**|**96.2%**|
|AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared|100%|100%|
|AsyncResponse.DurableFlows.Internal.FlowStateTooLargeException|100%||
|AsyncResponse.DurableFlows.SqlServer.SqlServerDurableFlowOptions|100%|100%|
|AsyncResponse.DurableFlows.SqlServer.SqlServerFlowStateStore|99.4%|81.2%|
|Microsoft.Extensions.DependencyInjection.SqlServerDurableFlowServiceCollect<br/>ionExtensions|100%||

</details>
<details><summary>AsyncResponse.Transports.AzureServiceBus - 97.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.AzureServiceBus**|**97.7%**|**95%**|
|AsyncResponse.Transports.AzureServiceBus.AwaitingAzureServiceBusMessageDisp<br/>atcher|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusAsyncResponseOption<br/>s|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusBackgroundFailureCo<br/>ntext|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusClientResolver|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusCorrelationIdExtrac<br/>tor|98.4%|98.2%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusMessageDispatcher|100%|75%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOptionsValidator|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusOutboundMessage|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReceiverAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetOptions|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusResponseIngressSubs<br/>criber|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSenderAdapter|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberOptions|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberService|85.8%|88.8%|
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusSubscriberService.B<br/>atchProgress|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusTransportDelivery|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerSubscriber|100%||
|AsyncResponse.Transports.AzureServiceBus.AzureServiceBusWorkerTransport|100%|100%|
|AsyncResponse.Transports.AzureServiceBus.QueuedAzureServiceBusMessageDispat<br/>cher|100%|100%|
|Microsoft.Extensions.DependencyInjection.AzureServiceBusAsyncResponseServic<br/>eCollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Transports.GooglePubSub - 99.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.GooglePubSub**|**99.8%**|**99.3%**|
|AsyncResponse.Transports.GooglePubSub.AwaitingGooglePubSubMessageDispatcher|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubAsyncResponseOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubBackgroundFailureContext|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubMessageDispatcher|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubOptionsValidator|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubPublisherClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetOptions|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubResponseIngressSubscriber|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberClientAdapter|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberOptions|100%|100%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubSubscriberService|98.4%|75%|
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerSubscriber|100%||
|AsyncResponse.Transports.GooglePubSub.GooglePubSubWorkerTransport|100%|100%|
|AsyncResponse.Transports.GooglePubSub.QueuedGooglePubSubMessageDispatcher|100%|100%|
|Microsoft.Extensions.DependencyInjection.GooglePubSubAsyncResponseServiceCo<br/>llectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Transports.Kafka - 99.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Kafka**|**99.8%**|**98.7%**|
|AsyncResponse.Transports.Kafka.AwaitingKafkaMessageDispatcher|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaBackgroundFailureContext|100%||
|AsyncResponse.Transports.Kafka.KafkaConsumerClientAdapter|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaConsumerClientFactory|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaDelivery|100%||
|AsyncResponse.Transports.Kafka.KafkaIncomingMessage|100%||
|AsyncResponse.Transports.Kafka.KafkaMessageDispatcher|100%|97.7%|
|AsyncResponse.Transports.Kafka.KafkaProducerClientAdapter|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaPublishResult|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetOptions|100%||
|AsyncResponse.Transports.Kafka.KafkaReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaSubscriberService|100%|95.8%|
|AsyncResponse.Transports.Kafka.KafkaTransportClientDefaults|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportHeader|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportOptionsValidator|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportRetry|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaTransportTopicSchema|100%|100%|
|AsyncResponse.Transports.Kafka.KafkaWorkerSubscriber|100%||
|AsyncResponse.Transports.Kafka.KafkaWorkerTransport|100%|100%|
|AsyncResponse.Transports.Kafka.QueuedKafkaMessageDispatcher|99.1%|87.5%|
|Microsoft.Extensions.DependencyInjection.KafkaAsyncResponseTransportService<br/>CollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Transports.MongoDB - 98.2%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.MongoDB**|**98.2%**|**97.1%**|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|97%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.MongoDB.MongoDbBackgroundFailureContext|100%||
|AsyncResponse.Transports.MongoDB.MongoDbCorrelationIdExtractor|100%||
|AsyncResponse.Transports.MongoDB.MongoDbMessageDispatcher|100%||
|AsyncResponse.Transports.MongoDB.MongoDbReplyTargetOptions|100%||
|AsyncResponse.Transports.MongoDB.MongoDbReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbResponseIngressSubscriber|100%||
|AsyncResponse.Transports.MongoDB.MongoDbSubscriberOptions|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbSubscriberService|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbTransportDelivery|100%||
|AsyncResponse.Transports.MongoDB.MongoDbTransportOptionsValidator|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbTransportRetry|100%|100%|
|AsyncResponse.Transports.MongoDB.MongoDbTransportStore|96.4%|94%|
|AsyncResponse.Transports.MongoDB.MongoDbWorkerSubscriber|100%||
|AsyncResponse.Transports.MongoDB.MongoDbWorkerTransport|100%|62.5%|
|AsyncResponse.Transports.MongoDB.MongoTransportMessageDocument|100%||
|Microsoft.Extensions.DependencyInjection.MongoDbAsyncResponseTransportServi<br/>ceCollectionExtensions|97.9%|93.7%|

</details>
<details><summary>AsyncResponse.Transports.NATS - 100%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.NATS**|**100%**|**96.8%**|
|AsyncResponse.Transports.NATS.NatsAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.NATS.NatsBackgroundFailureContext|100%||
|AsyncResponse.Transports.NATS.NatsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.NATS.NatsJetStreamTransportAdapter|100%|94.4%|
|AsyncResponse.Transports.NATS.NatsJobDelivery|100%||
|AsyncResponse.Transports.NATS.NatsMessageDispatcher|100%|100%|
|AsyncResponse.Transports.NATS.NatsReplyTargetOptions|100%||
|AsyncResponse.Transports.NATS.NatsReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.NATS.NatsResponseIngressSubscriber|100%||
|AsyncResponse.Transports.NATS.NatsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.NATS.NatsSubscriberService|100%|83.3%|
|AsyncResponse.Transports.NATS.NatsTransportOptionsValidator|100%|100%|
|AsyncResponse.Transports.NATS.NatsTransportRetry|100%|100%|
|AsyncResponse.Transports.NATS.NatsTransportSubjectSchema|100%|100%|
|AsyncResponse.Transports.NATS.NatsWorkerSubscriber|100%||
|AsyncResponse.Transports.NATS.NatsWorkerTransport|100%|75%|
|Microsoft.Extensions.DependencyInjection.NatsAsyncResponseTransportServiceC<br/>ollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Transports.PostgreSQL - 97%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.PostgreSQL**|**97%**|**95.2%**|
|AsyncResponse.Internal.PostgreSqlRelationVerifier|92.6%|75%|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|97%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlBackgroundFailureContext|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlCorrelationIdExtractor|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlMessageDispatcher|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetOptions|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlResponseIngressSubscriber|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberOptions|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberService|98.6%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportDelivery|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportOptionsValidator|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportRetry|100%|100%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportStore|94.3%|97.6%|
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerSubscriber|100%||
|AsyncResponse.Transports.PostgreSQL.PostgreSqlWorkerTransport|100%|100%|
|Microsoft.Extensions.DependencyInjection.PostgreSqlAsyncResponseTransportSe<br/>rviceCollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Transports.RabbitMQ - 98.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.RabbitMQ**|**98.8%**|**96%**|
|AsyncResponse.Transports.RabbitMQ.AwaitingRabbitMqMessageDispatcher|100%|100%|
|AsyncResponse.Transports.RabbitMQ.QueuedRabbitMqMessageDispatcher|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqAsyncResponseOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqBackgroundFailureContext|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqChannelAdapter|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionAdapter|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqConnectionFactoryAdapter|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqDelivery|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqMessageDispatcher|100%|98.2%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqOptionsValidator|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetOptions|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqResponseIngressSubscriber|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberOptions|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqSubscriberService|87.8%|83.3%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqTopology|100%|100%|
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerSubscriber|100%||
|AsyncResponse.Transports.RabbitMQ.RabbitMqWorkerTransport|100%|72.7%|
|Microsoft.Extensions.DependencyInjection.RabbitMqAsyncResponseServiceCollec<br/>tionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Transports.Redis - 100%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.Redis**|**100%**|**98.4%**|
|AsyncResponse.Transports.Redis.AwaitingRedisMessageDispatcher|100%|100%|
|AsyncResponse.Transports.Redis.QueuedRedisMessageDispatcher|100%|100%|
|AsyncResponse.Transports.Redis.RedisAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.Redis.RedisBackgroundFailureContext|100%||
|AsyncResponse.Transports.Redis.RedisCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.Redis.RedisMessageDispatcher|100%|96%|
|AsyncResponse.Transports.Redis.RedisReplyTargetOptions|100%||
|AsyncResponse.Transports.Redis.RedisReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.Redis.RedisResponseIngressSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisStreamDatabaseAdapter|100%|100%|
|AsyncResponse.Transports.Redis.RedisStreamDatabaseAdapter<T>|100%|100%|
|AsyncResponse.Transports.Redis.RedisStreamDelivery|100%||
|AsyncResponse.Transports.Redis.RedisSubscriberOptions|100%|100%|
|AsyncResponse.Transports.Redis.RedisSubscriberService|100%|96.8%|
|AsyncResponse.Transports.Redis.RedisTransportKeySchema|100%|100%|
|AsyncResponse.Transports.Redis.RedisTransportOptionsValidator|100%|100%|
|AsyncResponse.Transports.Redis.RedisTransportRetry|100%|100%|
|AsyncResponse.Transports.Redis.RedisWorkerSubscriber|100%||
|AsyncResponse.Transports.Redis.RedisWorkerTransport|100%|100%|
|Microsoft.Extensions.DependencyInjection.RedisAsyncResponseTransportService<br/>CollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Transports.SqlServer - 96.8%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SqlServer**|**96.8%**|**99.1%**|
|AsyncResponse.Transports.DbCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.DbMessageDispatcherBase|96.4%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerAsyncResponseTransportOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerBackgroundFailureContext|100%||
|AsyncResponse.Transports.SqlServer.SqlServerCorrelationIdExtractor|100%||
|AsyncResponse.Transports.SqlServer.SqlServerMessageDispatcher|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetOptions|100%||
|AsyncResponse.Transports.SqlServer.SqlServerReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerResponseIngressSubscriber|100%||
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerSubscriberService|92%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerTransientFaults|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportDelivery|100%||
|AsyncResponse.Transports.SqlServer.SqlServerTransportOptionsValidator|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportRetry|100%|100%|
|AsyncResponse.Transports.SqlServer.SqlServerTransportStore|94%|95.2%|
|AsyncResponse.Transports.SqlServer.SqlServerWorkerSubscriber|100%||
|AsyncResponse.Transports.SqlServer.SqlServerWorkerTransport|100%|100%|
|Microsoft.Extensions.DependencyInjection.SqlServerAsyncResponseTransportSer<br/>viceCollectionExtensions|100%|100%|

</details>
<details><summary>AsyncResponse.Transports.SQS - 98.7%</summary>

|**Name**|**Line**|**Branch**|
|:---|---:|---:|
|**AsyncResponse.Transports.SQS**|**98.7%**|**94.8%**|
|AsyncResponse.Transports.SQS.AwaitingSqsMessageDispatcher|100%|100%|
|AsyncResponse.Transports.SQS.QueuedSqsMessageDispatcher|99%|100%|
|AsyncResponse.Transports.SQS.SqsAsyncResponseOptions|100%||
|AsyncResponse.Transports.SQS.SqsBackgroundFailureContext|100%||
|AsyncResponse.Transports.SQS.SqsClientAdapter|100%|100%|
|AsyncResponse.Transports.SQS.SqsClientFactory|100%|100%|
|AsyncResponse.Transports.SQS.SqsClientResolver|100%|100%|
|AsyncResponse.Transports.SQS.SqsCorrelationIdExtractor|100%|100%|
|AsyncResponse.Transports.SQS.SqsMessageDispatcher|98.9%|71.4%|
|AsyncResponse.Transports.SQS.SqsOptionsValidator|100%|100%|
|AsyncResponse.Transports.SQS.SqsOutboundMessage|100%||
|AsyncResponse.Transports.SQS.SqsQueueAddress|100%|83.3%|
|AsyncResponse.Transports.SQS.SqsQueueProvisioningService|98.2%|92.8%|
|AsyncResponse.Transports.SQS.SqsQueueProvisioningService<T>|98.2%|92.8%|
|AsyncResponse.Transports.SQS.SqsReceiveRequest|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetOptions|100%||
|AsyncResponse.Transports.SQS.SqsReplyTargetProvider|100%|100%|
|AsyncResponse.Transports.SQS.SqsResponseIngressSubscriber|100%||
|AsyncResponse.Transports.SQS.SqsSubscriberOptions|100%|100%|
|AsyncResponse.Transports.SQS.SqsSubscriberService|93.9%|95.8%|
|AsyncResponse.Transports.SQS.SqsSubscriberService.BatchProgress|100%||
|AsyncResponse.Transports.SQS.SqsTransportDelivery|100%||
|AsyncResponse.Transports.SQS.SqsWorkerSubscriber|100%||
|AsyncResponse.Transports.SQS.SqsWorkerTransport|100%|90%|
|Microsoft.Extensions.DependencyInjection.SqsAsyncResponseServiceCollectionE<br/>xtensions|100%|100%|

</details>
