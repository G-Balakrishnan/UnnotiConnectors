using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Unnoti.Core;
using Unnoti.Core.Execution;
using Unnoti.Core.IContracts;

namespace Unnoti.Connector.Base
{
    public abstract class ConnectorBase
    {
        /// <summary>
        /// Human-readable name (UI)
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// Stable identifier used in JobConfig.ConnectorType
        /// </summary>
        public abstract string ConnectorType { get; }

        protected UnnotiExecutionContext Context { get; private set; }
        protected ExecutionResult Result { get; private set; }
        protected CancellationToken CancellationToken { get; private set; }

        protected void Initialize(
            UnnotiExecutionContext context,
            CancellationToken cancellationToken)
        {
            Context = context;
            CancellationToken = cancellationToken;
            Result = new ExecutionResult();
        }

        protected void InitializeResult(
         )
        {
            Result = new ExecutionResult();
        }

        protected async Task ExecuteRecordAsync(
        Func<Task> recordAction,
        string recordId)
        {
            try
            {
                await recordAction();
                Result.RecordSuccess();
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                LogError($"Record failed [{recordId}]: {ex.Message}");
                Result.RecordFailure();
            }
        }

        protected abstract void LogError(string message);

        /// <summary>
        /// Executes a single record safely.
        /// One record failure NEVER stops the job.
        /// Cancellation is respected.
        /// </summary>
        protected async Task ExecuteRecordAsync(
            Func<CancellationToken, Task> recordAction,
            string recordId = null)
        {
            if (CancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();

            try
            {
                await recordAction(CancellationToken);
                Result.RecordSuccess();
            }
            catch (OperationCanceledException ex)
            {
                Context.Logger.Error(
                  $"Record failed [{recordId ?? "UNKNOWN"}] : {ex.Message}");
                Result.RecordFailure();
                throw; // bubble up → job stops cleanly
            }
            catch (Exception ex)
            {
                Context.Logger.Error(
                    $"Record failed [{recordId ?? "UNKNOWN"}] : {ex.Message}");
                Result.RecordFailure();
            }
        }

        protected T GetConnectionConfig<T>(Dictionary<string, string> config)
       where T : class, IConnectionConfig
        {
            if (!config.TryGetValue("ConnectionConfig", out var json) ||
                string.IsNullOrWhiteSpace(json))
            {
                throw new InvalidOperationException("ConnectionConfig is missing.");
            }

            var result = JsonConvert.DeserializeObject<T>(json);

            if (result == null)
                throw new InvalidOperationException("Invalid ConnectionConfig JSON.");

            return result;
        }

        /// <summary>
        /// Called once after all records are processed
        /// </summary>
        protected ExecutionResult Complete()
        {
            Result.Complete();
            return Result;
        }
    }

    public interface IConnector
    {
        string Name { get; }
        string ConnectorKey { get; }
        Task<ExecutionResult> ExecuteAsync(string connectorConfigPath, CancellationToken cancellationToken);
        IConnector GetConfigType();
        string GetConnectorConfigurations();
    }
}
