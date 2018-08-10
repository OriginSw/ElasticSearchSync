using Bardock.Utils.Extensions;
using Elasticsearch.Net;
using ElasticSearchSync.DTO;
using ElasticSearchSync.Helpers;
using log4net;
using log4net.Config;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearchSync
{
    public class Sync
    {
        private enum LoggingTypes
        {
            Log,
            BulkLog,
            Lock,
            LastLog
        }

        public ElasticLowLevelClient Client { get; set; }

        public ILog Log { get; set; }

        private Stopwatch Stopwatch { get; set; }

        private SyncConfiguration _config;
        private string _logIndex = ConfigSection.Default.Index.Name ?? "sqlserver_es_sync";
        private Dictionary<LoggingTypes, Dictionary<string, string>> _loggingTypes = new Dictionary<LoggingTypes, Dictionary<string, string>>();

        private const string _lastLogID = "1";

        public Sync(SyncConfiguration config)
        {
            _config = config;

            _logIndex = string.IsNullOrEmpty(config.LogIndex) ? (ConfigSection.Default.Index.Name ?? "sqlserver_es_sync") : config.LogIndex;

            XmlConfigurator.Configure();
            Log = LogManager.GetLogger(string.Format("SQLSERVER-ES Sync - {0}/{1}", string.Join(", ", config._Indexes.Select(x => x.Name).ToArray()), config._Type));
            Stopwatch = new Stopwatch();

            string LogType = "log";
            string BulkLogType = "bulk_log";
            string LockType = "lock";
            string LastLogType = "last_log";

            _loggingTypes[LoggingTypes.Log] = new Dictionary<string, string>();
            _loggingTypes[LoggingTypes.BulkLog] = new Dictionary<string, string>();
            _loggingTypes[LoggingTypes.Lock] = new Dictionary<string, string>();
            _loggingTypes[LoggingTypes.LastLog] = new Dictionary<string, string>();

            foreach (var indexName in config._Indexes)
            {
                _loggingTypes[LoggingTypes.Log][indexName.Name] = string.Format("{0}_{1}_{2}", LogType, indexName.Alias, _config._Type);
                _loggingTypes[LoggingTypes.BulkLog][indexName.Name] = string.Format("{0}_{1}_{2}", BulkLogType, indexName.Alias, _config._Type);
                _loggingTypes[LoggingTypes.Lock][indexName.Name] = string.Format("{0}_{1}_{2}", LockType, indexName.Alias, _config._Type);
                _loggingTypes[LoggingTypes.LastLog][indexName.Name] = string.Format("{0}_{1}_{2}", LastLogType, indexName.Alias, _config._Type);
            }
        }

        public Dictionary<string, SyncResponse> Exec(bool force = false)
        {
            try
            {
                var startedOn = DateTime.UtcNow;
                var syncResponses = new Dictionary<string, SyncResponse>();
                foreach (var indexName in _config._Indexes)
                    syncResponses[indexName.Name] = new SyncResponse(startedOn);

                Log.Debug("process started at " + startedOn.NormalizedFormat());
                Client = new ElasticLowLevelClient(_config.ElasticSearchConfiguration);

                using (var _lock = new SyncLock(Client, _logIndex, _loggingTypes[LoggingTypes.Lock].Select(x => x.Value).ToArray(), force))
                {
                    DateTime? lastSyncDate = ConfigureIncrementalProcess(_config.SqlCommand, _config.ColumnsToCompareWithLastSyncDate);
                    Log.Info(string.Format("last sync date: {0}", lastSyncDate != null ? lastSyncDate.ToString() : "null"));

                    //DELETE PROCESS
                    if (_config.DeleteConfiguration != null)
                    {
                        _config.SqlConnection.Open();
                        Dictionary<object, Dictionary<string, object>> deleteData = null;

                        if (lastSyncDate != null)
                            ConfigureIncrementalProcess(_config.DeleteConfiguration.SqlCommand, _config.DeleteConfiguration.ColumnsToCompareWithLastSyncDate, lastSyncDate);

                        using (SqlDataReader rdr = _config.DeleteConfiguration.SqlCommand.ExecuteReader())
                        {
                            deleteData = rdr.Serialize();
                        }
                        _config.SqlConnection.Close();

                        if (deleteData != null && deleteData.Any())
                            if (_config.DeleteConfiguration.DeleteQueryFunc != null)
                                syncResponses = DeleteByQueryProcess(deleteData, syncResponses);
                            else
                                syncResponses = DeleteProcess(deleteData, syncResponses);
                    }

                    //INDEX PROCESS
                    if (_config.SqlCommand != null)
                    {
                        var dataCount = 0;
                        try
                        {
                            _config.SqlConnection.Open();
                            if (_config.PageSize.HasValue)
                            {
                                var page = 0;
                                var size = _config.PageSize;
                                var commandText = _config.SqlCommand.CommandText;

                                while (true)
                                {
                                    var conditionBuilder = new StringBuilder("(");
                                    conditionBuilder
                                        .Append("RowNumber BETWEEN ")
                                        .Append(page * size + 1)
                                        .Append(" AND ")
                                        .Append(page * size + size)
                                        .Append(")");

                                    _config.SqlCommand.CommandText = AddSqlCondition(commandText, conditionBuilder.ToString());

                                    var pageData = GetSerializedObject();

                                    var pageDataCount = pageData.Count();
                                    dataCount += pageDataCount;

                                    Log.Info(string.Format("{0} objects have been serialized from page {1}.", pageDataCount, page));

                                    IndexProcess(pageData, syncResponses);

                                    pageData.Clear();
                                    pageData = null;
                                    GC.Collect(GC.MaxGeneration);

                                    if (pageDataCount < size)
                                        break;

                                    page++;
                                }
                            }
                            else
                            {
                                var data = GetSerializedObject();
                                dataCount = data.Count();
                                IndexProcess(data, syncResponses);
                            }

                            Log.Info(string.Format("{0} objects have been serialized.", dataCount));
                        }
                        finally
                        {
                            _config.SqlConnection.Close();
                        }
                    }

                    //LOG PROCESS
                    syncResponses = LogProcess(syncResponses);

                    foreach (var indexName in _config._Indexes)
                        Log.Debug(string.Format("process duration: {0}ms", Math.Truncate((syncResponses[indexName.Name].EndedOn - syncResponses[indexName.Name].StartedOn).TotalMilliseconds)));
                }
                return syncResponses;
            }
            catch (Exception ex)
            {
                Log.Error("an error has occurred: " + ex);
                throw ex;
            }
        }

        /// <summary>
        /// Build and add to the sql where clause the lastSyncDate condition, taken from elasticsearch sync log
        /// </summary>
        private DateTime? ConfigureIncrementalProcess(SqlCommand sqlCommand, string[] columnsToCompareWithLastSyncDate, DateTime? lastSyncDate = null)
        {
            if (_config.ColumnsToCompareWithLastSyncDate != null)
            {
                if (lastSyncDate == null)
                {
                    var lastSyncResponse = GetLastSync().First().Value;//TODO
                    if (lastSyncResponse == null || lastSyncResponse.Body == null || lastSyncResponse.Body.found == false)
                    {
                        sqlCommand.CommandText = sqlCommand.CommandText.Replace("{0}", "");
                        return null;
                    }

                    lastSyncDate = DateTime.Parse(lastSyncResponse.Body._source.date).ToUniversalTime();
                }

                var conditionBuilder = new StringBuilder("(");
                foreach (var col in columnsToCompareWithLastSyncDate)
                    conditionBuilder
                        .Append(col)
                        .Append(" >= '")
                        .Append(lastSyncDate.Value.NormalizedFormat())
                        .Append("' OR ");
                conditionBuilder.RemoveLastCharacters(4).Append(")");

                if (sqlCommand.CommandText.Contains("{0}"))
                {
                    conditionBuilder.Insert(0, " AND ");
                    sqlCommand.CommandText = string.Format(sqlCommand.CommandText, conditionBuilder.ToString());
                }
                else
                    sqlCommand.CommandText = AddSqlCondition(sqlCommand.CommandText, conditionBuilder.ToString());
            }

            return lastSyncDate;
        }

        private Dictionary<string, ElasticsearchResponse<GetResponseDTO>> GetLastSync()
        {
            Dictionary<string, ElasticsearchResponse<GetResponseDTO>> lastSyncResponses = new Dictionary<string, ElasticsearchResponse<GetResponseDTO>>();
            foreach (var indexName in _config._Indexes)
            {
                Stopwatch.Start();
                try
                {
                    lastSyncResponses[indexName.Name] = Client.Get<GetResponseDTO>(_logIndex, _loggingTypes[LoggingTypes.LastLog][indexName.Name], _lastLogID);
                }
                catch (WebException)
                { }
                Stopwatch.Stop();
                Log.Info(string.Format("last sync search duration: {0}ms", Stopwatch.ElapsedMilliseconds));
                Stopwatch.Reset();
            }

            return lastSyncResponses;
        }

        private Dictionary<string, SyncResponse> IndexProcess(Dictionary<object, Dictionary<string, object>> data, Dictionary<string, SyncResponse> syncResponses)
        {
            if (_config.ExternalConsumer != null && _config.ExternalConsumer.Enable)
                Task.Factory.StartNew(() => _config.ExternalConsumer.SendUpsertToExternal(_config._Type, data));

            var c = 0;
            while (c < data.Count())
            {
                var partialData = data.Skip(c).Take(_config.BulkSize).ToDictionary(x => x.Key, x => x.Value);

                var bulkResponses = BulkProcess(partialData, ElasticsearchHelpers.GetPartialIndexBulk);

                foreach (var bulkResponse in bulkResponses)
                {
                    if (ConfigSection.Default.Index.LogBulk)
                        syncResponses[bulkResponse.Key].BulkResponses.Add(bulkResponse.Value);

                    syncResponses[bulkResponse.Key].IndexedDocuments += bulkResponse.Value.AffectedDocuments;
                    syncResponses[bulkResponse.Key].Success = syncResponses[bulkResponse.Key].Success && bulkResponse.Value.Success;

                    Log.Info(string.Format("bulk duration: {0}ms. so far {1} documents have been indexed successfully.", bulkResponse.Value.Duration, syncResponses[bulkResponse.Key].IndexedDocuments));
                }
                partialData.Clear();
                partialData = null;
                GC.Collect(GC.MaxGeneration);

                c += _config.BulkSize;
            }

            return syncResponses;
        }

        private Dictionary<string, SyncResponse> DeleteProcess(Dictionary<object, Dictionary<string, object>> data, Dictionary<string, SyncResponse> syncResponses)
        {
            if (_config.ExternalConsumer != null && _config.ExternalConsumer.Enable)
                Task.Factory.StartNew(() => _config.ExternalConsumer.SendDeleteToExternal(_config._Type, data));

            var d = 0;
            while (d < data.Count())
            {
                var partialData = data.Skip(d).Take(_config.BulkSize).ToDictionary(x => x.Key, x => x.Value);

                var bulkResponses = BulkProcess(partialData, ElasticsearchHelpers.GetPartialDeleteBulk);

                foreach (var bulkResponse in bulkResponses)
                {
                    syncResponses[bulkResponse.Key].BulkResponses.Add(bulkResponse.Value);
                    syncResponses[bulkResponse.Key].DeletedDocuments += bulkResponse.Value.AffectedDocuments;
                    syncResponses[bulkResponse.Key].Success = syncResponses[bulkResponse.Key].Success && bulkResponse.Value.Success;

                    Log.Info(string.Format("bulk duration: {0}ms. so far {1} documents have been deleted successfully.", bulkResponse.Value.Duration, syncResponses[bulkResponse.Key].DeletedDocuments));
                }
                d += _config.BulkSize;
            }

            return syncResponses;
        }

        private Dictionary<string, SyncResponse> DeleteByQueryProcess(Dictionary<object, Dictionary<string, object>> data, Dictionary<string, SyncResponse> syncResponses)
        {
            var bulkStartedOn = DateTime.UtcNow;

            var deleteQuery = _config.DeleteConfiguration.DeleteQueryFunc(data);

            List<Task> tasks = new List<Task>();
            foreach (var syncResponse in syncResponses)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    Stopwatch.Start();
                    var response = Client.DeleteByQuery<dynamic>(syncResponse.Key, _config._Type, deleteQuery);
                    Stopwatch.Stop();

                    var bulkResponse = new BulkResponse
                    {
                        Success = response.Success,
                        HttpStatusCode = response.HttpStatusCode,
                        AffectedDocuments = Convert.ToInt32(response.Body.total),
                        ESexception = response.OriginalException,
                        StartedOn = bulkStartedOn,
                        Duration = Stopwatch.ElapsedMilliseconds
                    };

                    syncResponse.Value.BulkResponses.Add(bulkResponse);
                    syncResponse.Value.DeletedDocuments += bulkResponse.AffectedDocuments;
                    syncResponse.Value.Success = syncResponse.Value.Success && bulkResponse.Success;

                    if (ConfigSection.Default.Index.LogBulk)
                        LogBulk(bulkResponse, _loggingTypes[LoggingTypes.BulkLog][syncResponse.Key]);

                    Stopwatch.Reset();
                }));
            }
            Task.WaitAll(tasks.ToArray());
            return syncResponses;
        }

        private Dictionary<string, BulkResponse> BulkProcess(
            Dictionary<object, Dictionary<string, object>> data,
            Func<string, object, Dictionary<string, object>, object, string> getPartialBulk)
        {
            Dictionary<string, BulkResponse> innerBulks = null;
            var partialBulkBuilder = new StringBuilder();
            var bulkStartedOn = DateTime.UtcNow;
            var bulkResponses = new Dictionary<string, BulkResponse>();
            string[] ids = new string[_config.BulkSize];
            Exception exception = null;

            int currentLength = 0;
            //build bulk data
            for (var i = 0; i < data.Count; i++)
            {
                var bulkData = data.ElementAt(i);
                object parentId = null;
                if (!string.IsNullOrEmpty(_config.Parent))
                    parentId = bulkData.Value[_config.Parent];

                var doc = getPartialBulk(_config._Type, bulkData.Key, bulkData.Value, parentId);
                currentLength += doc.Length;
                if (currentLength < ConfigSection.Default.Bulk.MaxMemoryBytes || i == 0)
                {
                    ids[i] = bulkData.Key.ToString();
                    partialBulkBuilder.Append(doc);
                }
                else
                {
                    innerBulks = BulkProcess(data.Skip(i).ToDictionary(x => x.Key, y => y.Value), getPartialBulk);
                    break;
                }
            }

            List<Task> tasks = new List<Task>();
            foreach (var indexName in _config._Indexes)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    Stopwatch.Start();
                    var bulkResponse = new BulkResponse();

                    Task<string> bulkLogTask = null;
                    if (ConfigSection.Default.Index.LogBulk)
                        bulkLogTask = Task.Factory.StartNew(() => InitLoggingBulk(_loggingTypes[LoggingTypes.BulkLog][indexName.Name], bulkStartedOn, ids, exception));

                    ElasticsearchResponse<dynamic> response = null;
                    try
                    {
                        response = Client.Bulk<dynamic>(indexName.Name, partialBulkBuilder.ToString());
                    }
                    catch (Exception e)
                    {
                        if (ConfigSection.Default.Index.LogBulk)
                        {
                            bulkResponse.Success = false;
                            bulkResponse.ESexception = new Exception("Error occurred in bulk request to Elasticsearch.", e);
                            bulkResponse.Duration = Stopwatch.ElapsedMilliseconds;
                            bulkLogTask.Wait();
                            UpdateBulkLog(_loggingTypes[LoggingTypes.BulkLog][indexName.Name], bulkLogTask.Result, bulkResponse, response?.RequestBodyInBytes, response?.ResponseBodyInBytes);
                        }
                        throw e;
                    }
                    Stopwatch.Stop();

                    if (!response.Success)
                    {
                        Log.Error("Error occurred in bulk request to Elasticsearch.", response.OriginalException);
                        if (ConfigSection.Default.Index.LogBulk)
                        {
                            bulkResponse.Success = false;
                            bulkResponse.ESexception = new Exception("Error occurred in bulk request to Elasticsearch.", response.OriginalException);
                            bulkResponse.HttpStatusCode = response.HttpStatusCode;
                            bulkResponse.Duration = Stopwatch.ElapsedMilliseconds;
                            bulkLogTask.Wait();
                            UpdateBulkLog(_loggingTypes[LoggingTypes.BulkLog][indexName.Name], bulkLogTask.Result, bulkResponse, response.RequestBodyInBytes, response.ResponseBodyInBytes);
                        }
                        throw response.OriginalException;
                    }

                    bulkResponse.Success = response.Success;
                    bulkResponse.HttpStatusCode = response.HttpStatusCode;
                    bulkResponse.AffectedDocuments = response.Body?.items != null ? response.Body.items.Count : 0;
                    bulkResponse.ESexception = response.OriginalException;
                    bulkResponse.StartedOn = bulkStartedOn;
                    bulkResponse.Duration = Stopwatch.ElapsedMilliseconds;

                    bulkResponses[indexName.Name] = bulkResponse;

                    if (innerBulks != null)
                        bulkResponses[indexName.Name].InnerBulkResponse = innerBulks[indexName.Name];

                    if (ConfigSection.Default.Index.LogBulk)
                    {
                        bulkLogTask.Wait();
                        UpdateBulkLog(_loggingTypes[LoggingTypes.BulkLog][indexName.Name], bulkLogTask.Result, bulkResponse);
                    }

                    Stopwatch.Reset();
                }));
            }

            Task.WaitAll(tasks.ToArray());
            return bulkResponses;
        }

        public string InitLoggingBulk(string bulkLogType, DateTime bulkStartedOn, string[] ids, Exception e)
        {
            var response = Client.Index<dynamic>(_logIndex, bulkLogType, new
            {
                startedOn = bulkStartedOn,
                documentsId = ids,
                exception = JsonConvert.SerializeObject(e),
            });
            return response.Body._id;
        }

        /// <summary>
        /// LogProcess in {logIndex}/{logBulkType} the bulk serializedNewObject and metrics
        /// </summary>
        private void LogBulk(BulkResponse bulkResponse, string bulkLogType)
        {
            Task.Factory.StartNew(() =>
                Client.Index<dynamic>(_logIndex, bulkLogType, new
                {
                    success = bulkResponse.Success,
                    httpStatusCode = bulkResponse.HttpStatusCode,
                    documentsIndexed = bulkResponse.AffectedDocuments,
                    startedOn = bulkResponse.StartedOn,
                    duration = bulkResponse.Duration + "ms",
                    exception = JsonConvert.SerializeObject(bulkResponse.ESexception)
                }));
        }

        private void UpdateBulkLog(string bulkLogType, string id, BulkResponse bulkResponse, byte[] request = null, byte[] response = null)
        {
            Task.Factory.StartNew(() =>
            {
                var elasticResponse = Client.Update<dynamic>(_logIndex, bulkLogType, id, new
                {
                    doc = new
                    {
                        success = bulkResponse.Success,
                        httpStatusCode = bulkResponse.HttpStatusCode,
                        documentsIndexed = bulkResponse.AffectedDocuments,
                        duration = bulkResponse.Duration + "ms",
                        exception = JsonConvert.SerializeObject(bulkResponse.ESexception),
                        request = JsonConvert.SerializeObject(request),
                        response = JsonConvert.SerializeObject(response)
                    }
                });
            });
        }

        /// <summary>
        /// LogProcess in {logIndex}/{logType} the synchronization results and metrics
        /// </summary>
        private Dictionary<string, SyncResponse> LogProcess(Dictionary<string, SyncResponse> syncResponses)
        {
            var endedOn = DateTime.UtcNow;
            foreach (var response in syncResponses)
            {
                Stopwatch.Start();
                response.Value.EndedOn = endedOn;
                var logBulk = ElasticsearchHelpers.GetPartialIndexBulk(_loggingTypes[LoggingTypes.Log][response.Key], new
                {
                    startedOn = response.Value.StartedOn,
                    endedOn = response.Value.EndedOn,
                    success = response.Value.Success,
                    indexedDocuments = response.Value.IndexedDocuments,
                    deletedDocuments = response.Value.DeletedDocuments,
                    bulks = response.Value.BulkResponses.Select(x => new
                    {
                        success = x.Success,
                        httpStatusCode = x.HttpStatusCode,
                        affectedDocuments = x.AffectedDocuments,
                        duration = x.Duration + "ms",
                        exception = JsonConvert.SerializeObject(x.ESexception)
                    })
                });

                if (_config.ColumnsToCompareWithLastSyncDate != null && _config.ColumnsToCompareWithLastSyncDate.Any())
                {
                    logBulk += ElasticsearchHelpers.GetPartialIndexBulk(_loggingTypes[LoggingTypes.LastLog][response.Key], _lastLogID, new
                    {
                        date = response.Value.StartedOn
                    });
                }
                Client.Bulk<dynamic>(_logIndex, logBulk);

                Stopwatch.Stop();
                Log.Info(string.Format("log index duration: {0}ms", Stopwatch.ElapsedMilliseconds));
                Stopwatch.Reset();
            }
            return syncResponses;
        }

        private Dictionary<object, Dictionary<string, object>> GetSerializedObject()
        {
            Dictionary<object, Dictionary<string, object>> data = null;
            _config.SqlCommand.CommandTimeout = 0;

            Stopwatch.Start();
            using (SqlDataReader rdr = _config.SqlCommand.ExecuteReader(CommandBehavior.SequentialAccess))
            {
                Stopwatch.Stop();
                Log.Info(string.Format("sql execute reader duration: {0}ms", Stopwatch.ElapsedMilliseconds));
                Stopwatch.Reset();

                data = rdr.Serialize(_config.XmlFields);
            }

            if (!data.Any())
                return data;

            var dataIds = data.Select(x => "'" + x.Key + "'").ToArray();

            foreach (var arrayConfig in _config.ArraysConfiguration)
            {
                var cmd = arrayConfig.SqlCommand.Clone();
                cmd.CommandTimeout = 0;

                if (_config.PageSize.HasValue)
                    cmd.CommandText = AddSqlCondition(cmd.CommandText, string.Format("_id IN ({0})", string.Join(",", dataIds)));

                Stopwatch.Start();
                using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                {
                    Stopwatch.Stop();
                    Log.Info(string.Format("array sql execute reader duration: {0}ms", Stopwatch.ElapsedMilliseconds));
                    Stopwatch.Reset();

                    data = rdr.SerializeArray(data, arrayConfig.AttributeName, arrayConfig.XmlFields, arrayConfig.InsertIntoArrayComparerKey);
                }
            }

            foreach (var objectConfig in _config.ObjectsConfiguration)
            {
                var cmd = objectConfig.SqlCommand.Clone();
                cmd.CommandTimeout = 0;

                if (_config.PageSize.HasValue)
                    cmd.CommandText = AddSqlCondition(cmd.CommandText, string.Format("_id IN ({0})", string.Join(",", dataIds)));

                Stopwatch.Start();
                using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                {
                    Stopwatch.Stop();
                    Log.Info(string.Format("object sql execute reader duration: {0}ms", Stopwatch.ElapsedMilliseconds));
                    Stopwatch.Reset();

                    data = rdr.SerializeObject(data, objectConfig.AttributeName, objectConfig.InsertIntoArrayComparerKey);
                }
            }

            return data;
        }

        private string AddSqlCondition(string sql, string condition)
        {
            return new StringBuilder(sql).Insert(
                sql.LastIndexOf("where", StringComparison.InvariantCultureIgnoreCase) + "where ".Length,
                new StringBuilder(condition).Append(" AND ").ToString()).ToString();
        }
    }
}