using Bardock.Utils.Extensions;
using Elasticsearch.Net;
using ElasticSearchSync.DTO;
using ElasticSearchSync.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;

namespace ElasticSearchSync
{
    public class Sync
    {
        public ElasticLowLevelClient client { get; set; }

        public log4net.ILog log { get; set; }

        private Stopwatch stopwatch { get; set; }

        private SyncConfiguration _config;
        private string LogIndex = ConfigSection.Default.Index.Name ?? "sqlserver_es_sync";
        private string LogType = "log";
        private string BulkLogType = "bulk_log";
        private string LockType = "lock";
        private string LastLogType = "last_log";
        private string LastLogID = "1";

        public Sync(SyncConfiguration config)
        {
            _config = config;
            log4net.Config.XmlConfigurator.Configure();
            log = log4net.LogManager.GetLogger(string.Format("SQLSERVER-ES Sync - {0}/{1}", config._Index.Name, config._Type));
            stopwatch = new Stopwatch();

            var indexNameForLogTypes = string.IsNullOrEmpty(config._Index.Alias) ? config._Index.Name : config._Index.Alias;
            LogType = string.Format("{0}_{1}_{2}", LogType, indexNameForLogTypes, _config._Type);
            BulkLogType = string.Format("{0}_{1}_{2}", BulkLogType, indexNameForLogTypes, _config._Type);
            LockType = string.Format("{0}_{1}_{2}", LockType, indexNameForLogTypes, _config._Type);
            LastLogType = string.Format("{0}_{1}_{2}", LastLogType, indexNameForLogTypes, _config._Type);
        }

        public SyncResponse Exec(bool force = false)
        {
            try
            {
                var startedOn = DateTime.UtcNow;
                var syncResponse = new SyncResponse(startedOn);
                log.Debug("process started at " + startedOn.NormalizedFormat());
                client = new ElasticLowLevelClient(_config.ElasticSearchConfiguration);

                if (ValidatePeriodicity(client, LogIndex, LastLogType))
                    using (var _lock = new SyncLock(client, LogIndex, LockType, force))
                    {
                        DateTime? lastSyncDate = ConfigureIncrementalProcess(_config.SqlCommand, _config.ColumnsToCompareWithLastSyncDate);
                        log.Info(string.Format("last sync date: {0}", lastSyncDate != null ? lastSyncDate.ToString() : "null"));

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

                            syncResponse = DeleteProcess(deleteData, syncResponse);
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

                                        log.Info(string.Format("{0} objects have been serialized from page {1}.", pageDataCount, page));

                                        IndexProcess(pageData, syncResponse);

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
                                    IndexProcess(data, syncResponse);
                                }

                                log.Info(string.Format("{0} objects have been serialized.", dataCount));
                            }
                            finally
                            {
                                _config.SqlConnection.Close();
                            }
                        }

                        //LOG PROCESS
                        syncResponse = LogProcess(syncResponse);

                        log.Debug(string.Format("process duration: {0}ms", Math.Truncate((syncResponse.EndedOn - syncResponse.StartedOn).TotalMilliseconds)));
                    }
                return syncResponse;
            }
            catch (Exception ex)
            {
                log.Error("an error has occurred: " + ex);
                throw ex;
            }
        }

        private bool ValidatePeriodicity(ElasticLowLevelClient client, string index, string type)
        {
            const string _id = "1";
            var minPeriod = ConfigSection.Default.Periodicity.MinPeriod;

            if (minPeriod == null)
                return true;

            var _lastLog = client.Get<GetResponseDTO>(index, type, _id);
            if (_lastLog.HttpStatusCode == 404 || (!_lastLog.Body.found))
            {
                return true;
            }
            else
            {
                DateTime logDate = DateTime.ParseExact(
                     _lastLog.Body._source.date,
                    "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal |
                    DateTimeStyles.AdjustToUniversal);

                if (DateTime.UtcNow - logDate >= minPeriod)
                    return true;
            }
            return false;
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
                    var lastSyncResponse = GetLastSync();
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

        private ElasticsearchResponse<GetResponseDTO> GetLastSync()
        {
            stopwatch.Start();
            ElasticsearchResponse<GetResponseDTO> lastSyncResponse = null;
            try
            {
                lastSyncResponse = client.Get<GetResponseDTO>(LogIndex, LastLogType, LastLogID);
            }
            catch (WebException)
            { }

            stopwatch.Stop();
            log.Info(string.Format("last sync search duration: {0}ms", stopwatch.ElapsedMilliseconds));
            stopwatch.Reset();

            return lastSyncResponse;
        }

        private BulkResponse BulkIndexProcess(Dictionary<object, Dictionary<string, object>> data)
        {
            return BulkProcess(data, ElasticsearchHelpers.GetPartialIndexBulk);
        }

        private SyncResponse IndexProcess(Dictionary<object, Dictionary<string, object>> data, SyncResponse syncResponse)
        {
            var c = 0;
            while (c < data.Count())
            {
                var partialData = data.Skip(c).Take(_config.BulkSize).ToDictionary(x => x.Key, x => x.Value);

                var bulkResponse = BulkIndexProcess(partialData);

                if (ConfigSection.Default.Index.LogBulk)
                    syncResponse.BulkResponses.Add(bulkResponse);

                syncResponse.IndexedDocuments += bulkResponse.AffectedDocuments;
                syncResponse.Success = syncResponse.Success && bulkResponse.Success;

                log.Info(string.Format("bulk duration: {0}ms. so far {1} documents have been indexed successfully.", bulkResponse.Duration, syncResponse.IndexedDocuments));
                c += _config.BulkSize;
            }

            return syncResponse;
        }

        private BulkResponse BulkDeleteProcess(Dictionary<object, Dictionary<string, object>> data)
        {
            return BulkProcess(data, ElasticsearchHelpers.GetPartialDeleteBulk);
        }

        private SyncResponse DeleteProcess(Dictionary<object, Dictionary<string, object>> data, SyncResponse syncResponse)
        {
            var d = 0;
            while (d < data.Count())
            {
                var partialData = data.Skip(d).Take(_config.BulkSize).ToDictionary(x => x.Key, x => x.Value);

                var bulkResponse = BulkDeleteProcess(partialData);

                syncResponse.BulkResponses.Add(bulkResponse);
                syncResponse.DeletedDocuments += bulkResponse.AffectedDocuments;
                syncResponse.Success = syncResponse.Success && bulkResponse.Success;

                log.Info(string.Format("bulk duration: {0}ms. so far {1} documents have been deleted successfully.", bulkResponse.Duration, syncResponse.DeletedDocuments));
                d += _config.BulkSize;
            }

            return syncResponse;
        }

        private BulkResponse BulkProcess(
            Dictionary<object, Dictionary<string, object>> data,
            Func<string, string, object, Dictionary<string, object>, string> getPartialBulk)
        {
            stopwatch.Start();
            StringBuilder partialBulkBuilder = new StringBuilder();
            var bulkStartedOn = DateTime.UtcNow;

            //build bulk data
            foreach (var bulkData in data)
                partialBulkBuilder.Append(getPartialBulk(_config._Index.Name, _config._Type, bulkData.Key, bulkData.Value));

            var response = client.Bulk<dynamic>(partialBulkBuilder.ToString());
            stopwatch.Stop();

            var bulkResponse = new BulkResponse
            {
                Success = response.Success,
                HttpStatusCode = response.HttpStatusCode,
                AffectedDocuments = response.Body.items != null ? response.Body.items.Count : 0,
                ESexception = response.OriginalException,
                StartedOn = bulkStartedOn,
                Duration = stopwatch.ElapsedMilliseconds
            };

            if (ConfigSection.Default.Index.LogBulk)
                LogBulk(bulkResponse);

            stopwatch.Reset();

            return bulkResponse;
        }

        /// <summary>
        /// LogProcess in {logIndex}/{logBulkType} the bulk serializedNewObject and metrics
        /// </summary>
        private void LogBulk(BulkResponse bulkResponse)
        {
            client.Index<dynamic>(LogIndex, BulkLogType, new
            {
                success = bulkResponse.Success,
                httpStatusCode = bulkResponse.HttpStatusCode,
                documentsIndexed = bulkResponse.AffectedDocuments,
                startedOn = bulkResponse.StartedOn,
                duration = bulkResponse.Duration + "ms",
                exception = bulkResponse.ESexception != null ? ((Exception)bulkResponse.ESexception).Message : null
            });
        }

        /// <summary>
        /// LogProcess in {logIndex}/{logType} the synchronization results and metrics
        /// </summary>
        private SyncResponse LogProcess(SyncResponse syncResponse)
        {
            stopwatch.Start();
            syncResponse.EndedOn = DateTime.UtcNow;
            var logBulk = ElasticsearchHelpers.GetPartialIndexBulk(LogIndex, LogType, new
            {
                startedOn = syncResponse.StartedOn,
                endedOn = syncResponse.EndedOn,
                success = syncResponse.Success,
                indexedDocuments = syncResponse.IndexedDocuments,
                deletedDocuments = syncResponse.DeletedDocuments,
                bulks = syncResponse.BulkResponses.Select(x => new
                {
                    success = x.Success,
                    httpStatusCode = x.HttpStatusCode,
                    affectedDocuments = x.AffectedDocuments,
                    duration = x.Duration + "ms",
                    exception = x.ESexception != null ? ((Exception)x.ESexception).Message : null
                })
            });

            if (_config.ColumnsToCompareWithLastSyncDate != null && _config.ColumnsToCompareWithLastSyncDate.Any())
            {
                logBulk += ElasticsearchHelpers.GetPartialIndexBulk(LogIndex, LastLogType, LastLogID, new
                {
                    date = syncResponse.StartedOn
                });
            }
            client.Bulk<dynamic>(logBulk);

            stopwatch.Stop();
            log.Info(string.Format("log index duration: {0}ms", stopwatch.ElapsedMilliseconds));
            stopwatch.Reset();

            return syncResponse;
        }

        private Dictionary<object, Dictionary<string, object>> GetSerializedObject()
        {
            Dictionary<object, Dictionary<string, object>> data = null;
            _config.SqlCommand.CommandTimeout = 0;

            stopwatch.Start();
            using (SqlDataReader rdr = _config.SqlCommand.ExecuteReader(CommandBehavior.SequentialAccess))
            {
                stopwatch.Stop();
                log.Info(string.Format("sql execute reader duration: {0}ms", stopwatch.ElapsedMilliseconds));
                stopwatch.Reset();

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

                stopwatch.Start();
                using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                {
                    stopwatch.Stop();
                    log.Info(string.Format("array sql execute reader duration: {0}ms", stopwatch.ElapsedMilliseconds));
                    stopwatch.Reset();

                    data = rdr.SerializeArray(data, arrayConfig.AttributeName, arrayConfig.XmlFields, arrayConfig.InsertIntoArrayComparerKey);
                }
            }

            foreach (var objectConfig in _config.ObjectsConfiguration)
            {
                var cmd = objectConfig.SqlCommand.Clone();
                cmd.CommandTimeout = 0;

                if (_config.PageSize.HasValue)
                    cmd.CommandText = AddSqlCondition(cmd.CommandText, string.Format("_id IN ({0})", string.Join(",", dataIds)));

                stopwatch.Start();
                using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                {
                    stopwatch.Stop();
                    log.Info(string.Format("object sql execute reader duration: {0}ms", stopwatch.ElapsedMilliseconds));
                    stopwatch.Reset();

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