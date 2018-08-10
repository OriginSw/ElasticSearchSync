using Elasticsearch.Net;
using ElasticSearchSync.DTO;
using ElasticSearchSync.Helpers;
using System;
using System.Globalization;

namespace ElasticSearchSync
{
    public class SyncLock : IDisposable
    {
        private const string _id = "1";

        public ElasticLowLevelClient Client { get; set; }

        public string LockIndex { get; set; }

        public string[] LockTypes { get; set; }

        public bool Force { get; set; }

        public SyncLock(ElasticLowLevelClient client, string index, string[] types, bool force = false)
        {
            Client = client;
            LockIndex = index;
            LockTypes = types;
            Force = force;

            Open();
        }

        private void Open()
        {
            if (Force)
                return;

            var body = new
            {
                date = DateTime.UtcNow
            };

            foreach (var type in LockTypes)
            {
                ElasticsearchResponse<GetResponseDTO> _lock;
                _lock = Client.Get<GetResponseDTO>(LockIndex, type, _id);
                if (_lock.HttpStatusCode == 404 || !_lock.Body.found)
                {
                    IndexLock(type, body);
                }
                else
                {
                    DateTime lockDate = DateTime.ParseExact(
                        _lock.Body._source.date,
                        "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'",
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal |
                        DateTimeStyles.AdjustToUniversal);
                    var duration = ConfigSection.Default.Concurrency.Duration;

                    if (duration == null || lockDate + duration >= body.date)
                        throw new SyncConcurrencyException();

                    Client.Delete<dynamic>(LockIndex, type, _id);
                    IndexLock(type, body);
                }
            }
        }

        private void IndexLock(string type, object body)
        {
            var indexLock = Client.Index<dynamic>(LockIndex, type, _id, body, q => q.OpType(OpType.Create));
            if (!indexLock.Success)
                throw new SyncConcurrencyException(indexLock.OriginalException.Message);
        }

        public void Dispose()
        {
            if (Force)
                return;

            foreach (var type in LockTypes)
            {
                var response = Client.Delete<dynamic>(LockIndex, type, _id);
                if (!response.Success)
                    throw new Exception(response.OriginalException.Message);
            }
        }

        public class SyncConcurrencyException : Exception
        {
            public SyncConcurrencyException()
                : base() { }

            public SyncConcurrencyException(string message)
                : base(message) { }
        }
    }
}