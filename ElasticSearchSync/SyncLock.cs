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

        public string LockType { get; set; }

        public bool Force { get; set; }

        public SyncLock(ElasticLowLevelClient client, string index, string type, bool force = false)
        {
            Client = client;
            LockIndex = index;
            LockType = type;
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

            var _lock = Client.Get<GetResponseDTO>(LockIndex, LockType, _id);
            if (_lock.HttpStatusCode == 404 || !_lock.Body.found)
            {
                IndexLock(body);
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

                Client.Delete<dynamic>(LockIndex, LockType, _id);
                IndexLock(body);
            }
        }

        private void IndexLock(object body)
        {
            var indexLock = Client.Index<dynamic>(LockIndex, LockType, _id, body, q => q.OpType(OpType.Create));
            if (!indexLock.Success)
                throw new SyncConcurrencyException(indexLock.OriginalException.Message);
        }

        public void Dispose()
        {
            if (Force)
                return;

            var d = Client.Delete<dynamic>(LockIndex, LockType, _id);
            if (!d.Success)
                throw new Exception(d.OriginalException.Message);
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