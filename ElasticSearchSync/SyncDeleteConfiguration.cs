using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace ElasticSearchSync
{
    public class SyncDeleteConfiguration
    {
        /// <summary>
        /// Sql exec must return a datareader containing mandatorily a column with document _id
        /// </summary>
        public SqlCommand SqlCommand { get; set; }

        /// <summary>
        /// Add to the WHERE clause the condition that objects to consider in the process have been created or updated after the last synchronization
        /// If this property has value, process will expect the SqlCommand to have a WHERE clause
        /// </summary>
        public string[] ColumnsToCompareWithLastSyncDate { get; set; }

        /// <summary>
        /// Allows to use the function "_delete_by_query" of Elasticsearch. Must be the complete Elasticsearch query.
        /// If null, basic delete will be used.
        /// </summary>
        public Func<Dictionary<object, Dictionary<string, object>>,string> DeleteQueryFunc { get; set; }
    }
}