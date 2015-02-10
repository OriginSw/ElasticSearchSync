﻿using Elasticsearch.Net.Connection;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace ElasticSearchSync
{
    public class SyncConfiguration
    {
        /// <summary>
        /// SqlServer connection
        /// </summary>
        public SqlConnection SqlConnection { get; set; }

        /// <summary>
        /// First column of sql script will be used as document _id
        /// </summary>
        public SqlCommand SqlCommand { get; set; }

        /// <summary>
        /// First column of sql script must be the same column used for document _id
        /// </summary>
        public IEnumerable<SqlCommand> ArraySqlCommands { get; set; }

        /// <summary>
        /// Sql exec must return a datareader containing a single column with document _id
        /// </summary>
        public SqlCommand DeleteSqlCommand { get; set; }

        /// <summary>
        /// Add to the WHERE clause an IN condition for ids of the objects in arrays.
        /// If this property has value, process will expect the WHERE clause of ArraySqlCommands to be with format (to use String.Replace)
        /// Property ParentIdColumn must have a value
        /// </summary>
        public bool FilterArrayByParentsIds { get; set; }

        public string ParentIdColumn { get; set; }

        /// <summary>
        /// Add to the WHERE clause the condition that objects to consider in the process have been created or updated after the last synchronization
        /// If this property has value, process will expect the SqlCommand to have a WHERE clause:
        /// </summary>
        public string[] ColumnsToCompareWithLastSyncDate { get; set; }

        public ConnectionConfiguration ElasticSearchConfiguration { get; set; }

        /// <summary>
        /// Max number of documents in a single bulk request
        /// </summary>
        public int BulkSize { get; set; }

        /// <summary>
        /// Elasticsearch index
        /// </summary>
        public string _Index { get; set; }

        /// <summary>
        /// Elasticsearch type
        /// </summary>
        public string _Type { get; set; }

        public SyncConfiguration()
        {
            BulkSize = 5000;
            ArraySqlCommands = new List<SqlCommand>();
        }
    }
}