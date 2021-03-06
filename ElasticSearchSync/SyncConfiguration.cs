﻿using Elasticsearch.Net;
using System;
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

        public IEnumerable<SyncArrayConfiguration> ArraysConfiguration { get; set; }

        public IEnumerable<SyncObjectConfiguration> ObjectsConfiguration { get; set; }

        public SyncDeleteConfiguration DeleteConfiguration { get; set; }

        /// <summary>
        /// Add to the WHERE clause the condition that objects to consider in the process have been created or updated after the last synchronization
        /// If this property has value, process will expect the SqlCommand to have a WHERE clause
        /// </summary>
        public string[] ColumnsToCompareWithLastSyncDate { get; set; }

        /// <summary>
        /// Sql columns that contains xml data
        /// </summary>
        public string[] XmlFields { get; set; }

        public ConnectionConfiguration ElasticSearchConfiguration { get; set; }

        /// <summary>
        /// Max number of documents in a single bulk request
        /// </summary>
        public int BulkSize { get; set; }

        /// <summary>
        /// Configures pagination on Sql queries.
        /// </summary>
        public int? PageSize { get; set; }

        /// <summary>
        /// Elasticsearch index name
        /// </summary>
        public IEnumerable<Index> _Indexes { get; set; }

        /// <summary>
        /// Elasticsearch type
        /// </summary>
        public string _Type { get; set; }

        /// <summary>
        /// Elasticsearch index for logging
        /// </summary>
        public string LogIndex { get; set; }

        /// <summary>
        /// Parent id for child objects of parent-child relationships
        /// </summary>
        public string Parent { get; set; }

        public SyncConfiguration()
        {
            BulkSize = 1000;
            ArraysConfiguration = new List<SyncArrayConfiguration>();
            ObjectsConfiguration = new List<SyncObjectConfiguration>();
        }

        /// <summary>
        /// For sending the data to an external service like an event bus
        /// </summary>
        public ExternalConsumer ExternalConsumer { get; set; }
    }

    public class ExternalConsumer
    {
        public bool Enable { get; set; }

        public Action<string, Dictionary<object, Dictionary<string, object>>> SendUpsertToExternal { get; set; }

        public Action<string, Dictionary<object, Dictionary<string, object>>> SendDeleteToExternal { get; set; }
    }

    public class Index
    {
        public string Name { get; set; }

        public string Alias { get; set; }

        public Index(string name, string alias = null)
        {
            Name = name;
            Alias = alias;
        }
    }
}