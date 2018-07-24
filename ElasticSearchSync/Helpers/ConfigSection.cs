using System;
using System.Configuration;
using System.Runtime.Serialization;

namespace ElasticSearchSync.Helpers
{
    public class ConfigSection : ConfigurationSection
    {
        [IgnoreDataMember()]
        public static ConfigSection Default
        {
            get { return (ConfigSection)ConfigurationManager.GetSection("ElasticSearchSync"); }
        }

        [IgnoreDataMember]
        [ConfigurationProperty("index")]
        public IndexConfigurationElement Index
        {
            get { return (IndexConfigurationElement)this["index"]; }
            set { this["index"] = value; }
        }

        [IgnoreDataMember]
        [ConfigurationProperty("concurrency")]
        public ConcurrencyConfigurationElement Concurrency
        {
            get { return (ConcurrencyConfigurationElement)this["concurrency"]; }
            set { this["concurrency"] = value; }
        }

        [IgnoreDataMember]
        [ConfigurationProperty("periodicity")]
        public PeriodicityConfigurationElement Periodicity
        {
            get { return (PeriodicityConfigurationElement)this["periodicity"]; }
            set { this["periodicity"] = value; }
        }

        [IgnoreDataMember]
        [ConfigurationProperty("bulk")]
        public BulkConfigurationElement Bulk
        {
            get { return (BulkConfigurationElement)this["bulk"]; }
            set { this["bulk"] = value; }
        }

        [DataContract]
        public class IndexConfigurationElement : ConfigurationElement
        {
            [DataMember]
            [ConfigurationProperty("name", IsRequired = true)]
            public string Name
            {
                get { return (string)this["name"]; }
                set { this["name"] = value; }
            }

            [DataMember]
            [ConfigurationProperty("logBulk", IsRequired = false)]
            public bool LogBulk
            {
                get { return (bool)this["logBulk"]; }
                set { this["logBulk"] = value; }
            }
        }

        [DataContract]
        public class ConcurrencyConfigurationElement : ConfigurationElement
        {
            [DataMember]
            [ConfigurationProperty("duration", IsRequired = false)]
            public TimeSpan Duration
            {
                get { return (TimeSpan)this["duration"]; }
                set { this["duration"] = value; }
            }
        }

        [DataContract]
        public class PeriodicityConfigurationElement : ConfigurationElement
        {
            [DataMember]
            [ConfigurationProperty("minPeriod", IsRequired = false)]
            public TimeSpan? MinPeriod
            {
                get { return (TimeSpan?)this["minPeriod"]; }
                set { this["minPeriod"] = value; }
            }
        }

        [DataContract]
        public class BulkConfigurationElement : ConfigurationElement
        {
            [DataMember]
            [ConfigurationProperty("maxMemoryBytes", IsRequired = false, DefaultValue = 10000000)]
            public int MaxMemoryBytes
            {
                get { return (int)this["maxMemoryBytes"]; }
                set { this["maxMemoryBytes"] = value; }
            }
        }
    }
}