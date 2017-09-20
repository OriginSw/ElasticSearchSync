using Newtonsoft.Json;

namespace ElasticSearchSync.Helpers
{
    public static class ElasticsearchHelpers
    {
        public static string GetPartialDeleteBulk(string index, string type, object id, object value = null, object parent = null)
        {
            return string.Format("{0}\n",
                JsonConvert.SerializeObject(new { delete = new { _index = index, _type = type, _id = id } }, Formatting.None));
        }

        public static string GetPartialIndexBulk(string index, string type, object value)
        {
            return string.Format("{0}\n{1}\n",
                JsonConvert.SerializeObject(new { index = new { _index = index, _type = type } }, Formatting.None),
                JsonConvert.SerializeObject(value, Formatting.None));
        }

        public static string GetPartialIndexBulk(string index, string type, object id, object value, object parent = null)
        {
            if (parent == null)
                return string.Format("{0}\n{1}\n",
                    JsonConvert.SerializeObject(new { index = new { _index = index, _type = type, _id = id } }, Formatting.None),
                    JsonConvert.SerializeObject(value, Formatting.None));
            else

                return string.Format("{0}\n{1}\n",
                    JsonConvert.SerializeObject(new { index = new { _index = index, _type = type, _id = id, parent = parent } }, Formatting.None),
                    JsonConvert.SerializeObject(value, Formatting.None));
        }
    }
}