namespace ElasticSearchSync.DTO
{
    public class GetResponseDTO
    {
        public string _index { get; set; }

        public string _type { get; set; }

        public string _id { get; set; }

        public int? _version { get; set; }

        public bool found { get; set; }

        public dynamic _source { get; set; }
    }
}