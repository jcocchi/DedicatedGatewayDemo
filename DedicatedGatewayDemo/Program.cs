using Microsoft.Azure.Cosmos;
using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;

namespace DedicatedGatewayDemo
{
    class Program
    {
        private static readonly string CosmosDBConnection = ConfigurationManager.AppSettings["CosmosDedicatedGatewayConnection"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string ContainerName = ConfigurationManager.AppSettings["ContainerName"];
        private static readonly string PartitionKeyPath = ConfigurationManager.AppSettings["PartitionKeyPath"];

        public static async Task Main(string[] args)
        {
            Console.WriteLine("Azure Cosmos DB Integrated Cache Demo\n");

            CosmosClientOptions options = new CosmosClientOptions()
            {
                ConnectionMode = ConnectionMode.Gateway,
                ConsistencyLevel = ConsistencyLevel.Session
            };
            CosmosClient client = new CosmosClient(CosmosDBConnection, options);

            var container = await client.GetDatabase(DatabaseName).CreateContainerIfNotExistsAsync(ContainerName, PartitionKeyPath);

            var docId = Guid.NewGuid().ToString();

            Console.WriteLine("\nSample write: ");
            await WriteItem(container, docId);

            Console.WriteLine("\nTest item cache: ");
            await TestItemCache(container, docId);

            Console.WriteLine("\nTest query cache: ");
            await TestQueryCache(container);
            await TestQueryCache(container);

            Console.WriteLine("\nTest cache staleness: ");
            await TestCacheStaleness(container, docId);
        }

        public static async Task WriteItem(Container container, string docId)
        {
            var requestOptions = new ItemRequestOptions()
            {
                ConsistencyLevel = ConsistencyLevel.Session
            };

            var response = await container.CreateItemAsync(new { id = docId }, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Write item request charge for document id {docId}:\t{response.RequestCharge:0.00} RU/s");
        }

        public static async Task TestItemCache(Container container, string docId)
        {
            var requestOptions = new ItemRequestOptions() 
            { 
                ConsistencyLevel = ConsistencyLevel.Eventual
            };

            ItemResponse<dynamic> response = await container.ReadItemAsync<dynamic>(docId, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Point read 1 request charge for document id {docId}:\t{response.RequestCharge:0.00} RU/s");

            ItemResponse<dynamic> response2 = await container.ReadItemAsync<dynamic>(docId, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Point read 2 request charge for document id {docId}:\t{response2.RequestCharge:0.00} RU/s\n");
        }

        public static async Task TestQueryCache(Container container)
        {            
            string sqlText = "SELECT * FROM c";
            QueryDefinition query = new QueryDefinition(sqlText);

            QueryRequestOptions queryOptions = new QueryRequestOptions()
            {
                ConsistencyLevel = ConsistencyLevel.Eventual
            };
            FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(query, requestOptions: queryOptions);

            double totalRequestCharge = 0;
            while (iterator.HasMoreResults)
            {
                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                totalRequestCharge += response.RequestCharge;
                Console.WriteLine($"Query request charge:\t\t{response.RequestCharge:0.00} RU/s");
            }

            Console.WriteLine($"Total query request charge:\t{totalRequestCharge:0.00} RU/s\n");
        }

        public static async Task TestCacheStaleness(Container container, string docId)
        {
            var cacheExpireSeconds = 5;
            var requestOptions = new ItemRequestOptions()
            {
                ConsistencyLevel = ConsistencyLevel.Session,
                DedicatedGatewayRequestOptions = new DedicatedGatewayRequestOptions()
                {
                    MaxIntegratedCacheStaleness = TimeSpan.FromSeconds(cacheExpireSeconds)
                }
            };
            ItemResponse<dynamic> response = await container.ReadItemAsync<dynamic>(docId, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Point read 1 request charge for document id {docId}:\t{response.RequestCharge:0.00} RU/s");

            ItemResponse<dynamic> response2 = await container.ReadItemAsync<dynamic>(docId, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Point read 2 request charge for document id {docId}:\t{response2.RequestCharge:0.00} RU/s\n");

            Console.WriteLine($"Waiting {cacheExpireSeconds} seconds for the cache to expire...\n");
            Thread.Sleep(cacheExpireSeconds*1000);

            ItemResponse<dynamic> responseExpired = await container.ReadItemAsync<dynamic>(docId, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Point read 3 request charge for document id {docId}:\t{responseExpired.RequestCharge:0.00} RU/s");

            ItemResponse<dynamic> responseExpired2 = await container.ReadItemAsync<dynamic>(docId, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Point read 4 request charge for document id {docId}:\t{responseExpired2.RequestCharge:0.00} RU/s\n");
        }
    }
}
