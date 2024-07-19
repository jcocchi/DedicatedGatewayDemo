using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
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

            var db = await client.CreateDatabaseIfNotExistsAsync(DatabaseName);
            var container = await db.Database.CreateContainerIfNotExistsAsync(ContainerName, PartitionKeyPath);

            var docId = Guid.NewGuid().ToString();

            Console.WriteLine("\nSample write: ");
            await WriteItem(container, docId);

            Console.WriteLine("\nTest item cache: ");
            await TestItemCache(container, docId);

            Console.WriteLine("\nTest query cache: ");
            await TestQueryCache(container, false);
            await TestQueryCache(container, false);

            Console.WriteLine("\nTest bypassing query cache: ");
            await TestQueryCache(container, true);

            Console.WriteLine("\nTest ReadMany: ");
            IList<string> docIds = new List<string>() { Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
            await TestReadMany(container, docIds, true);
            await TestReadMany(container, docIds, false);
            docIds.Add(docId);
            await TestReadMany(container, docIds, false);
            await TestReadMany(container, docIds, false);

            Console.WriteLine("\nTest Update then ReadMany: ");
            await UpdateItem(container, docId);
            await TestReadMany(container, docIds, false);
            await TestReadMany(container, docIds, false);
            
            Console.WriteLine("\nTest cache staleness: ");
            await TestCacheStaleness(container, docId);
        }

        public static async Task WriteItem(Container container, string docId)
        {
            var requestOptions = new ItemRequestOptions()
            {
                ConsistencyLevel = ConsistencyLevel.Session,
                DedicatedGatewayRequestOptions = new DedicatedGatewayRequestOptions()
                {
                    BypassIntegratedCache = false
                }
            };

            var response = await container.CreateItemAsync(new { id = docId }, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Write item request charge for document id {docId}:\t{response.RequestCharge:0.00} RU/s\n");
        }

        public static async Task UpdateItem(Container container, string docId)
        {
            var response = await container.ReplaceItemAsync(new { id = docId, update = true }, docId);
            Console.WriteLine($"Update item request charge for document id {docId}:\t{response.RequestCharge:0.00} RU/s");
            Console.WriteLine($"Updated item:\t{response.Resource}\n");
        }

        public static async Task TestItemCache(Container container, string docId)
        {
            var requestOptions = new ItemRequestOptions() 
            { 
                ConsistencyLevel = ConsistencyLevel.Session
            };

            ItemResponse<dynamic> response = await container.ReadItemAsync<dynamic>(docId, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Point read 1 request charge for document id {docId}:\t{response.RequestCharge:0.00} RU/s");

            ItemResponse<dynamic> response2 = await container.ReadItemAsync<dynamic>(docId, new PartitionKey(docId), requestOptions);
            Console.WriteLine($"Point read 2 request charge for document id {docId}:\t{response2.RequestCharge:0.00} RU/s\n");
        }

        public static async Task TestQueryCache(Container container, bool bypassCache)
        {            
            string sqlText = "SELECT * FROM c";
            QueryDefinition query = new QueryDefinition(sqlText);

            QueryRequestOptions queryOptions = new QueryRequestOptions()
            {
                ConsistencyLevel = ConsistencyLevel.Eventual,
                DedicatedGatewayRequestOptions = new DedicatedGatewayRequestOptions()
                {
                    BypassIntegratedCache = bypassCache
                }
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

        public static async Task TestReadMany(Container container, IList<string> docIds, bool createDocs)
        {
            IList<(string, PartitionKey)> itemList = new List<(string, PartitionKey)>();
            foreach(var id in docIds)
            {
                if (createDocs)
                {
                    var response1 = await container.CreateItemAsync(new { id = id }, new PartitionKey(id));
                    Console.WriteLine($"Write item request charge for document id {id}:\t{response1.RequestCharge:0.00} RU/s");
                }

                itemList.Add((id, new PartitionKey(id)));
            }

            FeedResponse<dynamic> feedResponse = await container.ReadManyItemsAsync<dynamic>(itemList.AsReadOnly());
            Console.WriteLine($"Read many request charge:\t{feedResponse.RequestCharge:0.00} RU/s");
            foreach(var item in feedResponse.Resource)
            {
                Console.WriteLine("Read many item: " + item);
            }
            Console.WriteLine();
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
