# Azure Cosmos DB Dedicated Gateway Demo

This demo requires an Azure Cosmos DB for NoSQL account with a [Dedicated Gateway provisioned](https://learn.microsoft.com/en-us/azure/cosmos-db/dedicated-gateway#provisioning-the-dedicated-gateway).

## Setup

Rename the `App.sample.config` file to `App.config` and fill in the `CosmosDedicatedGatewayConnection` with your [Azure Cosmos DB for NoSQL Dedicated Gateway connection string](https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-configure-integrated-cache?tabs=java#configuring-the-integrated-cache).

The project is configured to create a database and container with the following values if one doesn't already exist. Modify them in the `App.config` file if you wish. The container will be provisioned with the default 400 RUs.

|Config |Default value |
|-------|--------------|
|DatabaseName |db |
|ContainerName |container |
|PartitionKeyPath |/pk |

## Run the application

Enter `CTRL + F5` in Visual Studio or enter `dotnet run` at the command line from the `/DedicatedGatewayDemo` folder.
