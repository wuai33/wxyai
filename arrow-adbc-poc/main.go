package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// protobuf version printing removed due to internal package restriction
	// v1.36.1
	fmt.Println("protobuf version:", "v1.36.6")

	addr := "localhost:32010" // 移除 grpc+tcp:// 前缀
	client, err := flightsql.NewClient(
		addr,
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to create Flight SQL client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. ListFlights（底层 Flight API）
	var criteria flight.Criteria
	criteria.Expression = []byte("my_table")

	flights, err := client.Client.ListFlights(ctx,
		&criteria)
	if err != nil {
		log.Printf("ListFlights error: %v", err)
	} else {
		for {
			info, err := flights.Recv()
			if err != nil {
				fmt.Printf("ListFlights recv error: %v", err)
				break
			}
			fmt.Printf("Flight: %v\n", info)
		}
	}

	// 2. 查询数据（Execute + DoGet）
	query := "SELECT * FROM my_table"
	flightInfo, err := client.Execute(ctx, query)
	if err != nil {
		log.Printf("Execute error: %v", err)
	} else {
		for _, endpoint := range flightInfo.Endpoint {
			stream, err := client.DoGet(ctx, endpoint.Ticket)
			if err != nil {
				log.Printf("DoGet error: %v", err)
				continue
			}
			for stream.Next() {
				rec := stream.Record()
				fmt.Printf("Statement batch: %v\n", rec)
			}
			stream.Release()
		}
	}

	// 3. 获取所有表元数据（GetTables + DoGet）
	tablesFlightInfo, err := client.GetTables(ctx, &flightsql.GetTablesOpts{})
	if err != nil {
		log.Printf("GetTables error: %v", err)
	} else {

		log.Printf("GetTables: %v", tablesFlightInfo)

		for _, endpoint := range tablesFlightInfo.Endpoint {

			log.Printf("Do Get meta data from endpoint: %v, ticket: %v", endpoint.Location, endpoint.Ticket)

			// endpoint.Ticket。
			stream, err := client.DoGet(ctx, endpoint.Ticket)
			if err != nil {
				log.Printf("DoGet error: %v", err)
				continue
			}
			for stream.Next() {
				rec := stream.Record()
				fmt.Printf("DoGet successfully for meta batch: %v\n", rec)
			}

			log.Printf("Do Get meta data from endpoint: %v end", endpoint.Location)
			stream.Release()

			if string(endpoint.GetAppMetadata()) == "table_data" {
				// 初始化要给连接 flight server的client, 调用 DoGet获取真正的表数据
				for _, location := range endpoint.Location {
					log.Printf("Do Get table raw data stream from location: %v", location)
					addr := "localhost:9393"
					rawClient, err := flight.NewClientWithMiddleware(
						addr,
						nil,
						nil,
						grpc.WithInsecure())

					if err != nil {
						log.Printf("Failed to create base Flight client: %v", err)
						continue
					}
					defer rawClient.Close()

					// 使用与 endpoint.Ticket 相同的 ticket 调用 DoGet
					tableServerTicket := flight.Ticket{
						Ticket: []byte("example_data"),
					}
					rawStream, err := rawClient.DoGet(ctx, &tableServerTicket)
					if err != nil {
						log.Printf("DoGet from %s error: %v", location.GetUri(), err)
						continue
					}
					for {
						info, err := rawStream.Recv()
						if err != nil {
							fmt.Printf("DoGet from table server recv error: %v", err)
							break
						}
						fmt.Printf("DoGet from table server(%s) recv successfully: %v\n", location.GetUri(), info)
					}
				}
			}
		}
	}

	// 5. 获取主键信息（GetPrimaryKeys + DoGetPrimaryKeys）
	// pkInfo, err := client.GetPrimaryKeys(ctx, flightsql.TableRef{Table: "my_table"})

	// 6. 获取 schemas（GetDBSchemas + DoGetDbSchemas）
	// schemasInfo, err := client.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{})

	// 7. 获取 catalogs（GetCatalogs + DoGetCatalogs）
	// catalogsInfo, err := client.GetCatalogs(ctx)

	// 8. 获取类型信息（GetXdbcTypeInfo + DoGetXdbcTypeInfo）
	// typeInfo, err := client.GetXdbcTypeInfo(ctx, nil)

	// 9. 获取 SQL 信息（GetSqlInfo + DoGetSqlInfo）
	// sqlInfo, err := client.GetSqlInfo(ctx, nil)

}