package main

import (
	"context"
	_ "cortex-cassandra-store/grpc"
	rpc "cortex-cassandra-store/grpc"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (c *server) ListTables(context.Context, *empty.Empty) (*rpc.ListTablesResponse, error) {
	c.Logger.Info("listing the tables ")
	md, err := c.Session.KeyspaceMetadata(c.Cfg.Keyspace)
	if err != nil {
		c.Logger.Error("failed in fetching key-space metadata %s", zap.Error(err))
		return nil, errors.WithStack(err)
	}
	result := &rpc.ListTablesResponse{}
	for name := range md.Tables {
		result.TableNames = append(result.TableNames, name)
	}
	return result, nil
}

func (c *server) CreateTable(ctx context.Context, req *rpc.CreateTableRequest) (*empty.Empty, error) {
	c.Logger.Info("creating the table ", zap.String("Table Name", req.Desc.Name))
	err := c.Session.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			hash text,
			range blob,
			value blob,
			PRIMARY KEY (hash, range)
		)`, req.Desc.Name)).WithContext(ctx).Exec()
	if err != nil {
		c.Logger.Error("failed to create the table %s", zap.Error(err))
	}
	return &empty.Empty{}, errors.WithStack(err)
}

func (c *server) DeleteTable(ctx context.Context, tableName *rpc.DeleteTableRequest) (*empty.Empty, error) {
	c.Logger.Info("deleting the table ", zap.String("Table Name", tableName.TableName))
	name := tableName.TableName
	err := c.Session.Query(fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;`, name)).WithContext(ctx).Exec()
	if err != nil {
		c.Logger.Error("failed to delete the table %s", zap.Error(err))
	}
	return &empty.Empty{}, errors.WithStack(err)
}

func (c *server) DescribeTable(ctx context.Context, tableName *rpc.DescribeTableRequest) (*rpc.DescribeTableResponse, error) {
	c.Logger.Info("describing the table ", zap.String("Table Name", tableName.TableName))
	name := tableName.TableName
	return &rpc.DescribeTableResponse{
		Desc: &rpc.TableDesc{
			Name: name,
		},
		IsActive: true,
	}, nil
}

func (c *server) UpdateTable(context.Context, *rpc.UpdateTableRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
