package main

import (
	"context"
	"cortex-cassandra-store/grpc"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (s *server) WriteIndex(ctx context.Context, batch *grpc.WriteIndexRequest) (*empty.Empty, error) {
	for i, entry := range batch.Writes {
		s.Logger.Info("performing batch write. ", zap.String("Table name ", batch.Writes[i].TableName))
		err := s.Session.Query(fmt.Sprintf("INSERT INTO %s (hash, range, value) VALUES (?, ?, ?)",
			entry.TableName), entry.HashValue, entry.RangeValue, entry.Value).WithContext(ctx).Exec()
		if err != nil {
			s.Logger.Error("failed to perform batch write ", zap.Error(err))
			return &empty.Empty{}, errors.WithStack(err)
		}
	}
	return &empty.Empty{}, nil
}

func (s *server) QueryIndex(query *grpc.QueryIndexRequest, queryStreamer grpc.GrpcStore_QueryIndexServer) error {
	var q *gocql.Query
	s.Logger.Info("performing Query Pages ", zap.String("table name ", query.TableName))
	switch {
	case len(query.RangeValuePrefix) > 0 && query.ValueEqual == nil:
		q = s.Session.Query(fmt.Sprintf("SELECT range, value FROM %s WHERE hash = ? AND range >= ? AND range < ?",
			query.TableName), query.HashValue, query.RangeValuePrefix, append(query.RangeValuePrefix, '\xff'))

	case len(query.RangeValuePrefix) > 0 && query.ValueEqual != nil:
		q = s.Session.Query(fmt.Sprintf("SELECT range, value FROM %s WHERE hash = ? AND range >= ? AND range < ? AND value = ? ALLOW FILTERING",
			query.TableName), query.HashValue, query.RangeValuePrefix, append(query.RangeValuePrefix, '\xff'), query.ValueEqual)

	case len(query.RangeValueStart) > 0 && query.ValueEqual == nil:
		q = s.Session.Query(fmt.Sprintf("SELECT range, value FROM %s WHERE hash = ? AND range >= ?",
			query.TableName), query.HashValue, query.RangeValueStart)

	case len(query.RangeValueStart) > 0 && query.ValueEqual != nil:
		q = s.Session.Query(fmt.Sprintf("SELECT range, value FROM %s WHERE hash = ? AND range >= ? AND value = ? ALLOW FILTERING",
			query.TableName), query.HashValue, query.RangeValueStart, query.ValueEqual)

	case query.ValueEqual == nil:
		q = s.Session.Query(fmt.Sprintf("SELECT range, value FROM %s WHERE hash = ?",
			query.TableName), query.HashValue)

	case query.ValueEqual != nil:
		q = s.Session.Query(fmt.Sprintf("SELECT range, value FROM %s WHERE hash = ? value = ? ALLOW FILTERING",
			query.TableName), query.HashValue, query.ValueEqual)
	}

	iter := q.WithContext(context.Background()).Iter()
	defer iter.Close()
	scanner := iter.Scanner()
	b1 := &grpc.QueryIndexResponse{
		Rows: []*grpc.Row{},
	}
	for scanner.Next() {
		b := &grpc.Row{}
		if err := scanner.Scan(&b.RangeValue, &b.Value); err != nil {
			s.Logger.Error("error with query pages ", zap.Error(err))
			return errors.WithStack(err)
		}

		b1.Rows = append(b1.Rows, b)
	}
	// you can add custom logic here to break rows and send as stream instead of sending all at once.
	err := queryStreamer.Send(b1)
	if err != nil {
		s.Logger.Error("Unable to stream the results")
	}

	return nil
}

func (s *server) DeleteIndex(ctx context.Context, request *grpc.DeleteIndexRequest) (*empty.Empty, error) {
	for _, entry := range request.Deletes {
		err := s.Session.Query(fmt.Sprintf("DELETE FROM %s WHERE hash = ? and range = ?",
			entry.TableName), entry.HashValue, entry.RangeValue).WithContext(ctx).Exec()
		if err != nil {
			s.Logger.Error("failed to perform batch write ", zap.Error(err))
			return &empty.Empty{}, errors.WithStack(err)
		}
	}
	return &empty.Empty{}, nil
}