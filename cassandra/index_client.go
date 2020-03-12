package main

import (
	"context"
	"cortex-cassandra-store/grpc"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (c *server) BatchWrite(ctx context.Context, batch *grpc.WriteBatch) (*grpc.Nothing, error){
	c.Logger.Info("performing batch write.")
	for _, entry := range batch.IndexEntry {
		err := c.Session.Query(fmt.Sprintf("INSERT INTO %s (hash, range, value) VALUES (?, ?, ?)",
			entry.TableName), entry.HashValue, entry.RangeValue, entry.Value).WithContext(ctx).Exec()
		if err != nil {
			c.Logger.Error("failed to perform batch write ", zap.Error(err))
			return &grpc.Nothing{}, errors.WithStack(err)
		}
	}
	return &grpc.Nothing{},nil
}

func (s *server) QueryPages(ctx context.Context, query *grpc.IndexQuery) (*grpc.ReadBatch, error) {
	var q *gocql.Query
	s.Logger.Info("performing Query Pages")
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

	iter := q.WithContext(ctx).Iter()
	defer iter.Close()
	scanner := iter.Scanner()
	b1 := &grpc.ReadBatch{
		RangeValue:           []byte{},
		Value:                []byte{},
	}
	for scanner.Next() {
		b := &readBatch{}
		b1 = &grpc.ReadBatch{
			RangeValue:           b.rangeValue,
			Value:                b.value,
		}
		if err := scanner.Scan(&b1.RangeValue, &b1.Value); err != nil {
			s.Logger.Error("error with query pages ", zap.Error(err))
			return b1, errors.WithStack(err)
		}
	}
	if scanner.Err() != nil {
		s.Logger.Error("error with query pages ", zap.Error(scanner.Err()))
		return b1, errors.WithStack(scanner.Err())
	}
	return b1, nil
}

