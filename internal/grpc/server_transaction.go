package grpc

import (
	"context"
	"database/sql"

	"google.golang.org/grpc"

	"github.com/v8tix/eda/di"
	"github.com/v8tix/mallbots-depot-proto/pb"
	"github.com/v8tix/mallbots-depot/internal/application"
)

type serverTx struct {
	c di.Container
	pb.UnimplementedDepotServiceServer
}

var _ pb.DepotServiceServer = (*serverTx)(nil)

func RegisterServerTx(container di.Container, registrar grpc.ServiceRegistrar) error {
	pb.RegisterDepotServiceServer(registrar, serverTx{
		c: container,
	})
	return nil
}

func (s serverTx) CreateShoppingList(ctx context.Context, request *pb.CreateShoppingListRequest) (resp *pb.CreateShoppingListResponse, err error) {
	ctx = s.c.Scoped(ctx)
	defer func(tx *sql.Tx) {
		err = s.closeTx(tx, err)
	}(di.Get(ctx, "tx").(*sql.Tx))

	next := server{app: di.Get(ctx, "app").(application.App)}

	return next.CreateShoppingList(ctx, request)
}

func (s serverTx) CancelShoppingList(ctx context.Context, request *pb.CancelShoppingListRequest) (resp *pb.CancelShoppingListResponse, err error) {
	ctx = s.c.Scoped(ctx)
	defer func(tx *sql.Tx) {
		err = s.closeTx(tx, err)
	}(di.Get(ctx, "tx").(*sql.Tx))

	next := server{app: di.Get(ctx, "app").(application.App)}

	return next.CancelShoppingList(ctx, request)
}

func (s serverTx) AssignShoppingList(ctx context.Context, request *pb.AssignShoppingListRequest) (resp *pb.AssignShoppingListResponse, err error) {
	ctx = s.c.Scoped(ctx)
	defer func(tx *sql.Tx) {
		err = s.closeTx(tx, err)
	}(di.Get(ctx, "tx").(*sql.Tx))

	next := server{app: di.Get(ctx, "app").(application.App)}

	return next.AssignShoppingList(ctx, request)
}

func (s serverTx) CompleteShoppingList(ctx context.Context, request *pb.CompleteShoppingListRequest) (resp *pb.CompleteShoppingListResponse, err error) {
	ctx = s.c.Scoped(ctx)
	defer func(tx *sql.Tx) {
		err = s.closeTx(tx, err)
	}(di.Get(ctx, "tx").(*sql.Tx))

	next := server{app: di.Get(ctx, "app").(application.App)}

	return next.CompleteShoppingList(ctx, request)
}

func (s serverTx) closeTx(tx *sql.Tx, err error) error {
	if p := recover(); p != nil {
		_ = tx.Rollback()
		panic(p)
	} else if err != nil {
		_ = tx.Rollback()
		return err
	} else {
		return tx.Commit()
	}
}
