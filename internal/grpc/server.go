package grpc

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/v8tix/mallbots-depot-proto/pb"
	"github.com/v8tix/mallbots-depot/internal/application"
	"github.com/v8tix/mallbots-depot/internal/application/commands"
)

type server struct {
	app application.App
	pb.UnimplementedDepotServiceServer
}

var _ pb.DepotServiceServer = (*server)(nil)

func Register(app application.App, registrar grpc.ServiceRegistrar) error {
	pb.RegisterDepotServiceServer(registrar, server{app: app})
	return nil
}

func (s server) CreateShoppingList(ctx context.Context, request *pb.CreateShoppingListRequest) (*pb.CreateShoppingListResponse, error) {
	/*

		ctx, cleanup := s.container.Scoped(ctx) //di.Scoped(ctx)
		defer cleanup()
		// ...

		app := di.Get(ctx, "app").(application.App)

		err := app.???(ctx)
		return &pb.CreateShoppingListResponse{Id: id}, err
	*/

	id := uuid.New().String()

	items := make([]commands.OrderItem, 0, len(request.GetItems()))
	for _, item := range request.GetItems() {
		items = append(items, s.itemToDomain(item))
	}

	err := s.app.CreateShoppingList(ctx, commands.CreateShoppingList{
		ID:      id,
		OrderID: request.GetOrderId(),
		Items:   items,
	})

	return &pb.CreateShoppingListResponse{Id: id}, err
}

func (s server) CancelShoppingList(ctx context.Context, request *pb.CancelShoppingListRequest) (*pb.CancelShoppingListResponse, error) {
	err := s.app.CancelShoppingList(ctx, commands.CancelShoppingList{
		ID: request.GetId(),
	})

	return &pb.CancelShoppingListResponse{}, err
}

func (s server) AssignShoppingList(ctx context.Context, request *pb.AssignShoppingListRequest) (*pb.AssignShoppingListResponse, error) {
	err := s.app.AssignShoppingList(ctx, commands.AssignShoppingList{
		ID:    request.GetId(),
		BotID: request.GetBotId(),
	})
	return &pb.AssignShoppingListResponse{}, err
}

func (s server) CompleteShoppingList(ctx context.Context, request *pb.CompleteShoppingListRequest) (*pb.CompleteShoppingListResponse, error) {
	err := s.app.CompleteShoppingList(ctx, commands.CompleteShoppingList{ID: request.GetId()})
	return &pb.CompleteShoppingListResponse{}, err
}

func (s server) itemToDomain(item *pb.OrderItem) commands.OrderItem {
	return commands.OrderItem{
		StoreID:   item.GetStoreId(),
		ProductID: item.GetProductId(),
		Quantity:  int(item.GetQuantity()),
	}
}
