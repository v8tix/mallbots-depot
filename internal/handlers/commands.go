package handlers

import (
	"context"

	"github.com/google/uuid"

	"github.com/v8tix/eda/am"
	"github.com/v8tix/eda/ddd"
	"github.com/v8tix/mallbots-depot-proto/pb"
	"github.com/v8tix/mallbots-depot/internal/application"
	"github.com/v8tix/mallbots-depot/internal/application/commands"
)

type commandHandlers struct {
	app application.App
}

func NewCommandHandlers(app application.App) ddd.CommandHandler[ddd.Command] {
	return commandHandlers{
		app: app,
	}
}

func RegisterCommandHandlers(subscriber am.RawMessageSubscriber, handlers am.RawMessageHandler) error {
	return subscriber.Subscribe(pb.CommandChannel, handlers, am.MessageFilter{
		pb.CreateShoppingListCommand,
		pb.CancelShoppingListCommand,
		pb.InitiateShoppingCommand,
	}, am.GroupName("depot-commands"))
}

func (h commandHandlers) HandleCommand(ctx context.Context, cmd ddd.Command) (ddd.Reply, error) {
	switch cmd.CommandName() {
	case pb.CreateShoppingListCommand:
		return h.doCreateShoppingList(ctx, cmd)
	case pb.CancelShoppingListCommand:
		return h.doCancelShoppingList(ctx, cmd)
	}

	return nil, nil
}

func (h commandHandlers) doCreateShoppingList(ctx context.Context, cmd ddd.Command) (ddd.Reply, error) {
	payload := cmd.Payload().(*pb.CreateShoppingList)

	id := uuid.New().String()

	items := make([]commands.OrderItem, 0, len(payload.GetItems()))
	for _, item := range payload.GetItems() {
		items = append(items, commands.OrderItem{
			StoreID:   item.GetStoreId(),
			ProductID: item.GetProductId(),
			Quantity:  int(item.GetQuantity()),
		})
	}

	err := h.app.CreateShoppingList(ctx, commands.CreateShoppingList{
		ID:      id,
		OrderID: payload.GetOrderId(),
		Items:   items,
	})

	return ddd.NewReply(pb.CreatedShoppingListReply, &pb.CreatedShoppingList{Id: id}), err
}

func (h commandHandlers) doCancelShoppingList(ctx context.Context, cmd ddd.Command) (ddd.Reply, error) {
	payload := cmd.Payload().(*pb.CancelShoppingList)

	err := h.app.CancelShoppingList(ctx, commands.CancelShoppingList{ID: payload.GetId()})

	// returning nil returns a simple Success or Failure reply; err being nil determines which
	return nil, err
}

func (h commandHandlers) doInitiateShopping(ctx context.Context, cmd ddd.Command) (ddd.Reply, error) {
	payload := cmd.Payload().(*pb.InitiateShopping)

	err := h.app.InitiateShopping(ctx, commands.InitiateShopping{ID: payload.GetId()})

	// returning nil returns a simple Success or Failure reply; err being nil determines which
	return nil, err
}
