package handlers

import (
	"context"

	"github.com/v8tix/eda/am"
	"github.com/v8tix/eda/ddd"
	"github.com/v8tix/mallbots-depot-proto/pb"
	"github.com/v8tix/mallbots-depot/internal/domain"
)

type domainHandlers[T ddd.AggregateEvent] struct {
	publisher am.MessagePublisher[ddd.Event]
}

var _ ddd.EventHandler[ddd.AggregateEvent] = (*domainHandlers[ddd.AggregateEvent])(nil)

func NewDomainEventHandlers(publisher am.MessagePublisher[ddd.Event]) ddd.EventHandler[ddd.AggregateEvent] {
	return domainHandlers[ddd.AggregateEvent]{
		publisher: publisher,
	}
}

func RegisterDomainEventHandlers(subscriber ddd.EventSubscriber[ddd.AggregateEvent], handlers ddd.EventHandler[ddd.AggregateEvent]) {
	subscriber.Subscribe(handlers, domain.ShoppingListCompletedEvent)
}

func (h domainHandlers[T]) HandleEvent(ctx context.Context, event T) error {
	switch event.EventName() {
	case domain.ShoppingListCompletedEvent:
		return h.onShoppingListCompleted(ctx, event)
	}
	return nil
}

func (h domainHandlers[T]) onShoppingListCompleted(ctx context.Context, event ddd.AggregateEvent) error {
	completed := event.Payload().(*domain.ShoppingListCompleted)

	return h.publisher.Publish(ctx, pb.ShoppingListAggregateChannel, ddd.NewEvent(pb.ShoppingListCompletedEvent, &pb.ShoppingListCompleted{
		Id:      event.AggregateID(),
		OrderId: completed.ShoppingList.OrderID,
	}))
}
