package depot

import (
	"context"
	"database/sql"
	"github.com/v8tix/mallbots-depot/internal/ms"

	"github.com/rs/zerolog"

	"github.com/v8tix/eda/am"
	"github.com/v8tix/eda/ddd"
	"github.com/v8tix/eda/di"
	"github.com/v8tix/eda/jetstream"
	pg "github.com/v8tix/eda/postgres"
	"github.com/v8tix/eda/registry"
	"github.com/v8tix/eda/tm"
	"github.com/v8tix/mallbots-depot-proto/pb"
	"github.com/v8tix/mallbots-depot-proto/rest"
	"github.com/v8tix/mallbots-depot/internal/application"
	"github.com/v8tix/mallbots-depot/internal/domain"
	"github.com/v8tix/mallbots-depot/internal/grpc"
	"github.com/v8tix/mallbots-depot/internal/handlers"
	"github.com/v8tix/mallbots-depot/internal/logging"
	"github.com/v8tix/mallbots-depot/internal/postgres"
	storespb "github.com/v8tix/mallbots-stores-proto/pb"
)

type Module struct{}

func (Module) Startup(ctx context.Context, mono ms.Microservice) (err error) {
	container := di.New()

	// setup Driven adapters
	container.AddSingleton("registry", func(c di.Container) (any, error) {
		reg := registry.New()
		if err := storespb.Registrations(reg); err != nil {
			return nil, err
		}
		if err := pb.Registrations(reg); err != nil {
			return nil, err
		}
		return reg, nil
	})
	container.AddSingleton("logger", func(c di.Container) (any, error) {
		return mono.Logger(), nil
	})
	container.AddSingleton("stream", func(c di.Container) (any, error) {
		return jetstream.NewStream(mono.Config().Nats.Stream, mono.JS(), c.Get("logger").(zerolog.Logger)), nil
	})
	container.AddSingleton("domainDispatcher", func(c di.Container) (any, error) {
		return ddd.NewEventDispatcher[ddd.AggregateEvent](), nil
	})
	container.AddSingleton("db", func(c di.Container) (any, error) {
		return mono.DB(), nil
	})
	container.AddSingleton("conn", func(c di.Container) (any, error) {
		return grpc.Dial(ctx, mono.Config().RPC.Address())
	})
	container.AddSingleton("outboxProcessor", func(c di.Container) (any, error) {
		return tm.NewOutboxProcessor(
			c.Get("stream").(am.RawMessageStream),
			pg.NewOutboxStore("depot.outbox", c.Get("db").(*sql.DB)),
		), nil
	})
	container.AddScoped("tx", func(c di.Container) (any, error) {
		db := c.Get("db").(*sql.DB)
		return db.Begin()
	})
	container.AddScoped("txStream", func(c di.Container) (any, error) {
		tx := c.Get("tx").(*sql.Tx)
		outboxStore := pg.NewOutboxStore("depot.outbox", tx)
		return am.RawMessageStreamWithMiddleware(
			c.Get("stream").(am.RawMessageStream),
			tm.NewOutboxStreamMiddleware(outboxStore),
		), nil
	})
	container.AddScoped("eventStream", func(c di.Container) (any, error) {
		return am.NewEventStream(
			c.Get("registry").(registry.Registry),
			c.Get("txStream").(am.RawMessageStream),
		), nil
	})
	container.AddScoped("commandStream", func(c di.Container) (any, error) {
		return am.NewCommandStream(c.Get("registry").(registry.Registry), c.Get("txStream").(am.RawMessageStream)), nil
	})
	container.AddScoped("replyStream", func(c di.Container) (any, error) {
		return am.NewReplyStream(c.Get("registry").(registry.Registry), c.Get("txStream").(am.RawMessageStream)), nil
	})
	container.AddScoped("inboxMiddleware", func(c di.Container) (any, error) {
		tx := c.Get("tx").(*sql.Tx)
		inboxStore := pg.NewInboxStore("depot.inbox", tx)
		return tm.NewInboxHandlerMiddleware(inboxStore), nil
	})
	container.AddScoped("shoppingLists", func(c di.Container) (any, error) {
		return postgres.NewShoppingListRepository(
			"depot.shopping_lists",
			c.Get("tx").(*sql.Tx),
		), nil
	})
	container.AddScoped("stores", func(c di.Container) (any, error) {
		return postgres.NewStoreCacheRepository(
			"depot.stores_cache",
			c.Get("tx").(*sql.Tx),
			grpc.NewStoreRepository(c.Get("conn").(*grpc.ClientConn)),
		), nil
	})
	container.AddScoped("products", func(c di.Container) (any, error) {
		return postgres.NewProductCacheRepository(
			"depot.products_cache",
			c.Get("tx").(*sql.Tx),
			grpc.NewProductRepository(c.Get("conn").(*grpc.ClientConn)),
		), nil
	})

	// setup application
	container.AddScoped("app", func(c di.Container) (any, error) {
		return logging.LogApplicationAccess(
			application.New(
				c.Get("shoppingLists").(domain.ShoppingListRepository),
				c.Get("stores").(domain.StoreCacheRepository),
				c.Get("products").(domain.ProductCacheRepository),
				c.Get("domainDispatcher").(*ddd.EventDispatcher[ddd.AggregateEvent]),
			),
			c.Get("logger").(zerolog.Logger),
		), nil
	})
	container.AddScoped("domainEventHandlers", func(c di.Container) (any, error) {
		return logging.LogEventHandlerAccess[ddd.AggregateEvent](
			handlers.NewDomainEventHandlers(c.Get("eventStream").(am.EventStream)),
			"DomainEvents", c.Get("logger").(zerolog.Logger),
		), nil
	})
	container.AddScoped("integrationEventHandlers", func(c di.Container) (any, error) {
		return logging.LogEventHandlerAccess[ddd.Event](
			handlers.NewIntegrationEventHandlers(
				c.Get("stores").(domain.StoreCacheRepository),
				c.Get("products").(domain.ProductCacheRepository),
			),
			"IntegrationEvents", c.Get("logger").(zerolog.Logger),
		), nil
	})
	container.AddScoped("commandHandlers", func(c di.Container) (any, error) {
		return logging.LogCommandHandlerAccess[ddd.Command](
			handlers.NewCommandHandlers(c.Get("app").(application.App)),
			"Commands", c.Get("logger").(zerolog.Logger),
		), nil
	})

	// setup Driver adapters
	if err := grpc.RegisterServerTx(container, mono.RPC()); err != nil {
		return err
	}
	if err := rest.RegisterGateway(ctx, mono.Mux(), mono.Config().RPC.Address()); err != nil {
		return err
	}
	if err := rest.RegisterSwagger(mono.Mux()); err != nil {
		return err
	}
	handlers.RegisterDomainEventHandlersTx(container)
	if err = handlers.RegisterIntegrationEventHandlersTx(container); err != nil {
		return err
	}
	if err = handlers.RegisterCommandHandlersTx(container); err != nil {
		return err
	}
	startOutboxProcessor(ctx, container)

	return nil
}

func startOutboxProcessor(ctx context.Context, container di.Container) {
	outboxProcessor := container.Get("outboxProcessor").(tm.OutboxProcessor)
	logger := container.Get("logger").(zerolog.Logger)

	go func() {
		err := outboxProcessor.Start(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("depot outbox processor encountered an error")
		}
	}()
}
