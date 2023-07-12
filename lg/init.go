package lg

import (
	"context"
	"log"

	"go.uber.org/multierr"
)

func Init(ctx context.Context, name string) (context.Context, func(context.Context) error) {
	ctx, span := Span(ctx)
	defer span.End()

	stop := [2]func() error{}
	ctx, stop[0] = initMetrics(ctx, name)
	ctx, stop[1] = initTracing(ctx, name)

	reverse(stop[:])

	return ctx, func(context.Context) error {
		log.Println("flushing logs...")
		errs := make([]error, len(stop))
		for i, fn := range stop {
			if fn != nil {
				errs[i] = fn()
			}
		}
		log.Println("all stopped.")
		return multierr.Combine(errs...)
	}
}
