package mqtt

import (
	"context"

	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/mercury"
)

type mqttNotify struct{}

func (mqttNotify) SendNotify(ctx context.Context, n mercury.Notify) {
	_, span := lg.Span(ctx)
	defer span.End()
	// var m mqtt.Message
	// m, err = mqtt.NewMessage(n.URL, n)
	// if err != nil {
	// 	return
	// }
	// log.Debug(n)
	// err = mqtt.Publish(m)
	// return
}

func Register() {
	mercury.Registry.Register("mqtt-notify", func(s *mercury.Space) any { return &mqttNotify{} })
}
