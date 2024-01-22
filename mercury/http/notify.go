package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"

	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/mercury"
)

type httpNotify struct{}

func (httpNotify) SendNotify(ctx context.Context, n mercury.Notify) error {
	ctx, span := lg.Span(ctx)
	defer span.End()

	cl := &http.Client{}
	caCertPool, err := x509.SystemCertPool()
	if err != nil {
		caCertPool = x509.NewCertPool()
	}

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}

	cl.Transport = transport

	var req *http.Request
	req, err = http.NewRequestWithContext(ctx, n.Method, n.URL, bytes.NewBufferString(""))
	if err != nil {
		span.RecordError(err)
		return err
	}
	req.Header.Set("content-type", "application/json")

	span.AddEvent(fmt.Sprint("URL: ", n.URL))
	res, err := cl.Do(req)
	if err != nil {
		span.RecordError(err)
		return err
	}
	res.Body.Close()
	span.AddEvent(fmt.Sprint(res.Status))
	if res.StatusCode != 200 {
		span.RecordError(err)
		err = fmt.Errorf("unable to read config")
		return err
	}

	return nil
}

func Register() {
	mercury.Registry.Register("http-notify", func(s *mercury.Space) any { return httpNotify{} })
}
