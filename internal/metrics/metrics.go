package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

func Init(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Info().Msgf("metrics server listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Error().Err(err).Msg("metrics server failed")
		}
	}()
}