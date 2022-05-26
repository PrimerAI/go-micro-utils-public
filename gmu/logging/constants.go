package logging

const (
	// DataDogAgentEnvName contains the name of the environment variable storing the data dog agent host name.
	DataDogAgentEnvName = "DD_AGENT_HOST"
	// Default port for datadog statsd.
	DataDogStatsdPort = 8125

	// DataDog counters take a rate value between 0 and 1.0 to sample the counters.
	// A value of 1.0 means always send the value rather than subsample.
	// https://statsd.readthedocs.io/en/v3.2.1/types.html#counters
	DDAlwaysSample = 1.0
)
