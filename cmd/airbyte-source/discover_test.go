package airbyte_source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverInvalidSource(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}
	td := testDatabase{
		connectResponse: canConnectResponse{
			err: fmt.Errorf("[%v] is invalid", "username"),
		},
	}

	b := bytes.NewBufferString("")
	discover := DiscoverCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     internal.NewLogger(b),
	})
	discover.SetArgs([]string{"config source.json"})

	discover.SetOut(b)
	assert.NoError(t, discover.Flag("config").Value.Set("catalog.json"))
	assert.NoError(t, discover.Execute())

	var amsg internal.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
	assert.Contains(t, amsg.ConnectionStatus.Message, "[username] is invalid")
}

func TestDiscoverFailed(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}
	td := testDatabase{
		connectResponse: canConnectResponse{
			err: nil,
		},
		discoverSchemaResponse: discoverSchemaResponse{
			err: fmt.Errorf("unable to get catalog for %v", "keyspace"),
		},
	}
	b := bytes.NewBufferString("")
	discover := DiscoverCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     internal.NewLogger(b),
	})
	discover.SetArgs([]string{"config source.json"})

	discover.SetOut(b)
	assert.NoError(t, discover.Flag("config").Value.Set("catalog.json"))
	assert.NoError(t, discover.Execute())
	var amsg internal.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.LOG, amsg.Type)
	require.NotNil(t, amsg.Log)
	assert.Equal(t, internal.LOGLEVEL_ERROR, amsg.Log.Level)
	assert.Equal(t, "PlanetScale Source :: Unable to discover database, failed with [unable to get catalog for keyspace]", amsg.Log.Message)
}

// captureMySQLDSN replaces newMySQL for one test and records the DSN each
// command connects with. It returns an error so the command stops after
// EnsureDB.
func captureMySQLDSN(t *testing.T) *[]string {
	t.Helper()
	// End-to-end mode drops the @tablet suffix from the DSN.
	t.Setenv("PS_END_TO_END_TEST_RUN", "")
	var dsns []string
	orig := newMySQL
	newMySQL = func(psc *internal.PlanetScaleSource) (internal.PlanetScaleEdgeMysqlAccess, error) {
		dsns = append(dsns, psc.DSN())
		return nil, fmt.Errorf("stop after capturing the DSN")
	}
	t.Cleanup(func() { newMySQL = orig })
	return &dsns
}

const replicaSourceConfig = "{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\",\"use_replica\":true}"

func TestDiscoverConnectsToPrimaryWhenReplicaConfigured(t *testing.T) {
	dsns := captureMySQLDSN(t)
	b := bytes.NewBufferString("")
	discover := DiscoverCommand(&Helper{
		FileReader: testFileReader{content: []byte(replicaSourceConfig)},
		Logger:     internal.NewLogger(b),
	})
	discover.SetOut(b)
	require.NoError(t, discover.Flag("config").Value.Set("source.json"))
	require.NoError(t, discover.Execute())

	require.Len(t, *dsns, 1)
	assert.Contains(t, (*dsns)[0], "/database@primary?")
}

func TestCheckKeepsConfiguredReplica(t *testing.T) {
	dsns := captureMySQLDSN(t)
	b := bytes.NewBufferString("")
	check := CheckCommand(&Helper{
		FileReader: testFileReader{content: []byte(replicaSourceConfig)},
		Logger:     internal.NewLogger(b),
	})
	check.SetOut(b)
	require.NoError(t, check.Flag("config").Value.Set("source.json"))
	require.NoError(t, check.Execute())

	require.Len(t, *dsns, 1)
	assert.Contains(t, (*dsns)[0], "/database@replica?")
}
