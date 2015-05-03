// Copyright 2013 Ooyala, Inc.

package dogstatsd

import (
	"bytes"
	"net"
	"testing"
	"time"
)

var dogstatsdTests = []struct {
	GlobalNamespace string
	GlobalTags      []string
	Method          interface{}
	Metric          string
	Value           interface{}
	Tags            []string
	Rate            float64
	Expected        string
}{
	{"", nil, (*Client).Gauge, "test.gauge", 1.0, nil, 1.0, "test.gauge:1.000000|g"},
	{"", nil, (*Client).Gauge, "test.gauge", 1.0, nil, 0.999999, "test.gauge:1.000000|g|@0.999999"},
	{"", nil, (*Client).Gauge, "test.gauge", 1.0, []string{"tagA"}, 1.0, "test.gauge:1.000000|g|#tagA"},
	{"", nil, (*Client).Gauge, "test.gauge", 1.0, []string{"tagA", "tagB"}, 1.0, "test.gauge:1.000000|g|#tagA,tagB"},
	{"", nil, (*Client).Gauge, "test.gauge", 1.0, []string{"tagA"}, 0.999999, "test.gauge:1.000000|g|@0.999999|#tagA"},
	{"", nil, (*Client).Count, "test.count", int64(1), []string{"tagA"}, 1.0, "test.count:1|c|#tagA"},
	{"", nil, (*Client).Count, "test.count", int64(-1), []string{"tagA"}, 1.0, "test.count:-1|c|#tagA"},
	{"", nil, (*Client).Histogram, "test.histogram", 2.3, []string{"tagA"}, 1.0, "test.histogram:2.300000|h|#tagA"},
	{"", nil, (*Client).Set, "test.set", "uuid", []string{"tagA"}, 1.0, "test.set:uuid|s|#tagA"},
	{"flubber.", nil, (*Client).Set, "test.set", "uuid", []string{"tagA"}, 1.0, "flubber.test.set:uuid|s|#tagA"},
	{"", []string{"tagC"}, (*Client).Set, "test.set", "uuid", []string{"tagA"}, 1.0, "test.set:uuid|s|#tagA,tagC"},
}

func TestClient(t *testing.T) {
	server := newServer(t)
	defer server.Close()

	client := newClient(t, server.LocalAddr().String())
	defer client.Close()

	for i, tt := range dogstatsdTests {
		client.SetGlobalNamespace(tt.GlobalNamespace)
		client.SetGlobalTags(tt.GlobalTags)

		var err error
		switch fct := tt.Method.(type) {
		// Gauge, Histogram
		case func(*Client, string, float64, []string, float64) error:
			err = fct(client, tt.Metric, tt.Value.(float64), tt.Tags, tt.Rate)
		// Count
		case func(*Client, string, int64, []string, float64) error:
			err = fct(client, tt.Metric, tt.Value.(int64), tt.Tags, tt.Rate)
		// Set
		case func(*Client, string, string, []string, float64) error:
			err = fct(client, tt.Metric, tt.Value.(string), tt.Tags, tt.Rate)
		default:
			t.Fatalf("Unkown method type: %T", fct)
		}
		if err != nil {
			t.Fatal(err)
		}

		if message := serverRead(t, server); message != tt.Expected {
			t.Errorf("\n[%d] Expected:\t%s\nActual:\t\t%s", i, tt.Expected, message)
		}
	}

}

type eventTest struct {
	logEvent func(*Client) error
	expected string
}

var eventTests = []eventTest{
	{
		logEvent: func(c *Client) error { return c.Warning("title", "text", []string{"tag1", "tag2"}) },
		expected: "_e{13,4}:flubber.title|text|t:warning|s:flubber|#tag1,tag2,flubber-warning,flubber",
	},
	{
		logEvent: func(c *Client) error { return c.Error("Error!", "some error", []string{"tag3"}) },
		expected: "_e{14,10}:flubber.Error!|some error|t:error|s:flubber|#tag3,flubber-error,flubber",
	},
	{
		logEvent: func(c *Client) error { return c.Info("FYI", "note", []string{}) },
		expected: "_e{11,4}:flubber.FYI|note|t:info|s:flubber|#flubber-info,flubber",
	},
	{
		logEvent: func(c *Client) error { return c.Success("Great News", "hurray", []string{"foo", "bar", "baz"}) },
		expected: "_e{18,6}:flubber.Great News|hurray|t:success|s:flubber|#foo,bar,baz,flubber-success,flubber",
	},
	{
		logEvent: func(c *Client) error { return c.Info("Unicode", "世界", []string{}) },
		// Expect character, not byte lengths
		expected: "_e{15,2}:flubber.Unicode|世界|t:info|s:flubber|#flubber-info,flubber",
	},
	{
		logEvent: func(c *Client) error {
			eo := EventOpts{
				DateHappened:   time.Date(2014, time.September, 18, 22, 56, 0, 0, time.UTC),
				Priority:       Normal,
				Host:           "node.example.com",
				AggregationKey: "foo",
				SourceTypeName: "bar",
				AlertType:      Success,
			}
			return c.Event("custom title", "custom body", &eo)
		},
		expected: "_e{20,11}:flubber.custom title|custom body|t:success|s:bar|d:1411080960|p:normal|h:node.example.com|k:foo|#flubber",
	},
}

func TestEvent(t *testing.T) {
	server := newServer(t)
	defer server.Close()

	client := newClient(t, server.LocalAddr().String())
	client.SetGlobalNamespace("flubber.")
	defer client.Close()

	for i, tt := range eventTests {
		if err := tt.logEvent(client); err != nil {
			t.Fatal(err)
		}
		if message := serverRead(t, server); message != tt.expected {
			t.Errorf("\n[%d] Expected:\t%s\nActual:\t\t%s", i, tt.expected, message)
		}
	}

	b := bytes.NewBuffer(nil)
	b.Write(bytes.Repeat([]byte("a"), maxEventBytes+1))
	if err := client.Error("too long", b.String(), []string{}); err == nil {
		t.Fatal("Expected error due to exceeded event byte length")
	} else if expect, got := err.Error(), "Event \"flubber.too long\" payload is too big (more that 8KB), event discarded"; expect != got {
		t.Errorf("Unexpected error message.\nExpect:\t%s\nGot:\t%s", expect, got)
	}
}

func serverRead(t *testing.T, server *net.UDPConn) string {
	buf := make([]byte, 1024)
	n, err := server.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	return string(buf[:n])
}

func newClient(t *testing.T, addr string) *Client {
	client, err := New(addr)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newServer(t *testing.T) *net.UDPConn {
	udpAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	return server
}
