package queue

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeGetter struct {
	msg        amqp.Delivery
	ok         bool
	err        error
	gotQueue   string
	gotAutoAck bool
}

func (f *fakeGetter) Get(queue string, autoAck bool) (amqp.Delivery, bool, error) {
	f.gotQueue, f.gotAutoAck = queue, autoAck
	return f.msg, f.ok, f.err
}

func TestNextPullsFromTheQueueWithoutAutoAck(t *testing.T) {
	g := &fakeGetter{msg: amqp.Delivery{Body: []byte(`{"jobId":3}`)}, ok: true}
	c := &Consumer{getter: g, queue: "jobs"}

	msg, ok, err := c.Next()

	if err != nil || !ok || string(msg.Body) != `{"jobId":3}` {
		t.Errorf("msg=%s ok=%v err=%v", msg.Body, ok, err)
	}
	if g.gotQueue != "jobs" || g.gotAutoAck {
		t.Errorf("queue=%q autoAck=%v, want the queue of the consumer and no auto ack", g.gotQueue, g.gotAutoAck)
	}
}

func TestNextReportsAnEmptyQueue(t *testing.T) {
	c := &Consumer{getter: &fakeGetter{ok: false}, queue: "jobs"}

	_, ok, err := c.Next()

	if err != nil || ok {
		t.Errorf("ok=%v err=%v, want an empty queue", ok, err)
	}
}

func TestNextWrapsTheError(t *testing.T) {
	failure := errors.New("connection closed")
	c := &Consumer{getter: &fakeGetter{err: failure}, queue: "jobs"}

	_, ok, err := c.Next()

	if ok || !errors.Is(err, failure) {
		t.Errorf("ok=%v err=%v, want the wrapped error", ok, err)
	}
}

func TestQueueIsDeclaredAsTheCoreDeclaresIt(t *testing.T) {
	if !queueDurable {
		t.Error("the queue of the core is durable")
	}

	args := queueArguments()

	// the type matters: the broker compares the argument with the core's, an int
	if priority, ok := args["x-max-priority"].(int32); !ok || priority != 2 {
		t.Errorf("x-max-priority = %#v, want int32(2)", args["x-max-priority"])
	}
	if len(args) != 1 {
		t.Errorf("arguments = %v, want only x-max-priority", args)
	}
}
