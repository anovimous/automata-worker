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

// fakeCloser hands out a channel the test controls, like amqp's NotifyClose.
type fakeCloser struct {
	ch chan *amqp.Error
}

func (f *fakeCloser) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	f.ch = receiver
	return receiver
}

func TestNotifyClosedMergesTheConnectionAndTheChannel(t *testing.T) {
	conn, channel := &fakeCloser{}, &fakeCloser{}
	c := &Consumer{closers: []closeNotifier{conn, channel}}

	closed := c.NotifyClosed()
	channel.ch <- &amqp.Error{Code: 406, Reason: "PRECONDITION_FAILED"}
	conn.ch <- &amqp.Error{Code: 320, Reason: "CONNECTION_FORCED"}
	close(channel.ch)
	close(conn.ch)

	var reasons []string
	for err := range closed {
		var amqpErr *amqp.Error
		if !errors.As(err, &amqpErr) {
			t.Fatalf("err = %v, want an *amqp.Error", err)
		}
		reasons = append(reasons, amqpErr.Reason)
	}
	if len(reasons) != 2 {
		t.Errorf("errors = %v, want one for the channel and one for the connection", reasons)
	}
}

func TestNotifyClosedEndsWithoutAnErrorOnAGracefulClose(t *testing.T) {
	conn, channel := &fakeCloser{}, &fakeCloser{}
	c := &Consumer{closers: []closeNotifier{conn, channel}}

	closed := c.NotifyClosed()
	close(conn.ch)
	close(channel.ch)

	for err := range closed {
		t.Errorf("got %v after a graceful close, want none", err)
	}
}

// fakeAcknowledger records the nack of a delivery.
type fakeAcknowledger struct {
	tag               uint64
	multiple, requeue bool
	nacked            int
	err               error
}

func (f *fakeAcknowledger) Ack(tag uint64, multiple bool) error { return nil }

func (f *fakeAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	f.nacked++
	f.tag, f.multiple, f.requeue = tag, multiple, requeue
	return f.err
}

func (f *fakeAcknowledger) Reject(tag uint64, requeue bool) error { return nil }

func TestNackGivesTheMessageBack(t *testing.T) {
	for _, requeue := range []bool{true, false} {
		a := &fakeAcknowledger{}
		msg := amqp.Delivery{Acknowledger: a, DeliveryTag: 9}

		if err := (&Consumer{}).Nack(msg, requeue); err != nil {
			t.Fatalf("Nack returned an error: %v", err)
		}
		if a.nacked != 1 || a.tag != 9 || a.multiple || a.requeue != requeue {
			t.Errorf("nack = %+v, want this message only, requeue %v", a, requeue)
		}
	}
}

func TestNackWrapsTheError(t *testing.T) {
	failure := errors.New("channel closed")
	msg := amqp.Delivery{Acknowledger: &fakeAcknowledger{err: failure}}

	if err := (&Consumer{}).Nack(msg, true); !errors.Is(err, failure) {
		t.Errorf("err = %v, want the wrapped error", err)
	}
}
