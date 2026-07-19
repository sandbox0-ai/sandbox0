package teamquota

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

const (
	edgeTrafficAdmissionContextKey  = "sandbox0.teamquota.edge-traffic-admission"
	deletionRetryAdmittedContextKey = "sandbox0.teamquota.deletion-retry-admitted"
	deletionRetryResponseMaxBytes   = 16 << 10
	deletionRetryTrafficUnavailable = "team deletion retry traffic unavailable"
)

// AdmitEdgeTraffic accounts both byte directions and conditionally acquires a
// live-connection lease for requests marked long-lived.
func (c *Controller) AdmitEdgeTraffic(trustForwardedProof bool) gin.HandlerFunc {
	return func(ginCtx *gin.Context) {
		cleanup, ok := c.AttachEdgeTraffic(ginCtx, trustForwardedProof)
		if !ok {
			return
		}
		defer cleanup()
		ginCtx.Next()
	}
}

// AttachEdgeTraffic applies edge traffic admission around handlers invoked
// directly, including Gin NoRoute fallbacks. The returned cleanup must remain
// active until request forwarding has completed.
func (c *Controller) AttachEdgeTraffic(
	ginCtx *gin.Context,
	trustForwardedProof bool,
) (func(), bool) {
	authCtx := gatewaymiddleware.GetAuthContext(ginCtx)
	if isSystemAdminQuotaRepairRequest(ginCtx, authCtx) {
		return func() {}, true
	}
	longLived := ginCtx.Request != nil &&
		proxy.LongLivedRequest(ginCtx.Request.Context())
	if authCtx == nil || authCtx.TeamID == "" {
		if longLived {
			c.abortUnavailable(ginCtx, "team quota requires an authenticated team", nil)
			return func() {}, false
		}
		return func() {}, true
	}

	chargeIngress := !shouldSkipForwardedKey(
		ginCtx,
		trustForwardedProof,
		coreteamquota.KeyNetworkIngressBytes,
	)
	chargeEgress := !shouldSkipForwardedKey(
		ginCtx,
		trustForwardedProof,
		coreteamquota.KeyNetworkEgressBytes,
	)
	if longLived {
		chargeConnection := !shouldSkipForwardedKey(
			ginCtx,
			trustForwardedProof,
			coreteamquota.KeyActiveConnectionCount,
		)
		if !chargeConnection {
			return c.attachNetworkTrafficKeys(
				ginCtx,
				authCtx.TeamID,
				chargeIngress,
				chargeEgress,
			)
		}
		// Install the lease context before the network context so lease loss
		// also cancels network accounting for every long-lived request.
		if chargeIngress && chargeEgress {
			return c.attachTrafficAdmission(ginCtx, authCtx.TeamID, true)
		}
		connectionCleanup, ok := c.attachTrafficAdmission(
			ginCtx,
			authCtx.TeamID,
			false,
		)
		if !ok {
			return func() {}, false
		}
		networkCleanup, ok := c.attachNetworkTrafficKeys(
			ginCtx,
			authCtx.TeamID,
			chargeIngress,
			chargeEgress,
		)
		if !ok {
			connectionCleanup()
			return func() {}, false
		}
		var once sync.Once
		return func() {
			once.Do(func() {
				networkCleanup()
				connectionCleanup()
			})
		}, true
	}
	return c.attachNetworkTrafficKeys(
		ginCtx,
		authCtx.TeamID,
		chargeIngress,
		chargeEgress,
	)
}

// AdmitLongLivedConnections acquires one regional live-connection lease for
// every authenticated route marked long-lived. A matching signed forwarding
// proof may skip this key when the regional gateway owns the lease.
func (c *Controller) AdmitLongLivedConnections(
	trustForwardedProof bool,
) gin.HandlerFunc {
	return func(ginCtx *gin.Context) {
		if ginCtx.Request == nil || !proxy.LongLivedRequest(ginCtx.Request.Context()) {
			ginCtx.Next()
			return
		}
		authCtx := gatewaymiddleware.GetAuthContext(ginCtx)
		if shouldSkipForwardedKey(
			ginCtx,
			trustForwardedProof,
			coreteamquota.KeyActiveConnectionCount,
		) {
			ginCtx.Next()
			return
		}
		if isSystemAdminQuotaRepairRequest(ginCtx, authCtx) {
			ginCtx.Next()
			return
		}
		if authCtx == nil || authCtx.TeamID == "" {
			c.abortUnavailable(ginCtx, "team quota requires an authenticated team", nil)
			return
		}
		cleanup, ok := c.attachTrafficAdmission(ginCtx, authCtx.TeamID, false)
		if !ok {
			return
		}
		defer cleanup()
		ginCtx.Next()
	}
}

// LimitNetworkTraffic accounts one external API boundary. Each direction is
// skipped independently only when a matching signed forwarding proof says an
// upstream trusted hop already owns that key.
func (c *Controller) LimitNetworkTraffic(
	trustForwardedProof bool,
) gin.HandlerFunc {
	return func(ginCtx *gin.Context) {
		authCtx := gatewaymiddleware.GetAuthContext(ginCtx)
		if isSystemAdminQuotaRepairRequest(ginCtx, authCtx) {
			ginCtx.Next()
			return
		}
		if authCtx == nil || authCtx.TeamID == "" {
			ginCtx.Next()
			return
		}
		cleanup, ok := c.attachNetworkTrafficKeys(
			ginCtx,
			authCtx.TeamID,
			!shouldSkipForwardedKey(
				ginCtx,
				trustForwardedProof,
				coreteamquota.KeyNetworkIngressBytes,
			),
			!shouldSkipForwardedKey(
				ginCtx,
				trustForwardedProof,
				coreteamquota.KeyNetworkEgressBytes,
			),
		)
		if !ok {
			return
		}
		defer cleanup()
		ginCtx.Next()
	}
}

// AdmitPublicExposure acquires one live slot and wraps both byte directions
// before charging the team aggregate invocation rate. This keeps rate and
// route rejections inside both the connection and network-accounting boundary.
func (c *Controller) AdmitPublicExposure(
	ginCtx *gin.Context,
	teamID string,
) (func(), bool) {
	cleanup, ok := c.attachTrafficAdmission(ginCtx, teamID, true)
	if !ok {
		return func() {}, false
	}
	if err := c.TakeRate(
		ginCtx.Request.Context(),
		teamID,
		coreteamquota.KeySandboxServiceRequests,
		1,
	); err != nil {
		if coreteamquota.IsRateExceeded(err) {
			c.abortRateExceeded(ginCtx, err, "team sandbox_service_requests quota exceeded")
		} else {
			c.abortUnavailable(ginCtx, "sandbox service Team Quota unavailable", err)
		}
		cleanup()
		return func() {}, false
	}
	return cleanup, true
}

func (c *Controller) attachTrafficAdmission(
	ginCtx *gin.Context,
	teamID string,
	withNetwork bool,
) (func(), bool) {
	if c == nil || c.concurrencyLimiter == nil {
		c.abortUnavailable(ginCtx, "team quota concurrency limiter unavailable", nil)
		return func() {}, false
	}
	lease, err := c.concurrencyLimiter.Acquire(
		ginCtx.Request.Context(),
		teamID,
		coreteamquota.KeyActiveConnectionCount,
	)
	if err != nil {
		c.abortConcurrencyError(ginCtx, err)
		return func() {}, false
	}

	originalRequest := ginCtx.Request
	leaseCtx, cancelLeaseContext := context.WithCancelCause(originalRequest.Context())
	ginCtx.Request = originalRequest.WithContext(leaseCtx)
	originalWriter := ginCtx.Writer
	ginCtx.Writer = &connectionLeaseResponseWriter{
		ResponseWriter: originalWriter,
		ctx:            leaseCtx,
	}
	watchStop := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-lease.Done():
			cancelLeaseContext(lease.Err())
		case <-watchStop:
		}
	}()
	releaseAdmission := func() {
		ginCtx.Writer = originalWriter
		close(watchStop)
		<-watchDone
		cancelLeaseContext(nil)
		releaseLease(lease)
	}

	networkCleanup := func() {}
	if withNetwork {
		var ok bool
		networkCleanup, ok = c.attachNetworkTraffic(ginCtx, teamID)
		if !ok {
			releaseAdmission()
			return func() {}, false
		}
	}
	if err := RecordAdmittedKeys(
		ginCtx,
		coreteamquota.KeyActiveConnectionCount,
	); err != nil {
		func() {
			defer releaseAdmission()
			c.abortUnavailable(ginCtx, "team quota admission proof unavailable", err)
			networkCleanup()
		}()
		return func() {}, false
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			defer releaseAdmission()
			networkCleanup()
		})
	}, true
}

func (c *Controller) attachNetworkTraffic(
	ginCtx *gin.Context,
	teamID string,
) (func(), bool) {
	return c.attachNetworkTrafficKeys(ginCtx, teamID, true, true)
}

func (c *Controller) attachNetworkTrafficKeys(
	ginCtx *gin.Context,
	teamID string,
	chargeIngress bool,
	chargeEgress bool,
) (func(), bool) {
	if !chargeIngress && !chargeEgress {
		return func() {}, true
	}
	if c == nil || c.networkLimiter == nil {
		c.abortUnavailable(ginCtx, "team quota network limiter unavailable", nil)
		return func() {}, false
	}
	originalRequest := ginCtx.Request
	trafficCtx, cancelTraffic := context.WithCancelCause(originalRequest.Context())
	ginCtx.Request = originalRequest.WithContext(trafficCtx)
	var requestBody *quotaRequestBody
	if chargeIngress && originalRequest.Body != nil {
		requestBody = &quotaRequestBody{
			reader: &quotaDirectionReader{
				reader:     originalRequest.Body,
				ctx:        trafficCtx,
				cancel:     cancelTraffic,
				controller: c,
				teamID:     teamID,
				key:        coreteamquota.KeyNetworkIngressBytes,
			},
			closer: originalRequest.Body,
		}
		ginCtx.Request.Body = requestBody
	}
	originalWriter := ginCtx.Writer
	trafficWriter := &quotaResponseWriter{
		ResponseWriter:  originalWriter,
		ctx:             trafficCtx,
		cancel:          cancelTraffic,
		controller:      c,
		teamID:          teamID,
		chargeIngress:   chargeIngress,
		chargeEgress:    chargeEgress,
		ownTeamDeletion: isOwnTeamDeletionRequest(ginCtx, teamID),
	}
	ginCtx.Writer = trafficWriter
	ginCtx.Set(edgeTrafficAdmissionContextKey, &edgeTrafficAdmission{
		writer: trafficWriter,
	})
	keys := make([]coreteamquota.Key, 0, 2)
	if chargeIngress {
		keys = append(keys, coreteamquota.KeyNetworkIngressBytes)
	}
	if chargeEgress {
		keys = append(keys, coreteamquota.KeyNetworkEgressBytes)
	}
	if err := RecordAdmittedKeys(ginCtx, keys...); err != nil {
		ginCtx.Writer = originalWriter
		ginCtx.Request = originalRequest
		cancelTraffic(err)
		c.abortUnavailable(ginCtx, "team quota admission proof unavailable", err)
		return func() {}, false
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			var bodyErr error
			if requestBody != nil {
				bodyErr = requestBody.drainAndClose()
				if bodyErr != nil {
					cancelTraffic(bodyErr)
				}
			}
			if trafficWriter.deletionRetryEnabled() {
				retryErr := errors.Join(bodyErr, trafficWriter.deletionRetryError())
				if retryErr != nil {
					trafficWriter.resetDeletionRetryResponse()
					c.abortUnavailable(ginCtx, deletionRetryTrafficUnavailable, retryErr)
				}
				if flushErr := trafficWriter.flushDeletionRetryResponse(); flushErr != nil {
					cancelTraffic(flushErr)
				}
			}
			ginCtx.Writer = originalWriter
			cancelTraffic(nil)
		})
	}, true
}

func (c *Controller) abortConcurrencyError(ginCtx *gin.Context, err error) {
	if coreteamquota.IsConcurrencyExceeded(err) {
		var exceeded *coreteamquota.ConcurrencyExceededError
		if !errors.As(err, &exceeded) {
			c.abortUnavailable(ginCtx, "team quota concurrency decision unavailable", err)
			return
		}
		ginCtx.Header("Retry-After", "1")
		spec.JSONError(
			ginCtx,
			http.StatusTooManyRequests,
			spec.CodeQuotaExceeded,
			"team active_connection_count quota exceeded",
			gin.H{
				"limit": exceeded.Limit,
				"used":  exceeded.Used,
			},
		)
		ginCtx.Abort()
		return
	}
	c.abortUnavailable(ginCtx, "team quota concurrency limiter unavailable", err)
}

func releaseLease(lease ConnectionLease) {
	if lease == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = lease.Release(ctx)
}

type edgeTrafficAdmission struct {
	writer *quotaResponseWriter
}

// enableDeletionRetryEgress opens the bounded response lane installed by edge
// traffic admission. A missing or malformed lane fails closed.
func enableDeletionRetryEgress(ginCtx *gin.Context) error {
	if ginCtx == nil {
		return fmt.Errorf("team deletion retry traffic context is required")
	}
	value, ok := ginCtx.Get(edgeTrafficAdmissionContextKey)
	if !ok {
		return fmt.Errorf("team deletion retry traffic admission is not attached")
	}
	admission, ok := value.(*edgeTrafficAdmission)
	if !ok || admission == nil || admission.writer == nil {
		return fmt.Errorf("team deletion retry traffic context is invalid")
	}
	return admission.writer.enableDeletionRetry()
}

// quotaRequestBody preserves handler-visible Close semantics while deferring
// the physical close until cleanup can account every unread ingress byte.
type quotaRequestBody struct {
	mu sync.Mutex

	reader          io.Reader
	closer          io.Closer
	closedByHandler bool
	finalized       bool
	readErr         error
	finalizationErr error
}

func (b *quotaRequestBody) Read(payload []byte) (int, error) {
	if b == nil {
		return 0, http.ErrBodyReadAfterClose
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closedByHandler || b.finalized {
		return 0, http.ErrBodyReadAfterClose
	}
	n, err := b.reader.Read(payload)
	if err != nil && !errors.Is(err, io.EOF) {
		b.readErr = errors.Join(b.readErr, err)
	}
	return n, err
}

func (b *quotaRequestBody) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closedByHandler = true
	return nil
}

func (b *quotaRequestBody) drainAndClose() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.finalized {
		return b.finalizationErr
	}
	b.finalized = true
	_, drainErr := io.Copy(io.Discard, b.reader)
	closeErr := b.closer.Close()
	b.finalizationErr = errors.Join(b.readErr, drainErr, closeErr)
	return b.finalizationErr
}

type quotaResponseWriter struct {
	gin.ResponseWriter
	ctx             context.Context
	cancel          context.CancelCauseFunc
	controller      *Controller
	teamID          string
	chargeIngress   bool
	chargeEgress    bool
	ownTeamDeletion bool

	mu                   sync.Mutex
	deletionRetry        bool
	deletionRetryWritten bool
	deletionRetryStatus  int
	deletionRetryBody    bytes.Buffer
	deletionRetryErr     error
	deletionRetryFlushed bool
}

// connectionLeaseResponseWriter binds every hijacked connection to the
// admission lease context. This is intentionally separate from network
// accounting: callers may install the two wrappers in either order.
type connectionLeaseResponseWriter struct {
	gin.ResponseWriter
	ctx context.Context
}

func (w *connectionLeaseResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, buffered, err := w.ResponseWriter.Hijack()
	if err != nil {
		return nil, nil, err
	}
	return newContextBoundConn(w.ctx, conn), buffered, nil
}

func (w *connectionLeaseResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// contextBoundConn closes the underlying connection when its context is
// canceled and unregisters that callback when the connection closes first.
type contextBoundConn struct {
	net.Conn

	closeOnce sync.Once
	closeErr  error
	stopClose func() bool
}

func newContextBoundConn(ctx context.Context, conn net.Conn) *contextBoundConn {
	bound := &contextBoundConn{Conn: conn}
	bound.stopClose = context.AfterFunc(ctx, func() {
		bound.close(false)
	})
	return bound
}

func (c *contextBoundConn) Close() error {
	return c.close(true)
}

func (c *contextBoundConn) close(stopCallback bool) error {
	c.closeOnce.Do(func() {
		if stopCallback {
			c.stopClose()
		}
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}

func (w *quotaResponseWriter) Write(payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.deletionRetry {
		return w.writeDeletionRetryLocked(payload)
	}
	if w.chargeEgress {
		if err := w.wait(len(payload)); err != nil {
			if transitionErr := w.enableDeletionTransitionLocked(err); transitionErr == nil {
				return w.writeDeletionRetryLocked(payload)
			} else {
				err = transitionErr
			}
			w.cancel(err)
			return 0, err
		}
	}
	return w.ResponseWriter.Write(payload)
}

func (w *quotaResponseWriter) WriteString(payload string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.deletionRetry {
		return w.writeDeletionRetryLocked([]byte(payload))
	}
	if w.chargeEgress {
		if err := w.wait(len(payload)); err != nil {
			if transitionErr := w.enableDeletionTransitionLocked(err); transitionErr == nil {
				return w.writeDeletionRetryLocked([]byte(payload))
			} else {
				err = transitionErr
			}
			w.cancel(err)
			return 0, err
		}
	}
	return w.ResponseWriter.WriteString(payload)
}

func (w *quotaResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deletionRetry {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if status > 0 && !w.deletionRetryWritten {
		w.deletionRetryStatus = status
	}
}

func (w *quotaResponseWriter) WriteHeaderNow() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deletionRetry {
		w.ResponseWriter.WriteHeaderNow()
		return
	}
	w.markDeletionRetryWrittenLocked()
}

func (w *quotaResponseWriter) Written() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deletionRetry {
		return w.ResponseWriter.Written()
	}
	return w.deletionRetryWritten
}

func (w *quotaResponseWriter) Status() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deletionRetry {
		return w.ResponseWriter.Status()
	}
	if w.deletionRetryStatus == 0 {
		return http.StatusOK
	}
	return w.deletionRetryStatus
}

func (w *quotaResponseWriter) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deletionRetry {
		return w.ResponseWriter.Size()
	}
	if !w.deletionRetryWritten {
		return -1
	}
	return w.deletionRetryBody.Len()
}

func (w *quotaResponseWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deletionRetry {
		w.ResponseWriter.Flush()
		return
	}
	w.markDeletionRetryWrittenLocked()
}

func (w *quotaResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.mu.Lock()
	if w.deletionRetry {
		err := fmt.Errorf("team deletion retry does not support connection hijacking")
		w.deletionRetryErr = errors.Join(w.deletionRetryErr, err)
		w.mu.Unlock()
		w.cancel(err)
		return nil, nil, err
	}
	w.mu.Unlock()
	conn, buffered, err := w.ResponseWriter.Hijack()
	if err != nil {
		return nil, nil, err
	}
	limited := &quotaConn{
		Conn:          conn,
		ctx:           w.ctx,
		cancel:        w.cancel,
		controller:    w.controller,
		teamID:        w.teamID,
		chargeIngress: w.chargeIngress,
		chargeEgress:  w.chargeEgress,
	}
	go limited.closeWhenCanceled()
	reader := buffered.Reader
	if w.chargeIngress {
		reader = bufio.NewReader(&quotaDirectionReader{
			reader:     buffered.Reader,
			ctx:        w.ctx,
			cancel:     w.cancel,
			controller: w.controller,
			teamID:     w.teamID,
			key:        coreteamquota.KeyNetworkIngressBytes,
		})
	}
	writer := buffered.Writer
	if w.chargeEgress {
		writer = bufio.NewWriter(&flushThroughQuotaWriter{
			writer: &quotaDirectionWriter{
				writer:     buffered.Writer,
				ctx:        w.ctx,
				cancel:     w.cancel,
				controller: w.controller,
				teamID:     w.teamID,
				key:        coreteamquota.KeyNetworkEgressBytes,
			},
			flush: buffered.Flush,
		})
	}
	return limited, bufio.NewReadWriter(reader, writer), nil
}

func (w *quotaResponseWriter) Pusher() http.Pusher {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.deletionRetry {
		return nil
	}
	return w.ResponseWriter.Pusher()
}

func (w *quotaResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *quotaResponseWriter) wait(bytes int) error {
	if bytes <= 0 {
		return nil
	}
	return w.controller.networkLimiter.WaitN(
		w.ctx,
		w.teamID,
		coreteamquota.KeyNetworkEgressBytes,
		bytes,
	)
}

func (w *quotaResponseWriter) enableDeletionRetry() error {
	if w == nil {
		return fmt.Errorf("team deletion retry response writer is required")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enableDeletionRetryLocked()
}

func (w *quotaResponseWriter) enableDeletionRetryLocked() error {
	if w.deletionRetry {
		return nil
	}
	if w.ResponseWriter.Written() {
		return fmt.Errorf("team deletion retry response was already committed")
	}
	w.deletionRetry = true
	w.deletionRetryStatus = w.ResponseWriter.Status()
	return nil
}

// enableDeletionTransitionLocked opens the bounded escape lane when this
// request disabled its own Team admission before writing the response.
func (w *quotaResponseWriter) enableDeletionTransitionLocked(admissionErr error) error {
	if !w.ownTeamDeletion || !coreteamquota.IsTeamAdmissionDisabled(admissionErr) {
		return admissionErr
	}
	if err := w.enableDeletionRetryLocked(); err != nil {
		return errors.Join(admissionErr, err)
	}
	return nil
}

func (w *quotaResponseWriter) deletionRetryEnabled() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deletionRetry
}

func (w *quotaResponseWriter) deletionRetryError() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deletionRetryErr
}

func (w *quotaResponseWriter) resetDeletionRetryResponse() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deletionRetry || w.deletionRetryFlushed {
		return
	}
	clear(w.Header())
	w.deletionRetryWritten = false
	w.deletionRetryStatus = http.StatusOK
	w.deletionRetryBody.Reset()
	w.deletionRetryErr = nil
}

func (w *quotaResponseWriter) flushDeletionRetryResponse() error {
	if w == nil {
		return fmt.Errorf("team deletion retry response writer is required")
	}
	w.mu.Lock()
	if !w.deletionRetry || w.deletionRetryFlushed {
		w.mu.Unlock()
		return nil
	}
	if w.deletionRetryErr != nil {
		err := w.deletionRetryErr
		w.mu.Unlock()
		return err
	}
	status := w.deletionRetryStatus
	if status == 0 {
		status = http.StatusOK
	}
	payload := bytes.Clone(w.deletionRetryBody.Bytes())
	w.deletionRetryFlushed = true
	w.mu.Unlock()

	w.ResponseWriter.WriteHeader(status)
	if len(payload) == 0 {
		w.ResponseWriter.WriteHeaderNow()
		return nil
	}
	n, err := w.ResponseWriter.Write(payload)
	if err != nil {
		return err
	}
	if n != len(payload) {
		return io.ErrShortWrite
	}
	return nil
}

func (w *quotaResponseWriter) writeDeletionRetryLocked(payload []byte) (int, error) {
	if w.deletionRetryErr != nil {
		return 0, w.deletionRetryErr
	}
	if w.deletionRetryFlushed {
		err := fmt.Errorf("team deletion retry response is already finalized")
		w.deletionRetryErr = err
		w.cancel(err)
		return 0, err
	}
	if w.deletionRetryBody.Len()+len(payload) > deletionRetryResponseMaxBytes {
		err := fmt.Errorf(
			"team deletion retry response exceeds %d bytes",
			deletionRetryResponseMaxBytes,
		)
		w.deletionRetryErr = err
		w.cancel(err)
		return 0, err
	}
	w.markDeletionRetryWrittenLocked()
	return w.deletionRetryBody.Write(payload)
}

func (w *quotaResponseWriter) markDeletionRetryWrittenLocked() {
	if w.deletionRetryStatus == 0 {
		w.deletionRetryStatus = http.StatusOK
	}
	w.deletionRetryWritten = true
}

type quotaConn struct {
	net.Conn
	ctx           context.Context
	cancel        context.CancelCauseFunc
	controller    *Controller
	teamID        string
	chargeIngress bool
	chargeEgress  bool
}

func (c *quotaConn) Read(payload []byte) (int, error) {
	n, err := c.Conn.Read(payload)
	if n > 0 && c.chargeIngress {
		if quotaErr := c.controller.networkLimiter.WaitN(
			c.ctx,
			c.teamID,
			coreteamquota.KeyNetworkIngressBytes,
			n,
		); quotaErr != nil {
			c.cancel(quotaErr)
			_ = c.Close()
			return 0, quotaErr
		}
	}
	return n, err
}

func (c *quotaConn) Write(payload []byte) (int, error) {
	if c.chargeEgress {
		if err := c.controller.networkLimiter.WaitN(
			c.ctx,
			c.teamID,
			coreteamquota.KeyNetworkEgressBytes,
			len(payload),
		); err != nil {
			c.cancel(err)
			_ = c.Close()
			return 0, err
		}
	}
	return c.Conn.Write(payload)
}

func (c *quotaConn) closeWhenCanceled() {
	<-c.ctx.Done()
	_ = c.Close()
}

type quotaDirectionReader struct {
	reader     io.Reader
	ctx        context.Context
	cancel     context.CancelCauseFunc
	controller *Controller
	teamID     string
	key        coreteamquota.Key
}

func (r *quotaDirectionReader) Read(payload []byte) (int, error) {
	n, err := r.reader.Read(payload)
	if n > 0 {
		if quotaErr := r.controller.networkLimiter.WaitN(r.ctx, r.teamID, r.key, n); quotaErr != nil {
			r.cancel(quotaErr)
			return 0, quotaErr
		}
	}
	return n, err
}

type quotaDirectionWriter struct {
	writer     io.Writer
	ctx        context.Context
	cancel     context.CancelCauseFunc
	controller *Controller
	teamID     string
	key        coreteamquota.Key
}

func (w *quotaDirectionWriter) Write(payload []byte) (int, error) {
	if err := w.controller.networkLimiter.WaitN(w.ctx, w.teamID, w.key, len(payload)); err != nil {
		w.cancel(err)
		return 0, err
	}
	return w.writer.Write(payload)
}

type flushThroughQuotaWriter struct {
	writer io.Writer
	flush  func() error
}

func (w *flushThroughQuotaWriter) Write(payload []byte) (int, error) {
	n, err := w.writer.Write(payload)
	if err != nil {
		return n, err
	}
	if err := w.flush(); err != nil {
		return n, err
	}
	return n, nil
}

var _ gin.ResponseWriter = (*quotaResponseWriter)(nil)
var _ net.Conn = (*quotaConn)(nil)
