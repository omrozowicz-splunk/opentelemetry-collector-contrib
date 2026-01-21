// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package recombine // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/transformer/recombine"

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
)

const DefaultSourceIdentifier = "DefaultSourceIdentifier"

// Transformer is an operator that combines a field from consecutive log entries into a single
type Transformer struct {
	helper.TransformerOperator
	matchFirstLine        bool
	prog                  *vm.Program
	maxBatchSize          int
	maxUnmatchedBatchSize int
	maxSources            int
	overwriteWithNewest   bool
	combineField          entry.Field
	combineWith           string
	ticker                *time.Ticker
	forceFlushTimeout     time.Duration
	chClose               chan struct{}
	sourceIdentifier      entry.Field

	sync.Mutex
	batchPool  sync.Pool
	batchMap   map[string]*sourceBatch
	maxLogSize int64
}

// sourceBatch contains the status info of a batch
type sourceBatch struct {
	baseEntry              *entry.Entry
	numEntries             int
	recombined             *bytes.Buffer
	firstEntryObservedTime time.Time
	matchDetected          bool
}

func (t *Transformer) Start(_ operator.Persister) error {
	t.Logger().Info("Starting recombine transformer",
		zap.Bool("match_first_line", t.matchFirstLine),
		zap.Int("max_batch_size", t.maxBatchSize),
		zap.Int("max_unmatched_batch_size", t.maxUnmatchedBatchSize),
		zap.Int("max_sources", t.maxSources),
		zap.Int64("max_log_size", t.maxLogSize),
		zap.Duration("force_flush_timeout", t.forceFlushTimeout),
		zap.String("force_flush_timeout_human", t.forceFlushTimeout.String()),
		zap.Float64("force_flush_timeout_seconds", t.forceFlushTimeout.Seconds()),
		zap.Bool("overwrite_with_newest", t.overwriteWithNewest),
		zap.String("combine_field", t.combineField.String()),
		zap.String("combine_with", t.combineWith),
		zap.String("source_identifier", t.sourceIdentifier.String()))
	go t.flushLoop()
	return nil
}

func (t *Transformer) flushLoop() {
	t.Logger().Debug("Starting flush loop",
		zap.String("force_flush_timeout", t.forceFlushTimeout.String()),
		zap.String("check_interval", (t.forceFlushTimeout/5).String()))

	for {
		select {
		case <-t.ticker.C:
			t.Lock()
			timeNow := time.Now()

			if len(t.batchMap) > 0 {
				t.Logger().Debug("Flush loop tick - checking batches",
					zap.Int("num_batches", len(t.batchMap)),
					zap.String("force_flush_timeout", t.forceFlushTimeout.String()))
			}

			for source, batch := range t.batchMap {
				timeSinceFirstEntry := timeNow.Sub(batch.firstEntryObservedTime)

				t.Logger().Debug("Checking batch for timeout flush",
					zap.String("source", source),
					zap.String("batch_age", timeSinceFirstEntry.String()),
					zap.Float64("batch_age_seconds", timeSinceFirstEntry.Seconds()),
					zap.String("force_flush_timeout", t.forceFlushTimeout.String()),
					zap.Float64("force_flush_timeout_seconds", t.forceFlushTimeout.Seconds()),
					zap.Int("num_entries", batch.numEntries),
					zap.Bool("will_flush", timeSinceFirstEntry >= t.forceFlushTimeout))

				if timeSinceFirstEntry < t.forceFlushTimeout {
					continue
				}

				t.Logger().Debug("Timeout reached, flushing batch",
					zap.String("source", source),
					zap.String("batch_age", timeSinceFirstEntry.String()),
					zap.Float64("batch_age_seconds", timeSinceFirstEntry.Seconds()),
					zap.String("configured_timeout", t.forceFlushTimeout.String()),
					zap.Float64("configured_timeout_seconds", t.forceFlushTimeout.Seconds()),
					zap.Int("num_entries_in_batch", batch.numEntries),
					zap.Int("combined_buffer_size", batch.recombined.Len()),
					zap.Bool("match_was_detected", batch.matchDetected),
					zap.String("partial_content", truncateString(batch.recombined.String(), 150)))

				if err := t.flushSource(context.Background(), source); err != nil {
					t.Logger().Error("there was error flushing combined logs", zap.Error(err))
				}
			}
			// check every 1/5 forceFlushTimeout
			t.ticker.Reset(t.forceFlushTimeout / 5)
			t.Unlock()
		case <-t.chClose:
			t.Logger().Debug("Flush loop stopping")
			t.ticker.Stop()
			return
		}
	}
}

func (t *Transformer) Stop() error {
	t.Logger().Info("Stopping recombine transformer",
		zap.Int("pending_batches", len(t.batchMap)))

	t.Lock()
	defer t.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t.flushAllSources(ctx)

	close(t.chClose)
	t.Logger().Debug("Recombine transformer stopped")
	return nil
}

func (t *Transformer) ProcessBatch(ctx context.Context, entries []*entry.Entry) error {
	return t.ProcessBatchWith(ctx, entries, t.Process)
}

func (t *Transformer) Process(ctx context.Context, e *entry.Entry) error {
	// Lock the recombine operator because process can't run concurrently
	t.Lock()
	defer t.Unlock()

	t.Logger().Debug("Processing entry for recombine",
		zap.Time("entry_timestamp", e.Timestamp),
		zap.Time("entry_observed", e.ObservedTimestamp),
		zap.Any("entry_body", e.Body))

	// Get the environment for executing the expression.
	// In the future, we may want to provide access to the currently
	// batched entries so users can do comparisons to other entries
	// rather than just use absolute rules.
	env := helper.GetExprEnv(e)
	defer helper.PutExprEnv(env)

	m, err := expr.Run(t.prog, env)
	if err != nil {
		t.Logger().Error("Failed to evaluate expression", zap.Error(err), zap.Any("entry_body", e.Body))
		return t.HandleEntryError(ctx, e, err)
	}

	// this is guaranteed to be a boolean because of expr.AsBool
	matches := m.(bool)
	var s string
	err = e.Read(t.sourceIdentifier, &s)
	if err != nil {
		t.Logger().Warn("entry does not contain the source_identifier, so it may be pooled with other sources",
			zap.String("source_identifier_field", t.sourceIdentifier.String()),
			zap.Error(err))
		s = DefaultSourceIdentifier
	}

	if s == "" {
		t.Logger().Debug("Source identifier is empty, using default",
			zap.String("default_source", DefaultSourceIdentifier))
		s = DefaultSourceIdentifier
	}

	t.Logger().Debug("Expression evaluation result",
		zap.Bool("matches", matches),
		zap.Bool("is_match_first_line", t.matchFirstLine),
		zap.String("source", s),
		zap.Int("current_active_sources", len(t.batchMap)),
		zap.Any("entry_body", e.Body))

	switch {
	// This is the first entry in the next batch
	case matches && t.matchFirstLine:
		t.Logger().Debug("Detected first entry of new batch, flushing existing batch",
			zap.String("source", s),
			zap.Any("entry_body", e.Body))
		// Flush the existing batch
		if err := t.flushSource(ctx, s); err != nil {
			return err
		}

		// Add the current log to the new batch
		t.addToBatch(ctx, e, s, matches)
		return nil
	// This is the last entry in a complete batch
	case matches && !t.matchFirstLine:
		t.Logger().Debug("Detected last entry of batch, adding and flushing",
			zap.String("source", s),
			zap.Any("entry_body", e.Body))
		t.addToBatch(ctx, e, s, matches)
		return t.flushSource(ctx, s)
	}

	// This is neither the first entry of a new log,
	// nor the last entry of a log, so just add it to the batch
	t.Logger().Debug("Adding entry to existing batch (no match or middle entry)",
		zap.String("source", s),
		zap.Bool("matches", matches),
		zap.Any("entry_body", e.Body))
	t.addToBatch(ctx, e, s, matches)
	return nil
}

// addToBatch adds the current entry to the current batch of entries that will be combined
func (t *Transformer) addToBatch(ctx context.Context, e *entry.Entry, source string, matches bool) {
	batch, ok := t.batchMap[source]
	if !ok {
		t.Logger().Debug("Creating new batch for source",
			zap.String("source", source),
			zap.Int("existing_sources", len(t.batchMap)),
			zap.Int("max_sources", t.maxSources))
		if len(t.batchMap) >= t.maxSources {
			t.Logger().Error("Too many sources. Flushing all batched logs. Consider increasing max_sources parameter",
				zap.Int("current_sources", len(t.batchMap)),
				zap.Int("max_sources", t.maxSources))
			t.flushAllSources(ctx)
		}
		batch = t.addNewBatch(source, e)
	} else {
		t.Logger().Debug("Adding to existing batch",
			zap.String("source", source),
			zap.Int("current_num_entries", batch.numEntries),
			zap.Int("current_buffer_size", batch.recombined.Len()))
		batch.numEntries++
		if t.overwriteWithNewest {
			t.Logger().Debug("Overwriting base entry with newest",
				zap.String("source", source))
			batch.baseEntry = e
		}
	}

	// mark that match occurred to use max_unmatched_batch_size only when match didn't occur
	if matches && !batch.matchDetected {
		t.Logger().Debug("First match detected for batch",
			zap.String("source", source),
			zap.Int("num_entries", batch.numEntries))
		batch.matchDetected = true
	}

	// Combine the combineField of each entry in the batch,
	// separated by newlines
	var s string
	err := e.Read(t.combineField, &s)
	if err != nil {
		t.Logger().Error("entry does not contain the combine_field",
			zap.String("combine_field", t.combineField.String()),
			zap.String("source", source),
			zap.Error(err))
		return
	}
	if batch.recombined.Len() > 0 {
		batch.recombined.WriteString(t.combineWith)
	}
	batch.recombined.WriteString(s)

	t.Logger().Debug("Batch state after adding entry",
		zap.String("source", source),
		zap.Int("num_entries", batch.numEntries),
		zap.Int("buffer_size", batch.recombined.Len()),
		zap.Bool("match_detected", batch.matchDetected),
		zap.Int64("max_log_size", t.maxLogSize),
		zap.Int("max_batch_size", t.maxBatchSize),
		zap.Int("max_unmatched_batch_size", t.maxUnmatchedBatchSize))

	if (t.maxLogSize > 0 && int64(batch.recombined.Len()) > t.maxLogSize) ||
		batch.numEntries >= t.maxBatchSize ||
		(!batch.matchDetected && t.maxUnmatchedBatchSize > 0 && batch.numEntries >= t.maxUnmatchedBatchSize) {
		t.Logger().Debug("Size limit reached, triggering flush",
			zap.String("source", source),
			zap.Int("current_buffer_size", batch.recombined.Len()),
			zap.Int("current_num_entries", batch.numEntries))
		if err := t.flushSource(ctx, source); err != nil {
			t.Logger().Error("there was error flushing combined logs", zap.Error(err))
		}
	}
}

// truncateString truncates a string to maxLen characters for logging
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// flushAllSources flushes all sources.
func (t *Transformer) flushAllSources(ctx context.Context) {
	t.Logger().Debug("Flushing all sources",
		zap.Int("num_sources", len(t.batchMap)))
	var errs error
	for source := range t.batchMap {
		errs = multierr.Append(errs, t.flushSource(ctx, source))
	}
	if errs != nil {
		t.Logger().Error("there was error flushing combined logs %s", zap.Error(errs))
	}
}

// flushSource combines the entries currently in the batch into a single entry,
// then forwards them to the next operator in the pipeline
func (t *Transformer) flushSource(ctx context.Context, source string) error {
	batch := t.batchMap[source]
	// Skip flushing a combined log if the batch is empty
	if batch == nil {
		t.Logger().Debug("Attempted to flush empty batch",
			zap.String("source", source))
		return nil
	}

	batchAge := time.Since(batch.firstEntryObservedTime)
	t.Logger().Debug("Flushing batch for source",
		zap.String("source", source),
		zap.Int("num_entries", batch.numEntries),
		zap.Int("combined_size", batch.recombined.Len()),
		zap.String("batch_age", batchAge.String()),
		zap.Float64("batch_age_seconds", batchAge.Seconds()),
		zap.Bool("match_detected", batch.matchDetected))

	if batch.baseEntry == nil {
		t.Logger().Warn("Batch has no base entry, removing without flushing",
			zap.String("source", source),
			zap.Int("num_entries", batch.numEntries))
		t.removeBatch(source)
		return nil
	}

	// Set the recombined field on the entry
	err := batch.baseEntry.Set(t.combineField, batch.recombined.String())
	if err != nil {
		t.Logger().Error("Failed to set combined field on entry",
			zap.String("source", source),
			zap.String("combine_field", t.combineField.String()),
			zap.Error(err))
		return err
	}

	t.Logger().Debug("Writing combined entry",
		zap.String("source", source),
		zap.Int("num_entries_combined", batch.numEntries),
		zap.String("combined_content_preview", truncateString(batch.recombined.String(), 200)),
		zap.Time("base_entry_timestamp", batch.baseEntry.Timestamp))

	err = t.Write(ctx, batch.baseEntry)
	t.removeBatch(source)

	if err != nil {
		t.Logger().Error("Failed to write combined entry",
			zap.String("source", source),
			zap.Error(err))
	} else {
		t.Logger().Debug("Successfully flushed and wrote combined entry",
			zap.String("source", source))
	}

	return err
}

// addNewBatch creates a new batch for the given source and adds the entry to it.
func (t *Transformer) addNewBatch(source string, e *entry.Entry) *sourceBatch {
	batch := t.batchPool.Get().(*sourceBatch)
	batch.baseEntry = e
	batch.numEntries = 1
	batch.recombined.Reset()
	batch.firstEntryObservedTime = e.ObservedTimestamp
	batch.matchDetected = false
	t.batchMap[source] = batch

	t.Logger().Debug("Created new batch",
		zap.String("source", source),
		zap.Time("first_entry_time", e.ObservedTimestamp),
		zap.Int("total_active_batches", len(t.batchMap)))

	return batch
}

// removeBatch removes the batch for the given source.
func (t *Transformer) removeBatch(source string) {
	batch := t.batchMap[source]
	delete(t.batchMap, source)
	t.batchPool.Put(batch)

	t.Logger().Debug("Removed batch",
		zap.String("source", source),
		zap.Int("remaining_batches", len(t.batchMap)))
}
