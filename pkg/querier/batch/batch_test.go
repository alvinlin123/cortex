package batch

import (
	"fmt"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
)

func mkAlvinChunkOne(t require.TestingT) chunk.Chunk {
	metric := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}
	pc, err := promchunk.NewForEncoding(promchunk.PrometheusXorChunk)
	require.NoError(t, err)

	pc.Add(model.SamplePair{Timestamp: 1621634344998, Value: 40})

	return chunk.NewChunk(userID, fp, metric, pc, model.Time(1621634344998), model.Time(1621634344998))
}

func mkAlvinChunkTwo(t require.TestingT) chunk.Chunk {
	metric := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}
	pc, err := promchunk.NewForEncoding(promchunk.PrometheusXorChunk)
	require.NoError(t, err)

	pc.Add(model.SamplePair{Timestamp: 1621634657251, Value: 97})

	return chunk.NewChunk(userID, fp, metric, pc, model.Time(1621634657251), model.Time(1621634657251))
}

func TestChunk(t *testing.T) {

	db, err := tsdb.Open("/Users/alvinlin/Documents/gamm-dub-tsdb", nil, nil, &tsdb.Options{
		RetentionDuration: int64(time.Hour * 25 / time.Millisecond),
		NoLockfile:        true,
		MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),
		MaxBlockDuration:  int64(2 * time.Hour / time.Millisecond),
	})

	require.NoError(t, err)

	chunkQuerier, err := db.ChunkQuerier(nil, 1621634100000, 1621634700000)
	require.NoError(t, err)

	serieses := make([]storage.Series, 0, 10)

	chunkSeriesSetRaw := chunkQuerier.Select(false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "canary_long_sample1"))
	chunkSeriesSet := storage.NewSeriesSetFromChunkSeriesSet(chunkSeriesSetRaw)

	var clientChunks []client.Chunk
	for chunkSeriesSetRaw.Next() {
		chunkSeriesRaw := chunkSeriesSetRaw.At()
		rawIter := chunkSeriesRaw.Iterator()

		for rawIter.Next() {
			meta := rawIter.At()
			fmt.Println("MinTime is ", meta.MinTime)
			fmt.Println("MaxTime is ", meta.MaxTime)
			fmt.Println("Number samples is ", meta.Chunk.NumSamples())
			fmt.Println("Encoding is ", meta.Chunk.Encoding())

			chunkInMeta := meta.Chunk.Iterator(nil)
			clientChunk := client.Chunk{
				StartTimestampMs: meta.MinTime,
				EndTimestampMs:   meta.MaxTime,
				Data:             meta.Chunk.Bytes(),
				Encoding:         int32(encoding.PrometheusXorChunk),
			}
			clientChunks = append(clientChunks, clientChunk)
			for chunkInMeta.Next() {
				fmt.Println(chunkInMeta.At())
			}
		}
	}

	/*chunks := []chunk.Chunk{
		mkAlvinChunkOne(t),
		mkAlvinChunkTwo(t),
	}*/

	dbChunks, err := chunkcompat.FromChunks("blah", nil, clientChunks)
	require.NoError(t, err)

	for chunkSeriesSet.Next() {
		chunkSeries := chunkSeriesSet.At()
		_ = append(serieses, chunkSeries)
	}

	chunkSeries := &chunkSeries{
		labels:            nil,
		chunks:            dbChunks,
		chunkIteratorFunc: NewChunkMergeIterator,
		// chunkIteratorFunc: iterators.NewChunkMergeIterator,
		// chunkIteratorFunc: mergeChunks,
		mint: 1621627445432,
		maxt: 1621641600000,
	}

	serieses = append(serieses, chunkSeries)

	var sets []storage.SeriesSet
	sets = append(sets, series.NewConcreteSeriesSet(serieses))
	mergeSeriesSet := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)

	fmt.Println("Giving you data points now")

	for mergeSeriesSet.Next() {
		mergeSeries := mergeSeriesSet.At()
		memIter := storage.NewMemoizedEmptyIterator(300000)
		memIter.Reset(mergeSeries.Iterator())

		/*fmt.Println(memIter.Seek(1621634400000))
		t, v := memIter.Values()
		fmt.Println(t > 1621634400000)
		fmt.Println(t)
		fmt.Println(v)
		fmt.Println(memIter.PeekPrev())*/

		for ts := int64(1621634400000); ts <= 1621634700000; ts += 60000 {
			fmt.Print("sample", ts)
			fmt.Print("  ")
			fmt.Println(vectorSelectorSingle(memIter, ts))
		}
	}
}

func vectorSelectorSingle(it *storage.MemoizedSeriesIterator, ts int64) (int64, float64, bool) {
	refTime := ts
	var t int64
	var v float64

	ok := it.Seek(refTime)
	if !ok {
		if it.Err() != nil {
			fmt.Println(it.Err())
		}
	}

	if ok {
		t, v = it.Values()
	}

	if !ok || t > refTime {
		t, v, ok = it.PeekPrev()
		if !ok || t < refTime-300000 {
			return 0, 0, false
		}
	}
	if value.IsStaleNaN(v) {
		return 0, 0, false
	}
	return t, v, true
}

func mergeChunks(chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator {
	samples := make([][]model.SamplePair, 0, len(chunks))
	for _, c := range chunks {
		ss, err := c.Samples(from, through)
		if err != nil {
			return series.NewErrIterator(err)
		}

		samples = append(samples, ss)
	}

	merged := util.MergeNSampleSets(samples...)
	return series.NewConcreteSeriesIterator(series.NewConcreteSeries(nil, merged))
}

func BenchmarkNewChunkMergeIterator_CreateAndIterate(b *testing.B) {
	scenarios := []struct {
		numChunks          int
		numSamplesPerChunk int
		duplicationFactor  int
		enc                promchunk.Encoding
	}{
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.Bigchunk},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.Bigchunk},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.Varbit},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.Varbit},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.DoubleDelta},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.DoubleDelta},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusXorChunk},
	}

	for _, scenario := range scenarios {
		name := fmt.Sprintf("chunks: %d samples per chunk: %d duplication factor: %d encoding: %s",
			scenario.numChunks,
			scenario.numSamplesPerChunk,
			scenario.duplicationFactor,
			scenario.enc.String())

		chunks := createChunks(b, scenario.numChunks, scenario.numSamplesPerChunk, scenario.duplicationFactor, scenario.enc)

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for n := 0; n < b.N; n++ {
				it := NewChunkMergeIterator(chunks, 0, 0)
				for it.Next() {
					it.At()
				}

				// Ensure no error occurred.
				if it.Err() != nil {
					b.Fatal(it.Err().Error())
				}
			}
		})
	}
}

func createChunks(b *testing.B, numChunks, numSamplesPerChunk, duplicationFactor int, enc promchunk.Encoding) []chunk.Chunk {
	result := make([]chunk.Chunk, 0, numChunks)

	for d := 0; d < duplicationFactor; d++ {
		for c := 0; c < numChunks; c++ {
			minTime := step * time.Duration(c*numSamplesPerChunk)
			result = append(result, mkChunk(b, model.Time(minTime.Milliseconds()), numSamplesPerChunk, enc))
		}
	}

	return result
}

type chunkIteratorFunc func(chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator

// Implements SeriesWithChunks
type chunkSeries struct {
	labels            labels.Labels
	chunks            []chunk.Chunk
	chunkIteratorFunc chunkIteratorFunc
	mint, maxt        int64
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.labels
}

// Iterator returns a new iterator of the data of the series.
func (s *chunkSeries) Iterator() chunkenc.Iterator {
	return s.chunkIteratorFunc(s.chunks, model.Time(s.mint), model.Time(s.maxt))
}

// Chunks implements SeriesWithChunks interface.
func (s *chunkSeries) Chunks() []chunk.Chunk {
	return s.chunks
}
