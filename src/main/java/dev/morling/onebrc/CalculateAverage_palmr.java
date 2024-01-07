/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CalculateAverage_palmr {
    private static final String FILE = "./measurements.txt";
    private static final int THREAD_COUNT = Math.min(8, Runtime.getRuntime().availableProcessors());
    public static final boolean PARALLEL = true;
    private static final byte SEPARATOR_CHAR = ';';
    private static final byte END_OF_RECORD = '\n';
    private static final byte MINUS_CHAR = '-';
    private static final byte DECIMAL_POINT_CHAR = '.';

    public static void main(String[] args) throws IOException {

        final var file = new RandomAccessFile(FILE, "r");
        final var channel = file.getChannel();

        final TreeMap<String, MeasurementAggregator> results = StreamSupport.stream(ThreadChunk.chunk(file, THREAD_COUNT), PARALLEL)
                .map(chunk -> parseChunk(chunk, channel))
                .flatMap(bakm -> bakm.getAsUnorderedList().stream())
                .collect(Collectors.toMap(m -> new String(m.stationNameBytes, StandardCharsets.UTF_8), m -> m, MeasurementAggregator::merge, TreeMap::new));
        System.out.println(results);
    }

    private record ThreadChunk(long startPoint, long endPoint, long size) {
        public static Spliterator<CalculateAverage_palmr.ThreadChunk> chunk(final RandomAccessFile file, final int chunkCount) throws IOException {
            final var fileSize = file.length();
            final var idealChunkSize = fileSize / THREAD_COUNT;
            final var chunks = new CalculateAverage_palmr.ThreadChunk[chunkCount];

            var validChunks = 0;
            var startPoint = 0L;
            for (int i = 0; i < chunkCount; i++) {
                var endPoint = Math.min(startPoint + idealChunkSize, fileSize);
                if (startPoint + idealChunkSize < fileSize) {
                    file.seek(endPoint);
                    while (file.readByte() != END_OF_RECORD && endPoint++ < fileSize) {
                        Thread.onSpinWait();
                    }
                }

                final var actualSize = endPoint - startPoint;
                if (actualSize > 1) {
                    chunks[i] = new CalculateAverage_palmr.ThreadChunk(startPoint, endPoint, actualSize);
                    startPoint += actualSize + 1;
                    validChunks++;
                } else {
                    break;
                }
            }

            return Spliterators.spliterator(chunks, 0, validChunks,
                            Spliterator.DISTINCT |
                            Spliterator.NONNULL |
                            Spliterator.IMMUTABLE |
                            Spliterator.CONCURRENT
            );
        }
    }

    private static ByteArrayKeyedMap parseChunk(final ThreadChunk chunk, final FileChannel channel) {
        final MappedByteBuffer byteBuffer;
        try {
            byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, chunk.startPoint, chunk.size);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        var aggregators = new ByteArrayKeyedMap();

        var currentChar = -1;
        var ptr = 0;
        while (ptr < chunk.size) {
            if (byteBuffer.get(ptr) == END_OF_RECORD) {
                break;
            }
            var stationNamePointerStart = ptr;
            var stationNameLength = 0;
            var stationNamePointerHashCode = 0;
            var measurementValue = 0;

            while (ptr < chunk.endPoint && (currentChar = byteBuffer.get(ptr++)) != SEPARATOR_CHAR) {
                stationNameLength++;
                stationNamePointerHashCode = 31 * stationNamePointerHashCode + (currentChar & 0xFF);
            }

            var d1 = byteBuffer.get(ptr++);
            var d2 = byteBuffer.get(ptr++);
            var d3 = byteBuffer.get(ptr++);
            if (d1 == MINUS_CHAR) {
                if (d3 == DECIMAL_POINT_CHAR) {
                    // -x.x
                    // 1234
                    measurementValue = -((d2 - '0') * 10 + (byteBuffer.get(ptr++) - '0'));
                }
                else {
                    // -xx.x
                    // 12345
                    measurementValue = -((d2 - '0') * 100 + (d3 - '0') * 10 + (byteBuffer.get(ptr += 2) - '0'));
                }
            }
            else {
                if (d2 == DECIMAL_POINT_CHAR) {
                    // x.x
                    // 123
                    measurementValue = ((d1 - '0') * 10 + (d3 - '0'));
                }
                else {
                    // xx.x
                    // 1234
                    measurementValue = ((d1 - '0') * 100 + (d2 - '0') * 10 + (byteBuffer.get(ptr++) - '0'));
                }
            }

            MeasurementAggregator aggregator = aggregators.computeIfAbsent(byteBuffer, stationNamePointerStart, stationNameLength, stationNamePointerHashCode);
            aggregator.count++;
            aggregator.min = Math.min(aggregator.min, measurementValue);
            aggregator.max = Math.max(aggregator.max, measurementValue);
            aggregator.sum += measurementValue;
        }

        return aggregators;
    }

    private static class MeasurementAggregator {
        final byte[] stationNameBytes;
        final int stationNameHashCode;
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long sum;
        private long count;

        public MeasurementAggregator(final byte[] stationNameBytes, final int stationNameHashCode) {
            this.stationNameBytes = stationNameBytes;
            this.stationNameHashCode = stationNameHashCode;
        }

        public String toString() {
            return round(min * 0.1) + "/" + round((sum * 0.1) / count) + "/" + round(max * 0.1);
        }

        private double round(final double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        private MeasurementAggregator merge(final MeasurementAggregator b) {
            this.count += b.count;
            this.min = Math.min(this.min, b.min);
            this.max = Math.max(this.max, b.max);
            this.sum += b.sum;
            return this;
        }
    }

    /**
     * Very basic hash table implementation, only implementing computeIfAbsent since that's all the code needs.
     * It's sized to give minimal collisions with the example test set. this may not hold true if the stations list
     * changes, but it should still perform fairly well.
     * It uses Open Addressing, meaning it's just one array, rather Separate Chaining which is what the default java HashMap uses.
     * IT also uses Linear probing for collision resolution, which given the minimal collision count should hold up well.
     */
    private static class ByteArrayKeyedMap {
        private final int BUCKET_COUNT = 0xFFFF;
        private final MeasurementAggregator[] buckets = new MeasurementAggregator[BUCKET_COUNT + 1];
        private final List<MeasurementAggregator> compactUnorderedBuckets = new ArrayList<>(413);

        public MeasurementAggregator computeIfAbsent(final ByteBuffer byteBuffer,
                                                     final int stationNamePointerStart,
                                                     final int stationNameLength,
                                                     final int stationNamePointerHashCode) {
            var index = stationNamePointerHashCode & BUCKET_COUNT;

            outer: while (true) {
                MeasurementAggregator maybe = buckets[index];
                if (maybe != null) {
                    if (maybe.stationNameBytes.length != stationNameLength) {
                        index++;
                        index &= BUCKET_COUNT;
                        continue;
                    }
                    for (var i = 0; i < stationNameLength; i++) {
                        if (maybe.stationNameBytes[i] != byteBuffer.get(stationNamePointerStart + i)) {
                            index++;
                            index &= BUCKET_COUNT;
                            continue outer;
                        }
                    }
                    return maybe;
                }
                else {
                    var copiedKey = new byte[stationNameLength];
                    byteBuffer.get(stationNamePointerStart, copiedKey);
                    MeasurementAggregator measurementAggregator = new MeasurementAggregator(copiedKey, stationNamePointerHashCode);
                    buckets[index] = measurementAggregator;
                    compactUnorderedBuckets.add(measurementAggregator);
                    return measurementAggregator;
                }
            }
        }

        public List<MeasurementAggregator> getAsUnorderedList() {
            return compactUnorderedBuckets;
        }
    }
}
