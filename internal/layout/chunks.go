package layout

import "sort"

// BuildChunks splits a non-negative object length into zero-based contiguous
// chunk intervals of at most chunkSize bytes. The returned intervals exactly
// cover the object and are ordered by ascending sequence number. An empty
// object yields an empty slice.
func BuildChunks(objectLength, chunkSize int64) ([]Chunk, error) {
	if objectLength < 0 {
		return nil, ErrNegativeLength
	}
	if err := ValidateChunkSize(chunkSize); err != nil {
		return nil, err
	}
	if objectLength == 0 {
		return nil, nil
	}
	n := (objectLength + chunkSize - 1) / chunkSize
	chunks := make([]Chunk, 0, n)
	var offset int64
	for no := int64(0); offset < objectLength; no++ {
		length := chunkSize
		if remaining := objectLength - offset; remaining < chunkSize {
			length = remaining
		}
		chunks = append(chunks, Chunk{No: no, Offset: offset, Length: length})
		offset += length
	}
	return chunks, nil
}

// ChunkForOffset returns the sequence number of the chunk covering a byte
// offset within an object of the given length and chunk size. It returns an
// error when the offset lies outside [0, objectLength).
func ChunkForOffset(objectLength, chunkSize, offset int64) (int64, error) {
	if offset < 0 || offset >= objectLength {
		return 0, ErrChunkOutOfBounds
	}
	if objectLength == 0 {
		return 0, ErrChunkOutOfBounds
	}
	return offset / chunkSize, nil
}

// SortChunks orders chunk digests by sequence number in place, mirroring the
// deterministic ordering used by RootDigest.
func SortChunks(chunks []ChunkDigest) {
	sort.SliceStable(chunks, func(i, j int) bool { return chunks[i].No < chunks[j].No })
}
