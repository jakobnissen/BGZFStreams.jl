# Note: Two important features of this code is that it's multithreaded, and that the
# de/compression threads are launched asyncronously, such that it can de/compress one
# block while it's reading other blocks

"""
	BGZFCodec{T, IO} <: Codec

Codec used to compress or decompress data from the input IO source of type `IO`.
`T` must be `LibDeflate.Compressor` or `LibDeflate.Decompressor`.
"""
mutable struct BGZFCodec{T <: DE_COMPRESSOR, O <: IO} <: TranscodingStreams.Codec
	buffer::Vector{UInt8}
	blocks::Vector{Block{T}}

	# This has the (offset, decompress_len) of current block at pos 1
	# then previous blocks. Only used for DecompressorCodec. We use it to
	# keep track of VirtualOffsets of blocks we may have shipped off to the output
	# buffer long ago
	offsets::Vector{Tuple{Int, Int}}

	# We need this in order to write an EOF on end for the CompressorCodec
	io::O

	# Index of currently used block
	index::Int
	bufferlen::Int
end

const CompressorCodec{O} = BGZFCodec{Compressor, O}
const DecompressorCodec{O} = BGZFCodec{Decompressor, O}
const BGZFCompressorStream = TranscodingStream{<:CompressorCodec}
const BGZFDecompressorStream = TranscodingStream{<:DecompressorCodec}

"""
	BGZFCompressorStream(io::IO; threads=nthreads(), compresslevel=6)

Create a `TranscodingStream` which block-gzip compresses data from the underlying `io` with
the given `compresslevel`. The stream will compress up to `threads` block concurrently.
"""
function BGZFCompressorStream(io::IO; nthreads=Threads.nthreads(), compresslevel::Int=6)
    codec = CompressorCodec(io, nthreads, compresslevel)
    return TranscodingStream(codec, io; bufsize=nfull(Block{Compressor}))
end

"""
	BGZFDecompressorStream(io::IO; threads=nthreads())

Create a `TranscodingStream` which decompresses block-gzipped data from the underlying `io`.
The stream will compress up to `threads` block concurrently.
"""
function BGZFDecompressorStream(io::IO; nthreads=Threads.nthreads())
    codec = DecompressorCodec(io, nthreads)
    return TranscodingStream(codec, io; bufsize=nfull(Block{Decompressor}))
end

function CompressorCodec(io::IO, nthreads, compresslevel)
	nthreads < 1 && throw(ArgumentError("Must use at least 1 thread"))
    buffer = Vector{UInt8}(undef, SAFE_BLOCK_SIZE)
    blocks = [Block(Compressor(compresslevel)) for i in 1:nthreads]
    offsets = fill((0,0), 16)
    return CompressorCodec{typeof(io)}(buffer, blocks, offsets, io, 1, 0)
end

function DecompressorCodec(io::IO, nthreads)
	nthreads < 1 && throw(ArgumentError("Must use at least 1 thread"))
	buffer = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
    blocks = [Block(Decompressor()) for i in 1:nthreads]
    offsets = fill((0,0), 16)
	return DecompressorCodec{typeof(io)}(buffer, blocks, offsets, io, 1, 0)
end

nblocks(c::BGZFCodec) = length(c.blocks)
get_block(c::BGZFCodec) = @inbounds c.blocks[c.index]

"Get number of remaning bytes in the codec's buffer before a new block can be indexed"
remaining(codec::BGZFCodec{T}) where T = nfull(Block{T}) - codec.bufferlen

"""Switches the code to the next block to process, given whether the input stream is eof.
Returns block index, or nothing if there are no more blocks to process"""
function increment_block!(codec::BGZFCodec, eof::Bool)
	# Next block index
	i = ifelse(codec.index == nblocks(codec), 1, codec.index + 1)
	
	# We need to wait for the blocks to make sure they're actually empty
	wait(codec.blocks[i])

	# If we're at EOF, we don't need to load any data into the blocks, so we
	# just skip to the next block with data.
	if eof
		while isempty(codec.blocks[i])

			# If we have checked all blocks, we have nothing more to do. Just check
			# for EOF block, and return nothing
			if i == codec.index
				codec isa DecompressorCodec && check_eof_block(get_block(codec))
				return nothing
			end
			i = ifelse(i == nblocks(codec), 1, i + 1)
			wait(codec.blocks[i])
		end
	end
	codec.index = i

	push_offsets!(codec)
	return i
end

"Add the current block's offsets and lengths to the codex offset vector"
function push_offsets!(codec)
	unsafe_copyto!(codec.offsets, 2, codec.offsets, 1, 15)
	block = get_block(codec)
	codec.offsets[1] = (block.offset, block.outlen)
end

"Get the offset for the soon-to-be indexed block based on the previous block"
function get_new_offset(codec)
	i = ifelse(codec.index == 1, nblocks(codec), codec.index - 1)
	lastblock = @inbounds codec.blocks[i]
	return lastblock.offset + lastblock.blocklen
end

function reset!(s::BGZFDecompressorStream)
    TranscodingStreams.initbuffer!(s.state.buffer1)
    TranscodingStreams.initbuffer!(s.state.buffer2)
	for block in s.codec.blocks
		empty!(block)
	end
	s.codec.index = 1
	s.codec.bufferlen = 0
	fill!(s.codec.offsets, (0, 0))
    return s
end

function _seek(s::BGZFDecompressorStream, i::Integer)
    reset!(s)
    seek(s.stream, i)
    last(s.codec.blocks).offset = i
    return s
end

"""
	seekstart(stream::BGZFDecompressorStream)

Seek the `stream`'s input stream to its beginning, and resets the `stream`.
"""
function Base.seekstart(s::BGZFDecompressorStream)
    reset!(s)
    seekstart(s.stream)
    last(s.codec.blocks).offset = 0
    return s
end

"""
	seek(stream::BGZFDecompressorStream, v::VirtualOffset)

Seek `stream` to the given `VirtualOffset` `v`. `v` must be a valid virtual offset for the given
`stream`, i.e. its coffset must be the offset of a valid BGZF block, and its uoffset must be an
integer in [0, block_len]. Furthermore, `stream`'s underlying stream must be seekable.
"""
function Base.seek(s::BGZFDecompressorStream, v::VirtualOffset)
    block_offset, byte_offset = offsets(v)
    seek(s, block_offset)

    # Read one byte to fill in buffer
    read(s, UInt8)

    # Now advance buffer block_offset minus the one byte we just read
    if byte_offset > get_block(s.codec).outlen
        throw(ArgumentError("Too large offset for block"))
    end
    s.state.buffer1.bufferpos += (byte_offset % Int - 1)
    return s
end

"""
	VirtualOffset(stream::BGZFDecompressorStream)

Obtain the `VirtualOffset` of the curret position of `stream`. If `stream's` input stream is
seekable, seeking to this offset will leave the stream in an equivalent state to its current state.
A `BGZFDecompressorStream` only tracks the offset of the 16 previous blocks. If more than 16 blocks
are stored in `stream`'s output buffer, this operation will fail.
"""
function VirtualOffset(s::BGZFDecompressorStream)
	# This is a little tricky, because the output buffer may buffer an arbitrary
	# large amount of blocks, and we can't keep track of all these blocks'
	# offsets
	n_buffered = s.state.buffer1.bufferpos - s.state.buffer1.markpos - 1

	# First we removed the buffered data from the current block
	n_buffered -= (get_block(s.codec).outpos - 1)
	blockindex = 1

	# Next we backtrace until we reach the correct
	while n_buffered >= 0
		blockindex += 1
		if blockindex > length(s.codec.offsets)
			bgzferror("Too many blocks buffered to retrace original block offset")
		end
		(offset, decompressed) = s.codec.blocks[blockindex]
		n_buffered -= decompressed
	end

	offset, decompressed = s.codec.offsets[blockindex]
	return VirtualOffset(offset, n_buffered + decompressed)
end

function TranscodingStreams.finalize(codec::CompressorCodec)
    write(codec.io, EOF_BLOCK)
end

"Return data already prepared in the current block"
function copy_from_outbuffer(codec::BGZFCodec, output::Memory, consumed::Integer)
	block = get_block(codec)
	available = block.outlen - block.outpos + 1
    n = min(available, length(output))
    unsafe_copyto!(output.ptr, pointer(block.outdata, block.outpos), n)
	block.outpos += n
    return (Int(consumed), n, :ok)
end

function TranscodingStreams.process(codec::BGZFCodec{T}, input::Memory, output::Memory, error::Error) where T
	consumed = 0
    block = get_block(codec)
    eof = iszero(length(input))

	# If we have spare data in the current block, just give that
    isempty(block) || return copy_from_outbuffer(codec, output, consumed)

	# If there is data to be read in, we do that.
	if !eof
    	consumed = min(remaining(codec), length(input))
    	unsafe_copyto!(pointer(codec.buffer, codec.bufferlen + 1), input.ptr, consumed)
    	codec.bufferlen += consumed

		# If we have read in data, but still not enough to queue a block, return no data
		# and wait for more data to be passed
    	codec.bufferlen < nfull(Block{T}) && return (consumed, 0, :ok)
    end

    # At this point, if there is any data in the buffer, it must be enough
    # to queue a whole block (since the buffer is either full, or input is EOF)
    if !iszero(codec.bufferlen)
    	used_buffer = index!(codec, block)
    	queue!(block)
    end

	blockindex = increment_block!(codec, eof)

	# This happens if there is no block to go to to either load new data or return
	# existing data. Then we are done.
	blockindex === nothing && return (0, 0, :end)

	# If next block is empty, it's because we need to load data into it, so we just
	# return here and wait for process to be called again so the new block can be
	# filled
	if isempty(get_block(codec))
		return (consumed, 0, :ok)
	end
	
	# Else, the new block has data and we may as well just return that immediately.
	return copy_from_outbuffer(codec, output, consumed)
end
